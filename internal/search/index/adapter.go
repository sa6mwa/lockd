package index

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

const indexCursorPrefix = "indexv1:"

// AdapterConfig configures the index query adapter.
type AdapterConfig struct {
	Store  *Store
	Logger pslog.Logger
}

// Adapter executes queries against immutable index segments.
type Adapter struct {
	store  *Store
	logger pslog.Logger
}

// NewAdapter builds a query adapter backed by the index store.
func NewAdapter(cfg AdapterConfig) (*Adapter, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("index adapter: store required")
	}
	return &Adapter{
		store:  cfg.Store,
		logger: cfg.Logger,
	}, nil
}

// Capabilities reports whether the index engine is available for the namespace.
func (a *Adapter) Capabilities(context.Context, string) (search.Capabilities, error) {
	if a == nil || a.store == nil {
		return search.Capabilities{}, nil
	}
	return search.Capabilities{Index: true}, nil
}

// Query evaluates the selector via the index and returns matching keys.
func (a *Adapter) Query(ctx context.Context, req search.Request) (search.Result, error) {
	if a == nil || a.store == nil {
		return search.Result{}, fmt.Errorf("index adapter unavailable")
	}
	if req.Namespace == "" {
		return search.Result{}, fmt.Errorf("index adapter: namespace required")
	}
	manifest, _, err := a.store.LoadManifest(ctx, req.Namespace)
	if err != nil {
		return search.Result{}, fmt.Errorf("load manifest: %w", err)
	}
	if manifest == nil {
		manifest = NewManifest()
	}
	reader := newSegmentReader(req.Namespace, manifest, a.store, a.logger)
	var matches keySet
	if zeroSelector(req.Selector) {
		matches, err = reader.allKeys(ctx)
	} else {
		eval := selectorEvaluator{reader: reader}
		matches, err = eval.evaluate(ctx, req.Selector)
	}
	if err != nil {
		return search.Result{}, err
	}
	sortedKeys := matches.sorted()
	startIdx, err := startIndexFromCursor(req.Cursor, sortedKeys)
	if err != nil {
		return search.Result{}, fmt.Errorf("%w: %v", search.ErrInvalidCursor, err)
	}
	if startIdx >= len(sortedKeys) {
		return search.Result{IndexSeq: manifest.Seq}, nil
	}
	limit := req.Limit
	if limit <= 0 {
		limit = len(sortedKeys) - startIdx
	}
	if limit > len(sortedKeys)-startIdx {
		limit = len(sortedKeys) - startIdx
	}
	if limit <= 0 {
		return search.Result{IndexSeq: manifest.Seq}, nil
	}
	visible := make([]string, 0, min(limit, len(sortedKeys)-startIdx))
	nextIndex := len(sortedKeys)
	var lastReturned string
	for i := startIdx; i < len(sortedKeys); i++ {
		if len(visible) >= limit {
			nextIndex = i
			break
		}
		if err := ctx.Err(); err != nil {
			return search.Result{}, err
		}
		key := sortedKeys[i]
		meta, _, metaErr := a.store.backend.LoadMeta(ctx, req.Namespace, key)
		if metaErr != nil {
			if errors.Is(metaErr, storage.ErrNotFound) {
				continue
			}
			return search.Result{}, fmt.Errorf("load meta %s: %w", key, metaErr)
		}
		if meta == nil || meta.PublishedVersion == 0 || meta.QueryExcluded() {
			continue
		}
		visible = append(visible, key)
		lastReturned = key
		nextIndex = i + 1
	}
	result := search.Result{
		Keys:     visible,
		IndexSeq: manifest.Seq,
	}
	if manifest.UpdatedAt.Unix() > 0 {
		if result.Metadata == nil {
			result.Metadata = make(map[string]string)
		}
		result.Metadata["index_updated_at"] = manifest.UpdatedAt.UTC().Format(time.RFC3339)
	}
	if len(visible) == limit && nextIndex < len(sortedKeys) && lastReturned != "" {
		result.Cursor = encodeCursor(lastReturned)
	}
	return result, nil
}

type segmentReader struct {
	namespace string
	manifest  *Manifest
	store     *Store
	logger    pslog.Logger
	cache     map[string]*Segment
}

func newSegmentReader(namespace string, manifest *Manifest, store *Store, logger pslog.Logger) *segmentReader {
	return &segmentReader{
		namespace: namespace,
		manifest:  manifest,
		store:     store,
		logger:    logger,
		cache:     make(map[string]*Segment),
	}
}

func (r *segmentReader) allKeys(ctx context.Context) (keySet, error) {
	result := make(keySet)
	err := r.forEachSegment(ctx, func(seg *Segment) error {
		for _, block := range seg.Fields {
			for _, keys := range block.Postings {
				for _, key := range keys {
					result[key] = struct{}{}
				}
			}
		}
		return nil
	})
	return result, err
}

func (r *segmentReader) keysForEq(ctx context.Context, term *api.Term) (keySet, error) {
	field := normalizeField(term)
	if field == "" {
		return make(keySet), nil
	}
	value := normalizeTermValue(term.Value)
	result := make(keySet)
	err := r.forEachPosting(ctx, field, func(termValue string, keys []string) error {
		if termValue == value {
			for _, key := range keys {
				result[key] = struct{}{}
			}
		}
		return nil
	})
	return result, err
}

func (r *segmentReader) keysForPrefix(ctx context.Context, term *api.Term) (keySet, error) {
	field := normalizeField(term)
	if field == "" {
		return make(keySet), nil
	}
	prefix := normalizeTermValue(term.Value)
	result := make(keySet)
	err := r.forEachPosting(ctx, field, func(termValue string, keys []string) error {
		if strings.HasPrefix(termValue, prefix) {
			for _, key := range keys {
				result[key] = struct{}{}
			}
		}
		return nil
	})
	return result, err
}

func (r *segmentReader) keysForRange(ctx context.Context, term *api.RangeTerm) (keySet, error) {
	field := normalizeRangeField(term)
	if field == "" {
		return make(keySet), nil
	}
	result := make(keySet)
	err := r.forEachPosting(ctx, field, func(termValue string, keys []string) error {
		num, err := strconv.ParseFloat(termValue, 64)
		if err != nil || math.IsNaN(num) {
			return nil
		}
		if !rangeMatches(term, num) {
			return nil
		}
		for _, key := range keys {
			result[key] = struct{}{}
		}
		return nil
	})
	return result, err
}

func (r *segmentReader) keysForExists(ctx context.Context, field string) (keySet, error) {
	field = strings.TrimSpace(field)
	if field == "" {
		return make(keySet), nil
	}
	result := make(keySet)
	err := r.forEachPosting(ctx, field, func(_ string, keys []string) error {
		for _, key := range keys {
			result[key] = struct{}{}
		}
		return nil
	})
	return result, err
}

func (r *segmentReader) keysForIn(ctx context.Context, term *api.InTerm) (keySet, error) {
	field := strings.TrimSpace(term.Field)
	if field == "" || len(term.Any) == 0 {
		return make(keySet), nil
	}
	unionSet := make(keySet)
	for _, candidate := range term.Any {
		eqSet, err := r.keysForEq(ctx, &api.Term{Field: field, Value: candidate})
		if err != nil {
			return nil, err
		}
		unionSet = union(unionSet, eqSet)
	}
	return unionSet, nil
}

func (r *segmentReader) forEachSegment(ctx context.Context, fn func(*Segment) error) error {
	if r.manifest == nil || len(r.manifest.Shards) == 0 {
		return nil
	}
	for _, shard := range r.manifest.Shards {
		if shard == nil {
			continue
		}
		for _, ref := range shard.Segments {
			if err := ctx.Err(); err != nil {
				return err
			}
			segment, err := r.loadSegment(ctx, ref.ID)
			if err != nil {
				return err
			}
			if segment == nil {
				continue
			}
			if err := fn(segment); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *segmentReader) forEachPosting(ctx context.Context, field string, fn func(term string, keys []string) error) error {
	return r.forEachSegment(ctx, func(seg *Segment) error {
		block, ok := seg.Fields[field]
		if !ok {
			return nil
		}
		for term, keys := range block.Postings {
			if err := fn(term, keys); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *segmentReader) loadSegment(ctx context.Context, id string) (*Segment, error) {
	if seg, ok := r.cache[id]; ok {
		return seg, nil
	}
	segment, err := r.store.LoadSegment(ctx, r.namespace, id)
	if err != nil {
		return nil, err
	}
	r.cache[id] = segment
	return segment, nil
}

type selectorEvaluator struct {
	reader *segmentReader
}

func (e selectorEvaluator) evaluate(ctx context.Context, sel api.Selector) (keySet, error) {
	if len(sel.Or) > 0 {
		unionSet := make(keySet)
		for _, branch := range sel.Or {
			branchSet, err := e.evaluate(ctx, branch)
			if err != nil {
				return nil, err
			}
			unionSet = union(unionSet, branchSet)
		}
		return unionSet, nil
	}
	var (
		result      keySet
		initialized bool
	)
	intersectWith := func(set keySet) {
		if !initialized {
			result = set.clone()
			initialized = true
			return
		}
		result = intersect(result, set)
	}
	if sel.Eq != nil {
		set, err := e.reader.keysForEq(ctx, sel.Eq)
		if err != nil {
			return nil, err
		}
		intersectWith(set)
	}
	if sel.Prefix != nil {
		set, err := e.reader.keysForPrefix(ctx, sel.Prefix)
		if err != nil {
			return nil, err
		}
		intersectWith(set)
	}
	if sel.Range != nil {
		set, err := e.reader.keysForRange(ctx, sel.Range)
		if err != nil {
			return nil, err
		}
		intersectWith(set)
	}
	if sel.In != nil {
		set, err := e.reader.keysForIn(ctx, sel.In)
		if err != nil {
			return nil, err
		}
		intersectWith(set)
	}
	if sel.Exists != "" {
		set, err := e.reader.keysForExists(ctx, sel.Exists)
		if err != nil {
			return nil, err
		}
		intersectWith(set)
	}
	for _, clause := range sel.And {
		clauseSet, err := e.evaluate(ctx, clause)
		if err != nil {
			return nil, err
		}
		intersectWith(clauseSet)
	}
	if sel.Not != nil {
		notSet, err := e.evaluate(ctx, *sel.Not)
		if err != nil {
			return nil, err
		}
		if len(notSet) > 0 {
			if !initialized {
				all, err := e.reader.allKeys(ctx)
				if err != nil {
					return nil, err
				}
				result = subtract(all, notSet)
				initialized = true
			} else {
				result = subtract(result, notSet)
			}
		}
	}
	if !initialized {
		return e.reader.allKeys(ctx)
	}
	return result, nil
}

type keySet map[string]struct{}

func (s keySet) clone() keySet {
	if len(s) == 0 {
		return make(keySet)
	}
	out := make(keySet, len(s))
	for key := range s {
		out[key] = struct{}{}
	}
	return out
}

func (s keySet) sorted() []string {
	if len(s) == 0 {
		return nil
	}
	keys := make([]string, 0, len(s))
	for key := range s {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func union(a, b keySet) keySet {
	if len(a) == 0 && len(b) == 0 {
		return make(keySet)
	}
	if len(a) == 0 {
		return b.clone()
	}
	if len(b) == 0 {
		return a.clone()
	}
	out := make(keySet, len(a)+len(b))
	for key := range a {
		out[key] = struct{}{}
	}
	for key := range b {
		out[key] = struct{}{}
	}
	return out
}

func intersect(a, b keySet) keySet {
	if len(a) == 0 || len(b) == 0 {
		return make(keySet)
	}
	var smaller, larger keySet
	if len(a) <= len(b) {
		smaller, larger = a, b
	} else {
		smaller, larger = b, a
	}
	out := make(keySet)
	for key := range smaller {
		if _, ok := larger[key]; ok {
			out[key] = struct{}{}
		}
	}
	return out
}

func subtract(a, b keySet) keySet {
	if len(a) == 0 {
		return make(keySet)
	}
	if len(b) == 0 {
		return a.clone()
	}
	out := make(keySet, len(a))
	for key := range a {
		if _, hidden := b[key]; hidden {
			continue
		}
		out[key] = struct{}{}
	}
	return out
}

func normalizeField(term *api.Term) string {
	if term == nil {
		return ""
	}
	return strings.TrimSpace(term.Field)
}

func normalizeRangeField(term *api.RangeTerm) string {
	if term == nil {
		return ""
	}
	return strings.TrimSpace(term.Field)
}

func normalizeTermValue(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func rangeMatches(term *api.RangeTerm, value float64) bool {
	if term == nil {
		return false
	}
	if term.GTE != nil && value < *term.GTE {
		return false
	}
	if term.GT != nil && value <= *term.GT {
		return false
	}
	if term.LTE != nil && value > *term.LTE {
		return false
	}
	if term.LT != nil && value >= *term.LT {
		return false
	}
	return true
}

func zeroSelector(sel api.Selector) bool {
	return len(sel.And) == 0 &&
		len(sel.Or) == 0 &&
		sel.Not == nil &&
		sel.Eq == nil &&
		sel.Prefix == nil &&
		sel.Range == nil &&
		sel.In == nil &&
		sel.Exists == ""
}

func encodeCursor(key string) string {
	if key == "" {
		return ""
	}
	return indexCursorPrefix + base64.RawURLEncoding.EncodeToString([]byte(key))
}

func decodeCursor(cursor string) (string, error) {
	if cursor == "" {
		return "", nil
	}
	if !strings.HasPrefix(cursor, indexCursorPrefix) {
		return "", fmt.Errorf("unsupported cursor prefix")
	}
	data, err := base64.RawURLEncoding.DecodeString(cursor[len(indexCursorPrefix):])
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func startIndexFromCursor(cursor string, keys []string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	value, err := decodeCursor(cursor)
	if err != nil {
		return 0, err
	}
	pos := sort.SearchStrings(keys, value)
	if pos < len(keys) && keys[pos] == value {
		return pos + 1, nil
	}
	return pos, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
