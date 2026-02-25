package index

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/jsonpointer"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lql"
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

	docMetaHits    atomic.Uint64
	docMetaMisses  atomic.Uint64
	docMetaInvalid atomic.Uint64
	docMetaLogAt   atomic.Int64
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
	manifestRes, err := a.store.LoadManifest(ctx, req.Namespace)
	if err != nil {
		return search.Result{}, fmt.Errorf("load manifest: %w", err)
	}
	manifest := manifestRes.Manifest
	if manifest == nil {
		manifest = NewManifest()
	}
	reader := newSegmentReader(req.Namespace, manifest, a.store, a.logger)
	visibility, err := a.visibilityMap(ctx, req.Namespace)
	if err != nil {
		return search.Result{}, err
	}
	var (
		matches         keySet
		postFilterPlan  lql.QueryStreamPlan
		requirePostEval bool
	)
	selector := cloneSelector(req.Selector)
	useLegacyFilter := selectorSupportsLegacyIndexFilter(req.Selector)
	if !selector.IsEmpty() {
		if normalizeErr := normalizeSelectorFieldsForLQL(&selector); normalizeErr != nil {
			return search.Result{}, fmt.Errorf("normalize selector: %w", normalizeErr)
		}
	}
	if selector.IsEmpty() || !useLegacyFilter {
		matches, err = reader.allKeys(ctx)
	} else {
		eval := selectorEvaluator{
			reader:        reader,
			containsNgram: manifest.Format >= IndexFormatVersionV4,
		}
		matches, err = eval.evaluate(ctx, req.Selector)
	}
	if !selector.IsEmpty() && !useLegacyFilter {
		compiled, compileErr := lql.NewQueryStreamPlan(selector)
		if compileErr != nil {
			return search.Result{}, fmt.Errorf("compile selector: %w", compileErr)
		}
		postFilterPlan = compiled
		requirePostEval = true
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
		return search.Result{IndexSeq: manifest.Seq, Format: manifest.Format}, nil
	}
	limit := req.Limit
	if limit <= 0 {
		limit = len(sortedKeys) - startIdx
	}
	if limit > len(sortedKeys)-startIdx {
		limit = len(sortedKeys) - startIdx
	}
	if limit <= 0 {
		return search.Result{IndexSeq: manifest.Seq, Format: manifest.Format}, nil
	}
	visible := make([]string, 0, min(limit, len(sortedKeys)-startIdx))
	canUseDocMeta := manifest.Format >= IndexFormatVersionV3
	var docMeta map[string]DocumentMetadata
	docMetaReady := false
	getDocMeta := func() (map[string]DocumentMetadata, error) {
		if docMetaReady {
			return docMeta, nil
		}
		docMetaReady = true
		metaMap, err := reader.docMetaMap(ctx)
		if err != nil {
			return nil, err
		}
		docMeta = metaMap
		return docMeta, nil
	}
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
		visibleKey, ok, err := a.visibleFromLedger(ctx, req.Namespace, key, visibility)
		if err != nil {
			return search.Result{}, err
		}
		if ok && !visibleKey {
			continue
		}
		if !ok {
			if canUseDocMeta {
				metaMap, err := getDocMeta()
				if err != nil {
					return search.Result{}, err
				}
				if meta, ok := metaMap[key]; ok {
					if meta.PublishedVersion == 0 || meta.QueryExcluded {
						continue
					}
				} else {
					metaRes, metaErr := a.store.backend.LoadMeta(ctx, req.Namespace, key)
					if metaErr != nil {
						if errors.Is(metaErr, storage.ErrNotFound) {
							continue
						}
						return search.Result{}, fmt.Errorf("load meta %s: %w", key, metaErr)
					}
					meta := metaRes.Meta
					if meta == nil || meta.PublishedVersion == 0 || meta.QueryExcluded() {
						continue
					}
				}
			} else {
				metaRes, metaErr := a.store.backend.LoadMeta(ctx, req.Namespace, key)
				if metaErr != nil {
					if errors.Is(metaErr, storage.ErrNotFound) {
						continue
					}
					return search.Result{}, fmt.Errorf("load meta %s: %w", key, metaErr)
				}
				meta := metaRes.Meta
				if meta == nil || meta.PublishedVersion == 0 || meta.QueryExcluded() {
					continue
				}
			}
		}
		if requirePostEval {
			metaRes, metaErr := a.store.backend.LoadMeta(ctx, req.Namespace, key)
			if metaErr != nil {
				if errors.Is(metaErr, storage.ErrNotFound) {
					continue
				}
				return search.Result{}, fmt.Errorf("load meta %s: %w", key, metaErr)
			}
			meta := metaRes.Meta
			if meta == nil || meta.StateETag == "" {
				continue
			}
			matched, matchErr := a.matchesSelector(ctx, req.Namespace, key, meta, postFilterPlan)
			if matchErr != nil {
				return search.Result{}, fmt.Errorf("evaluate selector %s: %w", key, matchErr)
			}
			if !matched {
				continue
			}
		}
		visible = append(visible, key)
		lastReturned = key
		nextIndex = i + 1
	}
	result := search.Result{
		Keys:     visible,
		IndexSeq: manifest.Seq,
		Format:   manifest.Format,
	}
	if req.IncludeDocMeta && len(visible) > 0 {
		meta, err := a.collectDocMeta(ctx, visible, reader)
		if err != nil {
			return search.Result{}, err
		}
		result.DocMeta = meta
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

// QueryDocuments evaluates the selector via the index and streams matching
// documents directly to sink.
func (a *Adapter) QueryDocuments(ctx context.Context, req search.Request, sink search.DocumentSink) (search.Result, error) {
	if a == nil || a.store == nil {
		return search.Result{}, fmt.Errorf("index adapter unavailable")
	}
	if req.Namespace == "" {
		return search.Result{}, fmt.Errorf("index adapter: namespace required")
	}
	if sink == nil {
		return search.Result{}, fmt.Errorf("index adapter: document sink required")
	}
	manifestRes, err := a.store.LoadManifest(ctx, req.Namespace)
	if err != nil {
		return search.Result{}, fmt.Errorf("load manifest: %w", err)
	}
	manifest := manifestRes.Manifest
	if manifest == nil {
		manifest = NewManifest()
	}
	reader := newSegmentReader(req.Namespace, manifest, a.store, a.logger)
	visibility, err := a.visibilityMap(ctx, req.Namespace)
	if err != nil {
		return search.Result{}, err
	}
	var (
		matches         keySet
		postFilterPlan  lql.QueryStreamPlan
		requirePostEval bool
	)
	selector := cloneSelector(req.Selector)
	useLegacyFilter := selectorSupportsLegacyIndexFilter(req.Selector)
	if !selector.IsEmpty() {
		if normalizeErr := normalizeSelectorFieldsForLQL(&selector); normalizeErr != nil {
			return search.Result{}, fmt.Errorf("normalize selector: %w", normalizeErr)
		}
	}
	if selector.IsEmpty() || !useLegacyFilter {
		matches, err = reader.allKeys(ctx)
	} else {
		eval := selectorEvaluator{
			reader:        reader,
			containsNgram: manifest.Format >= IndexFormatVersionV4,
		}
		matches, err = eval.evaluate(ctx, req.Selector)
	}
	if !selector.IsEmpty() && !useLegacyFilter {
		compiled, compileErr := lql.NewQueryStreamPlan(selector)
		if compileErr != nil {
			return search.Result{}, fmt.Errorf("compile selector: %w", compileErr)
		}
		postFilterPlan = compiled
		requirePostEval = true
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
		return search.Result{IndexSeq: manifest.Seq, Format: manifest.Format}, nil
	}
	limit := req.Limit
	if limit <= 0 {
		limit = len(sortedKeys) - startIdx
	}
	if limit > len(sortedKeys)-startIdx {
		limit = len(sortedKeys) - startIdx
	}
	if limit <= 0 {
		return search.Result{IndexSeq: manifest.Seq, Format: manifest.Format}, nil
	}
	streamed := make([]string, 0, min(limit, len(sortedKeys)-startIdx))
	nextIndex := len(sortedKeys)
	var lastReturned string
	for i := startIdx; i < len(sortedKeys); i++ {
		if len(streamed) >= limit {
			nextIndex = i
			break
		}
		if err := ctx.Err(); err != nil {
			return search.Result{}, err
		}
		key := sortedKeys[i]
		visibleKey, ok, err := a.visibleFromLedger(ctx, req.Namespace, key, visibility)
		if err != nil {
			return search.Result{}, err
		}
		if ok && !visibleKey {
			continue
		}
		metaRes, metaErr := a.store.backend.LoadMeta(ctx, req.Namespace, key)
		if metaErr != nil {
			if errors.Is(metaErr, storage.ErrNotFound) || storage.IsTransient(metaErr) {
				continue
			}
			return search.Result{}, fmt.Errorf("load meta %s: %w", key, metaErr)
		}
		meta := metaRes.Meta
		if meta == nil || meta.PublishedVersion == 0 || meta.QueryExcluded() {
			continue
		}
		version := meta.PublishedVersion
		if version == 0 {
			version = meta.Version
		}
		if !requirePostEval {
			if err := a.streamDocument(ctx, req.Namespace, key, version, meta, sink); err != nil {
				if errors.Is(err, storage.ErrNotFound) || storage.IsTransient(err) {
					continue
				}
				return search.Result{}, fmt.Errorf("stream document %s: %w", key, err)
			}
			streamed = append(streamed, key)
			lastReturned = key
			nextIndex = i + 1
			continue
		}
		if meta.StateETag == "" {
			continue
		}
		matched, err := a.matchAndStreamDocument(ctx, req.Namespace, key, version, meta, postFilterPlan, sink)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) || storage.IsTransient(err) {
				continue
			}
			return search.Result{}, fmt.Errorf("evaluate+stream selector %s: %w", key, err)
		}
		if !matched {
			continue
		}
		streamed = append(streamed, key)
		lastReturned = key
		nextIndex = i + 1
	}
	result := search.Result{
		Keys:     streamed,
		IndexSeq: manifest.Seq,
		Format:   manifest.Format,
	}
	if manifest.UpdatedAt.Unix() > 0 {
		if result.Metadata == nil {
			result.Metadata = make(map[string]string)
		}
		result.Metadata["index_updated_at"] = manifest.UpdatedAt.UTC().Format(time.RFC3339)
	}
	if len(streamed) == limit && nextIndex < len(sortedKeys) && lastReturned != "" {
		result.Cursor = encodeCursor(lastReturned)
	}
	return result, nil
}

func (a *Adapter) matchesSelector(ctx context.Context, namespace, key string, meta *storage.Meta, plan lql.QueryStreamPlan) (bool, error) {
	stateCtx := ctx
	if meta != nil && len(meta.StateDescriptor) > 0 {
		stateCtx = storage.ContextWithStateDescriptor(stateCtx, meta.StateDescriptor)
	}
	if meta != nil && meta.StatePlaintextBytes > 0 {
		stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
	}
	stateRes, err := a.store.backend.ReadState(stateCtx, namespace, key)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) || storage.IsTransient(err) {
			return true, nil
		}
		return false, err
	}
	defer stateRes.Reader.Close()

	matched := false
	_, err = lql.QueryStreamWithResult(lql.QueryStreamRequest{
		Ctx:               stateCtx,
		Reader:            stateRes.Reader,
		Plan:              plan,
		Mode:              lql.QueryDecisionOnly,
		MaxMatches:        1,
		MaxCandidateBytes: 100 * 1024 * 1024,
		OnDecision: func(d lql.QueryStreamDecision) error {
			if !d.Matched {
				return nil
			}
			matched = true
			return lql.ErrStreamStop
		},
	})
	if err != nil {
		return false, err
	}
	return matched, nil
}

func (a *Adapter) streamDocument(ctx context.Context, namespace, key string, version int64, meta *storage.Meta, sink search.DocumentSink) error {
	stateCtx := ctx
	if meta != nil && len(meta.StateDescriptor) > 0 {
		stateCtx = storage.ContextWithStateDescriptor(stateCtx, meta.StateDescriptor)
	}
	if meta != nil && meta.StatePlaintextBytes > 0 {
		stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
	}
	stateRes, err := a.store.backend.ReadState(stateCtx, namespace, key)
	if err != nil {
		return err
	}
	if stateRes.Reader == nil {
		return nil
	}
	defer stateRes.Reader.Close()
	return sink.OnDocument(ctx, namespace, key, version, stateRes.Reader)
}

func (a *Adapter) matchAndStreamDocument(
	ctx context.Context,
	namespace string,
	key string,
	version int64,
	meta *storage.Meta,
	plan lql.QueryStreamPlan,
	sink search.DocumentSink,
) (bool, error) {
	stateCtx := ctx
	if meta != nil && len(meta.StateDescriptor) > 0 {
		stateCtx = storage.ContextWithStateDescriptor(stateCtx, meta.StateDescriptor)
	}
	if meta != nil && meta.StatePlaintextBytes > 0 {
		stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
	}
	stateRes, err := a.store.backend.ReadState(stateCtx, namespace, key)
	if err != nil {
		return false, err
	}
	if stateRes.Reader == nil {
		return false, nil
	}
	defer stateRes.Reader.Close()

	matched := false
	_, err = lql.QueryStreamWithResult(lql.QueryStreamRequest{
		Ctx:               stateCtx,
		Reader:            stateRes.Reader,
		Plan:              plan,
		Mode:              lql.QueryDecisionPlusValue,
		MatchedOnly:       true,
		SpoolMemoryBytes:  1,
		MaxCandidateBytes: 100 * 1024 * 1024,
		MaxMatches:        1,
		CapturePolicy:     lql.QueryCaptureMatchesOnlyBestEffort,
		OnValue: func(v lql.QueryStreamValue) error {
			if !v.Matched {
				return nil
			}
			docReader, openErr := v.OpenJSON()
			if openErr != nil {
				return openErr
			}
			defer docReader.Close()
			if err := sink.OnDocument(ctx, namespace, key, version, docReader); err != nil {
				return err
			}
			matched = true
			return nil
		},
	})
	if err != nil {
		return false, err
	}
	if !matched {
		return false, nil
	}
	return true, nil
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
	return r.keysForTerm(ctx, field, normalizeTermValue(term.Value))
}

func (r *segmentReader) keysForTerm(ctx context.Context, field, term string) (keySet, error) {
	field = strings.TrimSpace(field)
	if field == "" {
		return make(keySet), nil
	}
	result := make(keySet)
	err := r.forEachSegment(ctx, func(seg *Segment) error {
		block, ok := seg.Fields[field]
		if !ok {
			return nil
		}
		keys := block.Postings[term]
		for _, key := range keys {
			result[key] = struct{}{}
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

func (r *segmentReader) keysForContains(ctx context.Context, term *api.Term, useTrigramIndex bool) (keySet, error) {
	field := normalizeField(term)
	if field == "" {
		return make(keySet), nil
	}
	needle := normalizeTermValue(term.Value)
	if needle == "" {
		return r.keysForExists(ctx, field)
	}
	var candidateFilter keySet
	if useTrigramIndex {
		grams := normalizedTrigrams(needle)
		if len(grams) > 0 {
			var out keySet
			for i, gram := range grams {
				gramSet, err := r.keysForTerm(ctx, containsGramField(field), gram)
				if err != nil {
					return nil, err
				}
				if i == 0 {
					out = gramSet
					continue
				}
				out = intersect(out, gramSet)
			}
			if len(out) > 0 {
				candidateFilter = out
			}
		}
	}
	result := make(keySet)
	err := r.forEachPosting(ctx, field, func(termValue string, keys []string) error {
		if strings.Contains(termValue, needle) {
			for _, key := range keys {
				if len(candidateFilter) > 0 {
					if _, ok := candidateFilter[key]; !ok {
						continue
					}
				}
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

func (r *segmentReader) docMetaMap(ctx context.Context) (map[string]DocumentMetadata, error) {
	if r == nil {
		return nil, nil
	}
	out := make(map[string]DocumentMetadata)
	err := r.forEachSegment(ctx, func(seg *Segment) error {
		if seg == nil || len(seg.DocMeta) == 0 {
			return nil
		}
		for key, meta := range seg.DocMeta {
			if key == "" {
				continue
			}
			if _, exists := out[key]; exists {
				continue
			}
			out[key] = meta
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

type selectorEvaluator struct {
	reader        *segmentReader
	containsNgram bool
}

type selectorClauseResolver struct {
	family  string
	resolve func(ctx context.Context, e selectorEvaluator, sel api.Selector) (keySet, bool, error)
}

var selectorClauseResolvers = []selectorClauseResolver{
	{
		family: "eq",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (keySet, bool, error) {
			if sel.Eq == nil {
				return nil, false, nil
			}
			set, err := e.reader.keysForEq(ctx, sel.Eq)
			return set, true, err
		},
	},
	{
		family: "prefix",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (keySet, bool, error) {
			if sel.Prefix == nil {
				return nil, false, nil
			}
			set, err := e.reader.keysForPrefix(ctx, sel.Prefix)
			return set, true, err
		},
	},
	{
		family: "prefix",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (keySet, bool, error) {
			if sel.IPrefix == nil {
				return nil, false, nil
			}
			set, err := e.reader.keysForPrefix(ctx, sel.IPrefix)
			return set, true, err
		},
	},
	{
		family: "contains",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (keySet, bool, error) {
			if sel.Contains == nil {
				return nil, false, nil
			}
			set, err := e.reader.keysForContains(ctx, sel.Contains, e.containsNgram)
			return set, true, err
		},
	},
	{
		family: "contains",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (keySet, bool, error) {
			if sel.IContains == nil {
				return nil, false, nil
			}
			set, err := e.reader.keysForContains(ctx, sel.IContains, e.containsNgram)
			return set, true, err
		},
	},
	{
		family: "range",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (keySet, bool, error) {
			if sel.Range == nil {
				return nil, false, nil
			}
			set, err := e.reader.keysForRange(ctx, sel.Range)
			return set, true, err
		},
	},
	{
		family: "in",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (keySet, bool, error) {
			if sel.In == nil {
				return nil, false, nil
			}
			set, err := e.reader.keysForIn(ctx, sel.In)
			return set, true, err
		},
	},
	{
		family: "exists",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (keySet, bool, error) {
			if sel.Exists == "" {
				return nil, false, nil
			}
			set, err := e.reader.keysForExists(ctx, sel.Exists)
			return set, true, err
		},
	},
}

var selectorSupportedFamilies = func() map[string]struct{} {
	out := map[string]struct{}{
		"and": {},
		"or":  {},
		"not": {},
	}
	for _, resolver := range selectorClauseResolvers {
		out[resolver.family] = struct{}{}
	}
	return out
}()

var errDocMetaComplete = errors.New("doc meta complete")

func (a *Adapter) collectDocMeta(ctx context.Context, keys []string, reader *segmentReader) (map[string]search.DocMetadata, error) {
	if len(keys) == 0 || reader == nil {
		return nil, nil
	}
	out := make(map[string]search.DocMetadata, len(keys))
	remaining := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		remaining[key] = struct{}{}
	}
	invalid := 0
	err := reader.forEachSegment(ctx, func(seg *Segment) error {
		if len(remaining) == 0 {
			return errDocMetaComplete
		}
		for key := range remaining {
			meta, ok := seg.DocMeta[key]
			if !ok {
				continue
			}
			entry := search.DocMetadata{
				StateETag:           meta.StateETag,
				StatePlaintextBytes: meta.StatePlaintextBytes,
				PublishedVersion:    meta.PublishedVersion,
			}
			if meta.StateETag == "" || meta.PublishedVersion == 0 {
				invalid++
			}
			if len(meta.StateDescriptor) > 0 {
				entry.StateDescriptor = append([]byte(nil), meta.StateDescriptor...)
			}
			out[key] = entry
			delete(remaining, key)
			if len(remaining) == 0 {
				return errDocMetaComplete
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, errDocMetaComplete) {
		return nil, err
	}
	found := len(out)
	missing := len(remaining)
	if found < 0 {
		found = 0
	}
	if missing < 0 {
		missing = 0
	}
	a.recordDocMetaStats(found, missing, invalid)
	return out, nil
}

func (a *Adapter) recordDocMetaStats(found, missing, invalid int) {
	if a == nil {
		return
	}
	if found > 0 {
		a.docMetaHits.Add(uint64(found))
	}
	if missing > 0 {
		a.docMetaMisses.Add(uint64(missing))
	}
	if invalid > 0 {
		a.docMetaInvalid.Add(uint64(invalid))
	}
	logger := a.logger
	if logger == nil {
		return
	}
	now := time.Now().UnixNano()
	last := a.docMetaLogAt.Load()
	if last != 0 && now-last < int64(10*time.Second) {
		return
	}
	if !a.docMetaLogAt.CompareAndSwap(last, now) {
		return
	}
	hits := a.docMetaHits.Load()
	misses := a.docMetaMisses.Load()
	bad := a.docMetaInvalid.Load()
	total := hits + misses
	if total == 0 {
		return
	}
	hitRatio := float64(hits) / float64(total)
	logger.Trace("index.docmeta.stats",
		"hits", hits,
		"misses", misses,
		"invalid", bad,
		"total", total,
		"hit_ratio", hitRatio,
	)
}

func (e selectorEvaluator) evaluate(ctx context.Context, sel api.Selector) (keySet, error) {
	baseSet, initialized, err := e.evaluateBase(ctx, sel)
	if err != nil {
		return nil, err
	}
	if len(sel.Or) == 0 {
		if !initialized {
			return e.reader.allKeys(ctx)
		}
		return baseSet, nil
	}
	unionSet := make(keySet)
	for _, branch := range sel.Or {
		branchSet, err := e.evaluate(ctx, branch)
		if err != nil {
			return nil, err
		}
		unionSet = union(unionSet, branchSet)
	}
	if !initialized {
		return unionSet, nil
	}
	return intersect(baseSet, unionSet), nil
}

func (e selectorEvaluator) evaluateBase(ctx context.Context, sel api.Selector) (keySet, bool, error) {
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
	for _, resolver := range selectorClauseResolvers {
		set, present, err := resolver.resolve(ctx, e, sel)
		if err != nil {
			return nil, false, err
		}
		if !present {
			continue
		}
		intersectWith(set)
	}
	for _, clause := range sel.And {
		clauseSet, err := e.evaluate(ctx, clause)
		if err != nil {
			return nil, false, err
		}
		intersectWith(clauseSet)
	}
	if sel.Not != nil {
		notSet, err := e.evaluate(ctx, *sel.Not)
		if err != nil {
			return nil, false, err
		}
		if len(notSet) > 0 {
			if !initialized {
				all, err := e.reader.allKeys(ctx)
				if err != nil {
					return nil, false, err
				}
				result = subtract(all, notSet)
				initialized = true
			} else {
				result = subtract(result, notSet)
			}
		}
	}
	return result, initialized, nil
}

func (a *Adapter) visibilityMap(ctx context.Context, namespace string) (map[string]bool, error) {
	if a == nil || a.store == nil {
		return nil, nil
	}
	entries, _, err := a.store.VisibilityEntries(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (a *Adapter) visibleFromLedger(ctx context.Context, namespace, key string, entries map[string]bool) (bool, bool, error) {
	if entries == nil {
		return false, false, nil
	}
	visible, ok := entries[key]
	return visible, ok, nil
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

func selectorSupportsLegacyIndexFilter(sel api.Selector) bool {
	if sel.IsEmpty() {
		return true
	}
	caps := lql.InspectSelectorCapabilities(sel)
	if caps.WildcardPath || caps.RecursivePath {
		return false
	}
	families := caps.Families()
	if len(families) == 0 {
		return false
	}
	for _, family := range families {
		if _, ok := selectorSupportedFamilies[family]; !ok {
			return false
		}
	}
	return true
}

func cloneSelector(sel api.Selector) api.Selector {
	payload, err := json.Marshal(sel)
	if err != nil {
		return sel
	}
	if len(payload) == 0 || string(payload) == "{}" {
		return api.Selector{}
	}
	var out api.Selector
	if err := json.Unmarshal(payload, &out); err != nil {
		return sel
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

func normalizeSelectorFieldsForLQL(sel *api.Selector) error {
	if sel == nil {
		return nil
	}
	if sel.Eq != nil {
		normalized, err := normalizePointerLenient(sel.Eq.Field)
		if err != nil {
			return fmt.Errorf("eq.field: %w", err)
		}
		sel.Eq.Field = normalized
	}
	if sel.Contains != nil {
		normalized, err := normalizePointerLenient(sel.Contains.Field)
		if err != nil {
			return fmt.Errorf("contains.field: %w", err)
		}
		sel.Contains.Field = normalized
	}
	if sel.IContains != nil {
		normalized, err := normalizePointerLenient(sel.IContains.Field)
		if err != nil {
			return fmt.Errorf("icontains.field: %w", err)
		}
		sel.IContains.Field = normalized
	}
	if sel.Prefix != nil {
		normalized, err := normalizePointerLenient(sel.Prefix.Field)
		if err != nil {
			return fmt.Errorf("prefix.field: %w", err)
		}
		sel.Prefix.Field = normalized
	}
	if sel.IPrefix != nil {
		normalized, err := normalizePointerLenient(sel.IPrefix.Field)
		if err != nil {
			return fmt.Errorf("iprefix.field: %w", err)
		}
		sel.IPrefix.Field = normalized
	}
	if sel.Range != nil {
		normalized, err := normalizePointerLenient(sel.Range.Field)
		if err != nil {
			return fmt.Errorf("range.field: %w", err)
		}
		sel.Range.Field = normalized
	}
	if sel.In != nil {
		normalized, err := normalizePointerLenient(sel.In.Field)
		if err != nil {
			return fmt.Errorf("in.field: %w", err)
		}
		sel.In.Field = normalized
	}
	if sel.Exists != "" {
		normalized, err := normalizePointerLenient(sel.Exists)
		if err != nil {
			return fmt.Errorf("exists.field: %w", err)
		}
		sel.Exists = normalized
	}
	for i := range sel.And {
		if err := normalizeSelectorFieldsForLQL(&sel.And[i]); err != nil {
			return err
		}
	}
	for i := range sel.Or {
		if err := normalizeSelectorFieldsForLQL(&sel.Or[i]); err != nil {
			return err
		}
	}
	if sel.Not != nil {
		if err := normalizeSelectorFieldsForLQL(sel.Not); err != nil {
			return err
		}
	}
	return nil
}

func normalizePointerLenient(field string) (string, error) {
	field = strings.TrimSpace(field)
	if field == "" || field == "/" {
		return "", nil
	}
	if !strings.HasPrefix(field, "/") {
		field = "/" + field
	}
	return jsonpointer.Normalize(field)
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
