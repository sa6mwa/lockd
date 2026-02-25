package scan

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"pkt.systems/lockd/internal/jsonpointer"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lql"
	"pkt.systems/pslog"
)

const cursorPrefix = "scanv1:"

var reservedQueryPrefixes = []string{"lockd-diagnostics/"}

// Config wires the scan adapter to a backend.
type Config struct {
	Backend          storage.Backend
	Logger           pslog.Logger
	MaxDocumentBytes int64
}

// Adapter implements search.Adapter by scanning metadata/state directly from the backend.
type Adapter struct {
	backend          storage.Backend
	logger           pslog.Logger
	maxDocumentBytes int64
	plans            *queryPlanCache
}

// New returns a scan adapter wired according to cfg.
func New(cfg Config) (*Adapter, error) {
	if cfg.Backend == nil {
		return nil, fmt.Errorf("scan: backend required")
	}
	maxBytes := cfg.MaxDocumentBytes
	if maxBytes <= 0 {
		maxBytes = 100 * 1024 * 1024
	}
	return &Adapter{
		backend:          cfg.Backend,
		logger:           cfg.Logger,
		maxDocumentBytes: maxBytes,
		plans:            newQueryPlanCache(defaultQueryPlanCacheLimit),
	}, nil
}

// Capabilities reports that the scan adapter always supports the scan engine
// (and never provides an index engine).
func (a *Adapter) Capabilities(context.Context, string) (search.Capabilities, error) {
	return search.Capabilities{Scan: true}, nil
}

// Query enumerates namespace keys, applying selector filters via LQL streaming.
func (a *Adapter) Query(ctx context.Context, req search.Request) (search.Result, error) {
	if req.Namespace == "" {
		return search.Result{}, fmt.Errorf("scan: namespace required")
	}
	keys, err := a.backend.ListMetaKeys(ctx, req.Namespace)
	if err != nil {
		return search.Result{}, err
	}
	if len(keys) == 0 {
		return search.Result{}, nil
	}
	sort.Strings(keys)
	startIdx, err := startIndexFromCursor(req.Cursor, keys)
	if err != nil {
		return search.Result{}, fmt.Errorf("%w: %v", search.ErrInvalidCursor, err)
	}
	if startIdx >= len(keys) {
		return search.Result{}, nil
	}
	matchAll := req.Selector.IsEmpty()
	compatNumericSelector := !matchAll && selectorNeedsNumericSegmentCompat(req.Selector)
	var plan lql.QueryStreamPlan
	if !matchAll && !compatNumericSelector {
		compiled, compileErr := a.compiledPlan(req.Selector)
		if compileErr != nil {
			return search.Result{}, fmt.Errorf("scan: compile selector: %w", compileErr)
		}
		plan = compiled
	}
	limit := req.Limit
	if limit <= 0 || limit > len(keys) {
		limit = len(keys)
	}
	matches := make([]string, 0, limit)
	var lastMatch string
	more := false
	for i := startIdx; i < len(keys); i++ {
		if len(matches) >= limit {
			if i+1 < len(keys) {
				more = true
			}
			break
		}
		if err := ctx.Err(); err != nil {
			return search.Result{}, err
		}
		key := keys[i]
		if hasReservedPrefix(key) {
			continue
		}
		res, err := a.backend.LoadMeta(ctx, req.Namespace, key)
		if err != nil {
			if shouldSkipReadError(err) {
				a.logDebug("search.scan.load_meta", "namespace", req.Namespace, "key", key, "error", err)
				continue
			}
			return search.Result{}, fmt.Errorf("scan: load meta %s: %w", key, err)
		}
		meta := res.Meta
		if meta == nil {
			continue
		}
		if meta.QueryExcluded() {
			continue
		}
		if meta.PublishedVersion == 0 {
			continue
		}
		if matchAll {
			matches = append(matches, key)
			lastMatch = key
			continue
		}
		if meta.StateETag == "" {
			continue
		}
		var matched bool
		if compatNumericSelector {
			matched, err = a.matchesSelectorCompat(ctx, req.Namespace, key, meta, req.Selector)
		} else {
			matched, err = a.matchesSelector(ctx, req.Namespace, key, meta, plan)
		}
		if err != nil {
			if shouldSkipReadError(err) {
				a.logDebug("search.scan.load_state", "namespace", req.Namespace, "key", key, "error", err)
				continue
			}
			return search.Result{}, fmt.Errorf("scan: load state %s: %w", key, err)
		}
		if matched {
			matches = append(matches, key)
			lastMatch = key
		}
	}
	result := search.Result{Keys: matches}
	if len(matches) == limit && more && lastMatch != "" {
		result.Cursor = encodeCursor(lastMatch)
	}
	return result, nil
}

// QueryDocuments enumerates namespace keys and streams matching documents in a
// single read pass per candidate.
func (a *Adapter) QueryDocuments(ctx context.Context, req search.Request, sink search.DocumentSink) (search.Result, error) {
	if req.Namespace == "" {
		return search.Result{}, fmt.Errorf("scan: namespace required")
	}
	if sink == nil {
		return search.Result{}, fmt.Errorf("scan: document sink required")
	}
	keys, err := a.backend.ListMetaKeys(ctx, req.Namespace)
	if err != nil {
		return search.Result{}, err
	}
	if len(keys) == 0 {
		return search.Result{}, nil
	}
	sort.Strings(keys)
	startIdx, err := startIndexFromCursor(req.Cursor, keys)
	if err != nil {
		return search.Result{}, fmt.Errorf("%w: %v", search.ErrInvalidCursor, err)
	}
	if startIdx >= len(keys) {
		return search.Result{}, nil
	}
	matchAll := req.Selector.IsEmpty()
	compatNumericSelector := !matchAll && selectorNeedsNumericSegmentCompat(req.Selector)
	var plan lql.QueryStreamPlan
	if !matchAll && !compatNumericSelector {
		compiled, compileErr := a.compiledPlan(req.Selector)
		if compileErr != nil {
			return search.Result{}, fmt.Errorf("scan: compile selector: %w", compileErr)
		}
		plan = compiled
	}
	limit := req.Limit
	if limit <= 0 || limit > len(keys) {
		limit = len(keys)
	}
	matches := make([]string, 0, limit)
	var streamStats search.StreamStats
	var lastMatch string
	more := false
	for i := startIdx; i < len(keys); i++ {
		if len(matches) >= limit {
			if i+1 < len(keys) {
				more = true
			}
			break
		}
		if err := ctx.Err(); err != nil {
			return search.Result{}, err
		}
		key := keys[i]
		if hasReservedPrefix(key) {
			continue
		}
		res, err := a.backend.LoadMeta(ctx, req.Namespace, key)
		if err != nil {
			if shouldSkipReadError(err) {
				a.logDebug("search.scan.load_meta", "namespace", req.Namespace, "key", key, "error", err)
				continue
			}
			return search.Result{}, fmt.Errorf("scan: load meta %s: %w", key, err)
		}
		meta := res.Meta
		if meta == nil || meta.QueryExcluded() || meta.PublishedVersion == 0 {
			continue
		}
		if !matchAll && meta.StateETag == "" {
			continue
		}
		version := meta.PublishedVersion
		if version == 0 {
			version = meta.Version
		}
		stateCtx := ctx
		if len(meta.StateDescriptor) > 0 {
			stateCtx = storage.ContextWithStateDescriptor(stateCtx, meta.StateDescriptor)
		}
		if meta.StatePlaintextBytes > 0 {
			stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
		}
		stateRes, err := a.backend.ReadState(stateCtx, req.Namespace, key)
		if err != nil {
			if shouldSkipReadError(err) {
				a.logDebug("search.scan.load_state", "namespace", req.Namespace, "key", key, "error", err)
				continue
			}
			return search.Result{}, fmt.Errorf("scan: load state %s: %w", key, err)
		}
		if matchAll {
			docReader := stateRes.Reader
			if err := sink.OnDocument(ctx, req.Namespace, key, version, docReader); err != nil {
				docReader.Close()
				return search.Result{}, err
			}
			docReader.Close()
			matches = append(matches, key)
			streamStats.AddCandidate(true)
			lastMatch = key
			continue
		}
		if compatNumericSelector {
			_ = stateRes.Reader.Close()
			matched, err := a.matchesSelectorCompat(ctx, req.Namespace, key, meta, req.Selector)
			if err != nil {
				if shouldSkipReadError(err) {
					a.logDebug("search.scan.load_state", "namespace", req.Namespace, "key", key, "error", err)
					continue
				}
				return search.Result{}, fmt.Errorf("scan: load state %s: %w", key, err)
			}
			streamStats.AddCandidate(matched)
			if !matched {
				continue
			}
			stateRes, err = a.backend.ReadState(stateCtx, req.Namespace, key)
			if err != nil {
				if shouldSkipReadError(err) {
					a.logDebug("search.scan.load_state", "namespace", req.Namespace, "key", key, "error", err)
					continue
				}
				return search.Result{}, fmt.Errorf("scan: load state %s: %w", key, err)
			}
			docReader := stateRes.Reader
			if err := sink.OnDocument(ctx, req.Namespace, key, version, docReader); err != nil {
				docReader.Close()
				return search.Result{}, err
			}
			docReader.Close()
			matches = append(matches, key)
			lastMatch = key
			continue
		}
		matched, queryResult, err := a.matchAndStreamDocument(ctx, req.Namespace, key, version, stateRes.Reader, plan, sink)
		if err != nil {
			return search.Result{}, err
		}
		streamStats.Add(
			queryResult.CandidatesSeen,
			queryResult.CandidatesMatched,
			queryResult.BytesCaptured,
			queryResult.SpillCount,
			queryResult.SpillBytes,
		)
		if matched {
			matches = append(matches, key)
			lastMatch = key
		}
	}
	result := search.Result{Keys: matches}
	result.Metadata = streamStats.ApplyMetadata(result.Metadata)
	if len(matches) == limit && more && lastMatch != "" {
		result.Cursor = encodeCursor(lastMatch)
	}
	return result, nil
}

func (a *Adapter) matchesSelector(ctx context.Context, namespace, key string, meta *storage.Meta, plan lql.QueryStreamPlan) (bool, error) {
	stateCtx := ctx
	if len(meta.StateDescriptor) > 0 {
		stateCtx = storage.ContextWithStateDescriptor(stateCtx, meta.StateDescriptor)
	}
	if meta.StatePlaintextBytes > 0 {
		stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
	}
	stateRes, err := a.backend.ReadState(stateCtx, namespace, key)
	if err != nil {
		return false, err
	}
	defer stateRes.Reader.Close()

	matched := false
	_, err = lql.QueryStreamWithResult(lql.QueryStreamRequest{
		Ctx:               stateCtx,
		Reader:            stateRes.Reader,
		Plan:              plan,
		Mode:              lql.QueryDecisionOnly,
		MaxCandidateBytes: a.maxDocumentBytes,
		MaxMatches:        1,
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

func (a *Adapter) matchesSelectorCompat(ctx context.Context, namespace, key string, meta *storage.Meta, selector lql.Selector) (bool, error) {
	stateCtx := ctx
	if len(meta.StateDescriptor) > 0 {
		stateCtx = storage.ContextWithStateDescriptor(stateCtx, meta.StateDescriptor)
	}
	if meta.StatePlaintextBytes > 0 {
		stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
	}
	stateRes, err := a.backend.ReadState(stateCtx, namespace, key)
	if err != nil {
		return false, err
	}
	defer stateRes.Reader.Close()

	dec := json.NewDecoder(stateRes.Reader)
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return false, err
	}
	return lql.MatchesValue(selector, value), nil
}

func (a *Adapter) matchAndStreamDocument(
	ctx context.Context,
	namespace string,
	key string,
	version int64,
	reader io.ReadCloser,
	plan lql.QueryStreamPlan,
	sink search.DocumentSink,
) (bool, lql.QueryStreamResult, error) {
	if reader == nil {
		return false, lql.QueryStreamResult{}, nil
	}
	defer reader.Close()

	matched := false
	result, err := lql.QueryStreamWithResult(lql.QueryStreamRequest{
		Ctx:               ctx,
		Reader:            reader,
		Plan:              plan,
		Mode:              lql.QueryDecisionPlusValue,
		MatchedOnly:       true,
		SpoolMemoryBytes:  1,
		MaxCandidateBytes: a.maxDocumentBytes,
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
		return false, lql.QueryStreamResult{}, fmt.Errorf("scan: evaluate state %s: %w", key, err)
	}
	if !matched {
		return false, result, nil
	}
	return true, result, nil
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

func encodeCursor(key string) string {
	data := base64.RawURLEncoding.EncodeToString([]byte(key))
	return cursorPrefix + data
}

func decodeCursor(cursor string) (string, error) {
	if cursor == "" {
		return "", nil
	}
	if len(cursor) <= len(cursorPrefix) || cursor[:len(cursorPrefix)] != cursorPrefix {
		return "", fmt.Errorf("unsupported cursor prefix")
	}
	payload, err := base64.RawURLEncoding.DecodeString(cursor[len(cursorPrefix):])
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func hasReservedPrefix(key string) bool {
	for _, prefix := range reservedQueryPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func (a *Adapter) logDebug(msg string, fields ...any) {
	if a.logger == nil {
		return
	}
	a.logger.Debug(msg, fields...)
}

func shouldSkipReadError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, storage.ErrNotFound) || storage.IsTransient(err)
}

func selectorNeedsNumericSegmentCompat(sel lql.Selector) bool {
	if sel.Eq != nil && fieldHasNumericSegment(sel.Eq.Field) {
		return true
	}
	if sel.Contains != nil && fieldHasNumericSegment(sel.Contains.Field) {
		return true
	}
	if sel.IContains != nil && fieldHasNumericSegment(sel.IContains.Field) {
		return true
	}
	if sel.Prefix != nil && fieldHasNumericSegment(sel.Prefix.Field) {
		return true
	}
	if sel.IPrefix != nil && fieldHasNumericSegment(sel.IPrefix.Field) {
		return true
	}
	if sel.Range != nil && fieldHasNumericSegment(sel.Range.Field) {
		return true
	}
	if sel.In != nil && fieldHasNumericSegment(sel.In.Field) {
		return true
	}
	if sel.Exists != "" && fieldHasNumericSegment(sel.Exists) {
		return true
	}
	for i := range sel.And {
		if selectorNeedsNumericSegmentCompat(sel.And[i]) {
			return true
		}
	}
	for i := range sel.Or {
		if selectorNeedsNumericSegmentCompat(sel.Or[i]) {
			return true
		}
	}
	if sel.Not != nil && selectorNeedsNumericSegmentCompat(*sel.Not) {
		return true
	}
	return false
}

func fieldHasNumericSegment(field string) bool {
	field = strings.TrimSpace(field)
	if field == "" || field == "/" {
		return false
	}
	segments, err := jsonpointer.Split(field)
	if err != nil || len(segments) == 0 {
		return false
	}
	segments = expandBracketSugarSegments(segments)
	for _, segment := range segments {
		switch segment {
		case "*", "[]", "**", "...":
			continue
		}
		if isDigits(segment) {
			return true
		}
	}
	return false
}

func expandBracketSugarSegments(segments []string) []string {
	out := make([]string, 0, len(segments))
	for _, segment := range segments {
		if segment == "" || segment == "[]" || segment == "*" || segment == "**" || segment == "..." || !strings.HasSuffix(segment, "[]") {
			out = append(out, segment)
			continue
		}
		base := segment
		count := 0
		for strings.HasSuffix(base, "[]") && base != "[]" {
			base = strings.TrimSuffix(base, "[]")
			count++
		}
		if base == "" {
			out = append(out, segment)
			continue
		}
		out = append(out, base)
		for i := 0; i < count; i++ {
			out = append(out, "[]")
		}
	}
	return out
}

func isDigits(segment string) bool {
	if segment == "" {
		return false
	}
	for _, ch := range segment {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	// Keep semantics aligned with stream path compilation (non-negative int tokens).
	_, err := strconv.Atoi(segment)
	return err == nil
}

func (a *Adapter) compiledPlan(sel lql.Selector) (lql.QueryStreamPlan, error) {
	payload, err := json.Marshal(sel)
	if err != nil {
		return lql.QueryStreamPlan{}, err
	}
	cacheKey := string(payload)
	if a != nil && a.plans != nil {
		if plan, ok := a.plans.get(cacheKey); ok {
			return plan, nil
		}
	}
	plan, err := lql.NewQueryStreamPlan(sel)
	if err != nil {
		return lql.QueryStreamPlan{}, err
	}
	if a != nil && a.plans != nil {
		a.plans.put(cacheKey, plan)
	}
	return plan, nil
}
