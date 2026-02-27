package index

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
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
const queryStreamPayloadSpoolMemoryBytes = 256 * 1024

var docIDScratchPool = sync.Pool{
	New: func() any {
		buf := make(docIDSet, 0, 256)
		return &buf
	},
}

var docIDAccumulatorPool = sync.Pool{
	New: func() any {
		return &docIDAccumulator{
			current: make(docIDSet, 0, 256),
			scratch: make(docIDSet, 0, 256),
		}
	},
}

var keyPresencePool = sync.Pool{
	New: func() any {
		buf := make([]bool, 0, 256)
		return &buf
	},
}

var querySegmentReaderPool = sync.Pool{
	New: func() any {
		return &segmentReader{}
	},
}

func releaseQuerySegmentReader(reader *segmentReader) {
	if reader == nil || !reader.immutable {
		return
	}
	*reader = segmentReader{}
	querySegmentReaderPool.Put(reader)
}

// AdapterConfig configures the index query adapter.
type AdapterConfig struct {
	Store  *Store
	Logger pslog.Logger
}

// Adapter executes queries against immutable index segments.
type Adapter struct {
	store   *Store
	logger  pslog.Logger
	plans   *selectorPlanCache
	readers *preparedReaderCache
	sorted  *sortedKeysCache

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
		store:   cfg.Store,
		logger:  cfg.Logger,
		plans:   newSelectorPlanCache(defaultSelectorPlanCacheLimit),
		readers: newPreparedReaderCache(defaultPreparedReaderCacheLimit),
		sorted:  newSortedKeysCache(defaultSortedKeysCacheLimit),
	}, nil
}

// Capabilities reports whether the index engine is available for the namespace.
func (a *Adapter) Capabilities(context.Context, string) (search.Capabilities, error) {
	if a == nil || a.store == nil {
		return search.Capabilities{}, nil
	}
	return search.Capabilities{Index: true}, nil
}

func (a *Adapter) prepareSelectorExecutionPlan(sel api.Selector) (selectorExecutionPlan, error) {
	if sel.IsEmpty() {
		return selectorExecutionPlan{selector: api.Selector{}, useLegacyFilter: true}, nil
	}
	needsNormalization, err := selectorNeedsNormalization(sel)
	if err != nil {
		return selectorExecutionPlan{}, fmt.Errorf("normalize selector: %w", err)
	}
	normalized := sel
	if needsNormalization {
		normalized = cloneSelector(sel)
		if normalizeErr := normalizeSelectorFieldsForLQL(&normalized); normalizeErr != nil {
			return selectorExecutionPlan{}, fmt.Errorf("normalize selector: %w", normalizeErr)
		}
	}
	cacheKey := ""
	if shouldCacheSelectorPlan(normalized) {
		keyPayload, err := json.Marshal(normalized)
		if err != nil {
			return selectorExecutionPlan{}, fmt.Errorf("marshal selector cache key: %w", err)
		}
		cacheKey = string(keyPayload)
		if a != nil && a.plans != nil && cacheKey != "" {
			if cached, ok := a.plans.get(cacheKey); ok {
				return cached, nil
			}
		}
	}
	useLegacy := selectorSupportsLegacyIndexFilter(sel)
	out := selectorExecutionPlan{
		cacheKey:        cacheKey,
		selector:        normalized,
		useLegacyFilter: useLegacy,
	}
	if !useLegacy {
		compiled, compileErr := lql.NewQueryStreamPlan(normalized)
		if compileErr != nil {
			return selectorExecutionPlan{}, fmt.Errorf("compile selector: %w", compileErr)
		}
		out.requirePostEval = true
		out.postFilterPlan = compiled
	}
	if a != nil && a.plans != nil && cacheKey != "" {
		a.plans.put(cacheKey, out)
	}
	return out, nil
}

func (a *Adapter) queryReader(ctx context.Context, namespace string, manifest *Manifest, manifestETag string) (*segmentReader, error) {
	if manifest == nil {
		manifest = NewManifest()
	}
	if a == nil || a.store == nil || a.readers == nil {
		var store *Store
		var logger pslog.Logger
		if a != nil {
			store = a.store
			logger = a.logger
		}
		return newSegmentReader(namespace, manifest, store, logger), nil
	}
	key := preparedReaderCacheKey(namespace, manifest, manifestETag)
	if template, ok := a.readers.get(key); ok && template != nil {
		return template.cloneForQuery(manifest), nil
	}
	template := newSegmentReader(namespace, manifest, a.store, a.logger)
	if err := template.prime(ctx); err != nil {
		return nil, err
	}
	a.readers.put(key, template)
	return template.cloneForQuery(manifest), nil
}

func (a *Adapter) sortedMatchedKeys(ctx context.Context, reader *segmentReader, manifestETag string, format uint32, plan selectorExecutionPlan) ([]string, error) {
	cacheKey := ""
	if manifestETag != "" {
		cacheKey = manifestETag + "|" + plan.cacheKey
	}
	if cacheKey != "" && a != nil && a.sorted != nil {
		if cached, ok := a.sorted.get(cacheKey); ok {
			return cached, nil
		}
	}
	selector := plan.selector
	useLegacyFilter := plan.useLegacyFilter
	var (
		matches docIDSet
		err     error
	)
	if selector.IsEmpty() || !useLegacyFilter {
		matches, err = reader.allDocIDs(ctx)
	} else {
		eval := selectorEvaluator{
			reader:        reader,
			containsNgram: format >= IndexFormatVersionV4,
		}
		matches, err = eval.evaluate(ctx, selector)
	}
	if err != nil {
		return nil, err
	}
	sortedKeys := reader.sortedKeysForDocIDs(matches)
	if cacheKey != "" && a != nil && a.sorted != nil {
		a.sorted.put(cacheKey, sortedKeys)
	}
	return sortedKeys, nil
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
	reader, err := a.queryReader(ctx, req.Namespace, manifest, manifestRes.ETag)
	if err != nil {
		return search.Result{}, err
	}
	defer releaseQuerySegmentReader(reader)
	visibility, err := a.visibilityMap(ctx, req.Namespace)
	if err != nil {
		return search.Result{}, err
	}
	execPlan, err := a.prepareSelectorExecutionPlan(req.Selector)
	if err != nil {
		return search.Result{}, err
	}
	sortedKeys, err := a.sortedMatchedKeys(ctx, reader, manifestRes.ETag, manifest.Format, execPlan)
	if err != nil {
		return search.Result{}, err
	}
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
	docMeta, err := reader.docMetaMap(ctx)
	if err != nil {
		return search.Result{}, err
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
		visibleKey, ok, err := a.visibleFromLedger(ctx, req.Namespace, key, visibility)
		if err != nil {
			return search.Result{}, err
		}
		if ok && !visibleKey {
			continue
		}
		docInfo, hasDocMeta := docMeta[key]
		if !ok && hasDocMeta {
			if docInfo.PublishedVersion == 0 || docInfo.QueryExcluded {
				continue
			}
		}
		if execPlan.requirePostEval {
			var stateMeta *DocumentMetadata
			if hasDocMeta {
				docInfoCopy := docInfo
				if docInfoCopy.StateETag == "" {
					continue
				}
				stateMeta = &docInfoCopy
			}
			matched, matchErr := a.matchesSelector(ctx, req.Namespace, key, stateMeta, execPlan.postFilterPlan)
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
	reader, err := a.queryReader(ctx, req.Namespace, manifest, manifestRes.ETag)
	if err != nil {
		return search.Result{}, err
	}
	defer releaseQuerySegmentReader(reader)
	visibility, err := a.visibilityMap(ctx, req.Namespace)
	if err != nil {
		return search.Result{}, err
	}
	execPlan, err := a.prepareSelectorExecutionPlan(req.Selector)
	if err != nil {
		return search.Result{}, err
	}
	sortedKeys, err := a.sortedMatchedKeys(ctx, reader, manifestRes.ETag, manifest.Format, execPlan)
	if err != nil {
		return search.Result{}, err
	}
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
	docMeta, err := reader.docMetaMap(ctx)
	if err != nil {
		return search.Result{}, err
	}
	streamed := make([]string, 0, min(limit, len(sortedKeys)-startIdx))
	var streamStats search.StreamStats
	nextIndex := len(sortedKeys)
	var lastReturned string
	prefetch := req.DocPrefetch
	if prefetch <= 1 || execPlan.requirePostEval {
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
			docInfo, hasDocMeta := docMeta[key]
			var stateMeta *DocumentMetadata
			var version int64
			if hasDocMeta {
				if docInfo.PublishedVersion == 0 || docInfo.QueryExcluded {
					continue
				}
				docInfoCopy := docInfo
				stateMeta = &docInfoCopy
				version = docInfoCopy.PublishedVersion
			}
			if !execPlan.requirePostEval {
				if err := a.streamDocument(ctx, req.Namespace, key, version, stateMeta, sink); err != nil {
					if errors.Is(err, storage.ErrNotFound) || storage.IsTransient(err) {
						continue
					}
					return search.Result{}, fmt.Errorf("stream document %s: %w", key, err)
				}
				streamed = append(streamed, key)
				streamStats.AddCandidate(true)
				lastReturned = key
				nextIndex = i + 1
				continue
			}
			if stateMeta != nil && stateMeta.StateETag == "" {
				continue
			}
			matched, queryResult, err := a.matchAndStreamDocument(ctx, req.Namespace, key, version, stateMeta, execPlan.postFilterPlan, sink)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) || storage.IsTransient(err) {
					continue
				}
				return search.Result{}, fmt.Errorf("evaluate+stream selector %s: %w", key, err)
			}
			streamStats.Add(
				queryResult.CandidatesSeen,
				queryResult.CandidatesMatched,
				queryResult.BytesCaptured,
				queryResult.SpillCount,
				queryResult.SpillBytes,
			)
			if !matched {
				continue
			}
			streamed = append(streamed, key)
			lastReturned = key
			nextIndex = i + 1
		}
	} else {
		maxBatch := prefetch * 4
		if maxBatch < 1 {
			maxBatch = 1
		}
		for i := startIdx; i < len(sortedKeys); {
			if len(streamed) >= limit {
				nextIndex = i
				break
			}
			if err := ctx.Err(); err != nil {
				return search.Result{}, err
			}
			remaining := limit - len(streamed)
			batchSize := remaining
			if batchSize > maxBatch {
				batchSize = maxBatch
			}
			candidates := make([]documentStreamCandidate, 0, batchSize)
			for ; i < len(sortedKeys) && len(candidates) < batchSize; i++ {
				key := sortedKeys[i]
				visibleKey, ok, err := a.visibleFromLedger(ctx, req.Namespace, key, visibility)
				if err != nil {
					return search.Result{}, err
				}
				if ok && !visibleKey {
					continue
				}
				docInfo, hasDocMeta := docMeta[key]
				var stateMeta *DocumentMetadata
				var version int64
				if hasDocMeta {
					if docInfo.PublishedVersion == 0 || docInfo.QueryExcluded {
						continue
					}
					docInfoCopy := docInfo
					stateMeta = &docInfoCopy
					version = docInfoCopy.PublishedVersion
				}
				candidates = append(candidates, documentStreamCandidate{
					key:     key,
					version: version,
					meta:    stateMeta,
				})
			}
			if len(candidates) == 0 {
				nextIndex = i
				continue
			}
			batchStreamed, err := a.streamDocumentBatch(ctx, req.Namespace, candidates, sink, prefetch)
			if err != nil {
				return search.Result{}, err
			}
			for _, key := range batchStreamed {
				streamed = append(streamed, key)
				streamStats.AddCandidate(true)
				lastReturned = key
				nextIndex = i
				if len(streamed) >= limit {
					break
				}
			}
		}
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
	result.Metadata = streamStats.ApplyMetadata(result.Metadata)
	if len(streamed) == limit && nextIndex < len(sortedKeys) && lastReturned != "" {
		result.Cursor = encodeCursor(lastReturned)
	}
	return result, nil
}

func (a *Adapter) matchesSelector(ctx context.Context, namespace, key string, meta *DocumentMetadata, plan lql.QueryStreamPlan) (bool, error) {
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

func (a *Adapter) streamDocument(ctx context.Context, namespace, key string, version int64, meta *DocumentMetadata, sink search.DocumentSink) error {
	reader, err := a.openDocumentReader(ctx, namespace, key, meta)
	if err != nil {
		return err
	}
	if reader == nil {
		return nil
	}
	defer reader.Close()
	return sink.OnDocument(ctx, namespace, key, version, reader)
}

func (a *Adapter) openDocumentReader(ctx context.Context, namespace, key string, meta *DocumentMetadata) (io.ReadCloser, error) {
	stateRes, err := a.store.backend.ReadState(ctx, namespace, key)
	if err != nil && meta != nil && len(meta.StateDescriptor) > 0 && isMissingStateDescriptorError(err) {
		stateCtx := storage.ContextWithStateDescriptor(ctx, meta.StateDescriptor)
		if meta.StatePlaintextBytes > 0 {
			stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
		}
		stateRes, err = a.store.backend.ReadState(stateCtx, namespace, key)
	}
	if err != nil {
		return nil, err
	}
	if stateRes.Reader == nil {
		return nil, nil
	}
	return stateRes.Reader, nil
}

func isMissingStateDescriptorError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "missing state descriptor")
}

type documentStreamCandidate struct {
	key     string
	version int64
	meta    *DocumentMetadata
}

type documentStreamBatchResult struct {
	index   int
	key     string
	version int64
	reader  io.ReadCloser
	err     error
}

func (a *Adapter) streamDocumentBatch(ctx context.Context, namespace string, candidates []documentStreamCandidate, sink search.DocumentSink, prefetch int) ([]string, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	if prefetch <= 1 || len(candidates) == 1 {
		streamed := make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			if err := a.streamDocument(ctx, namespace, candidate.key, candidate.version, candidate.meta, sink); err != nil {
				if errors.Is(err, storage.ErrNotFound) || storage.IsTransient(err) {
					continue
				}
				return nil, fmt.Errorf("stream document %s: %w", candidate.key, err)
			}
			streamed = append(streamed, candidate.key)
		}
		return streamed, nil
	}
	if prefetch > len(candidates) {
		prefetch = len(candidates)
	}
	jobs := make(chan int)
	results := make(chan documentStreamBatchResult, len(candidates))
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for idx := range jobs {
			candidate := candidates[idx]
			reader, err := a.openDocumentReader(ctx, namespace, candidate.key, candidate.meta)
			results <- documentStreamBatchResult{
				index:   idx,
				key:     candidate.key,
				version: candidate.version,
				reader:  reader,
				err:     err,
			}
		}
	}
	wg.Add(prefetch)
	for i := 0; i < prefetch; i++ {
		go worker()
	}
	go func() {
		defer close(jobs)
		for idx := range len(candidates) {
			select {
			case <-ctx.Done():
				return
			case jobs <- idx:
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()
	ordered := make([]documentStreamBatchResult, len(candidates))
	for res := range results {
		ordered[res.index] = res
	}
	streamed := make([]string, 0, len(candidates))
	for _, res := range ordered {
		if res.err != nil {
			if errors.Is(res.err, storage.ErrNotFound) || storage.IsTransient(res.err) {
				continue
			}
			for _, pending := range ordered {
				if pending.reader != nil {
					_ = pending.reader.Close()
				}
			}
			return nil, fmt.Errorf("stream document %s: %w", res.key, res.err)
		}
		if res.reader == nil {
			continue
		}
		if err := sink.OnDocument(ctx, namespace, res.key, res.version, res.reader); err != nil {
			_ = res.reader.Close()
			for _, pending := range ordered {
				if pending.reader != nil {
					_ = pending.reader.Close()
				}
			}
			return nil, err
		}
		_ = res.reader.Close()
		streamed = append(streamed, res.key)
	}
	return streamed, nil
}

func (a *Adapter) matchAndStreamDocument(
	ctx context.Context,
	namespace string,
	key string,
	version int64,
	meta *DocumentMetadata,
	plan lql.QueryStreamPlan,
	sink search.DocumentSink,
) (bool, lql.QueryStreamResult, error) {
	stateCtx := ctx
	if meta != nil && len(meta.StateDescriptor) > 0 {
		stateCtx = storage.ContextWithStateDescriptor(stateCtx, meta.StateDescriptor)
	}
	if meta != nil && meta.StatePlaintextBytes > 0 {
		stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
	}
	stateRes, err := a.store.backend.ReadState(stateCtx, namespace, key)
	if err != nil {
		return false, lql.QueryStreamResult{}, err
	}
	if stateRes.Reader == nil {
		return false, lql.QueryStreamResult{}, nil
	}
	defer stateRes.Reader.Close()

	matched := false
	result, err := lql.QueryStreamWithResult(lql.QueryStreamRequest{
		Ctx:               stateCtx,
		Reader:            stateRes.Reader,
		Plan:              plan,
		Mode:              lql.QueryDecisionPlusValue,
		MatchedOnly:       true,
		SpoolMemoryBytes:  queryStreamPayloadSpoolMemoryBytes,
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
		return false, lql.QueryStreamResult{}, err
	}
	if !matched {
		return false, result, nil
	}
	return true, result, nil
}

type segmentReader struct {
	namespace             string
	manifest              *Manifest
	store                 *Store
	logger                pslog.Logger
	cache                 map[string]*Segment
	compiled              map[string]*compiledSegment
	singleCompiled        *compiledSegment
	singleCompiledReady   bool
	keyIDs                map[string]uint32
	keys                  []string
	fieldIDs              map[string]uint32
	fields                []string
	fieldList             []string
	fieldListReady        bool
	fieldSegments         map[string][]string
	fieldResolutionCached map[string][]string
	sharedFieldResolution *fieldResolutionCache
	fieldTrie             *fieldPathTrie
	docMeta               map[string]DocumentMetadata
	docMetaReady          bool
	keyOrderByName        []uint32
	keyOrderByNameReady   bool
	immutable             bool
}

func newSegmentReader(namespace string, manifest *Manifest, store *Store, logger pslog.Logger) *segmentReader {
	return &segmentReader{
		namespace:             namespace,
		manifest:              manifest,
		store:                 store,
		logger:                logger,
		cache:                 make(map[string]*Segment),
		compiled:              make(map[string]*compiledSegment),
		keyIDs:                make(map[string]uint32),
		fieldIDs:              make(map[string]uint32),
		fieldSegments:         make(map[string][]string),
		sharedFieldResolution: newFieldResolutionCache(),
	}
}

func (r *segmentReader) allDocIDs(ctx context.Context) (docIDSet, error) {
	out := borrowDocIDScratch()
	defer releaseDocIDScratch(out)
	decoded := borrowDocIDScratch()
	defer releaseDocIDScratch(decoded)
	err := r.forEachCompiledSegment(ctx, func(seg *compiledSegment) error {
		for _, fieldID := range seg.fieldIDs {
			block, ok := seg.fieldsByID[fieldID]
			if !ok {
				continue
			}
			for _, posting := range block.postingsByID {
				docIDs := posting.decodeInto((*decoded)[:0])
				*decoded = docIDs
				*out = append(*out, docIDs...)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sortUniqueDocIDs(append(docIDSet(nil), (*out)...)), nil
}

func (r *segmentReader) resolveSelectorFieldIDs(ctx context.Context, field string) ([]uint32, error) {
	resolved, err := r.resolveSelectorFields(ctx, field)
	if err != nil {
		return nil, err
	}
	if len(resolved) == 0 {
		return nil, nil
	}
	ids := make([]uint32, 0, len(resolved))
	for _, name := range resolved {
		id, ok := r.fieldID(name)
		if !ok {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

func (r *segmentReader) docIDsForEq(ctx context.Context, term *api.Term) (docIDSet, error) {
	fieldIDs, err := r.resolveSelectorFieldIDs(ctx, normalizeField(term))
	if err != nil {
		return nil, err
	}
	if len(fieldIDs) == 0 {
		return nil, nil
	}
	normalizedTerm := normalizeTermValue(term.Value)
	acc := borrowDocIDAccumulator()
	defer releaseDocIDAccumulator(acc)
	for _, fieldID := range fieldIDs {
		set, err := r.docIDsForTermID(ctx, fieldID, normalizedTerm)
		if err != nil {
			return nil, err
		}
		acc.union(set)
	}
	return acc.result(), nil
}

func (r *segmentReader) docIDsForTermID(ctx context.Context, fieldID uint32, term string) (docIDSet, error) {
	if strings.TrimSpace(term) == "" {
		return nil, nil
	}
	out := borrowDocIDScratch()
	defer releaseDocIDScratch(out)
	if err := r.fillDocIDsForTermID(ctx, fieldID, term, out); err != nil {
		return nil, err
	}
	if len(*out) == 0 {
		return nil, nil
	}
	return append(docIDSet(nil), (*out)...), nil
}

func (r *segmentReader) fillDocIDsForTermID(ctx context.Context, fieldID uint32, term string, out *docIDSet) error {
	if out == nil {
		return nil
	}
	*out = (*out)[:0]
	if strings.TrimSpace(term) == "" {
		return nil
	}
	if single, ok, err := r.singleCompiledSegment(ctx); err != nil {
		return err
	} else if ok {
		block, present := single.fieldsByID[fieldID]
		if !present {
			return nil
		}
		*out = block.docIDsForTermInto(term, (*out)[:0])
		return nil
	}
	decoded := borrowDocIDScratch()
	defer releaseDocIDScratch(decoded)
	segmentsWithDocs := 0
	if err := r.forEachCompiledSegment(ctx, func(seg *compiledSegment) error {
		block, ok := seg.fieldsByID[fieldID]
		if !ok {
			return nil
		}
		docIDs := block.docIDsForTermInto(term, (*decoded)[:0])
		*decoded = docIDs
		if len(docIDs) == 0 {
			return nil
		}
		segmentsWithDocs++
		*out = append(*out, docIDs...)
		return nil
	}); err != nil {
		return err
	}
	if segmentsWithDocs > 1 {
		*out = sortUniqueDocIDs(*out)
	}
	return nil
}

func (r *segmentReader) docIDsForPrefix(ctx context.Context, term *api.Term) (docIDSet, error) {
	fieldIDs, err := r.resolveSelectorFieldIDs(ctx, normalizeField(term))
	if err != nil {
		return nil, err
	}
	if len(fieldIDs) == 0 {
		return nil, nil
	}
	prefix := normalizeTermValue(term.Value)
	out := borrowDocIDScratch()
	defer releaseDocIDScratch(out)
	for _, fieldID := range fieldIDs {
		if err := r.forEachPostingDocIDsByFieldID(ctx, fieldID, func(termValue string, docIDs []uint32) error {
			if strings.HasPrefix(termValue, prefix) {
				*out = append(*out, docIDs...)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return sortUniqueDocIDs(append(docIDSet(nil), (*out)...)), nil
}

func (r *segmentReader) docIDsForContains(ctx context.Context, term *api.Term, useTrigramIndex bool) (docIDSet, error) {
	fields, err := r.resolveSelectorFields(ctx, normalizeField(term))
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, nil
	}
	needle := normalizeTermValue(term.Value)
	if needle == "" {
		return r.docIDsForExists(ctx, normalizeField(term))
	}
	var grams []string
	if useTrigramIndex {
		grams = normalizedTrigrams(needle)
	}
	out := borrowDocIDScratch()
	defer releaseDocIDScratch(out)
	for _, field := range fields {
		fieldID, ok := r.fieldID(field)
		if !ok {
			continue
		}
		set, err := r.docIDsForContainsField(ctx, field, fieldID, needle, useTrigramIndex, grams, nil)
		if err != nil {
			return nil, err
		}
		if len(set) == 0 {
			continue
		}
		*out = append(*out, set...)
	}
	return sortUniqueDocIDs(append(docIDSet(nil), (*out)...)), nil
}

func (r *segmentReader) docIDsForIContains(ctx context.Context, term *api.Term, useTrigramIndex bool) (docIDSet, error) {
	normalizedField := normalizeField(term)
	fields, err := r.resolveSelectorFields(ctx, normalizedField)
	if err != nil {
		return nil, err
	}
	needle := normalizeTermValue(term.Value)
	if needle == "" {
		return r.docIDsForExists(ctx, normalizedField)
	}
	tokens := simpleTextAnalyzer{}.Tokens(needle)
	useAllText := strings.TrimSpace(normalizedField) == "/..."
	var grams []string
	if useTrigramIndex {
		grams = normalizedTrigrams(needle)
	}
	if len(fields) == 0 {
		prefilter, hasTokenIndex, err := r.tokenPrefilterForField(ctx, normalizedField, tokens, useAllText)
		if err != nil {
			return nil, err
		}
		if hasTokenIndex {
			return prefilter, nil
		}
		return nil, nil
	}
	out := borrowDocIDScratch()
	defer releaseDocIDScratch(out)
	for _, field := range fields {
		prefilter, hasTokenIndex, err := r.tokenPrefilterForField(ctx, field, tokens, useAllText)
		if err != nil {
			return nil, err
		}
		if hasTokenIndex && len(prefilter) == 0 {
			continue
		}
		if hasTokenIndex {
			rawExists, err := r.docIDsForExists(ctx, field)
			if err != nil {
				return nil, err
			}
			if len(rawExists) == 0 {
				if len(prefilter) > 0 {
					*out = append(*out, prefilter...)
				}
				continue
			}
		}
		fieldID, ok := r.fieldID(field)
		if !ok {
			continue
		}
		set, err := r.docIDsForContainsField(ctx, field, fieldID, needle, useTrigramIndex, grams, prefilter)
		if err != nil {
			return nil, err
		}
		if len(set) == 0 {
			continue
		}
		*out = append(*out, set...)
	}
	return sortUniqueDocIDs(append(docIDSet(nil), (*out)...)), nil
}

func (r *segmentReader) tokenPrefilterForField(ctx context.Context, field string, tokens []string, useAllText bool) (docIDSet, bool, error) {
	if len(tokens) == 0 {
		return nil, false, nil
	}
	tokenField := tokenizedField(field)
	if useAllText {
		tokenField = tokenAllTextField
	}
	tokenExists, err := r.docIDsForExists(ctx, tokenField)
	if err != nil {
		return nil, false, err
	}
	if len(tokenExists) == 0 {
		return nil, false, nil
	}
	tokenFieldID, ok := r.fieldID(tokenField)
	if !ok {
		return nil, false, nil
	}
	acc := borrowDocIDAccumulator()
	defer releaseDocIDAccumulator(acc)
	termSet := borrowDocIDScratch()
	defer releaseDocIDScratch(termSet)
	for _, token := range tokens {
		if err := r.fillDocIDsForTermID(ctx, tokenFieldID, token, termSet); err != nil {
			return nil, false, err
		}
		acc.intersect(*termSet)
		if acc.initialized && len(acc.current) == 0 {
			break
		}
	}
	if !acc.initialized {
		return nil, true, nil
	}
	return acc.result(), true, nil
}

func (r *segmentReader) docIDsForContainsField(
	ctx context.Context,
	field string,
	fieldID uint32,
	needle string,
	useTrigramIndex bool,
	grams []string,
	prefilter docIDSet,
) (docIDSet, error) {
	candidateFilter := prefilter
	if useTrigramIndex {
		if len(grams) > 0 {
			sawGramPosting := false
			acc := borrowDocIDAccumulator()
			defer releaseDocIDAccumulator(acc)
			gramSet := borrowDocIDScratch()
			defer releaseDocIDScratch(gramSet)
			gramFieldID, ok := r.fieldID(containsGramField(field))
			if !ok {
				gramFieldID = 0
			}
			for _, gram := range grams {
				if gramFieldID == 0 {
					break
				}
				if err := r.fillDocIDsForTermID(ctx, gramFieldID, gram, gramSet); err != nil {
					return nil, err
				}
				if len(*gramSet) > 0 {
					sawGramPosting = true
				}
				acc.intersect(*gramSet)
				if acc.initialized && len(acc.current) == 0 {
					break
				}
			}
			candidate := acc.result()
			if len(candidateFilter) > 0 && len(candidate) > 0 {
				candidate = intersectDocIDs(candidateFilter, candidate)
			} else if len(candidateFilter) > 0 && len(candidate) == 0 {
				candidate = candidateFilter
			}
			if len(candidate) > 0 {
				candidateFilter = candidate
			} else if acc.initialized && sawGramPosting {
				return nil, nil
			}
		}
	}
	out := borrowDocIDScratch()
	defer releaseDocIDScratch(out)
	decoded := borrowDocIDScratch()
	defer releaseDocIDScratch(decoded)
	contributingPostings := 0
	if single, ok, err := r.singleCompiledSegment(ctx); err != nil {
		return nil, err
	} else if ok {
		block, present := single.fieldsByID[fieldID]
		if !present {
			return nil, nil
		}
		for termID, termValue := range block.terms {
			if !strings.Contains(termValue, needle) {
				continue
			}
			posting := block.postingsByID[termID]
			before := len(*out)
			if len(candidateFilter) > 0 {
				*out = posting.appendIntersectInto(*out, candidateFilter)
			} else {
				docIDs := posting.decodeInto((*decoded)[:0])
				*decoded = docIDs
				*out = append(*out, docIDs...)
			}
			if len(*out) > before {
				contributingPostings++
			}
		}
		if len(*out) == 0 {
			return nil, nil
		}
		if contributingPostings <= 1 {
			return append(docIDSet(nil), (*out)...), nil
		}
		return sortUniqueDocIDs(append(docIDSet(nil), (*out)...)), nil
	}
	if err := r.forEachCompiledSegment(ctx, func(seg *compiledSegment) error {
		block, ok := seg.fieldsByID[fieldID]
		if !ok {
			return nil
		}
		for termID, termValue := range block.terms {
			if !strings.Contains(termValue, needle) {
				continue
			}
			posting := block.postingsByID[termID]
			before := len(*out)
			if len(candidateFilter) > 0 {
				*out = posting.appendIntersectInto(*out, candidateFilter)
			} else {
				docIDs := posting.decodeInto((*decoded)[:0])
				*decoded = docIDs
				*out = append(*out, docIDs...)
			}
			if len(*out) > before {
				contributingPostings++
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if len(*out) == 0 {
		return nil, nil
	}
	if contributingPostings <= 1 {
		return append(docIDSet(nil), (*out)...), nil
	}
	return sortUniqueDocIDs(append(docIDSet(nil), (*out)...)), nil
}

func (r *segmentReader) docIDsForRange(ctx context.Context, term *api.RangeTerm) (docIDSet, error) {
	fieldIDs, err := r.resolveSelectorFieldIDs(ctx, normalizeRangeField(term))
	if err != nil {
		return nil, err
	}
	if len(fieldIDs) == 0 {
		return nil, nil
	}
	out := borrowDocIDScratch()
	defer releaseDocIDScratch(out)
	for _, fieldID := range fieldIDs {
		if err := r.forEachPostingDocIDsByFieldID(ctx, fieldID, func(termValue string, docIDs []uint32) error {
			num, err := strconv.ParseFloat(termValue, 64)
			if err != nil || math.IsNaN(num) {
				return nil
			}
			if !rangeMatches(term, num) {
				return nil
			}
			*out = append(*out, docIDs...)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return sortUniqueDocIDs(append(docIDSet(nil), (*out)...)), nil
}

func (r *segmentReader) docIDsForExists(ctx context.Context, field string) (docIDSet, error) {
	fieldIDs, err := r.resolveSelectorFieldIDs(ctx, strings.TrimSpace(field))
	if err != nil {
		return nil, err
	}
	if len(fieldIDs) == 0 {
		return nil, nil
	}
	out := borrowDocIDScratch()
	defer releaseDocIDScratch(out)
	for _, fieldID := range fieldIDs {
		if err := r.forEachPostingDocIDsByFieldID(ctx, fieldID, func(_ string, docIDs []uint32) error {
			*out = append(*out, docIDs...)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return sortUniqueDocIDs(append(docIDSet(nil), (*out)...)), nil
}

func (r *segmentReader) docIDsForIn(ctx context.Context, term *api.InTerm) (docIDSet, error) {
	fieldIDs, err := r.resolveSelectorFieldIDs(ctx, strings.TrimSpace(term.Field))
	if err != nil {
		return nil, err
	}
	if len(fieldIDs) == 0 || len(term.Any) == 0 {
		return nil, nil
	}
	acc := borrowDocIDAccumulator()
	defer releaseDocIDAccumulator(acc)
	for _, fieldID := range fieldIDs {
		for _, candidate := range term.Any {
			eqSet, err := r.docIDsForTermID(ctx, fieldID, normalizeTermValue(candidate))
			if err != nil {
				return nil, err
			}
			acc.union(eqSet)
		}
	}
	return acc.result(), nil
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

func (r *segmentReader) forEachCompiledSegment(ctx context.Context, fn func(*compiledSegment) error) error {
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
			segment, err := r.loadCompiledSegment(ctx, ref.ID)
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

func (r *segmentReader) singleCompiledSegment(ctx context.Context) (*compiledSegment, bool, error) {
	if r == nil {
		return nil, false, nil
	}
	if r.singleCompiledReady {
		return r.singleCompiled, r.singleCompiled != nil, nil
	}
	r.singleCompiledReady = true
	if r.manifest == nil || len(r.manifest.Shards) == 0 {
		return nil, false, nil
	}
	var segmentID string
	count := 0
	for _, shard := range r.manifest.Shards {
		if shard == nil {
			continue
		}
		for _, ref := range shard.Segments {
			count++
			if count > 1 {
				return nil, false, nil
			}
			segmentID = ref.ID
		}
	}
	if count != 1 || segmentID == "" {
		return nil, false, nil
	}
	seg, err := r.loadCompiledSegment(ctx, segmentID)
	if err != nil {
		return nil, false, err
	}
	r.singleCompiled = seg
	return seg, seg != nil, nil
}

func (r *segmentReader) forEachPostingDocIDsByFieldID(ctx context.Context, fieldID uint32, fn func(term string, docIDs []uint32) error) error {
	if single, ok, err := r.singleCompiledSegment(ctx); err != nil {
		return err
	} else if ok {
		block, present := single.fieldsByID[fieldID]
		if !present {
			return nil
		}
		decoded := borrowDocIDScratch()
		defer releaseDocIDScratch(decoded)
		for termID, termValue := range block.terms {
			docIDs := block.postingsByID[termID].decodeInto((*decoded)[:0])
			*decoded = docIDs
			if err := fn(termValue, docIDs); err != nil {
				return err
			}
		}
		return nil
	}
	decoded := borrowDocIDScratch()
	defer releaseDocIDScratch(decoded)
	return r.forEachCompiledSegment(ctx, func(seg *compiledSegment) error {
		block, ok := seg.fieldsByID[fieldID]
		if !ok {
			return nil
		}
		for termID, termValue := range block.terms {
			docIDs := block.postingsByID[termID].decodeInto((*decoded)[:0])
			*decoded = docIDs
			if err := fn(termValue, docIDs); err != nil {
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
	if r.immutable {
		return nil, fmt.Errorf("index reader immutable cache miss for segment %s", id)
	}
	segment, err := r.store.LoadSegment(ctx, r.namespace, id)
	if err != nil {
		return nil, err
	}
	r.cache[id] = segment
	return segment, nil
}

func (r *segmentReader) loadCompiledSegment(ctx context.Context, id string) (*compiledSegment, error) {
	if seg, ok := r.compiled[id]; ok {
		return seg, nil
	}
	if r.immutable {
		return nil, fmt.Errorf("index reader immutable cache miss for compiled segment %s", id)
	}
	raw, err := r.loadSegment(ctx, id)
	if err != nil {
		return nil, err
	}
	compiled := r.compileSegment(raw)
	r.compiled[id] = compiled
	return compiled, nil
}

func (r *segmentReader) compileSegment(seg *Segment) *compiledSegment {
	if seg == nil {
		return &compiledSegment{fieldsByID: make(map[uint32]compiledField)}
	}
	out := &compiledSegment{
		fieldsByID: make(map[uint32]compiledField, len(seg.Fields)),
		fieldIDs:   make([]uint32, 0, len(seg.Fields)),
	}
	for field, block := range seg.Fields {
		fieldID := r.internField(field)
		compiledBlock := compiledField{
			terms:        make([]string, 0, len(block.Postings)),
			postingsByID: make([]adaptivePosting, 0, len(block.Postings)),
		}
		terms := make([]string, 0, len(block.Postings))
		for term := range block.Postings {
			terms = append(terms, term)
		}
		sort.Strings(terms)
		if len(terms) > 8 {
			compiledBlock.termIDs = make(map[string]uint32, len(terms))
		}
		for _, term := range terms {
			keys := block.Postings[term]
			docIDs := make([]uint32, 0, len(keys))
			for _, key := range keys {
				docIDs = append(docIDs, r.internKey(key))
			}
			termID := uint32(len(compiledBlock.terms))
			if compiledBlock.termIDs != nil {
				compiledBlock.termIDs[term] = termID
			}
			compiledBlock.terms = append(compiledBlock.terms, term)
			compiledBlock.postingsByID = append(compiledBlock.postingsByID, newAdaptivePosting(docIDs))
		}
		out.fieldIDs = append(out.fieldIDs, fieldID)
		out.fieldsByID[fieldID] = compiledBlock
	}
	sort.Slice(out.fieldIDs, func(i, j int) bool { return out.fieldIDs[i] < out.fieldIDs[j] })
	return out
}

func (r *segmentReader) internKey(key string) uint32 {
	if id, ok := r.keyIDs[key]; ok {
		return id
	}
	id := uint32(len(r.keys))
	r.keys = append(r.keys, key)
	r.keyIDs[key] = id
	return id
}

func (r *segmentReader) internField(field string) uint32 {
	if id, ok := r.fieldIDs[field]; ok {
		return id
	}
	id := uint32(len(r.fields))
	r.fields = append(r.fields, field)
	r.fieldIDs[field] = id
	return id
}

func (r *segmentReader) keyByDocID(id uint32) string {
	if int(id) >= len(r.keys) {
		return ""
	}
	return r.keys[id]
}

func (r *segmentReader) prime(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if err := r.forEachCompiledSegment(ctx, func(*compiledSegment) error {
		return nil
	}); err != nil {
		return err
	}
	if _, err := r.allIndexFields(ctx); err != nil {
		return err
	}
	if _, err := r.docMetaMap(ctx); err != nil {
		return err
	}
	r.ensureKeyOrderByName()
	return nil
}

func (r *segmentReader) cloneForQuery(manifest *Manifest) *segmentReader {
	if r == nil {
		return nil
	}
	if manifest == nil {
		manifest = r.manifest
	}
	clone := querySegmentReaderPool.Get().(*segmentReader)
	*clone = segmentReader{
		namespace:             r.namespace,
		manifest:              manifest,
		store:                 r.store,
		logger:                r.logger,
		cache:                 r.cache,
		compiled:              r.compiled,
		singleCompiled:        r.singleCompiled,
		singleCompiledReady:   r.singleCompiledReady,
		keyIDs:                r.keyIDs,
		keys:                  r.keys,
		fieldIDs:              r.fieldIDs,
		fields:                r.fields,
		fieldList:             r.fieldList,
		fieldListReady:        r.fieldListReady,
		fieldSegments:         r.fieldSegments,
		fieldResolutionCached: nil,
		sharedFieldResolution: r.sharedFieldResolution,
		fieldTrie:             r.fieldTrie,
		docMeta:               r.docMeta,
		docMetaReady:          r.docMetaReady,
		keyOrderByName:        r.keyOrderByName,
		keyOrderByNameReady:   r.keyOrderByNameReady,
		immutable:             true,
	}
	return clone
}

func (r *segmentReader) fieldID(field string) (uint32, bool) {
	if r == nil {
		return 0, false
	}
	if id, ok := r.fieldIDs[field]; ok {
		return id, true
	}
	if r.immutable {
		return 0, false
	}
	return r.internField(field), true
}

type compiledSegment struct {
	fieldIDs   []uint32
	fieldsByID map[uint32]compiledField
}

type compiledField struct {
	termIDs      map[string]uint32
	terms        []string
	postingsByID []adaptivePosting
}

func (f compiledField) docIDsForTermInto(term string, dst docIDSet) docIDSet {
	if len(f.terms) <= 8 || len(f.termIDs) == 0 {
		for termID, candidate := range f.terms {
			if candidate != term {
				continue
			}
			if termID >= len(f.postingsByID) {
				return dst[:0]
			}
			return f.postingsByID[termID].decodeInto(dst)
		}
		return dst[:0]
	}
	termID, ok := f.termIDs[term]
	if !ok {
		return dst[:0]
	}
	if int(termID) >= len(f.postingsByID) {
		return dst[:0]
	}
	return f.postingsByID[termID].decodeInto(dst)
}

func (r *segmentReader) docMetaMap(ctx context.Context) (map[string]DocumentMetadata, error) {
	if r == nil {
		return nil, nil
	}
	if r.docMetaReady {
		return r.docMeta, nil
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
	r.docMeta = out
	r.docMetaReady = true
	return out, nil
}

type selectorEvaluator struct {
	reader        *segmentReader
	containsNgram bool
}

type selectorClauseResolver struct {
	family  string
	resolve func(ctx context.Context, e selectorEvaluator, sel api.Selector) (docIDSet, bool, error)
}

var selectorClauseResolvers = []selectorClauseResolver{
	{
		family: "eq",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (docIDSet, bool, error) {
			if sel.Eq == nil {
				return nil, false, nil
			}
			set, err := e.reader.docIDsForEq(ctx, sel.Eq)
			return set, true, err
		},
	},
	{
		family: "prefix",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (docIDSet, bool, error) {
			if sel.Prefix == nil {
				return nil, false, nil
			}
			set, err := e.reader.docIDsForPrefix(ctx, sel.Prefix)
			return set, true, err
		},
	},
	{
		family: "iprefix",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (docIDSet, bool, error) {
			if sel.IPrefix == nil {
				return nil, false, nil
			}
			set, err := e.reader.docIDsForPrefix(ctx, sel.IPrefix)
			return set, true, err
		},
	},
	{
		family: "contains",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (docIDSet, bool, error) {
			if sel.Contains == nil {
				return nil, false, nil
			}
			set, err := e.reader.docIDsForContains(ctx, sel.Contains, e.containsNgram)
			return set, true, err
		},
	},
	{
		family: "icontains",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (docIDSet, bool, error) {
			if sel.IContains == nil {
				return nil, false, nil
			}
			set, err := e.reader.docIDsForIContains(ctx, sel.IContains, e.containsNgram)
			return set, true, err
		},
	},
	{
		family: "range",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (docIDSet, bool, error) {
			if sel.Range == nil {
				return nil, false, nil
			}
			set, err := e.reader.docIDsForRange(ctx, sel.Range)
			return set, true, err
		},
	},
	{
		family: "in",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (docIDSet, bool, error) {
			if sel.In == nil {
				return nil, false, nil
			}
			set, err := e.reader.docIDsForIn(ctx, sel.In)
			return set, true, err
		},
	},
	{
		family: "exists",
		resolve: func(ctx context.Context, e selectorEvaluator, sel api.Selector) (docIDSet, bool, error) {
			if sel.Exists == "" {
				return nil, false, nil
			}
			set, err := e.reader.docIDsForExists(ctx, sel.Exists)
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

func (e selectorEvaluator) evaluate(ctx context.Context, sel api.Selector) (docIDSet, error) {
	baseSet, initialized, err := e.evaluateBase(ctx, sel)
	if err != nil {
		return nil, err
	}
	if len(sel.Or) == 0 {
		if !initialized {
			return e.reader.allDocIDs(ctx)
		}
		return baseSet, nil
	}
	acc := borrowDocIDAccumulator()
	defer releaseDocIDAccumulator(acc)
	for _, branch := range sel.Or {
		branchSet, err := e.evaluate(ctx, branch)
		if err != nil {
			return nil, err
		}
		acc.union(branchSet)
	}
	unionSet := acc.result()
	if !initialized {
		return unionSet, nil
	}
	return intersectDocIDs(baseSet, unionSet), nil
}

func (e selectorEvaluator) evaluateBase(ctx context.Context, sel api.Selector) (docIDSet, bool, error) {
	acc := borrowDocIDAccumulator()
	defer releaseDocIDAccumulator(acc)
	initialized := false
	intersectWith := func(set docIDSet) {
		if !initialized {
			acc.set(set)
			initialized = true
			return
		}
		acc.intersect(set)
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
				all, err := e.reader.allDocIDs(ctx)
				if err != nil {
					return nil, false, err
				}
				acc.set(all)
				acc.subtract(notSet)
				initialized = true
			} else {
				acc.subtract(notSet)
			}
		}
	}
	return acc.result(), initialized, nil
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

type docIDSet []uint32

type docIDAccumulator struct {
	current     docIDSet
	scratch     docIDSet
	initialized bool
}

func borrowDocIDScratch() *docIDSet {
	buf := docIDScratchPool.Get().(*docIDSet)
	*buf = (*buf)[:0]
	return buf
}

func releaseDocIDScratch(buf *docIDSet) {
	if buf == nil {
		return
	}
	if cap(*buf) > 1<<20 {
		*buf = make(docIDSet, 0, 256)
	} else {
		*buf = (*buf)[:0]
	}
	docIDScratchPool.Put(buf)
}

func borrowKeyPresence(size int) *[]bool {
	buf := keyPresencePool.Get().(*[]bool)
	if cap(*buf) < size {
		*buf = make([]bool, size)
	} else {
		*buf = (*buf)[:size]
	}
	return buf
}

func releaseKeyPresence(buf *[]bool) {
	if buf == nil {
		return
	}
	if cap(*buf) > 1<<20 {
		*buf = make([]bool, 0, 256)
	} else {
		*buf = (*buf)[:0]
	}
	keyPresencePool.Put(buf)
}

func borrowDocIDAccumulator() *docIDAccumulator {
	acc := docIDAccumulatorPool.Get().(*docIDAccumulator)
	acc.current = acc.current[:0]
	acc.scratch = acc.scratch[:0]
	acc.initialized = false
	return acc
}

func releaseDocIDAccumulator(acc *docIDAccumulator) {
	if acc == nil {
		return
	}
	const maxScratchCap = 1 << 20
	if cap(acc.current) > maxScratchCap {
		acc.current = make(docIDSet, 0, 256)
	} else {
		acc.current = acc.current[:0]
	}
	if cap(acc.scratch) > maxScratchCap {
		acc.scratch = make(docIDSet, 0, 256)
	} else {
		acc.scratch = acc.scratch[:0]
	}
	acc.initialized = false
	docIDAccumulatorPool.Put(acc)
}

func (a *docIDAccumulator) set(in docIDSet) {
	a.current = append(a.current[:0], in...)
	a.initialized = true
}

func (a *docIDAccumulator) union(in docIDSet) {
	if len(in) == 0 {
		return
	}
	if !a.initialized {
		a.set(in)
		return
	}
	a.scratch = unionDocIDsInto(a.scratch[:0], a.current, in)
	a.current, a.scratch = a.scratch, a.current[:0]
}

func (a *docIDAccumulator) intersect(in docIDSet) {
	if !a.initialized {
		a.set(in)
		return
	}
	a.scratch = intersectDocIDsInto(a.scratch[:0], a.current, in)
	a.current, a.scratch = a.scratch, a.current[:0]
}

func (a *docIDAccumulator) subtract(in docIDSet) {
	if !a.initialized || len(in) == 0 {
		return
	}
	a.scratch = subtractDocIDsInto(a.scratch[:0], a.current, in)
	a.current, a.scratch = a.scratch, a.current[:0]
}

func (a *docIDAccumulator) result() docIDSet {
	if !a.initialized || len(a.current) == 0 {
		return nil
	}
	return cloneDocIDs(a.current)
}

func cloneDocIDs(in docIDSet) docIDSet {
	if len(in) == 0 {
		return nil
	}
	out := make(docIDSet, len(in))
	copy(out, in)
	return out
}

func sortUniqueDocIDs(in docIDSet) docIDSet {
	if len(in) == 0 {
		return nil
	}
	slices.Sort(in)
	w := 1
	for i := 1; i < len(in); i++ {
		if in[i] == in[w-1] {
			continue
		}
		in[w] = in[i]
		w++
	}
	return in[:w]
}

func unionDocIDsInto(dst, a, b docIDSet) docIDSet {
	dst = dst[:0]
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		av := a[i]
		bv := b[j]
		if av == bv {
			dst = append(dst, av)
			i++
			j++
			continue
		}
		if av < bv {
			dst = append(dst, av)
			i++
			continue
		}
		dst = append(dst, bv)
		j++
	}
	if i < len(a) {
		dst = append(dst, a[i:]...)
	}
	if j < len(b) {
		dst = append(dst, b[j:]...)
	}
	return dst
}

func intersectDocIDs(a, b docIDSet) docIDSet {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	out := make(docIDSet, 0, min(len(a), len(b)))
	return intersectDocIDsInto(out, a, b)
}

func intersectDocIDsInto(dst, a, b docIDSet) docIDSet {
	dst = dst[:0]
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		av := a[i]
		bv := b[j]
		if av == bv {
			dst = append(dst, av)
			i++
			j++
			continue
		}
		if av < bv {
			i++
			continue
		}
		j++
	}
	return dst
}

func subtractDocIDsInto(dst, a, b docIDSet) docIDSet {
	dst = dst[:0]
	i, j := 0, 0
	for i < len(a) {
		if j >= len(b) {
			dst = append(dst, a[i:]...)
			break
		}
		av := a[i]
		bv := b[j]
		if av == bv {
			i++
			j++
			continue
		}
		if av < bv {
			dst = append(dst, av)
			i++
			continue
		}
		j++
	}
	return dst
}

func (r *segmentReader) sortedKeysForDocIDs(ids docIDSet) []string {
	if len(ids) == 0 {
		return nil
	}
	r.ensureKeyOrderByName()
	presentBuf := borrowKeyPresence(len(r.keys))
	defer releaseKeyPresence(presentBuf)
	present := *presentBuf
	touched := borrowDocIDScratch()
	defer releaseDocIDScratch(touched)
	touchedIDs := (*touched)[:0]
	var prev uint32
	first := true
	for _, id := range ids {
		if !first && id == prev {
			continue
		}
		first = false
		prev = id
		if int(id) >= len(r.keys) {
			continue
		}
		if present[id] {
			continue
		}
		present[id] = true
		touchedIDs = append(touchedIDs, id)
	}
	keys := make([]string, 0, len(ids))
	for _, id := range r.keyOrderByName {
		if int(id) >= len(present) || !present[id] {
			continue
		}
		keys = append(keys, r.keys[id])
	}
	for _, id := range touchedIDs {
		present[id] = false
	}
	*touched = touchedIDs[:0]
	*presentBuf = present
	return keys
}

func (r *segmentReader) ensureKeyOrderByName() {
	if r == nil || r.keyOrderByNameReady {
		return
	}
	n := len(r.keys)
	if n == 0 {
		r.keyOrderByName = nil
		r.keyOrderByNameReady = true
		return
	}
	ids := make([]uint32, n)
	for i := 0; i < n; i++ {
		ids[i] = uint32(i)
	}
	sort.Slice(ids, func(i, j int) bool {
		return r.keys[ids[i]] < r.keys[ids[j]]
	})
	r.keyOrderByName = ids
	r.keyOrderByNameReady = true
}

func selectorSupportsLegacyIndexFilter(sel api.Selector) bool {
	if sel.IsEmpty() {
		return true
	}
	caps := lql.InspectSelectorCapabilities(sel)
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
	out := sel
	if sel.Eq != nil {
		eq := *sel.Eq
		out.Eq = &eq
	}
	if sel.Contains != nil {
		contains := *sel.Contains
		out.Contains = &contains
	}
	if sel.IContains != nil {
		icontains := *sel.IContains
		out.IContains = &icontains
	}
	if sel.Prefix != nil {
		prefix := *sel.Prefix
		out.Prefix = &prefix
	}
	if sel.IPrefix != nil {
		iprefix := *sel.IPrefix
		out.IPrefix = &iprefix
	}
	if sel.Range != nil {
		rng := *sel.Range
		out.Range = &rng
	}
	if sel.In != nil {
		in := *sel.In
		if len(sel.In.Any) > 0 {
			in.Any = append([]string(nil), sel.In.Any...)
		}
		out.In = &in
	}
	if sel.Not != nil {
		notSel := cloneSelector(*sel.Not)
		out.Not = &notSel
	}
	if len(sel.And) > 0 {
		out.And = make([]api.Selector, len(sel.And))
		for i := range sel.And {
			out.And[i] = cloneSelector(sel.And[i])
		}
	}
	if len(sel.Or) > 0 {
		out.Or = make([]api.Selector, len(sel.Or))
		for i := range sel.Or {
			out.Or[i] = cloneSelector(sel.Or[i])
		}
	}
	return out
}

func shouldCacheSelectorPlan(sel api.Selector) bool {
	return !sel.IsEmpty()
}

func selectorNeedsNormalization(sel api.Selector) (bool, error) {
	needs := false
	checkField := func(field string) error {
		changed, err := pointerNeedsNormalization(field)
		if err != nil {
			return err
		}
		if changed {
			needs = true
		}
		return nil
	}
	if sel.Eq != nil {
		if err := checkField(sel.Eq.Field); err != nil {
			return false, fmt.Errorf("eq.field: %w", err)
		}
	}
	if sel.Contains != nil {
		if err := checkField(sel.Contains.Field); err != nil {
			return false, fmt.Errorf("contains.field: %w", err)
		}
	}
	if sel.IContains != nil {
		if err := checkField(sel.IContains.Field); err != nil {
			return false, fmt.Errorf("icontains.field: %w", err)
		}
	}
	if sel.Prefix != nil {
		if err := checkField(sel.Prefix.Field); err != nil {
			return false, fmt.Errorf("prefix.field: %w", err)
		}
	}
	if sel.IPrefix != nil {
		if err := checkField(sel.IPrefix.Field); err != nil {
			return false, fmt.Errorf("iprefix.field: %w", err)
		}
	}
	if sel.Range != nil {
		if err := checkField(sel.Range.Field); err != nil {
			return false, fmt.Errorf("range.field: %w", err)
		}
	}
	if sel.In != nil {
		if err := checkField(sel.In.Field); err != nil {
			return false, fmt.Errorf("in.field: %w", err)
		}
	}
	if sel.Exists != "" {
		if err := checkField(sel.Exists); err != nil {
			return false, fmt.Errorf("exists.field: %w", err)
		}
	}
	for i := range sel.And {
		childNeeds, err := selectorNeedsNormalization(sel.And[i])
		if err != nil {
			return false, err
		}
		if childNeeds {
			needs = true
		}
	}
	for i := range sel.Or {
		childNeeds, err := selectorNeedsNormalization(sel.Or[i])
		if err != nil {
			return false, err
		}
		if childNeeds {
			needs = true
		}
	}
	if sel.Not != nil {
		childNeeds, err := selectorNeedsNormalization(*sel.Not)
		if err != nil {
			return false, err
		}
		if childNeeds {
			needs = true
		}
	}
	return needs, nil
}

func pointerNeedsNormalization(field string) (bool, error) {
	trimmed := strings.TrimSpace(field)
	if trimmed != field {
		normalized, err := normalizePointerLenient(field)
		if err != nil {
			return false, err
		}
		return normalized != field, nil
	}
	switch trimmed {
	case "", "/":
		return trimmed == "/", nil
	}
	if strings.HasPrefix(trimmed, "/") && !strings.Contains(trimmed, "~") {
		return false, nil
	}
	normalized, err := normalizePointerLenient(field)
	if err != nil {
		return false, err
	}
	return normalized != field, nil
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
