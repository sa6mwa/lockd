package scan

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lql"
	"pkt.systems/pslog"
)

const cursorPrefix = "scanv1:"

var reservedQueryPrefixes = []string{"lockd-diagnostics/"}
var errScanLimitReached = errors.New("scan: limit reached")

const queryStreamPayloadSpoolMemoryBytes = 256 * 1024
const queryStreamReaderBufferBytes = 64 * 1024

var queryStreamReaderPool sync.Pool

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
	startAfter, err := decodeCursor(req.Cursor)
	if err != nil {
		return search.Result{}, fmt.Errorf("%w: %v", search.ErrInvalidCursor, err)
	}
	matchAll := req.Selector.IsEmpty()
	var plan lql.QueryStreamPlan
	if !matchAll {
		compiled, compileErr := a.compiledPlan(req.Selector)
		if compileErr != nil {
			return search.Result{}, fmt.Errorf("scan: compile selector: %w", compileErr)
		}
		plan = compiled
	}
	limit := req.Limit
	if limit < 0 {
		limit = 0
	}
	matches := make([]string, 0, limit)
	more := false
	nextStartAfter := startAfter
	scanReq := storage.ScanMetaSummariesRequest{
		Namespace: req.Namespace,
		Limit:     scanSummaryPageSize(limit),
	}
	for {
		scanReq.StartAfter = nextStartAfter
		page, err := storage.ScanMetaSummaries(ctx, a.backend, scanReq, func(row storage.ScanMetaSummaryRow) error {
			key := row.Key
			if hasReservedPrefix(key) {
				return nil
			}
			meta := row.Meta
			if meta == nil || meta.QueryExcluded || meta.PublishedVersion == 0 {
				return nil
			}
			if !matchAll && meta.StateETag == "" {
				return nil
			}
			if !matchAll {
				matched, matchErr := a.matchesSelector(ctx, req.Namespace, key, meta, plan)
				if matchErr != nil {
					if shouldSkipReadError(matchErr) {
						a.logDebug("search.scan.load_state", "namespace", req.Namespace, "key", key, "error", matchErr)
						return nil
					}
					return fmt.Errorf("scan: load state %s: %w", key, matchErr)
				}
				if !matched {
					return nil
				}
			}
			matches = append(matches, key)
			if limit > 0 && len(matches) > limit {
				return errScanLimitReached
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, errScanLimitReached) {
				more = true
				break
			}
			return search.Result{}, fmt.Errorf("scan: %w", err)
		}
		if !page.Truncated || page.NextStartAfter == "" {
			break
		}
		nextStartAfter = page.NextStartAfter
	}
	if limit > 0 && len(matches) > limit {
		matches = matches[:limit]
	}
	result := search.Result{Keys: matches}
	if limit > 0 && len(matches) == limit && more {
		result.Cursor = encodeCursor(matches[len(matches)-1])
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
	startAfter, err := decodeCursor(req.Cursor)
	if err != nil {
		return search.Result{}, fmt.Errorf("%w: %v", search.ErrInvalidCursor, err)
	}
	matchAll := req.Selector.IsEmpty()
	var plan lql.QueryStreamPlan
	if !matchAll {
		compiled, compileErr := a.compiledPlan(req.Selector)
		if compileErr != nil {
			return search.Result{}, fmt.Errorf("scan: compile selector: %w", compileErr)
		}
		plan = compiled
	}
	limit := req.Limit
	if limit < 0 {
		limit = 0
	}
	matches := make([]string, 0, limit)
	var streamStats search.StreamStats
	var lastMatch string
	more := false
	nextStartAfter := startAfter
	scanReq := storage.ScanMetaSummariesRequest{
		Namespace: req.Namespace,
		Limit:     scanSummaryPageSize(limit),
	}
	for {
		scanReq.StartAfter = nextStartAfter
		page, err := storage.ScanMetaSummaries(ctx, a.backend, scanReq, func(row storage.ScanMetaSummaryRow) error {
			key := row.Key
			if hasReservedPrefix(key) {
				return nil
			}
			meta := row.Meta
			if meta == nil || meta.QueryExcluded || meta.PublishedVersion == 0 {
				return nil
			}
			if !matchAll && meta.StateETag == "" {
				return nil
			}
			if limit > 0 && len(matches) >= limit {
				if matchAll {
					more = true
					return errScanLimitReached
				}
				matched, matchErr := a.matchesSelector(ctx, req.Namespace, key, meta, plan)
				if matchErr != nil {
					if shouldSkipReadError(matchErr) {
						a.logDebug("search.scan.load_state", "namespace", req.Namespace, "key", key, "error", matchErr)
						return nil
					}
					return fmt.Errorf("scan: load state %s: %w", key, matchErr)
				}
				if matched {
					more = true
					return errScanLimitReached
				}
				return nil
			}
			version := meta.EffectiveVersion()
			stateCtx := storage.ContextWithStateReadHintsBorrowed(ctx, meta.StateDescriptor, meta.StatePlaintextBytes)
			stateRes, readErr := a.backend.ReadState(stateCtx, req.Namespace, key)
			if readErr != nil {
				if shouldSkipReadError(readErr) {
					a.logDebug("search.scan.load_state", "namespace", req.Namespace, "key", key, "error", readErr)
					return nil
				}
				return fmt.Errorf("scan: load state %s: %w", key, readErr)
			}
			if matchAll {
				docReader := stateRes.Reader
				if err := sink.OnDocument(ctx, req.Namespace, key, version, docReader); err != nil {
					docReader.Close()
					return err
				}
				docReader.Close()
				matches = append(matches, key)
				streamStats.AddCandidate(true)
				lastMatch = key
				return nil
			}
			matched, queryResult, err := a.matchAndStreamDocument(ctx, req.Namespace, key, version, stateRes.Reader, plan, sink)
			if err != nil {
				return err
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
			return nil
		})
		if err != nil {
			if errors.Is(err, errScanLimitReached) {
				more = true
				break
			}
			return search.Result{}, fmt.Errorf("scan: %w", err)
		}
		if !page.Truncated || page.NextStartAfter == "" {
			break
		}
		nextStartAfter = page.NextStartAfter
	}
	result := search.Result{Keys: matches}
	result.Metadata = streamStats.ApplyMetadata(result.Metadata)
	if limit > 0 && len(matches) == limit && more && lastMatch != "" {
		result.Cursor = encodeCursor(lastMatch)
	}
	return result, nil
}

func (a *Adapter) matchesSelector(ctx context.Context, namespace, key string, meta *storage.MetaSummary, plan lql.QueryStreamPlan) (bool, error) {
	stateCtx := storage.ContextWithStateReadHintsBorrowed(ctx, meta.StateDescriptor, meta.StatePlaintextBytes)
	stateRes, err := a.backend.ReadState(stateCtx, namespace, key)
	if err != nil {
		return false, err
	}
	defer stateRes.Reader.Close()
	queryReader, pooled := borrowQueryStreamReader(stateRes.Reader)
	if pooled {
		defer releaseQueryStreamReader(queryReader)
	}

	matched := false
	_, err = lql.QueryStreamWithResult(lql.QueryStreamRequest{
		Ctx:               stateCtx,
		Reader:            queryReader,
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
	queryReader, pooled := borrowQueryStreamReader(reader)
	if pooled {
		defer releaseQueryStreamReader(queryReader)
	}

	matched := false
	result, err := lql.QueryStreamWithResult(lql.QueryStreamRequest{
		Ctx:               ctx,
		Reader:            queryReader,
		Plan:              plan,
		Mode:              lql.QueryDecisionPlusValue,
		MatchedOnly:       true,
		SpoolMemoryBytes:  queryStreamPayloadSpoolMemoryBytes,
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

func borrowQueryStreamReader(src io.Reader) (*bufio.Reader, bool) {
	if src == nil {
		return nil, false
	}
	if reader, ok := src.(*bufio.Reader); ok && reader.Size() >= queryStreamReaderBufferBytes {
		return reader, false
	}
	if pooled := queryStreamReaderPool.Get(); pooled != nil {
		if reader, ok := pooled.(*bufio.Reader); ok && reader.Size() >= queryStreamReaderBufferBytes {
			reader.Reset(src)
			return reader, true
		}
	}
	return bufio.NewReaderSize(src, queryStreamReaderBufferBytes), true
}

func releaseQueryStreamReader(reader *bufio.Reader) {
	if reader == nil {
		return
	}
	reader.Reset(scanEOFReader{})
	queryStreamReaderPool.Put(reader)
}

type scanEOFReader struct{}

func (scanEOFReader) Read([]byte) (int, error) {
	return 0, io.EOF
}

func scanSummaryPageSize(resultLimit int) int {
	if resultLimit <= 0 {
		return 2048
	}
	page := resultLimit * 2
	if page < 512 {
		page = 512
	}
	if page > 4096 {
		page = 4096
	}
	return page
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
