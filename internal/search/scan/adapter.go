package scan

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
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
	}, nil
}

// Capabilities reports that the scan adapter always supports the scan engine
// (and never provides an index engine).
func (a *Adapter) Capabilities(context.Context, string) (search.Capabilities, error) {
	return search.Capabilities{Scan: true}, nil
}

// Query enumerates namespace keys, applying selector filters in-memory.
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
	matchAll := zeroSelector(req.Selector)
	eval := newEvaluator(req.Selector)
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
		doc, err := a.loadDocument(ctx, req.Namespace, key, meta)
		if err != nil {
			if shouldSkipReadError(err) {
				a.logDebug("search.scan.load_state", "namespace", req.Namespace, "key", key, "error", err)
				continue
			}
			return search.Result{}, fmt.Errorf("scan: load state %s: %w", key, err)
		}
		if eval.matches(doc) {
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

func (a *Adapter) loadDocument(ctx context.Context, namespace, key string, meta *storage.Meta) (map[string]any, error) {
	stateCtx := ctx
	if len(meta.StateDescriptor) > 0 {
		stateCtx = storage.ContextWithStateDescriptor(stateCtx, meta.StateDescriptor)
	}
	if meta.StatePlaintextBytes > 0 {
		stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
	}
	stateRes, err := a.backend.ReadState(stateCtx, namespace, key)
	if err != nil {
		return nil, err
	}
	defer stateRes.Reader.Close()
	data, err := readAllLimited(stateRes.Reader, a.maxDocumentBytes)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	return doc, nil
}

func readAllLimited(r io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		return io.ReadAll(r)
	}
	lr := &io.LimitedReader{R: r, N: limit + 1}
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("payload exceeds %d bytes", limit)
	}
	return data, nil
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
