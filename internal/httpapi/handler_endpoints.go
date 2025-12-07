package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/correlation"
	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/lql"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

// When set (used by benchmark harness) emit minimal error details to stderr even
// if the logger is a noop, so failures are observable in no-log runs.
var benchLogErrors = os.Getenv("MEM_LQ_BENCH_LOG_ERRORS") == "1"

// handleAcquire godoc
// @Summary      Acquire an exclusive lease
// @Description  Acquire or wait for an exclusive lease on a key. When block_seconds > 0 the request will long-poll until a lease becomes available or the timeout elapses.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        key        query    string  false  "Key to acquire when the request body omits it"
// @Param        request  body      api.AcquireRequest  true  "Lease acquisition parameters"
// @Success      200      {object}  api.AcquireResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/acquire [post]
func (h *Handler) handleAcquire(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var payload api.AcquireRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		return httpError{
			Status: http.StatusBadRequest,
			Code:   "invalid_body",
			Detail: fmt.Sprintf("failed to parse request: %v", err),
		}
	}
	// Allow ?key override
	if payload.Key == "" {
		payload.Key = r.URL.Query().Get("key")
	}
	if id := clientIdentityFromContext(ctx); id != "" {
		payload.Owner = fmt.Sprintf("%s/%s", payload.Owner, id)
	}
	cmd := core.AcquireCommand{
		Namespace:    payload.Namespace,
		Key:          payload.Key,
		Owner:        payload.Owner,
		TTLSeconds:   payload.TTLSeconds,
		BlockSeconds: payload.BlockSecs,
		Idempotency:  strings.TrimSpace(r.Header.Get("X-Idempotency-Key")),
		ClientHint:   h.clientKeyFromRequest(r),
	}
	res, err := h.core.Acquire(ctx, cmd)
	if err != nil {
		return convertCoreError(err)
	}
	w.Header().Set(headerFencingToken, strconv.FormatInt(res.FencingToken, 10))
	headers := map[string]string{
		"X-Key-Version":     strconv.FormatInt(res.Version, 10),
		headerCorrelationID: res.CorrelationID,
	}
	if res.Meta != nil {
		if namespacedKey, err := h.namespacedKey(res.Namespace, res.Key); err == nil {
			h.cacheLease(res.LeaseID, namespacedKey, *res.Meta, res.MetaETag)
		}
	}
	h.writeJSON(w, http.StatusOK, api.AcquireResponse{
		Namespace:     res.Namespace,
		LeaseID:       res.LeaseID,
		Key:           res.Key,
		Owner:         res.Owner,
		ExpiresAt:     res.ExpiresAt,
		Version:       res.Version,
		StateETag:     res.StateETag,
		FencingToken:  res.FencingToken,
		CorrelationID: res.CorrelationID,
	}, headers)
	return nil
}

// handleKeepAlive godoc
// @Summary      Extend an active lease TTL
// @Description  Refresh an existing lease before it expires. Returns the new expiration timestamp.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        key        query    string  false  "Key override when the request body omits it"
// @Param        request  body      api.KeepAliveRequest  true  "Lease keepalive parameters"
// @Success      200      {object}  api.KeepAliveResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/keepalive [post]
func (h *Handler) handleKeepAlive(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	token, err := parseFencingToken(r)
	if err != nil {
		return err
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()

	var payload api.KeepAliveRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
	}
	if payload.Namespace == "" && r.URL.Query().Get("namespace") != "" {
		payload.Namespace = r.URL.Query().Get("namespace")
	}
	namespace, err := h.resolveNamespace(payload.Namespace)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	payload.Namespace = namespace
	h.observeNamespace(namespace)
	if payload.Key == "" && r.URL.Query().Get("key") != "" {
		payload.Key = r.URL.Query().Get("key")
	}
	if payload.Key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key is required"}
	}
	storageKey, err := h.namespacedKey(namespace, payload.Key)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_key", Detail: err.Error()}
	}
	keyComponent := relativeKey(namespace, storageKey)
	payload.Key = keyComponent
	if payload.LeaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_lease", Detail: "lease_id required"}
	}
	ttl := h.resolveTTL(payload.TTLSeconds)
	if ttl <= 0 {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_ttl", Detail: "ttl_seconds must be positive"}
	}
	key := payload.Key
	if queue.IsQueueStateKey(keyComponent) {
		return httpError{Status: http.StatusForbidden, Code: "queue_state_keepalive_unsupported", Detail: "queue state leases must be extended via queue extend"}
	}
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	verbose := logger
	verbose.Debug("keepalive.begin",
		"namespace", namespace,
		"key", key,
		"lease_id", payload.LeaseID,
		"ttl_seconds", ttl.Seconds(),
	)
	// Prefer cached meta to avoid extra LoadMeta round-trips.
	var knownMeta *storage.Meta
	knownETag := ""
	if cachedMeta, cachedETag, cachedKey, ok := h.leaseSnapshot(payload.LeaseID); ok && cachedKey == storageKey {
		knownMeta = &cachedMeta
		knownETag = cachedETag
	}
	res, err := h.core.KeepAlive(ctx, core.KeepAliveCommand{
		Namespace:     namespace,
		Key:           key,
		LeaseID:       payload.LeaseID,
		TTLSeconds:    payload.TTLSeconds,
		FencingToken:  token,
		KnownMeta:     knownMeta,
		KnownMetaETag: knownETag,
	})
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			h.dropLease(payload.LeaseID)
		}
		return convertCoreError(err)
	}
	if res.Meta != nil {
		h.cacheLease(payload.LeaseID, storageKey, *res.Meta, res.MetaETag)
	}
	w.Header().Set(headerFencingToken, strconv.FormatInt(res.FencingToken, 10))
	h.writeJSON(w, http.StatusOK, api.KeepAliveResponse{ExpiresAt: res.ExpiresAt}, nil)
	verbose.Debug("keepalive.success",
		"namespace", namespace,
		"key", key,
		"lease_id", payload.LeaseID,
		"expires_at", res.ExpiresAt,
		"fencing", res.FencingToken,
	)
	return nil
}

// handleRelease godoc
// @Summary      Release a held lease
// @Description  Releases the lease associated with the provided key and lease identifier.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        key        query    string  false  "Key override when the request body omits it"
// @Param        request  body      api.ReleaseRequest  true  "Lease release parameters"
// @Success      200      {object}  api.ReleaseResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/release [post]
func (h *Handler) handleRelease(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	fencingToken, err := parseFencingToken(r)
	if err != nil {
		return err
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var payload api.ReleaseRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
	}
	if payload.Namespace == "" && r.URL.Query().Get("namespace") != "" {
		payload.Namespace = r.URL.Query().Get("namespace")
	}
	namespace, err := h.resolveNamespace(payload.Namespace)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	payload.Namespace = namespace
	if payload.Key == "" && r.URL.Query().Get("key") != "" {
		payload.Key = r.URL.Query().Get("key")
	}
	if payload.Key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key is required"}
	}
	if payload.LeaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_lease", Detail: "lease_id required"}
	}
	storageKey, err := h.namespacedKey(namespace, payload.Key)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_key", Detail: err.Error()}
	}
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	verbose := logger
	verbose.Debug("release.begin", "namespace", namespace, "key", payload.Key, "lease_id", payload.LeaseID)
	var knownMeta *storage.Meta
	knownETag := ""
	if cachedMeta, cachedETag, cachedKey, ok := h.leaseSnapshot(payload.LeaseID); ok && cachedKey == storageKey {
		knownMeta = &cachedMeta
		knownETag = cachedETag
	}
	res, err := h.core.Release(ctx, core.ReleaseCommand{
		Namespace:     namespace,
		Key:           payload.Key,
		LeaseID:       payload.LeaseID,
		FencingToken:  fencingToken,
		KnownMeta:     knownMeta,
		KnownMetaETag: knownETag,
	})
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			h.dropLease(payload.LeaseID)
		}
		return convertCoreError(err)
	}
	h.dropLease(payload.LeaseID)
	h.writeJSON(w, http.StatusOK, api.ReleaseResponse{Released: res.Released}, nil)
	verbose.Debug("release.success", "namespace", namespace, "key", payload.Key, "lease_id", payload.LeaseID)
	return nil
}

// handleGet godoc
// @Summary      Read the JSON checkpoint for a key
// @Description  Streams the currently committed JSON state for the key owned by the caller's lease (or, when public=1, the latest published snapshot). Returns 204 when no state is present.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        namespace        query   string  false  "Namespace override (defaults to server setting)"
// @Param        key              query   string  true   "Lease key"
// @Param        public           query   bool    false  "Set to true to read without a lease (served from published data)"
// @Param        X-Lease-ID       header  string  false  "Lease identifier (required unless public=1)"
// @Param        X-Fencing-Token  header  string  false  "Optional fencing token proof (lease requests only)"
// @Success      200              {object}  map[string]interface{}  "Streamed JSON state"
// @Success      204              {string}  string          "No state stored for this key"
// @Failure      400              {object}  api.ErrorResponse
// @Failure      401              {object}  api.ErrorResponse
// @Failure      409              {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/get [post]
func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) error {
	key := strings.TrimSpace(r.URL.Query().Get("key"))
	if key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_params", Detail: "key query required"}
	}
	publicRequested := parseBoolQuery(r.URL.Query().Get("public"))
	leaseID := strings.TrimSpace(r.Header.Get("X-Lease-ID"))
	publicRead := publicRequested && leaseID == ""
	fencingToken := int64(0)
	if !publicRead {
		var err error
		fencingToken, err = parseFencingToken(r)
		if err != nil {
			return err
		}
	}
	res, err := h.core.Get(r.Context(), core.GetCommand{
		Namespace:    r.URL.Query().Get("namespace"),
		Key:          key,
		LeaseID:      leaseID,
		FencingToken: fencingToken,
		Public:       publicRead,
	})
	if err != nil {
		return convertCoreError(err)
	}
	if res.NoContent {
		w.WriteHeader(http.StatusNoContent)
		return nil
	}
	defer res.Reader.Close()
	if res.Info != nil {
		w.Header().Set("ETag", res.Info.ETag)
		if res.Info.Version > 0 {
			w.Header().Set("X-Key-Version", strconv.FormatInt(res.Info.Version, 10))
		} else {
			versionHeader := res.Meta.Version
			if res.Public {
				versionHeader = res.PublishedVersion
			}
			w.Header().Set("X-Key-Version", strconv.FormatInt(versionHeader, 10))
		}
	}
	if !res.Public && res.Meta != nil && res.Meta.Lease != nil {
		w.Header().Set(headerFencingToken, strconv.FormatInt(res.Meta.Lease.FencingToken, 10))
	}
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, res.Reader)
	return err
}

// handleQuery godoc
// @Summary      Query keys within a namespace
// @Description  Executes a selector-based search within a namespace and returns matching keys plus a cursor for pagination. Example request body: `{"namespace":"default","selector":{"eq":{"field":"type","value":"alpha"}},"limit":25,"return":"keys"}`
// @Tags         lease
// @Accept       json
// @Produce      json
// @Produce      application/x-ndjson
// @Param        namespace  query    string  false  "Namespace override (defaults to server setting)"
// @Param        limit      query    integer false  "Maximum number of keys to return (1-1000, defaults to 100)"
// @Param        cursor     query    string  false  "Opaque pagination cursor"
// @Param        selector   query    string  false  "Selector JSON (optional when providing a JSON body)"
// @Param        return     query    string  false  "Return mode (keys or documents)"
// @Param        request    body     api.QueryRequest  false  "Query request (selector, limit, cursor)"
// @Success      200        {object} api.QueryResponse
// @Failure      400        {object} api.ErrorResponse
// @Failure      404        {object} api.ErrorResponse
// @Failure      409        {object} api.ErrorResponse
// @Failure      503        {object} api.ErrorResponse
// @Router       /v1/query [post]
func (h *Handler) handleQuery(w http.ResponseWriter, r *http.Request) error {
	if err := h.maybeThrottleLock(); err != nil {
		return err
	}
	if h.searchAdapter == nil {
		return httpError{
			Status: http.StatusNotImplemented,
			Code:   "query_disabled",
			Detail: "query service not configured",
		}
	}
	var req api.QueryRequest
	if r.Body != nil && r.Body != http.NoBody {
		reader := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
		defer reader.Close()
		if err := json.NewDecoder(reader).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			return httpError{
				Status: http.StatusBadRequest,
				Code:   "invalid_body",
				Detail: fmt.Sprintf("failed to parse request: %v", err),
			}
		}
	} else if r.Body != nil {
		_ = r.Body.Close()
	}
	query := r.URL.Query()
	returnMode := api.QueryReturnKeys
	if req.Return != "" {
		normalized, err := parseQueryReturnMode(string(req.Return))
		if err != nil {
			return err
		}
		returnMode = normalized
	}
	if ns := strings.TrimSpace(query.Get("namespace")); ns != "" {
		req.Namespace = ns
	}
	if cursor := strings.TrimSpace(query.Get("cursor")); cursor != "" {
		req.Cursor = cursor
	}
	if limitStr := strings.TrimSpace(query.Get("limit")); limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil {
			req.Limit = parsed
		}
	}
	if selectorParam := strings.TrimSpace(query.Get("selector")); selectorParam != "" {
		var selector api.Selector
		if err := json.Unmarshal([]byte(selectorParam), &selector); err != nil {
			return httpError{
				Status: http.StatusBadRequest,
				Code:   "invalid_selector",
				Detail: fmt.Sprintf("failed to parse selector: %v", err),
			}
		}
		req.Selector = selector
	} else if zeroSelector(req.Selector) {
		selector, err := lql.ParseSelectorValues(query)
		if err != nil {
			return httpError{
				Status: http.StatusBadRequest,
				Code:   "invalid_selector",
				Detail: err.Error(),
			}
		}
		if !selector.IsEmpty() {
			req.Selector = selector
		}
	}
	if err := normalizeSelectorFields(&req.Selector); err != nil {
		return httpError{
			Status: http.StatusBadRequest,
			Code:   "invalid_selector",
			Detail: err.Error(),
		}
	}
	namespace, err := h.resolveNamespace(req.Namespace)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	h.observeNamespace(namespace)
	limit := req.Limit
	if limit <= 0 {
		limit = defaultQueryLimit
	}
	if limit > maxQueryLimit {
		limit = maxQueryLimit
	}
	fields := req.Fields
	if rawFields := strings.TrimSpace(query.Get("fields")); rawFields != "" {
		var parsed map[string]any
		if err := json.Unmarshal([]byte(rawFields), &parsed); err != nil {
			return httpError{
				Status: http.StatusBadRequest,
				Code:   "invalid_fields",
				Detail: fmt.Sprintf("failed to parse fields: %v", err),
			}
		}
		fields = parsed
	}
	engineHint, err := parseEngineHint(strings.TrimSpace(query.Get("engine")))
	if err != nil {
		return err
	}
	if rawReturn := strings.TrimSpace(query.Get("return")); rawReturn != "" {
		normalized, err := parseQueryReturnMode(rawReturn)
		if err != nil {
			return err
		}
		returnMode = normalized
	}
	req.Return = returnMode
	refreshMode, err := parseRefreshMode(strings.TrimSpace(query.Get("refresh")))
	if err != nil {
		return err
	}
	engine, err := h.selectQueryEngine(r.Context(), namespace, engineHint)
	if err != nil {
		return err
	}
	if engine == search.EngineIndex && refreshMode == refreshWaitFor {
		if err := h.waitForIndexReadable(r.Context(), namespace); err != nil {
			return err
		}
	}
	cmd := core.QueryCommand{
		Namespace: namespace,
		Selector:  req.Selector,
		Limit:     limit,
		Cursor:    req.Cursor,
		Fields:    fields,
		Engine:    engine,
		Return:    returnMode,
		Refresh:   core.RefreshMode(refreshMode),
	}
	result, err := h.core.Query(r.Context(), cmd)
	if err != nil {
		return convertCoreError(err)
	}
	if returnMode != api.QueryReturnDocuments {
		resp := api.QueryResponse{
			Namespace: namespace,
			Keys:      result.Keys,
			Cursor:    result.Cursor,
			IndexSeq:  result.IndexSeq,
			Metadata:  result.Metadata,
		}
		return h.writeQueryKeysResponse(w, resp, returnMode)
	}

	return h.writeQueryDocumentsCore(r.Context(), w, cmd)
}

// handleIndexFlush godoc
// @Summary      Flush namespace index segments
// @Description  Forces the namespace index writer to flush pending documents. Supports synchronous (wait) and asynchronous modes.
// @Tags         index
// @Accept       json
// @Produce      json
// @Param        namespace  query   string  false  "Namespace override"
// @Param        mode       query   string  false  "Flush mode (wait or async)"
// @Param        request    body    api.IndexFlushRequest  false  "Flush request"
// @Success      200  {object}  api.IndexFlushResponse
// @Success      202  {object}  api.IndexFlushResponse
// @Failure      400  {object}  api.ErrorResponse
// @Failure      404  {object}  api.ErrorResponse
// @Failure      409  {object}  api.ErrorResponse
// @Failure      503  {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/index/flush [post]
func (h *Handler) handleIndexFlush(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodPost {
		return httpError{Status: http.StatusMethodNotAllowed, Code: "method_not_allowed", Detail: "index flush requires POST"}
	}
	if h.indexControl == nil {
		return httpError{Status: http.StatusNotImplemented, Code: "index_unavailable", Detail: "indexing is disabled"}
	}
	defer r.Body.Close()
	var payload api.IndexFlushRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil && err != io.EOF {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: fmt.Sprintf("failed to decode request: %v", err)}
	}
	query := r.URL.Query()
	namespace := strings.TrimSpace(payload.Namespace)
	if ns := strings.TrimSpace(query.Get("namespace")); ns != "" {
		namespace = ns
	}
	resolved, err := h.resolveNamespace(namespace)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	mode := strings.TrimSpace(payload.Mode)
	if m := strings.TrimSpace(query.Get("mode")); m != "" {
		mode = m
	}
	switch strings.ToLower(mode) {
	case "", "wait", "sync":
		mode = "wait"
	case "async":
		mode = "async"
	default:
		return httpError{Status: http.StatusBadRequest, Code: "invalid_mode", Detail: fmt.Sprintf("unsupported flush mode %q", mode)}
	}
	h.observeNamespace(resolved)
	if mode == "async" {
		flushID := uuidv7.NewString()
		pending := h.indexControl.Pending(resolved)
		logger := pslog.LoggerFromContext(r.Context())
		if logger != nil {
			logger.Debug("index.flush.async_scheduled", "namespace", resolved, "flush_id", flushID)
		}
		go func(ns, reqID string) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := h.indexControl.FlushNamespace(ctx, ns); err != nil {
				if h.logger != nil {
					h.logger.Warn("index.flush.async_error", "namespace", ns, "flush_id", reqID, "error", err)
				}
			} else if h.logger != nil {
				h.logger.Debug("index.flush.async_complete", "namespace", ns, "flush_id", reqID)
			}
		}(resolved, flushID)
		resp := api.IndexFlushResponse{
			Namespace: resolved,
			Mode:      mode,
			Accepted:  true,
			Flushed:   false,
			Pending:   pending,
			FlushID:   flushID,
		}
		h.writeJSON(w, http.StatusAccepted, resp, nil)
		return nil
	}
	if err := h.indexControl.FlushNamespace(r.Context(), resolved); err != nil {
		return fmt.Errorf("index flush: %w", err)
	}
	seq, err := h.indexControl.ManifestSeq(r.Context(), resolved)
	if err != nil {
		return fmt.Errorf("load index manifest: %w", err)
	}
	resp := api.IndexFlushResponse{
		Namespace: resolved,
		Mode:      mode,
		Accepted:  true,
		Flushed:   true,
		Pending:   h.indexControl.Pending(resolved),
		IndexSeq:  seq,
	}
	h.writeJSON(w, http.StatusOK, resp, nil)
	return nil
}

// handleUpdate godoc
// @Summary      Atomically update the JSON state for a key
// @Description  Streams JSON from the request body, compacts it, and installs it if the caller holds the lease. Optional CAS headers guard against concurrent updates.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        namespace          query   string  false  "Namespace override (defaults to server setting)"
// @Param        key                query   string  true   "Lease key"
// @Param        X-Lease-ID         header  string  true   "Lease identifier"
// @Param        X-Fencing-Token    header  string  false  "Optional fencing token proof"
// @Param        X-If-Version       header  string  false  "Conditionally update when the current version matches"
// @Param        X-If-State-ETag    header  string  false  "Conditionally update when the state ETag matches"
// @Param        state              body    string  true   "New JSON state payload"
// @Success      200                {object}  api.UpdateResponse
// @Failure      400                {object}  api.ErrorResponse
// @Failure      404                {object}  api.ErrorResponse
// @Failure      409                {object}  api.ErrorResponse
// @Failure      503                {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/update [post]
func (h *Handler) handleUpdate(w http.ResponseWriter, r *http.Request) error {
	key := r.URL.Query().Get("key")
	if key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key query required"}
	}
	namespace, err := h.resolveNamespace(r.URL.Query().Get("namespace"))
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	h.observeNamespace(namespace)
	storageKey, err := h.namespacedKey(namespace, key)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_key", Detail: err.Error()}
	}
	leaseID := r.Header.Get("X-Lease-ID")
	if leaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_lease", Detail: "X-Lease-ID required"}
	}
	fencingToken, err := parseFencingToken(r)
	if err != nil {
		return err
	}
	expectVersion := int64(0)
	expectVersionSet := false
	if raw := strings.TrimSpace(r.Header.Get("X-If-Version")); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			expectVersion = parsed
			expectVersionSet = true
		} else {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_version", Detail: err.Error()}
		}
	}
	expectETag := strings.TrimSpace(r.Header.Get("X-If-State-ETag"))
	if _, err := parseMetadataHeaders(r); err != nil {
		return err
	} // TODO: pass through when core supports metadata patch
	var knownMeta *storage.Meta
	knownETag := ""
	if cachedMeta, cachedETag, cachedKey, ok := h.leaseSnapshot(leaseID); ok && cachedKey == storageKey {
		knownMeta = &cachedMeta
		knownETag = cachedETag
	}
	res, err := h.core.Update(r.Context(), core.UpdateCommand{
		Namespace:      r.URL.Query().Get("namespace"),
		Key:            key,
		LeaseID:        leaseID,
		FencingToken:   fencingToken,
		IfVersion:      expectVersion,
		IfStateETag:    expectETag,
		Body:           http.MaxBytesReader(w, r.Body, h.jsonMaxBytes),
		CompactWriter:  h.compactWriter,
		MaxBytes:       h.jsonMaxBytes,
		SpoolThreshold: h.spoolThreshold,
		KnownMeta:      knownMeta,
		KnownMetaETag:  knownETag,
		IfVersionSet:   expectVersionSet,
	})
	if err != nil {
		return convertCoreError(err)
	}
	if res.Meta != nil && res.Meta.Lease != nil {
		h.cacheLease(leaseID, storageKey, *res.Meta, res.MetaETag)
	}
	h.writeJSON(w, http.StatusOK, api.UpdateResponse{
		NewVersion:   res.NewVersion,
		NewStateETag: res.NewStateETag,
		Bytes:        res.Bytes,
		Metadata:     metadataAttributesFromMeta(res.Meta),
	}, map[string]string{
		"X-Key-Version": strconv.FormatInt(res.NewVersion, 10),
		"ETag":          res.NewStateETag,
	})
	return nil
}

// handleMetadata godoc
// @Summary      Update lock metadata
// @Description  Mutates lock metadata (e.g. query visibility) while holding the lease. Optional CAS headers guard against concurrent updates.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        namespace          query   string  false  "Namespace override (defaults to server setting)"
// @Param        key                query   string  true   "Lease key"
// @Param        X-Lease-ID         header  string  true   "Lease identifier"
// @Param        X-Fencing-Token    header  string  false  "Optional fencing token proof"
// @Param        X-If-Version       header  string  false  "Conditionally update when the current version matches"
// @Param        metadata           body    metadataMutation  true  "Metadata payload"
// @Success      200                {object}  api.MetadataUpdateResponse
// @Failure      400                {object}  api.ErrorResponse
// @Failure      404                {object}  api.ErrorResponse
// @Failure      409                {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/metadata [post]
func (h *Handler) handleMetadata(w http.ResponseWriter, r *http.Request) error {
	key := r.URL.Query().Get("key")
	if key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key query required"}
	}
	leaseID := r.Header.Get("X-Lease-ID")
	if leaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_lease", Detail: "X-Lease-ID required"}
	}
	fencingToken, err := parseFencingToken(r)
	if err != nil {
		return err
	}
	ifVersion := strings.TrimSpace(r.Header.Get("X-If-Version"))
	headerPatch, err := parseMetadataHeaders(r)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_metadata", Detail: err.Error()}
	}
	bodyPatch, err := decodeMetadataMutation(r.Body)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_metadata", Detail: err.Error()}
	}
	patch := metadataMutation{}
	patch.merge(bodyPatch)
	patch.merge(headerPatch)
	if patch.empty() {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_metadata", Detail: "no metadata fields provided"}
	}
	var ifVersionVal int64
	ifVersionSet := false
	if ifVersion != "" {
		parsed, parseErr := strconv.ParseInt(ifVersion, 10, 64)
		if parseErr != nil {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_version", Detail: parseErr.Error()}
		}
		ifVersionVal = parsed
		ifVersionSet = true
	}
	var knownMeta *storage.Meta
	knownETag := ""
	ns, err := h.resolveNamespace(r.URL.Query().Get("namespace"))
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	h.observeNamespace(ns)
	storageKey, err := h.namespacedKey(ns, key)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_key", Detail: err.Error()}
	}
	if cachedMeta, cachedETag, cachedKey, ok := h.leaseSnapshot(leaseID); ok && cachedKey == storageKey {
		knownMeta = &cachedMeta
		knownETag = cachedETag
	}
	res, err := h.core.Metadata(r.Context(), core.MetadataCommand{
		Namespace:     ns,
		Key:           key,
		LeaseID:       leaseID,
		FencingToken:  fencingToken,
		Mutation:      core.MetadataMutation{QueryHidden: patch.QueryHidden},
		KnownMeta:     knownMeta,
		KnownMetaETag: knownETag,
		IfVersion:     ifVersionVal,
		IfVersionSet:  ifVersionSet,
	})
	if err != nil {
		return convertCoreError(err)
	}
	if res.Meta != nil && res.Meta.Lease != nil {
		h.cacheLease(leaseID, storageKey, *res.Meta, res.MetaETag)
	}
	resp := api.MetadataUpdateResponse{
		Namespace: ns,
		Key:       key,
		Version:   res.Version,
		Metadata:  metadataAttributesFromMeta(res.Meta),
	}
	headers := map[string]string{
		"X-Key-Version": strconv.FormatInt(res.Version, 10),
	}
	h.writeJSON(w, http.StatusOK, resp, headers)
	return nil
}

// handleRemove godoc
// @Summary      Delete the JSON state for a key
// @Description  Removes the stored state blob if the caller holds the lease. Optional CAS headers guard against concurrent updates.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        namespace        query   string  false  "Namespace override (defaults to server setting)"
// @Param        key              query   string  true   "Lease key"
// @Param        X-Lease-ID       header  string  true   "Lease identifier"
// @Param        X-Fencing-Token  header  string  false  "Optional fencing token proof"
// @Param        X-If-Version     header  string  false  "Conditionally remove when version matches"
// @Param        X-If-State-ETag  header  string  false  "Conditionally remove when state ETag matches"
// @Success      200              {object}  api.RemoveResponse
// @Failure      400              {object}  api.ErrorResponse
// @Failure      404              {object}  api.ErrorResponse
// @Failure      409              {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/remove [post]
func (h *Handler) handleRemove(w http.ResponseWriter, r *http.Request) error {
	key := r.URL.Query().Get("key")
	if key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key query required"}
	}
	namespace, err := h.resolveNamespace(r.URL.Query().Get("namespace"))
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	h.observeNamespace(namespace)
	storageKey, err := h.namespacedKey(namespace, key)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_key", Detail: err.Error()}
	}
	leaseID := r.Header.Get("X-Lease-ID")
	if leaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_lease", Detail: "X-Lease-ID required"}
	}
	fencingToken, err := parseFencingToken(r)
	if err != nil {
		return err
	}
	ifMatch := strings.TrimSpace(r.Header.Get("X-If-State-ETag"))
	ifVersion := strings.TrimSpace(r.Header.Get("X-If-Version"))
	versionVal := int64(0)
	if ifVersion != "" {
		if parsed, perr := strconv.ParseInt(ifVersion, 10, 64); perr == nil {
			versionVal = parsed
		} else {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_version", Detail: perr.Error()}
		}
	}
	var knownMeta *storage.Meta
	knownETag := ""
	if cachedMeta, cachedETag, cachedKey, ok := h.leaseSnapshot(leaseID); ok && cachedKey == storageKey {
		knownMeta = &cachedMeta
		knownETag = cachedETag
	}
	res, err := h.core.Remove(r.Context(), core.RemoveCommand{
		Namespace:     namespace,
		Key:           key,
		LeaseID:       leaseID,
		FencingToken:  fencingToken,
		IfStateETag:   ifMatch,
		IfVersion:     versionVal,
		KnownMeta:     knownMeta,
		KnownMetaETag: knownETag,
	})
	if err != nil {
		return convertCoreError(err)
	}
	h.dropLease(leaseID)
	headers := map[string]string{
		"X-Key-Version": strconv.FormatInt(res.NewVersion, 10),
	}
	h.writeJSON(w, http.StatusOK, api.RemoveResponse{
		Removed:    res.Removed,
		NewVersion: res.NewVersion,
	}, headers)
	_ = ifMatch
	_ = ifVersion
	return nil
}

// handleDescribe godoc
// @Summary      Inspect metadata for a key
// @Description  Returns lease and state metadata without streaming the state payload.
// @Tags         lease
// @Produce      json
// @Param        namespace  query  string  false  "Namespace override (defaults to server setting)"
// @Param        key        query  string  true   "Lease key"
// @Success      200  {object}  api.DescribeResponse
// @Failure      400  {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/describe [get]
func (h *Handler) handleDescribe(w http.ResponseWriter, r *http.Request) error {
	key := r.URL.Query().Get("key")
	if key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key query required"}
	}
	res, err := h.core.Describe(r.Context(), core.DescribeCommand{
		Namespace: r.URL.Query().Get("namespace"),
		Key:       key,
	})
	if err != nil {
		return convertCoreError(err)
	}
	resp := api.DescribeResponse{
		Namespace: res.Namespace,
		Key:       res.Key,
		Version:   res.Version,
		StateETag: res.StateETag,
		UpdatedAt: res.UpdatedAt,
		Metadata:  metadataAttributesFromMeta(res.Meta),
	}
	if res.LeaseID != "" {
		resp.LeaseID = res.LeaseID
		resp.Owner = res.Owner
		resp.ExpiresAt = res.ExpiresAt
	}
	h.writeJSON(w, http.StatusOK, resp, nil)
	return nil
}

// handleQueueEnqueue godoc
// @Summary      Enqueue a message
// @Description  Writes a message into the durable queue. The payload is streamed directly and may include optional attributes.
// @Tags         queue
// @Accept       multipart/form-data
// @Produce      json
// @Param        namespace  query     string  false  "Namespace override when the metadata omits it"
// @Param        queue      query     string  false  "Queue override when the metadata omits it"
// @Param        meta     formData  string  true   "JSON encoded api.EnqueueRequest metadata"
// @Param        payload  formData  file    false  "Optional payload stream"
// @Success      200      {object}  api.EnqueueResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/enqueue [post]
func (h *Handler) handleQueueEnqueue(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.maybeThrottleQueue(qrf.KindQueueProducer); err != nil {
		return err
	}
	finish := h.beginQueueProducer()
	defer finish()
	qsvc, err := h.requireQueueService()
	if err != nil {
		return err
	}
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	contentType := r.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_content_type", Detail: "multipart form required"}
	}
	mr := multipart.NewReader(r.Body, params["boundary"])
	var meta api.EnqueueRequest
	var payload io.Reader
	payloadFound := false
	for {
		part, err := mr.NextPart()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return httpError{Status: http.StatusBadRequest, Code: "multipart_error", Detail: err.Error()}
		}
		switch part.FormName() {
		case "meta":
			metaBytes, readErr := io.ReadAll(part)
			if readErr != nil {
				return fmt.Errorf("queue enqueue multipart: %w", readErr)
			}
			if err := json.Unmarshal(metaBytes, &meta); err != nil {
				return fmt.Errorf("queue enqueue meta: %w", err)
			}
		case "payload":
			payload = part
			payloadFound = true
		default:
			if payload == nil {
				payload = part
			}
		}
		if payloadFound {
			break
		}
	}
	resolvedNamespace, err := h.resolveNamespace(meta.Namespace)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	queueName := strings.TrimSpace(meta.Queue)
	if queueName == "" {
		queueName = strings.TrimSpace(r.URL.Query().Get("queue"))
	}
	if queueName == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_queue", Detail: "queue is required"}
	}
	if payload == nil {
		payload = strings.NewReader("")
	}
	if meta.Attributes == nil {
		meta.Attributes = map[string]any{}
	}
	remoteAddr := r.RemoteAddr
	if last := strings.LastIndex(remoteAddr, ":"); last >= 0 {
		remoteAddr = remoteAddr[:last]
	}
	corr := correlation.ID(ctx)
	enqueueLogger := logger.With("namespace", resolvedNamespace, "queue", queueName, "remote_addr", remoteAddr, "cid", corr)

	opts := queue.EnqueueOptions{
		Delay:       time.Duration(meta.DelaySeconds) * time.Second,
		Visibility:  time.Duration(meta.VisibilityTimeoutSeconds) * time.Second,
		TTL:         time.Duration(meta.TTLSeconds) * time.Second,
		MaxAttempts: meta.MaxAttempts,
		Attributes:  meta.Attributes,
		ContentType: meta.PayloadContentType,
	}
	msg, err := qsvc.Enqueue(ctx, resolvedNamespace, queueName, payload, opts)
	if err != nil {
		enqueueLogger.Warn("queue.enqueue.fail",
			"delay_seconds", meta.DelaySeconds,
			"visibility_timeout_seconds", meta.VisibilityTimeoutSeconds,
			"ttl_seconds", meta.TTLSeconds,
			"content_type", meta.PayloadContentType,
			"error", err,
		)
		return err
	}
	if h.queueDisp != nil {
		h.queueDisp.Notify(resolvedNamespace, msg.Queue)
	}
	enqueueLogger.Info("queue.enqueue.success",
		"message_id", msg.ID,
		"payload_bytes", msg.PayloadBytes,
		"attempts", msg.Attempts,
		"max_attempts", msg.MaxAttempts,
		"not_visible_until", msg.NotVisibleUntil.Unix(),
		"visibility_timeout", msg.Visibility.Seconds(),
	)
	resp := api.EnqueueResponse{
		Namespace:                msg.Namespace,
		Queue:                    msg.Queue,
		MessageID:                msg.ID,
		Attempts:                 msg.Attempts,
		MaxAttempts:              msg.MaxAttempts,
		NotVisibleUntilUnix:      msg.NotVisibleUntil.Unix(),
		VisibilityTimeoutSeconds: int64(msg.Visibility.Seconds()),
		PayloadBytes:             msg.PayloadBytes,
		CorrelationID:            msg.CorrelationID,
	}
	h.writeJSON(w, http.StatusOK, resp, nil)
	return nil
}

// handleQueueDequeue godoc
// @Summary      Dequeue messages
// @Description  Dequeues one or more messages from the specified queue. Supports long polling via wait_seconds.
// @Tags         queue
// @Accept       json
// @Produce      json
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        queue      query    string  false  "Queue name override when the request body omits it"
// @Param        owner      query    string  false  "Owner override when the request body omits it"
// @Param        request  body      api.DequeueRequest  true  "Dequeue parameters (queue, owner, wait_seconds, visibility)"
// @Success      200      {object}  api.DequeueResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/dequeue [post]
func (h *Handler) handleQueueDequeue(w http.ResponseWriter, r *http.Request) error {
	baseCtx := r.Context()
	if err := h.maybeThrottleQueue(qrf.KindQueueConsumer); err != nil {
		return err
	}
	finish := h.beginQueueConsumer()
	defer finish()

	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var req api.DequeueRequest
	if err := json.NewDecoder(reqBody).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: fmt.Sprintf("failed to parse request: %v", err)}
	}

	namespace := strings.TrimSpace(req.Namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(r.URL.Query().Get("namespace"))
	}
	resolvedNamespace, err := h.resolveNamespace(namespace)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	req.Namespace = resolvedNamespace
	h.observeNamespace(resolvedNamespace)

	queueName := strings.TrimSpace(req.Queue)
	if queueName == "" {
		queueName = strings.TrimSpace(r.URL.Query().Get("queue"))
	}
	if queueName == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_queue", Detail: "queue is required"}
	}

	owner := strings.TrimSpace(req.Owner)
	if owner == "" {
		owner = strings.TrimSpace(r.URL.Query().Get("owner"))
	}

	blockSeconds := req.WaitSeconds
	waitDuration := time.Duration(blockSeconds) * time.Second
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 1
	}
	if pageSize > maxQueueDequeueBatch {
		pageSize = maxQueueDequeueBatch
	}
	ctx := baseCtx
	var cancel context.CancelFunc
	if blockSeconds > 0 {
		timeout := waitDuration + queueEnsureTimeoutGrace
		ctx, cancel = context.WithTimeout(baseCtx, timeout)
		defer cancel()
	}

	if !correlation.Has(ctx) {
		ctx = correlation.Set(ctx, correlation.Generate())
	}
	span := trace.SpanFromContext(ctx)
	ctx, logger := applyCorrelation(ctx, pslog.LoggerFromContext(ctx), span)
	r = r.WithContext(ctx)
	owner = h.appendQueueOwner(ctx, owner)
	if owner == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_owner", Detail: "owner is required"}
	}

	remoteAddr := h.clientKeyFromRequest(r)
	queueLogger := logger.With("namespace", resolvedNamespace, "queue", queueName, "owner", owner, "stateful", false)
	if state := h.currentShutdownState(); state.Draining {
		retry := durationToSeconds(state.Remaining)
		queueLogger.Info("queue.dequeue.reject_shutdown",
			"block_seconds", blockSeconds,
			"remaining_seconds", retry,
		)
		return httpError{
			Status:     http.StatusServiceUnavailable,
			Code:       "shutdown_draining",
			Detail:     "server is draining existing leases",
			RetryAfter: retry,
		}
	}
	queueLogger.Info("queue.consumer.connect",
		"block_seconds", blockSeconds,
		"block_mode", queueBlockModeLabel(blockSeconds),
		"remote_addr", remoteAddr,
		"page_size", pageSize,
	)
	disconnectStatus := "unknown"
	var deliveries []*core.QueueDelivery
	var nextCursor string
	defer func() {
		fields := []any{
			"status", disconnectStatus,
			"block_seconds", blockSeconds,
			"block_mode", queueBlockModeLabel(blockSeconds),
			"remote_addr", remoteAddr,
			"delivered_count", len(deliveries),
		}
		if disconnectStatus == "error" {
			queueLogger.Warn("queue.consumer.disconnect", fields...)
		} else {
			queueLogger.Info("queue.consumer.disconnect", fields...)
		}
	}()

	visibility := h.resolveTTL(req.VisibilityTimeoutSeconds)
	cmd := core.QueueDequeueCommand{
		Namespace:    resolvedNamespace,
		Queue:        queueName,
		Owner:        owner,
		Stateful:     false,
		Visibility:   visibility,
		BlockSeconds: blockSeconds,
		PageSize:     pageSize,
	}
	for {
		res, err := h.core.Dequeue(ctx, cmd)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, core.ErrQueueEmpty) {
				disconnectStatus = "empty"
				retryAfter := int64(1)
				switch {
				case blockSeconds == api.BlockNoWait:
					retryAfter = 0
				case blockSeconds > 0:
					retryAfter = max(int64(math.Ceil(waitDuration.Seconds())), 1)
				}
				return httpError{
					Status:     http.StatusConflict,
					Code:       "waiting",
					Detail:     "no messages available",
					RetryAfter: retryAfter,
				}
			}
			disconnectStatus = "error"
			h.logQueueSubscribeError(queueLogger, queueName, owner, err)
			return convertCoreError(err)
		}
		deliveries = res.Deliveries
		nextCursor = res.NextCursor
		break
	}

	success := false
	defer func() {
		for _, delivery := range deliveries {
			if delivery == nil {
				continue
			}
			if success {
				if delivery.Finalize != nil {
					delivery.Finalize(true)
				}
			} else {
				if delivery.Finalize != nil {
					delivery.Finalize(false)
				}
			}
		}
	}()

	if err := writeQueueDeliveryBatch(w, deliveries, nextCursor); err != nil {
		disconnectStatus = "error"
		h.logQueueSubscribeError(queueLogger, queueName, owner, err)
		return err
	}
	success = true
	for _, delivery := range deliveries {
		if delivery != nil {
			logQueueDeliveryInfo(queueLogger, delivery)
		}
	}
	disconnectStatus = "delivered"
	return nil
}

// handleQueueDequeueWithState godoc
// @Summary      Fetch queue messages with state attachments
// @Description  Dequeues messages and includes their associated state blobs in the multipart response when available.
// @Tags         queue
// @Accept       json
// @Produce      multipart/related
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        queue      query    string  false  "Queue name override when the request body omits it"
// @Param        owner      query    string  false  "Owner override when the request body omits it"
// @Param        request  body      api.DequeueRequest  true  "Dequeue parameters"
// @Success      200      {string}  string  "Multipart response with message metadata, payload, and state attachments"
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/dequeueWithState [post]
func (h *Handler) handleQueueDequeueWithState(w http.ResponseWriter, r *http.Request) error {
	baseCtx := r.Context()
	if err := h.maybeThrottleQueue(qrf.KindQueueConsumer); err != nil {
		return err
	}
	finish := h.beginQueueConsumer()
	defer finish()

	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var req api.DequeueRequest
	if err := json.NewDecoder(reqBody).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: fmt.Sprintf("failed to parse request: %v", err)}
	}

	namespace := strings.TrimSpace(req.Namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(r.URL.Query().Get("namespace"))
	}
	resolvedNamespace, err := h.resolveNamespace(namespace)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	req.Namespace = resolvedNamespace

	queueName := strings.TrimSpace(req.Queue)
	if queueName == "" {
		queueName = strings.TrimSpace(r.URL.Query().Get("queue"))
	}
	if queueName == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_queue", Detail: "queue is required"}
	}

	owner := strings.TrimSpace(req.Owner)
	if owner == "" {
		owner = strings.TrimSpace(r.URL.Query().Get("owner"))
	}

	blockSeconds := req.WaitSeconds
	waitDuration := time.Duration(blockSeconds) * time.Second
	ctx := baseCtx
	var cancel context.CancelFunc
	if blockSeconds > 0 {
		timeout := waitDuration + queueEnsureTimeoutGrace
		ctx, cancel = context.WithTimeout(baseCtx, timeout)
		defer cancel()
	}

	if !correlation.Has(ctx) {
		ctx = correlation.Set(ctx, correlation.Generate())
	}
	span := trace.SpanFromContext(ctx)
	ctx, logger := applyCorrelation(ctx, pslog.LoggerFromContext(ctx), span)
	r = r.WithContext(ctx)
	owner = h.appendQueueOwner(ctx, owner)
	if owner == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_owner", Detail: "owner is required"}
	}

	remoteAddr := h.clientKeyFromRequest(r)
	queueLogger := logger.With("namespace", resolvedNamespace, "queue", queueName, "owner", owner, "stateful", true)
	if state := h.currentShutdownState(); state.Draining {
		retry := durationToSeconds(state.Remaining)
		queueLogger.Info("queue.dequeue_state.reject_shutdown",
			"block_seconds", blockSeconds,
			"remaining_seconds", retry,
		)
		return httpError{
			Status:     http.StatusServiceUnavailable,
			Code:       "shutdown_draining",
			Detail:     "server is draining existing leases",
			RetryAfter: retry,
		}
	}
	queueLogger.Info("queue.consumer.connect",
		"block_seconds", blockSeconds,
		"block_mode", queueBlockModeLabel(blockSeconds),
		"remote_addr", remoteAddr,
	)
	disconnectStatus := "unknown"
	defer func() {
		fields := []any{
			"status", disconnectStatus,
			"block_seconds", blockSeconds,
			"block_mode", queueBlockModeLabel(blockSeconds),
			"remote_addr", remoteAddr,
		}
		if disconnectStatus == "error" {
			queueLogger.Warn("queue.consumer.disconnect", fields...)
		} else {
			queueLogger.Info("queue.consumer.disconnect", fields...)
		}
	}()

	visibility := h.resolveTTL(req.VisibilityTimeoutSeconds)
	res, err := h.core.Dequeue(ctx, core.QueueDequeueCommand{
		Namespace:    resolvedNamespace,
		Queue:        queueName,
		Owner:        owner,
		Stateful:     true,
		Visibility:   visibility,
		BlockSeconds: blockSeconds,
		PageSize:     1,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, core.ErrQueueEmpty) {
			disconnectStatus = "empty"
			retryAfter := int64(1)
			switch {
			case blockSeconds == api.BlockNoWait:
				retryAfter = 0
			case blockSeconds > 0:
				retryAfter = max(int64(math.Ceil(waitDuration.Seconds())), 1)
			}
			return httpError{
				Status:     http.StatusConflict,
				Code:       "waiting",
				Detail:     "no messages available",
				RetryAfter: retryAfter,
			}
		}
		disconnectStatus = "error"
		return convertCoreError(err)
	}
	deliveries := res.Deliveries
	nextCursor := res.NextCursor

	success := false
	defer func() {
		for _, d := range deliveries {
			if d != nil {
				if d.Finalize != nil {
					d.Finalize(success)
				}
			}
		}
	}()
	if err := writeQueueDeliveryBatch(w, deliveries, nextCursor); err != nil {
		disconnectStatus = "error"
		return err
	}
	success = true
	for _, delivery := range deliveries {
		if delivery != nil {
			logQueueDeliveryInfo(queueLogger, delivery)
		}
	}
	disconnectStatus = "delivered"
	return nil
}

// handleQueueSubscribe godoc
// @Summary      Stream queue deliveries
// @Description  Opens a long-lived multipart stream of deliveries for the specified queue owner. Each part contains message metadata and payload.
// @Tags         queue
// @Accept       json
// @Produce      multipart/related
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        queue      query    string  false  "Queue name override when the request body omits it"
// @Param        owner      query    string  false  "Owner override when the request body omits it"
// @Param        request  body      api.DequeueRequest  true  "Subscription parameters (queue, owner, wait_seconds, visibility)"
// @Success      200      {string}  string  "Multipart stream of message deliveries"
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/subscribe [post]
func (h *Handler) handleQueueSubscribe(w http.ResponseWriter, r *http.Request) error {
	return h.handleQueueSubscribeInternal(w, r, false)
}

// handleQueueSubscribeWithState godoc
// @Summary      Stream queue deliveries with state
// @Description  Opens a long-lived multipart stream where each part contains message metadata, payload, and state snapshot when available.
// @Tags         queue
// @Accept       json
// @Produce      multipart/related
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        queue      query    string  false  "Queue name override when the request body omits it"
// @Param        owner      query    string  false  "Owner override when the request body omits it"
// @Param        request  body      api.DequeueRequest  true  "Subscription parameters (queue, owner, wait_seconds, visibility)"
// @Success      200      {string}  string  "Multipart stream of message deliveries"
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/subscribeWithState [post]
func (h *Handler) handleQueueSubscribeWithState(w http.ResponseWriter, r *http.Request) error {
	return h.handleQueueSubscribeInternal(w, r, true)
}

func (h *Handler) handleQueueSubscribeInternal(w http.ResponseWriter, r *http.Request, stateful bool) error {
	baseCtx := r.Context()
	if err := h.maybeThrottleQueue(qrf.KindQueueConsumer); err != nil {
		return err
	}
	finish := h.beginQueueConsumer()
	defer finish()

	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()

	var req api.DequeueRequest
	if err := json.NewDecoder(reqBody).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: fmt.Sprintf("failed to parse request: %v", err)}
	}

	namespace := strings.TrimSpace(req.Namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(r.URL.Query().Get("namespace"))
	}
	resolvedNamespace, err := h.resolveNamespace(namespace)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	req.Namespace = resolvedNamespace

	queueName := strings.TrimSpace(req.Queue)
	if queueName == "" {
		queueName = strings.TrimSpace(r.URL.Query().Get("queue"))
	}
	if queueName == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_queue", Detail: "queue is required"}
	}

	owner := strings.TrimSpace(req.Owner)
	if owner == "" {
		owner = strings.TrimSpace(r.URL.Query().Get("owner"))
	}
	blockSeconds := req.WaitSeconds
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 1
	}

	ctx := baseCtx
	if !correlation.Has(ctx) {
		ctx = correlation.Set(ctx, correlation.Generate())
	}
	span := trace.SpanFromContext(ctx)
	ctx, logger := applyCorrelation(ctx, pslog.LoggerFromContext(ctx), span)
	r = r.WithContext(ctx)
	owner = h.appendQueueOwner(ctx, owner)
	if owner == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_owner", Detail: "owner is required"}
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		return httpError{Status: http.StatusInternalServerError, Code: "streaming_unsupported", Detail: "streaming not supported by response writer"}
	}

	visibility := h.resolveTTL(req.VisibilityTimeoutSeconds)
	remoteAddr := h.clientKeyFromRequest(r)
	queueLogger := logger.With("namespace", resolvedNamespace, "queue", queueName, "owner", owner, "stateful", stateful)
	if state := h.currentShutdownState(); state.Draining {
		retry := durationToSeconds(state.Remaining)
		queueLogger.Info("queue.subscribe.reject_shutdown",
			"block_seconds", blockSeconds,
			"remaining_seconds", retry,
			"prefetch", pageSize,
		)
		return httpError{
			Status:     http.StatusServiceUnavailable,
			Code:       "shutdown_draining",
			Detail:     "server is draining existing leases",
			RetryAfter: retry,
		}
	}
	queueLogger.Info("queue.subscribe.connect",
		"block_seconds", blockSeconds,
		"block_mode", queueBlockModeLabel(blockSeconds),
		"remote_addr", remoteAddr,
		"prefetch", pageSize,
	)
	defer h.releasePendingDeliveries(resolvedNamespace, queueName, owner)

	headersWritten := false
	var writer *multipart.Writer
	writerCreated := false
	var deliveredCount int
	disconnectStatus := "unknown"
	defer func() {
		fields := []any{
			"status", disconnectStatus,
			"delivered_count", deliveredCount,
			"block_seconds", blockSeconds,
			"block_mode", queueBlockModeLabel(blockSeconds),
			"remote_addr", remoteAddr,
		}
		if disconnectStatus == "error" {
			queueLogger.Warn("queue.subscribe.disconnect", fields...)
		} else {
			queueLogger.Info("queue.subscribe.disconnect", fields...)
		}
		if writerCreated && writer != nil {
			_ = writer.Close()
		}
	}()

	writeDeliveries := func(deliveries []*core.QueueDelivery, defaultCursor string) error {
		if writer == nil {
			writer = multipart.NewWriter(w)
			writerCreated = true
		}
		firstCID := ""
		for _, delivery := range deliveries {
			if delivery != nil && delivery.Message != nil && delivery.Message.CorrelationID != "" {
				firstCID = delivery.Message.CorrelationID
				break
			}
		}
		if !headersWritten {
			contentType := fmt.Sprintf("multipart/related; boundary=%s", writer.Boundary())
			w.Header().Set("Content-Type", contentType)
			if firstCID != "" {
				w.Header().Set(headerCorrelationID, firstCID)
			}
			w.WriteHeader(http.StatusOK)
			headersWritten = true
		}
		if _, err := writeQueueDeliveriesToWriter(writer, deliveries, defaultCursor); err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	}

	const subscribeHotWindow = 2 * time.Second
	var hotUntil time.Time

	for {
		if err := ctx.Err(); err != nil {
			disconnectStatus = "context"
			break
		}

		if blockSeconds > 1 && h.queueDisp != nil && h.queueDisp.HasActiveWatcher(resolvedNamespace, queueName) {
			blockSeconds = 1
		}

		loopBlock := blockSeconds
		if deliveredCount > 0 {
			now := time.Now()
			if hotUntil.After(now) {
				loopBlock = api.BlockNoWait
			} else if blockSeconds > 1 {
				loopBlock = 1
			}
		}
		if loopBlock > 1 && h.queueDisp != nil && h.queueDisp.HasActiveWatcher(resolvedNamespace, queueName) {
			loopBlock = 1
		}
		iterCtx := ctx
		var cancel context.CancelFunc
		if loopBlock > 0 {
			waitDuration := time.Duration(loopBlock) * time.Second
			iterCtx, cancel = context.WithTimeout(ctx, waitDuration)
		}
		cleanup := func() {
			if cancel != nil {
				cancel()
				cancel = nil
			}
		}

		res, err := h.core.Dequeue(iterCtx, core.QueueDequeueCommand{
			Namespace:    resolvedNamespace,
			Queue:        queueName,
			Owner:        owner,
			Stateful:     stateful,
			Visibility:   visibility,
			BlockSeconds: loopBlock,
			PageSize:     pageSize,
		})
		deliveries := []*core.QueueDelivery(nil)
		nextCursor := ""
		if res != nil {
			deliveries = res.Deliveries
			nextCursor = res.NextCursor
		}
		if err != nil {
			cleanup()
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, core.ErrQueueEmpty) {
				hotUntil = time.Time{}
				if deliveredCount == 0 && !headersWritten {
					disconnectStatus = "empty"
					retryAfter := int64(1)
					switch {
					case blockSeconds == api.BlockNoWait:
						retryAfter = 0
					case blockSeconds > 0:
						waitDuration := time.Duration(blockSeconds) * time.Second
						retryAfter = max(int64(math.Ceil(waitDuration.Seconds())), 1)
					}
					return httpError{
						Status:     http.StatusConflict,
						Code:       "waiting",
						Detail:     "no messages available",
						RetryAfter: retryAfter,
					}
				}
				if blockSeconds == api.BlockNoWait {
					disconnectStatus = "empty"
					break
				}
				// wait for more messages
				continue
			}
			disconnectStatus = "error"
			for _, delivery := range deliveries {
				if delivery != nil && delivery.Message != nil {
					h.clearPendingDelivery(resolvedNamespace, queueName, owner, delivery.Message.MessageID)
				}
				if delivery != nil && delivery.Finalize != nil {
					delivery.Finalize(false)
				}
			}
			cleanup()
			return convertCoreError(err)
		}

		for _, delivery := range deliveries {
			if delivery == nil {
				continue
			}
			hotUntil = time.Now().Add(subscribeHotWindow)
			h.trackPendingDelivery(resolvedNamespace, queueName, owner, delivery)
			writeErr := writeDeliveries([]*core.QueueDelivery{delivery}, nextCursor)
			if writeErr != nil {
				if delivery.Message != nil {
					h.clearPendingDelivery(resolvedNamespace, queueName, owner, delivery.Message.MessageID)
				}
				if debugQueueTiming {
					fmt.Fprintf(os.Stderr, "[%s] queue.subscribe.write_error queue=%s mid=%s err=%v\n",
						time.Now().Format(time.RFC3339Nano), queueName, delivery.Message.MessageID, writeErr)
				}
				if queueLogger != nil {
					fields := []any{
						"queue", queueName,
						"owner", owner,
						"error", writeErr,
					}
					if delivery.Message != nil {
						fields = append(fields,
							"mid", delivery.Message.MessageID,
							"lease", delivery.Message.LeaseID,
							"fencing", delivery.Message.FencingToken,
						)
					}
					queueLogger.Warn("queue.subscribe.write_error", fields...)
				}
				if delivery.Finalize != nil {
					delivery.Finalize(false)
				}
				disconnectStatus = "error"
				cleanup()
				return convertCoreError(writeErr)
			}
			if debugQueueTiming {
				fmt.Fprintf(os.Stderr, "[%s] queue.subscribe.delivered queue=%s mid=%s\n",
					time.Now().Format(time.RFC3339Nano), queueName, delivery.Message.MessageID)
			}
			logQueueDeliveryInfo(queueLogger, delivery)
			if delivery.Finalize != nil {
				delivery.Finalize(true)
			}
			deliveredCount++
			queueLogger.Trace("queue.subscribe.delivered", "mid", delivery.Message.MessageID, "cursor", delivery.NextCursor, "delivered", deliveredCount)
		}
		cleanup()
	}

	// If we didn't deliver anything and never wrote headers, return a waiting signal.
	if !headersWritten && deliveredCount == 0 {
		retryAfter := int64(1)
		switch {
		case blockSeconds == api.BlockNoWait:
			retryAfter = 0
		case blockSeconds > 0:
			retryAfter = max(blockSeconds, int64(1))
		}
		return httpError{
			Status:     http.StatusConflict,
			Code:       "waiting",
			Detail:     "no messages available",
			RetryAfter: retryAfter,
		}
	}

	if disconnectStatus == "unknown" {
		disconnectStatus = "complete"
	}

	return nil
}

// handleQueueAck godoc
// @Summary      Acknowledge a delivered message
// @Description  Confirms processing of a delivery and deletes the message or its retry lease.
// @Tags         queue
// @Accept       json
// @Produce      json
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        request  body      api.AckRequest  true  "Acknowledgement payload"
// @Success      200      {object}  api.AckResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/ack [post]
func (h *Handler) handleQueueAck(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.maybeThrottleQueue(qrf.KindQueueAck); err != nil {
		return err
	}
	finish := h.beginQueueAck()
	defer finish()
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var req api.AckRequest
	if err := json.NewDecoder(reqBody).Decode(&req); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: fmt.Sprintf("failed to parse request: %v", err)}
	}
	namespace := strings.TrimSpace(req.Namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(r.URL.Query().Get("namespace"))
	}
	resolvedNamespace, err := h.resolveNamespace(namespace)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	req.Namespace = resolvedNamespace
	h.observeNamespace(resolvedNamespace)
	if req.Queue == "" || req.MessageID == "" || req.LeaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_fields", Detail: "queue, message_id, lease_id are required"}
	}
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	queueLogger := logger.With(
		"namespace", resolvedNamespace,
		"queue", req.Queue,
		"mid", req.MessageID,
		"lease", req.LeaseID,
	)
	if req.StateLeaseID != "" {
		queueLogger = queueLogger.With("state_lease", req.StateLeaseID)
	}
	queueLogger.Debug("queue.ack.begin")

	res, err := h.core.Ack(ctx, core.QueueAckCommand{
		Namespace:         resolvedNamespace,
		Queue:             req.Queue,
		MessageID:         req.MessageID,
		MetaETag:          req.MetaETag,
		StateETag:         req.StateETag,
		LeaseID:           req.LeaseID,
		StateLeaseID:      req.StateLeaseID,
		Stateful:          req.StateLeaseID != "",
		FencingToken:      req.FencingToken,
		StateFencingToken: req.StateFencingToken,
	})
	if err != nil {
		queueLogger.Warn("queue.ack.error", "error", err)
		return convertCoreError(err)
	}

	if res.LeaseOwner != "" {
		h.clearPendingDelivery(resolvedNamespace, req.Queue, res.LeaseOwner, req.MessageID)
	}

	headers := map[string]string{}
	if res.CorrelationID != "" {
		headers[headerCorrelationID] = res.CorrelationID
	}
	queueLogger.Debug("queue.ack.success",
		"owner", res.LeaseOwner,
		"correlation", res.CorrelationID,
	)
	h.writeJSON(w, http.StatusOK, api.AckResponse{Acked: true, CorrelationID: res.CorrelationID}, headers)
	return nil
}

// handleQueueNack godoc
// @Summary      Return a message to the queue
// @Description  Requeues the delivery with optional delay and last error metadata.
// @Tags         queue
// @Accept       json
// @Produce      json
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        request  body      api.NackRequest  true  "Negative acknowledgement payload"
// @Success      200      {object}  api.NackResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/nack [post]
func (h *Handler) handleQueueNack(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.maybeThrottleQueue(qrf.KindQueueAck); err != nil {
		return err
	}
	finish := h.beginQueueAck()
	defer finish()
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var req api.NackRequest
	if err := json.NewDecoder(reqBody).Decode(&req); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: fmt.Sprintf("failed to parse request: %v", err)}
	}
	namespace := strings.TrimSpace(req.Namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(r.URL.Query().Get("namespace"))
	}
	resolvedNamespace, err := h.resolveNamespace(namespace)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	req.Namespace = resolvedNamespace
	h.observeNamespace(resolvedNamespace)
	if req.Queue == "" || req.MessageID == "" || req.LeaseID == "" || req.MetaETag == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_fields", Detail: "queue, message_id, lease_id, meta_etag are required"}
	}
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	queueLogger := logger.With(
		"namespace", resolvedNamespace,
		"queue", req.Queue,
		"mid", req.MessageID,
		"lease", req.LeaseID,
	)
	if req.StateLeaseID != "" {
		queueLogger = queueLogger.With("state_lease", req.StateLeaseID)
	}
	if req.DelaySeconds > 0 {
		queueLogger = queueLogger.With("delay_seconds", req.DelaySeconds)
	}
	queueLogger.Debug("queue.nack.begin")
	delay := time.Duration(req.DelaySeconds) * time.Second
	res, err := h.core.Nack(ctx, core.QueueNackCommand{
		Namespace:         resolvedNamespace,
		Queue:             req.Queue,
		MessageID:         req.MessageID,
		MetaETag:          req.MetaETag,
		LeaseID:           req.LeaseID,
		StateLeaseID:      req.StateLeaseID,
		Stateful:          req.StateLeaseID != "",
		Delay:             delay,
		LastError:         req.LastError,
		FencingToken:      req.FencingToken,
		StateFencingToken: req.StateFencingToken,
	})
	if err != nil {
		queueLogger.Warn("queue.nack.error", "error", err)
		return convertCoreError(err)
	}
	resp := api.NackResponse{
		Requeued:      res.Requeued,
		MetaETag:      res.MetaETag,
		CorrelationID: res.CorrelationID,
	}
	queueLogger.Debug("queue.nack.success",
		"correlation", res.CorrelationID,
	)
	headers := map[string]string{}
	if res.CorrelationID != "" {
		headers[headerCorrelationID] = res.CorrelationID
	}
	h.writeJSON(w, http.StatusOK, resp, headers)
	return nil
}

// handleQueueExtend godoc
// @Summary      Extend a delivery lease
// @Description  Extends the visibility timeout and lease window for an in-flight message.
// @Tags         queue
// @Accept       json
// @Produce      json
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        request  body      api.ExtendRequest  true  "Extend payload"
// @Success      200      {object}  api.ExtendResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/extend [post]
func (h *Handler) handleQueueExtend(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if err := h.maybeThrottleQueue(qrf.KindQueueAck); err != nil {
		return err
	}
	finish := h.beginQueueAck()
	defer finish()
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var req api.ExtendRequest
	if err := json.NewDecoder(reqBody).Decode(&req); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: fmt.Sprintf("failed to parse request: %v", err)}
	}
	namespace := strings.TrimSpace(req.Namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(r.URL.Query().Get("namespace"))
	}
	resolvedNamespace, err := h.resolveNamespace(namespace)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	req.Namespace = resolvedNamespace
	h.observeNamespace(resolvedNamespace)
	if req.Queue == "" || req.MessageID == "" || req.LeaseID == "" || req.MetaETag == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_fields", Detail: "queue, message_id, lease_id, meta_etag are required"}
	}

	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	queueLogger := logger.With(
		"namespace", resolvedNamespace,
		"queue", req.Queue,
		"mid", req.MessageID,
		"lease", req.LeaseID,
		"extend_by_seconds", req.ExtendBySeconds,
	)
	if req.StateLeaseID != "" {
		queueLogger = queueLogger.With("state_lease", req.StateLeaseID)
	}
	queueLogger.Debug("queue.extend.begin")

	extension := time.Duration(req.ExtendBySeconds) * time.Second
	res, err := h.core.Extend(ctx, core.QueueExtendCommand{
		Namespace:         resolvedNamespace,
		Queue:             req.Queue,
		MessageID:         req.MessageID,
		MetaETag:          req.MetaETag,
		LeaseID:           req.LeaseID,
		StateLeaseID:      req.StateLeaseID,
		Stateful:          req.StateLeaseID != "",
		Visibility:        extension,
		FencingToken:      req.FencingToken,
		StateFencingToken: req.StateFencingToken,
	})
	if err != nil {
		queueLogger.Warn("queue.extend.error", "error", err)
		return convertCoreError(err)
	}
	resp := api.ExtendResponse{
		LeaseExpiresAtUnix:       res.LeaseExpiresAtUnix,
		VisibilityTimeoutSeconds: res.VisibilityTimeoutSeconds,
		MetaETag:                 res.MetaETag,
		StateLeaseExpiresAtUnix:  res.StateLeaseExpiresAtUnix,
		CorrelationID:            res.CorrelationID,
	}
	queueLogger.Debug("queue.extend.success",
		"lease_expires_at", resp.LeaseExpiresAtUnix,
		"state_lease_expires_at", resp.StateLeaseExpiresAtUnix,
		"correlation", resp.CorrelationID,
	)
	headers := map[string]string{}
	if resp.CorrelationID != "" {
		headers[headerCorrelationID] = resp.CorrelationID
	}
	h.writeJSON(w, http.StatusOK, resp, headers)
	return nil
}

// handleHealth godoc
// @Summary      Liveness probe
// @Tags         system
// @Produce      plain
// @Success      200  {string}  string  "OK"
// @Router       /healthz [get]
func (h *Handler) handleHealth(w http.ResponseWriter, _ *http.Request) error {
	w.WriteHeader(http.StatusOK)
	return nil
}

// handleReady godoc
// @Summary      Readiness probe
// @Tags         system
// @Produce      plain
// @Success      200  {string}  string  "Ready"
// @Router       /readyz [get]
func (h *Handler) handleReady(w http.ResponseWriter, _ *http.Request) error {
	w.WriteHeader(http.StatusOK)
	return nil
}

func (h *Handler) handleError(ctx context.Context, w http.ResponseWriter, err error) {
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	verbose := logger
	var httpErr httpError
	if errors.As(err, &httpErr) {
		if benchLogErrors {
			fmt.Fprintf(os.Stderr, "[bench-error] code=%s status=%d detail=%s error=%v\n", httpErr.Code, httpErr.Status, httpErr.Detail, err)
		}
		verbose.Debug("http.request.failure",
			"status", httpErr.Status,
			"code", httpErr.Code,
			"detail", httpErr.Detail,
			"version", httpErr.Version,
			"etag", httpErr.ETag,
			"retry_after", httpErr.RetryAfter,
		)
		resp := api.ErrorResponse{
			ErrorCode:         httpErr.Code,
			Detail:            httpErr.Detail,
			CurrentVersion:    httpErr.Version,
			CurrentETag:       httpErr.ETag,
			RetryAfterSeconds: httpErr.RetryAfter,
		}
		headers := map[string]string{}
		if httpErr.RetryAfter > 0 {
			headers["Retry-After"] = strconv.FormatInt(httpErr.RetryAfter, 10)
		}
		if httpErr.Status == http.StatusTooManyRequests && h.qrf != nil {
			headers["X-Lockd-QRF-State"] = h.qrf.State().String()
		}
		h.writeJSON(w, httpErr.Status, resp, headers)
		return
	}
	if benchLogErrors {
		fmt.Fprintf(os.Stderr, "[bench-error] code=internal_error status=500 detail=%v\n", err)
	}
	logger.Error("http.request.panic", "error", err)
	resp := api.ErrorResponse{
		ErrorCode: "internal_error",
		Detail:    "internal server error",
	}
	h.writeJSON(w, http.StatusInternalServerError, resp, nil)
}

func (h *Handler) handleNamespaceConfig(w http.ResponseWriter, r *http.Request) error {
	if h.namespaceConfigs == nil {
		return httpError{
			Status: http.StatusNotImplemented,
			Code:   "namespace_config_unavailable",
			Detail: "namespace configuration endpoints are disabled",
		}
	}
	switch r.Method {
	case http.MethodGet:
		return h.handleNamespaceConfigGet(w, r)
	case http.MethodPost:
		return h.handleNamespaceConfigSet(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		return httpError{
			Status: http.StatusMethodNotAllowed,
			Code:   "method_not_allowed",
			Detail: "supported methods: GET, POST",
		}
	}
}

// handleNamespaceConfigGet godoc
// @Summary      Get namespace configuration
// @Description  Returns query engine preferences for the namespace.
// @Tags         namespace
// @Produce      json
// @Param        namespace  query   string  false  "Namespace override (defaults to server setting)"
// @Success      200        {object}  api.NamespaceConfigResponse
// @Failure      400        {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/namespace [get]
func (h *Handler) handleNamespaceConfigGet(w http.ResponseWriter, r *http.Request) error {
	namespace, err := h.resolveNamespace(r.URL.Query().Get("namespace"))
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	h.observeNamespace(namespace)
	cfg, etag, err := h.namespaceConfigs.Load(r.Context(), namespace)
	if err != nil {
		return fmt.Errorf("load namespace config: %w", err)
	}
	resp := api.NamespaceConfigResponse{
		Namespace: namespace,
		Query: api.NamespaceQueryConfig{
			PreferredEngine: string(cfg.Query.Preferred),
			FallbackEngine:  string(cfg.Query.Fallback),
		},
	}
	headers := map[string]string{}
	if etag != "" {
		headers["ETag"] = etag
	}
	h.writeJSON(w, http.StatusOK, resp, headers)
	return nil
}

// handleNamespaceConfigSet godoc
// @Summary      Update namespace configuration
// @Description  Updates query engine preferences for the namespace.
// @Tags         namespace
// @Accept       json
// @Produce      json
// @Param        namespace  query   string  false  "Namespace override (defaults to server setting)"
// @Param        If-Match   header  string  false  "Conditionally update when the ETag matches"
// @Param        config     body    api.NamespaceConfigRequest  true  "Namespace configuration payload"
// @Success      200        {object}  api.NamespaceConfigResponse
// @Failure      400        {object}  api.ErrorResponse
// @Failure      409        {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/namespace [post]
func (h *Handler) handleNamespaceConfigSet(w http.ResponseWriter, r *http.Request) error {
	namespace, err := h.resolveNamespace(r.URL.Query().Get("namespace"))
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	body := http.MaxBytesReader(w, r.Body, namespaceConfigBodyLimit)
	defer body.Close()
	var payload api.NamespaceConfigRequest
	if err := json.NewDecoder(body).Decode(&payload); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
	}
	if strings.TrimSpace(payload.Namespace) != "" {
		namespace, err = h.resolveNamespace(payload.Namespace)
		if err != nil {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
		}
	}
	if payload.Query == nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace_request", Detail: "query configuration required"}
	}
	h.observeNamespace(namespace)
	cfg, _, err := h.namespaceConfigs.Load(r.Context(), namespace)
	if err != nil {
		return fmt.Errorf("load namespace config: %w", err)
	}
	if val := strings.TrimSpace(payload.Query.PreferredEngine); val != "" {
		cfg.Query.Preferred = search.EngineHint(strings.ToLower(val))
	}
	if val := strings.TrimSpace(payload.Query.FallbackEngine); val != "" {
		cfg.Query.Fallback = namespaces.FallbackMode(strings.ToLower(val))
	}
	expectedETag := strings.Trim(strings.TrimSpace(r.Header.Get("If-Match")), "\"")
	newETag, err := h.namespaceConfigs.Save(r.Context(), namespace, cfg, expectedETag)
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			return httpError{
				Status: http.StatusConflict,
				Code:   "namespace_config_conflict",
				Detail: "namespace configuration changed concurrently",
			}
		}
		return fmt.Errorf("save namespace config: %w", err)
	}
	resp := api.NamespaceConfigResponse{
		Namespace: namespace,
		Query: api.NamespaceQueryConfig{
			PreferredEngine: string(cfg.Query.Preferred),
			FallbackEngine:  string(cfg.Query.Fallback),
		},
	}
	headers := map[string]string{}
	if newETag != "" {
		headers["ETag"] = newETag
	}
	h.writeJSON(w, http.StatusOK, resp, headers)
	return nil
}
