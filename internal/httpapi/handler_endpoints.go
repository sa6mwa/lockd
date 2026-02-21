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
	"sync"
	"time"

	"github.com/rs/xid"
	"go.opentelemetry.io/otel/trace"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/correlation"
	"pkt.systems/lockd/internal/nsauth"
	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/tccluster"
	"pkt.systems/lockd/internal/tcrm"
	"pkt.systems/lockd/internal/txncoord"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lql"
	"pkt.systems/pslog"
)

// When set (used by benchmark harness) emit minimal error details to stderr even
// if the logger is a noop, so failures are observable in no-log runs.
var benchLogErrors = os.Getenv("MEM_LQ_BENCH_LOG_ERRORS") == "1"

// handleAcquire godoc
// @Summary      Acquire an exclusive lease
// @Description  Acquire or wait for an exclusive lease on a key. When block_seconds > 0 the request will long-poll until a lease becomes available or the timeout elapses. Set if_not_exists=true to enforce create-only semantics and fail with already_exists when the key already exists. Returns a compact xid-based `lease_id` and `txn_id` (20-char lowercase base32, e.g. `c5v9d0sl70b3m3q8ndg0`) that must be echoed on write operations. Namespaces starting with `.` are reserved (e.g. `.txns`) and will be rejected.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        key        query    string  false  "Key to acquire when the request body omits it"
// @Param        X-Txn-ID   header   string  false  "Existing transaction identifier (xid) to join a multi-key transaction"
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
	if err := decodeJSONBody(reqBody, &payload, jsonDecodeOptions{disallowUnknowns: true}); err != nil {
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
	if payload.TxnID == "" {
		payload.TxnID = strings.TrimSpace(r.Header.Get("X-Txn-ID"))
	}
	if id := clientIdentityFromContext(ctx); id != "" {
		payload.Owner = fmt.Sprintf("%s/%s", payload.Owner, id)
	}
	namespace, err := h.resolveNamespace(payload.Namespace)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	if err := h.authorizeNamespace(r, namespace, nsauth.AccessAny); err != nil {
		return err
	}
	cmd := core.AcquireCommand{
		Namespace:    namespace,
		Key:          payload.Key,
		Owner:        payload.Owner,
		TTLSeconds:   payload.TTLSeconds,
		BlockSeconds: payload.BlockSecs,
		IfNotExists:  payload.IfNotExists,
		TxnID:        strings.TrimSpace(payload.TxnID),
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
		TxnID:         res.TxnID,
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
// @Description  Refresh an existing lease before it expires. Requires the xid lease identifier minted by Acquire and its fencing token; returns the new expiration timestamp.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        key        query    string  false  "Key override when the request body omits it"
// @Param        request  body      api.KeepAliveRequest  true  "Lease keepalive parameters"
// @Param        X-Fencing-Token  header  string  true   "Fencing token associated with the lease (from Acquire response)"
// @Param        X-Txn-ID   header   string  false  "Transaction identifier (required when the lease is enlisted in a txn)"
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
	if err := decodeJSONBody(reqBody, &payload, jsonDecodeOptions{disallowUnknowns: true}); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
	}
	if payload.Namespace == "" && r.URL.Query().Get("namespace") != "" {
		payload.Namespace = r.URL.Query().Get("namespace")
	}
	namespace, err := h.resolveNamespace(payload.Namespace)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	if err := h.authorizeNamespace(r, namespace, nsauth.AccessAny); err != nil {
		return err
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
	txnID := strings.TrimSpace(payload.TxnID)
	if hdr := strings.TrimSpace(r.Header.Get("X-Txn-ID")); hdr != "" {
		txnID = hdr
	}
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	verbose := logger
	// Prefer cached meta to avoid extra LoadMeta round-trips.
	var knownMeta *storage.Meta
	knownETag := ""
	if cachedMeta, cachedETag, cachedKey, ok := h.leaseSnapshot(payload.LeaseID); ok && cachedKey == storageKey {
		knownMeta = &cachedMeta
		knownETag = cachedETag
	}
	verbose.Debug("keepalive.begin",
		"namespace", namespace,
		"key", key,
		"lease_id", payload.LeaseID,
		"txn_id", txnID,
		"ttl_seconds", ttl.Seconds(),
	)
	res, err := h.core.KeepAlive(ctx, core.KeepAliveCommand{
		Namespace:     namespace,
		Key:           key,
		LeaseID:       payload.LeaseID,
		TTLSeconds:    payload.TTLSeconds,
		TxnID:         txnID,
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
		if res.Meta.Lease != nil && res.Meta.Lease.TxnID != "" {
			txnID = res.Meta.Lease.TxnID
		}
	}
	w.Header().Set(headerFencingToken, strconv.FormatInt(res.FencingToken, 10))
	h.writeJSON(w, http.StatusOK, api.KeepAliveResponse{ExpiresAt: res.ExpiresAt}, nil)
	verbose.Debug("keepalive.success",
		"namespace", namespace,
		"key", key,
		"lease_id", payload.LeaseID,
		"txn_id", txnID,
		"expires_at", res.ExpiresAt,
		"fencing", res.FencingToken,
	)
	return nil
}

// handleTxnReplay godoc
// @Summary      Replay a transaction decision
// @Description  Apply the recorded decision for a transaction (commit or rollback) to its participants. Pending transactions remain unchanged unless expired, in which case they roll back. Idempotent and safe to retry. Requires TC-auth when mTLS is enabled (unless tc-disable-auth is set).
// @Tags         transaction
// @Accept       json
// @Produce      json
// @Param        request  body      api.TxnReplayRequest  true  "Txn replay request"
// @Success      200      {object}  api.TxnReplayResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      403      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/txn/replay [post]
func (h *Handler) handleTxnReplay(w http.ResponseWriter, r *http.Request) error {
	if err := h.requireTCClient(r); err != nil {
		return err
	}
	ctx := r.Context()
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var payload api.TxnReplayRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
	}
	if payload.TxnID == "" {
		payload.TxnID = strings.TrimSpace(r.URL.Query().Get("txn_id"))
	}
	if payload.TxnID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_txn_id", Detail: "txn_id is required"}
	}
	if _, err := xid.FromString(payload.TxnID); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_txn_id", Detail: "txn_id must be an xid"}
	}
	if err := h.authorizeAllNamespaces(r, nsauth.AccessReadWrite); err != nil {
		return err
	}
	start := time.Now()
	txnLogger := logger.With("txn_id", payload.TxnID)
	txnLogger.Debug("txn.rm.replay.begin")
	state, err := h.core.ReplayTxn(ctx, payload.TxnID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			httpErr := httpError{Status: http.StatusNotFound, Code: "txn_not_found", Detail: "transaction not found"}
			logTxnRMError(txnLogger, "txn.rm.replay.error", httpErr, "duration_ms", time.Since(start).Milliseconds())
			return httpErr
		}
		httpErr := convertCoreError(err)
		logTxnRMError(txnLogger, "txn.rm.replay.error", httpErr, "duration_ms", time.Since(start).Milliseconds())
		return httpErr
	}
	txnLogger.Debug("txn.rm.replay.complete",
		"state", state,
		"duration_ms", time.Since(start).Milliseconds(),
	)
	h.writeJSON(w, http.StatusOK, api.TxnReplayResponse{TxnID: payload.TxnID, State: string(state)}, nil)
	return nil
}

// handleTxnDecide godoc
// @Summary      Record a transaction decision
// @Description  Record a transaction decision (pending|commit|rollback). Commit/rollback decisions are applied locally and fan out to RMs. TC-auth only; called by lockd RMs or TC tooling. Any TC endpoint can receive this call; non-leaders forward to the leader, which stamps the tc_term fencing token. Requires TC-auth when mTLS is enabled (unless tc-disable-auth is set).
// @Tags         transaction
// @Accept       json
// @Produce      json
// @Param        request  body      api.TxnDecisionRequest  true  "Txn decision request"
// @Success      200      {object}  api.TxnDecisionResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      403      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      502      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/txn/decide [post]
func (h *Handler) handleTxnDecide(w http.ResponseWriter, r *http.Request) error {
	if err := h.requireTCClient(r); err != nil {
		return err
	}
	ctx := r.Context()
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var payload api.TxnDecisionRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		if !errors.Is(err, io.EOF) {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
		}
	}
	if payload.TxnID == "" {
		payload.TxnID = strings.TrimSpace(r.URL.Query().Get("txn_id"))
	}
	if payload.TxnID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_txn_id", Detail: "txn_id is required"}
	}
	if _, err := xid.FromString(payload.TxnID); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_txn_id", Detail: "txn_id must be an xid"}
	}
	if payload.State == "" {
		payload.State = strings.TrimSpace(r.URL.Query().Get("state"))
	}
	state := core.TxnState(strings.ToLower(strings.TrimSpace(payload.State)))
	switch state {
	case core.TxnStatePending, core.TxnStateCommit, core.TxnStateRollback:
	default:
		return httpError{Status: http.StatusBadRequest, Code: "invalid_state", Detail: "state must be pending|commit|rollback"}
	}
	rec := core.TxnRecord{
		TxnID:         payload.TxnID,
		State:         state,
		ExpiresAtUnix: payload.ExpiresAtUnix,
		TCTerm:        payload.TCTerm,
	}
	if len(payload.Participants) == 0 {
		if err := h.authorizeAllNamespaces(r, nsauth.AccessReadWrite); err != nil {
			return err
		}
	}
	for _, p := range payload.Participants {
		if strings.TrimSpace(p.Namespace) == "" {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: "participant namespace is required and must not start with '.'"}
		}
		namespace, err := h.resolveNamespace(p.Namespace)
		if err != nil {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: fmt.Sprintf("participant namespace %q: %v", p.Namespace, err)}
		}
		if err := h.authorizeNamespace(r, namespace, nsauth.AccessReadWrite); err != nil {
			return err
		}
		if strings.TrimSpace(p.Key) == "" {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_key", Detail: "participant key is required"}
		}
		backendHash := strings.TrimSpace(p.BackendHash)
		if p.BackendHash != "" && backendHash == "" {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_backend_hash", Detail: "participant backend_hash is invalid"}
		}
		rec.Participants = append(rec.Participants, core.TxnParticipant{Namespace: namespace, Key: p.Key, BackendHash: backendHash})
	}
	decided, err := h.core.DecideTxnViaTC(ctx, rec)
	if err != nil {
		var fanoutErr *txncoord.FanoutError
		if errors.As(err, &fanoutErr) {
			return httpError{Status: http.StatusBadGateway, Code: "txn_fanout_failed", Detail: err.Error()}
		}
		return convertCoreError(err)
	}
	h.writeJSON(w, http.StatusOK, api.TxnDecisionResponse{TxnID: payload.TxnID, State: string(decided)}, nil)
	return nil
}

// handleTCLeaseAcquire godoc
// @Summary      Acquire a TC leader lease
// @Description  Acquire a TC leader lease as part of the TC election quorum. Requires TC-auth when mTLS is enabled (unless tc-disable-auth is set).
// @Tags         transaction
// @Accept       json
// @Produce      json
// @Param        request  body      api.TCLeaseAcquireRequest  true  "TC lease acquire request"
// @Success      200      {object}  api.TCLeaseAcquireResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      403      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/tc/lease/acquire [post]
func (h *Handler) handleTCLeaseAcquire(w http.ResponseWriter, r *http.Request) error {
	if err := h.requireTCClient(r); err != nil {
		return err
	}
	store, err := h.tcLeaseStore()
	if err != nil {
		return err
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var payload api.TCLeaseAcquireRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		if !errors.Is(err, io.EOF) {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
		}
	}
	payload.CandidateID = strings.TrimSpace(payload.CandidateID)
	payload.CandidateEndpoint = strings.TrimSpace(payload.CandidateEndpoint)
	if payload.CandidateID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_candidate_id", Detail: "candidate_id is required"}
	}
	if payload.CandidateEndpoint == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_candidate_endpoint", Detail: "candidate_endpoint is required"}
	}
	if payload.Term == 0 {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_term", Detail: "term must be > 0"}
	}
	if payload.TTLMillis <= 0 {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_ttl", Detail: "ttl_ms must be > 0"}
	}
	ttl := time.Duration(payload.TTLMillis) * time.Millisecond
	rec, leaseErr := store.Acquire(h.clock.Now(), payload.CandidateID, payload.CandidateEndpoint, payload.Term, ttl)
	if leaseErr != nil {
		status := http.StatusConflict
		if leaseErr.Code == "tc_invalid_ttl" {
			status = http.StatusBadRequest
		}
		return httpError{Status: status, Code: leaseErr.Code, Detail: leaseErr.Detail, LeaderEndpoint: leaseErr.LeaderEndpoint}
	}
	h.writeJSON(w, http.StatusOK, api.TCLeaseAcquireResponse{
		Granted:        true,
		LeaderID:       rec.LeaderID,
		LeaderEndpoint: rec.LeaderEndpoint,
		Term:           rec.Term,
		ExpiresAtUnix:  rec.ExpiresAt.UnixMilli(),
	}, nil)
	return nil
}

// handleTCLeaseRenew godoc
// @Summary      Renew a TC leader lease
// @Description  Renew a TC leader lease as part of the TC election quorum. Requires TC-auth when mTLS is enabled (unless tc-disable-auth is set).
// @Tags         transaction
// @Accept       json
// @Produce      json
// @Param        request  body      api.TCLeaseRenewRequest  true  "TC lease renew request"
// @Success      200      {object}  api.TCLeaseRenewResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      403      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/tc/lease/renew [post]
func (h *Handler) handleTCLeaseRenew(w http.ResponseWriter, r *http.Request) error {
	if err := h.requireTCClient(r); err != nil {
		return err
	}
	store, err := h.tcLeaseStore()
	if err != nil {
		return err
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var payload api.TCLeaseRenewRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		if !errors.Is(err, io.EOF) {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
		}
	}
	payload.LeaderID = strings.TrimSpace(payload.LeaderID)
	if payload.LeaderID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_leader_id", Detail: "leader_id is required"}
	}
	if payload.Term == 0 {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_term", Detail: "term must be > 0"}
	}
	if payload.TTLMillis <= 0 {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_ttl", Detail: "ttl_ms must be > 0"}
	}
	ttl := time.Duration(payload.TTLMillis) * time.Millisecond
	rec, leaseErr := store.Renew(h.clock.Now(), payload.LeaderID, payload.Term, ttl)
	if leaseErr != nil {
		status := http.StatusConflict
		if leaseErr.Code == "tc_invalid_ttl" {
			status = http.StatusBadRequest
		}
		return httpError{Status: status, Code: leaseErr.Code, Detail: leaseErr.Detail, LeaderEndpoint: leaseErr.LeaderEndpoint}
	}
	h.writeJSON(w, http.StatusOK, api.TCLeaseRenewResponse{
		Renewed:        true,
		LeaderID:       rec.LeaderID,
		LeaderEndpoint: rec.LeaderEndpoint,
		Term:           rec.Term,
		ExpiresAtUnix:  rec.ExpiresAt.UnixMilli(),
	}, nil)
	return nil
}

// handleTCLeaseRelease godoc
// @Summary      Release a TC leader lease
// @Description  Release a TC leader lease as part of the TC election quorum. Requires TC-auth when mTLS is enabled (unless tc-disable-auth is set).
// @Tags         transaction
// @Accept       json
// @Produce      json
// @Param        request  body      api.TCLeaseReleaseRequest  true  "TC lease release request"
// @Success      200      {object}  api.TCLeaseReleaseResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      403      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/tc/lease/release [post]
func (h *Handler) handleTCLeaseRelease(w http.ResponseWriter, r *http.Request) error {
	if err := h.requireTCClient(r); err != nil {
		return err
	}
	store, err := h.tcLeaseStore()
	if err != nil {
		return err
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var payload api.TCLeaseReleaseRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		if !errors.Is(err, io.EOF) {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
		}
	}
	payload.LeaderID = strings.TrimSpace(payload.LeaderID)
	if payload.LeaderID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_leader_id", Detail: "leader_id is required"}
	}
	if payload.Term == 0 {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_term", Detail: "term must be > 0"}
	}
	_, leaseErr := store.Release(h.clock.Now(), payload.LeaderID, payload.Term)
	if leaseErr != nil {
		return httpError{Status: http.StatusConflict, Code: leaseErr.Code, Detail: leaseErr.Detail, LeaderEndpoint: leaseErr.LeaderEndpoint}
	}
	h.writeJSON(w, http.StatusOK, api.TCLeaseReleaseResponse{Released: true}, nil)
	return nil
}

// handleTCLeader godoc
// @Summary      Return TC leader state
// @Description  Returns the observed TC leader state. Requires TC-auth when mTLS is enabled (unless tc-disable-auth is set).
// @Tags         transaction
// @Produce      json
// @Success      200  {object}  api.TCLeaderResponse
// @Failure      403  {object}  api.ErrorResponse
// @Failure      503  {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/tc/leader [get]
func (h *Handler) handleTCLeader(w http.ResponseWriter, r *http.Request) error {
	if err := h.requireTCClient(r); err != nil {
		return err
	}
	if h.tcLeader == nil || !h.tcLeader.Enabled() {
		return httpError{Status: http.StatusServiceUnavailable, Code: "tc_unavailable", Detail: "tc leader election not configured"}
	}
	info := h.tcLeader.Leader(h.clock.Now())
	resp := api.TCLeaderResponse{
		LeaderID:       info.LeaderID,
		LeaderEndpoint: info.LeaderEndpoint,
		Term:           info.Term,
	}
	if !info.ExpiresAt.IsZero() {
		resp.ExpiresAtUnix = info.ExpiresAt.UnixMilli()
	}
	h.writeJSON(w, http.StatusOK, resp, nil)
	return nil
}

// handleTCClusterAnnounce godoc
// @Summary      Announce TC cluster membership
// @Description  Refreshes the caller's membership lease. Requires TC-auth with a server certificate when mTLS is enabled (unless tc-disable-auth is set).
// @Tags         transaction
// @Accept       json
// @Produce      json
// @Param        request  body      api.TCClusterAnnounceRequest  true  "TC cluster announce request"
// @Success      200      {object}  api.TCClusterAnnounceResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      403      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/tc/cluster/announce [post]
func (h *Handler) handleTCClusterAnnounce(w http.ResponseWriter, r *http.Request) error {
	if err := h.requireTCServer(r); err != nil {
		return err
	}
	store, err := h.tcClusterStore()
	if err != nil {
		return err
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var payload api.TCClusterAnnounceRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		if !errors.Is(err, io.EOF) {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
		}
	}
	payload.SelfEndpoint = strings.TrimSpace(payload.SelfEndpoint)
	normalizedEndpoint, endpointErr := tccluster.NormalizeEndpoint(payload.SelfEndpoint)
	if endpointErr != nil {
		code := "invalid_self_endpoint"
		if strings.TrimSpace(payload.SelfEndpoint) == "" {
			code = "missing_self_endpoint"
		}
		return httpError{Status: http.StatusBadRequest, Code: code, Detail: endpointErr.Error()}
	}
	payload.SelfEndpoint = normalizedEndpoint
	peerCert := peerCertificate(r)
	if peerCert == nil {
		return httpError{Status: http.StatusForbidden, Code: "tc_cluster_identity_missing", Detail: "peer certificate identity required"}
	}
	identity := tccluster.IdentityFromCertificate(peerCert)
	if identity == "" {
		return httpError{Status: http.StatusForbidden, Code: "tc_cluster_identity_missing", Detail: "peer certificate identity required"}
	}
	isSelf := h != nil && strings.TrimSpace(h.tcClusterIdentity) != "" && identity == strings.TrimSpace(h.tcClusterIdentity)
	ttl := tccluster.DeriveLeaseTTL(0)
	if h.tcLeader != nil {
		ttl = tccluster.DeriveLeaseTTL(h.tcLeader.LeaseTTL())
	}
	record, err := store.Announce(r.Context(), identity, payload.SelfEndpoint, ttl)
	if err != nil {
		return httpError{Status: http.StatusServiceUnavailable, Code: "tc_cluster_unavailable", Detail: err.Error()}
	}
	active, err := store.Active(r.Context())
	if err != nil {
		return httpError{Status: http.StatusServiceUnavailable, Code: "tc_cluster_unavailable", Detail: err.Error()}
	}
	if err := h.syncTCLeader(active.Endpoints); err != nil {
		return httpError{Status: http.StatusInternalServerError, Code: "tc_cluster_sync_failed", Detail: err.Error()}
	}
	if isSelf && h.tcClusterJoinSelf != nil {
		h.tcClusterJoinSelf()
	}
	h.writeJSON(w, http.StatusOK, api.TCClusterAnnounceResponse{
		Endpoints:     active.Endpoints,
		UpdatedAtUnix: active.UpdatedAtUnix,
		ExpiresAtUnix: record.ExpiresAtUnix,
	}, nil)
	return nil
}

// handleTCClusterLeave godoc
// @Summary      Leave the TC cluster
// @Description  Deletes the caller's membership lease. Requires TC-auth with a server certificate when mTLS is enabled (unless tc-disable-auth is set).
// @Tags         transaction
// @Accept       json
// @Produce      json
// @Param        request  body      api.TCClusterLeaveRequest  true  "TC cluster leave request"
// @Success      200      {object}  api.TCClusterLeaveResponse
// @Failure      403      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/tc/cluster/leave [post]
func (h *Handler) handleTCClusterLeave(w http.ResponseWriter, r *http.Request) error {
	if err := h.requireTCServer(r); err != nil {
		return err
	}
	store, err := h.tcClusterStore()
	if err != nil {
		return err
	}
	identity := tccluster.IdentityFromCertificate(peerCertificate(r))
	if identity == "" {
		return httpError{Status: http.StatusForbidden, Code: "tc_cluster_identity_missing", Detail: "peer certificate identity required"}
	}
	isSelf := h != nil && strings.TrimSpace(h.tcClusterIdentity) != "" && identity == strings.TrimSpace(h.tcClusterIdentity)
	if isSelf && h.tcClusterLeaveSelf != nil {
		h.tcClusterLeaveSelf()
	}
	store.Pause(identity)
	if h.tcLeaveFanout != nil && strings.TrimSpace(r.Header.Get(headerTCLeaveFanout)) == "" {
		if err := h.tcLeaveFanout(r.Context()); err != nil {
			if h.logger != nil {
				h.logger.Warn("tc.cluster.leave.fanout_failed", "error", err)
			}
			if isSelf {
				store.Resume(identity)
				if h.tcClusterJoinSelf != nil {
					h.tcClusterJoinSelf()
				}
			}
			return httpError{Status: http.StatusServiceUnavailable, Code: "tc_cluster_unavailable", Detail: err.Error()}
		}
	}
	if err := store.Leave(r.Context(), identity); err != nil {
		if isSelf {
			store.Resume(identity)
			if h.tcClusterJoinSelf != nil {
				h.tcClusterJoinSelf()
			}
		}
		return httpError{Status: http.StatusServiceUnavailable, Code: "tc_cluster_unavailable", Detail: err.Error()}
	}
	active, err := store.Active(r.Context())
	if err != nil {
		return httpError{Status: http.StatusServiceUnavailable, Code: "tc_cluster_unavailable", Detail: err.Error()}
	}
	if isSelf {
		if h.tcLeader != nil {
			if err := h.tcLeader.SetEndpoints(nil); err != nil {
				return httpError{Status: http.StatusInternalServerError, Code: "tc_cluster_sync_failed", Detail: err.Error()}
			}
		}
	} else {
		if err := h.syncTCLeader(active.Endpoints); err != nil {
			return httpError{Status: http.StatusInternalServerError, Code: "tc_cluster_sync_failed", Detail: err.Error()}
		}
	}
	h.writeJSON(w, http.StatusOK, api.TCClusterLeaveResponse{
		Endpoints:     active.Endpoints,
		UpdatedAtUnix: active.UpdatedAtUnix,
	}, nil)
	return nil
}

// handleTCClusterList godoc
// @Summary      List TC cluster membership
// @Description  List the current active TC cluster membership list. Requires TC-auth when mTLS is enabled (unless tc-disable-auth is set).
// @Tags         transaction
// @Produce      json
// @Success      200  {object}  api.TCClusterListResponse
// @Failure      403  {object}  api.ErrorResponse
// @Failure      503  {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/tc/cluster/list [get]
func (h *Handler) handleTCClusterList(w http.ResponseWriter, r *http.Request) error {
	if err := h.requireTCClient(r); err != nil {
		return err
	}
	store, err := h.tcClusterStore()
	if err != nil {
		return err
	}
	result, err := store.Active(r.Context())
	if err != nil {
		return httpError{Status: http.StatusServiceUnavailable, Code: "tc_cluster_unavailable", Detail: err.Error()}
	}
	resp := api.TCClusterListResponse{
		Endpoints:     result.Endpoints,
		UpdatedAtUnix: result.UpdatedAtUnix,
	}
	h.writeJSON(w, http.StatusOK, resp, nil)
	return nil
}

// handleTCRMRegister godoc
// @Summary      Register an RM endpoint
// @Description  Register an RM endpoint for a backend hash. Replicates across the TC cluster. Requires TC-auth with a server certificate when mTLS is enabled (unless tc-disable-auth is set).
// @Tags         transaction
// @Accept       json
// @Produce      json
// @Param        request  body      api.TCRMRegisterRequest  true  "RM register request"
// @Success      200      {object}  api.TCRMRegisterResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      403      {object}  api.ErrorResponse
// @Failure      502      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/tc/rm/register [post]
func (h *Handler) handleTCRMRegister(w http.ResponseWriter, r *http.Request) error {
	if err := h.requireTCServer(r); err != nil {
		return err
	}
	store, err := h.tcRMStore()
	if err != nil {
		return err
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var payload api.TCRMRegisterRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		if !errors.Is(err, io.EOF) {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
		}
	}
	backendHash := strings.TrimSpace(payload.BackendHash)
	if backendHash == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_backend_hash", Detail: "backend_hash is required"}
	}
	endpoint := strings.TrimSpace(payload.Endpoint)
	normalizedEndpoint, endpointErr := tccluster.NormalizeEndpoint(endpoint)
	if endpointErr != nil {
		code := "invalid_endpoint"
		if endpoint == "" {
			code = "missing_endpoint"
		}
		return httpError{Status: http.StatusBadRequest, Code: code, Detail: endpointErr.Error()}
	}
	endpoint = normalizedEndpoint
	replica := strings.TrimSpace(r.Header.Get(headerTCReplica)) != ""
	var updated tcrm.UpdateResult
	if h.tcRMReplicator != nil {
		if replica {
			updated, err = h.tcRMReplicator.RegisterLocal(r.Context(), backendHash, endpoint)
		} else {
			headers := http.Header{headerTCReplica: []string{"1"}}
			updated, err = h.tcRMReplicator.Register(r.Context(), backendHash, endpoint, headers)
		}
	} else {
		updated, err = store.Register(r.Context(), backendHash, endpoint)
	}
	if err != nil {
		var repErr *tcrm.ReplicationError
		if errors.As(err, &repErr) {
			return httpError{Status: http.StatusBadGateway, Code: "tc_rm_replication_failed", Detail: err.Error()}
		}
		return httpError{Status: http.StatusServiceUnavailable, Code: "tc_rm_unavailable", Detail: err.Error()}
	}
	endpoints, updatedAt := findRMBackend(updated.Members, backendHash)
	h.writeJSON(w, http.StatusOK, api.TCRMRegisterResponse{
		BackendHash:   backendHash,
		Endpoints:     endpoints,
		UpdatedAtUnix: updatedAt,
	}, nil)
	return nil
}

// handleTCRMUnregister godoc
// @Summary      Unregister an RM endpoint
// @Description  Unregister an RM endpoint for a backend hash. Replicates across the TC cluster. Requires TC-auth with a server certificate when mTLS is enabled (unless tc-disable-auth is set).
// @Tags         transaction
// @Accept       json
// @Produce      json
// @Param        request  body      api.TCRMUnregisterRequest  true  "RM unregister request"
// @Success      200      {object}  api.TCRMUnregisterResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      403      {object}  api.ErrorResponse
// @Failure      502      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/tc/rm/unregister [post]
func (h *Handler) handleTCRMUnregister(w http.ResponseWriter, r *http.Request) error {
	if err := h.requireTCServer(r); err != nil {
		return err
	}
	store, err := h.tcRMStore()
	if err != nil {
		return err
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var payload api.TCRMUnregisterRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		if !errors.Is(err, io.EOF) {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
		}
	}
	backendHash := strings.TrimSpace(payload.BackendHash)
	if backendHash == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_backend_hash", Detail: "backend_hash is required"}
	}
	endpoint := strings.TrimSpace(payload.Endpoint)
	normalizedEndpoint, endpointErr := tccluster.NormalizeEndpoint(endpoint)
	if endpointErr != nil {
		code := "invalid_endpoint"
		if endpoint == "" {
			code = "missing_endpoint"
		}
		return httpError{Status: http.StatusBadRequest, Code: code, Detail: endpointErr.Error()}
	}
	endpoint = normalizedEndpoint
	replica := strings.TrimSpace(r.Header.Get(headerTCReplica)) != ""
	var updated tcrm.UpdateResult
	if h.tcRMReplicator != nil {
		if replica {
			updated, err = h.tcRMReplicator.UnregisterLocal(r.Context(), backendHash, endpoint)
		} else {
			headers := http.Header{headerTCReplica: []string{"1"}}
			updated, err = h.tcRMReplicator.Unregister(r.Context(), backendHash, endpoint, headers)
		}
	} else {
		updated, err = store.Unregister(r.Context(), backendHash, endpoint)
	}
	if err != nil {
		var repErr *tcrm.ReplicationError
		if errors.As(err, &repErr) {
			return httpError{Status: http.StatusBadGateway, Code: "tc_rm_replication_failed", Detail: err.Error()}
		}
		return httpError{Status: http.StatusServiceUnavailable, Code: "tc_rm_unavailable", Detail: err.Error()}
	}
	endpoints, updatedAt := findRMBackend(updated.Members, backendHash)
	h.writeJSON(w, http.StatusOK, api.TCRMUnregisterResponse{
		BackendHash:   backendHash,
		Endpoints:     endpoints,
		UpdatedAtUnix: updatedAt,
	}, nil)
	return nil
}

// handleTCRMList godoc
// @Summary      List RM registry entries
// @Description  List RM endpoints by backend hash. Requires TC-auth when mTLS is enabled (unless tc-disable-auth is set).
// @Tags         transaction
// @Produce      json
// @Success      200  {object}  api.TCRMListResponse
// @Failure      403  {object}  api.ErrorResponse
// @Failure      503  {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/tc/rm/list [get]
func (h *Handler) handleTCRMList(w http.ResponseWriter, r *http.Request) error {
	if err := h.requireTCClient(r); err != nil {
		return err
	}
	store, err := h.tcRMStore()
	if err != nil {
		return err
	}
	result, err := store.Load(r.Context())
	if err != nil {
		return httpError{Status: http.StatusServiceUnavailable, Code: "tc_rm_unavailable", Detail: err.Error()}
	}
	resp := api.TCRMListResponse{
		Backends:      buildRMBackends(result.Members),
		UpdatedAtUnix: result.Members.UpdatedAtUnix,
	}
	h.writeJSON(w, http.StatusOK, resp, nil)
	return nil
}

func findRMBackend(members tcrm.Members, backendHash string) ([]string, int64) {
	for _, backend := range members.Backends {
		if backend.BackendHash == backendHash {
			return append([]string(nil), backend.Endpoints...), backend.UpdatedAtUnix
		}
	}
	return nil, 0
}

func buildRMBackends(members tcrm.Members) []api.TCRMBackend {
	if len(members.Backends) == 0 {
		return nil
	}
	out := make([]api.TCRMBackend, 0, len(members.Backends))
	for _, backend := range members.Backends {
		out = append(out, api.TCRMBackend{
			BackendHash:   backend.BackendHash,
			Endpoints:     append([]string(nil), backend.Endpoints...),
			UpdatedAtUnix: backend.UpdatedAtUnix,
		})
	}
	return out
}

// handleTxnCommit godoc
// @Summary      Apply a commit decision on an RM
// @Description  Apply a commit decision for a transaction on the local RM. Requires tc_term when TC leadership is enabled. Requires TC-auth when mTLS is enabled (unless tc-disable-auth is set).
// @Tags         transaction
// @Accept       json
// @Produce      json
// @Param        request  body      api.TxnDecisionRequest  true  "Txn decision request"
// @Success      200      {object}  api.TxnDecisionResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      403      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/txn/commit [post]
func (h *Handler) handleTxnCommit(w http.ResponseWriter, r *http.Request) error {
	return h.handleTxnApply(w, r, core.TxnStateCommit)
}

// handleTxnRollback godoc
// @Summary      Apply a rollback decision on an RM
// @Description  Apply a rollback decision for a transaction on the local RM. Requires tc_term when TC leadership is enabled. Requires TC-auth when mTLS is enabled (unless tc-disable-auth is set).
// @Tags         transaction
// @Accept       json
// @Produce      json
// @Param        request  body      api.TxnDecisionRequest  true  "Txn decision request"
// @Success      200      {object}  api.TxnDecisionResponse
// @Failure      400      {object}  api.ErrorResponse
// @Failure      403      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/txn/rollback [post]
func (h *Handler) handleTxnRollback(w http.ResponseWriter, r *http.Request) error {
	return h.handleTxnApply(w, r, core.TxnStateRollback)
}

// handleTxnApply applies a decided transaction on an RM.
func (h *Handler) handleTxnApply(w http.ResponseWriter, r *http.Request, decision core.TxnState) error {
	if err := h.requireTCClient(r); err != nil {
		return err
	}
	ctx := r.Context()
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	reqBody := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer reqBody.Close()
	var payload api.TxnDecisionRequest
	if err := json.NewDecoder(reqBody).Decode(&payload); err != nil {
		if !errors.Is(err, io.EOF) {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
		}
	}
	if payload.TxnID == "" {
		payload.TxnID = strings.TrimSpace(r.URL.Query().Get("txn_id"))
	}
	if payload.TxnID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_txn_id", Detail: "txn_id is required"}
	}
	if _, err := xid.FromString(payload.TxnID); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_txn_id", Detail: "txn_id must be an xid"}
	}
	if h.tcLeader != nil && h.tcLeader.Enabled() {
		info, ok := h.currentTCLeader(h.clock.Now())
		if !ok {
			return httpError{Status: http.StatusServiceUnavailable, Code: "tc_unavailable", Detail: "tc leader unavailable"}
		}
		if payload.TCTerm == 0 {
			return httpError{Status: http.StatusBadRequest, Code: "tc_term_required", Detail: "tc_term is required"}
		}
		if payload.TCTerm != info.Term {
			return httpError{Status: http.StatusConflict, Code: "tc_term_stale", Detail: "tc_term does not match current leader term", LeaderEndpoint: info.LeaderEndpoint}
		}
	}
	start := time.Now()
	txnLogger := logger.With("txn_id", payload.TxnID, "state", decision, "tc_term", payload.TCTerm)
	targetHash := strings.TrimSpace(payload.TargetBackendHash)
	if targetHash != "" {
		localHash := strings.TrimSpace(h.core.BackendHash())
		if localHash == "" {
			httpErr := httpError{Status: http.StatusServiceUnavailable, Code: "backend_hash_unavailable", Detail: "backend hash unavailable"}
			logTxnRMError(txnLogger, "txn.rm.apply.error", httpErr, "duration_ms", time.Since(start).Milliseconds())
			return httpErr
		}
		if targetHash != localHash {
			httpErr := httpError{Status: http.StatusConflict, Code: "txn_backend_mismatch", Detail: fmt.Sprintf("target backend hash %s does not match local backend", targetHash)}
			logTxnRMError(txnLogger, "txn.rm.apply.error", httpErr, "duration_ms", time.Since(start).Milliseconds())
			return httpErr
		}
	}
	rec := core.TxnRecord{
		TxnID:         payload.TxnID,
		State:         decision,
		ExpiresAtUnix: payload.ExpiresAtUnix,
		TCTerm:        payload.TCTerm,
	}
	if len(payload.Participants) == 0 {
		if err := h.authorizeAllNamespaces(r, nsauth.AccessReadWrite); err != nil {
			return err
		}
	}
	for _, p := range payload.Participants {
		if strings.TrimSpace(p.Namespace) == "" {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: "participant namespace is required and must not start with '.'"}
		}
		namespace, err := h.resolveNamespace(p.Namespace)
		if err != nil {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: fmt.Sprintf("participant namespace %q: %v", p.Namespace, err)}
		}
		if err := h.authorizeNamespace(r, namespace, nsauth.AccessReadWrite); err != nil {
			return err
		}
		if strings.TrimSpace(p.Key) == "" {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_key", Detail: "participant key is required"}
		}
		backendHash := strings.TrimSpace(p.BackendHash)
		if p.BackendHash != "" && backendHash == "" {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_backend_hash", Detail: "participant backend_hash is invalid"}
		}
		rec.Participants = append(rec.Participants, core.TxnParticipant{Namespace: namespace, Key: p.Key, BackendHash: backendHash})
	}
	if targetHash != "" {
		txnLogger = txnLogger.With("target_backend_hash", targetHash)
	}
	txnLogger = txnLogger.With("participants", len(rec.Participants))
	txnLogger.Debug("txn.rm.apply.begin")

	if len(rec.Participants) == 0 {
		state, err := h.core.ApplyTxnDecision(ctx, payload.TxnID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				httpErr := httpError{Status: http.StatusNotFound, Code: "txn_not_found", Detail: "transaction not found"}
				logTxnRMError(txnLogger, "txn.rm.apply.error", httpErr, "duration_ms", time.Since(start).Milliseconds())
				return httpErr
			}
			httpErr := convertCoreError(err)
			logTxnRMError(txnLogger, "txn.rm.apply.error", httpErr, "duration_ms", time.Since(start).Milliseconds())
			return httpErr
		}
		if state == core.TxnStatePending {
			httpErr := httpError{Status: http.StatusConflict, Code: "txn_pending", Detail: "transaction decision not recorded"}
			logTxnRMError(txnLogger, "txn.rm.apply.error", httpErr, "duration_ms", time.Since(start).Milliseconds())
			return httpErr
		}
		txnLogger.Debug("txn.rm.apply.complete",
			"applied_state", state,
			"duration_ms", time.Since(start).Milliseconds(),
		)
		h.writeJSON(w, http.StatusOK, api.TxnDecisionResponse{TxnID: payload.TxnID, State: string(state)}, nil)
		return nil
	}

	var (
		state core.TxnState
		err   error
	)
	switch decision {
	case core.TxnStateCommit:
		state, err = h.core.CommitTxn(ctx, rec)
	case core.TxnStateRollback:
		state, err = h.core.RollbackTxn(ctx, rec)
	default:
		return httpError{Status: http.StatusBadRequest, Code: "invalid_state", Detail: "state must be commit|rollback"}
	}
	if err != nil {
		httpErr := convertCoreError(err)
		logTxnRMError(txnLogger, "txn.rm.apply.error", httpErr, "duration_ms", time.Since(start).Milliseconds())
		return httpErr
	}
	txnLogger.Debug("txn.rm.apply.complete",
		"applied_state", state,
		"duration_ms", time.Since(start).Milliseconds(),
	)
	h.writeJSON(w, http.StatusOK, api.TxnDecisionResponse{TxnID: payload.TxnID, State: string(state)}, nil)
	return nil
}

// handleRelease godoc
// @Summary      Release a held lease
// @Description  Releases the lease associated with the provided key and lease identifier. The request must include the xid `txn_id` from Acquire; set `rollback=true` to abandon staged changes, otherwise the release commits the staged state.
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
	if err := decodeJSONBody(reqBody, &payload, jsonDecodeOptions{disallowUnknowns: true}); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
	}
	if payload.Namespace == "" && r.URL.Query().Get("namespace") != "" {
		payload.Namespace = r.URL.Query().Get("namespace")
	}
	namespace, err := h.resolveNamespace(payload.Namespace)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	if err := h.authorizeNamespace(r, namespace, nsauth.AccessAny); err != nil {
		return err
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
	if strings.TrimSpace(payload.TxnID) == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_txn", Detail: "txn_id required"}
	}
	logger := pslog.LoggerFromContext(ctx)
	if logger == nil {
		logger = h.logger
	}
	verbose := logger
	verbose.Debug("release.begin", "namespace", namespace, "key", payload.Key, "lease_id", payload.LeaseID, "txn_id", payload.TxnID)
	var knownMeta *storage.Meta
	knownETag := ""
	res, err := h.core.Release(ctx, core.ReleaseCommand{
		Namespace:     namespace,
		Key:           payload.Key,
		LeaseID:       payload.LeaseID,
		TxnID:         payload.TxnID,
		Rollback:      payload.Rollback,
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
	verbose.Debug("release.success", "namespace", namespace, "key", payload.Key, "lease_id", payload.LeaseID, "txn_id", payload.TxnID)
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
	namespace, err := h.resolveNamespace(r.URL.Query().Get("namespace"))
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	requiredAccess := nsauth.AccessAny
	if publicRead {
		requiredAccess = nsauth.AccessRead
	}
	if err := h.authorizeNamespace(r, namespace, requiredAccess); err != nil {
		return err
	}
	res, err := h.core.Get(r.Context(), core.GetCommand{
		Namespace:    namespace,
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
// @Description  Executes a selector-based search within a namespace. When return=keys, responds with a JSON envelope containing keys + cursor. When return=documents, streams NDJSON rows (one document per line).
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
// @Success      200        {object}  api.QueryResponse  "Keys response when return=keys"
// @Success      200        {string}  string            "NDJSON stream when return=documents"
// @Failure      400        {object} api.ErrorResponse
// @Failure      404        {object} api.ErrorResponse
// @Failure      409        {object} api.ErrorResponse
// @Failure      503        {object} api.ErrorResponse
// @Router       /v1/query [post]
func (h *Handler) handleQuery(w http.ResponseWriter, r *http.Request) error {
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
	if err := h.authorizeNamespace(r, namespace, nsauth.AccessRead); err != nil {
		return err
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
	limit := int64(indexFlushBodyLimit)
	if h.jsonMaxBytes > 0 && h.jsonMaxBytes < limit {
		limit = h.jsonMaxBytes
	}
	reqBody := http.MaxBytesReader(w, r.Body, limit)
	defer reqBody.Close()
	var payload api.IndexFlushRequest
	if err := decodeJSONBody(reqBody, &payload, jsonDecodeOptions{
		allowEmpty:       true,
		disallowUnknowns: true,
	}); err != nil {
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
	if err := h.authorizeNamespace(r, resolved, nsauth.AccessReadWrite); err != nil {
		return err
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
		flushID, deduped, reserveErr := h.reserveAsyncIndexFlush(resolved)
		if reserveErr != nil {
			return reserveErr
		}
		pending := h.indexControl.Pending(resolved)
		logger := pslog.LoggerFromContext(r.Context())
		if logger != nil {
			logger.Debug("index.flush.async_scheduled", "namespace", resolved, "flush_id", flushID, "deduped", deduped)
		}
		if deduped {
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
		go func(ns, reqID string) {
			defer h.releaseAsyncIndexFlush(ns, reqID)
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
	_ = h.indexControl.WarmNamespace(r.Context(), resolved)
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
// @Param        X-Lease-ID         header  string  true   "Lease identifier (xid from Acquire, 20-char lowercase base32)"
// @Param        X-Txn-ID           header  string  true   "Transaction identifier (xid from Acquire)"
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
	if err := h.authorizeNamespace(r, namespace, nsauth.AccessWrite); err != nil {
		return err
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
	txnID := strings.TrimSpace(r.Header.Get("X-Txn-ID"))
	if txnID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_txn", Detail: "X-Txn-ID required"}
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
		TxnID:          txnID,
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
// @Param        X-Lease-ID         header  string  true   "Lease identifier (xid from Acquire, 20-char lowercase base32)"
// @Param        X-Txn-ID           header  string  true   "Transaction identifier (xid from Acquire)"
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
	txnID := strings.TrimSpace(r.Header.Get("X-Txn-ID"))
	if txnID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_txn", Detail: "X-Txn-ID required"}
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
	if err := h.authorizeNamespace(r, ns, nsauth.AccessWrite); err != nil {
		return err
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
		TxnID:         txnID,
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

func logTxnRMError(logger pslog.Logger, event string, err error, fields ...any) {
	if logger == nil || err == nil {
		return
	}
	fields = append(fields, "error", err)
	if httpErr, ok := err.(httpError); ok && httpErr.Status >= http.StatusBadRequest && httpErr.Status < http.StatusInternalServerError {
		logger.Debug(event, fields...)
		return
	}
	logger.Warn(event, fields...)
}

// handleRemove godoc
// @Summary      Delete the JSON state for a key
// @Description  Removes the stored state blob if the caller holds the lease. Optional CAS headers guard against concurrent updates.
// @Tags         lease
// @Accept       json
// @Produce      json
// @Param        namespace        query   string  false  "Namespace override (defaults to server setting)"
// @Param        key              query   string  true   "Lease key"
// @Param        X-Lease-ID       header  string  true   "Lease identifier (xid from Acquire, 20-char lowercase base32)"
// @Param        X-Txn-ID         header  string  true   "Transaction identifier (xid from Acquire)"
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
	if err := h.authorizeNamespace(r, namespace, nsauth.AccessWrite); err != nil {
		return err
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
	txnID := strings.TrimSpace(r.Header.Get("X-Txn-ID"))
	if txnID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_txn", Detail: "X-Txn-ID required"}
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
		TxnID:         txnID,
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
	namespace, err := h.resolveNamespace(r.URL.Query().Get("namespace"))
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	if err := h.authorizeNamespace(r, namespace, nsauth.AccessRead); err != nil {
		return err
	}
	res, err := h.core.Describe(r.Context(), core.DescribeCommand{
		Namespace: namespace,
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
	if err := h.authorizeNamespace(r, resolvedNamespace, nsauth.AccessWrite); err != nil {
		return err
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
		h.queueDisp.NotifyAt(resolvedNamespace, msg.Queue, msg.ID, msg.NotVisibleUntil)
	}
	enqueueLogger.Debug("queue.enqueue.success",
		"message_id", msg.ID,
		"payload_bytes", msg.PayloadBytes,
		"attempts", msg.Attempts,
		"max_attempts", msg.MaxAttempts,
		"failure_attempts", msg.FailureAttempts,
		"not_visible_until", msg.NotVisibleUntil.Unix(),
		"visibility_timeout", msg.Visibility.Seconds(),
	)
	resp := api.EnqueueResponse{
		Namespace:                msg.Namespace,
		Queue:                    msg.Queue,
		MessageID:                msg.ID,
		Attempts:                 msg.Attempts,
		MaxAttempts:              msg.MaxAttempts,
		FailureAttempts:          msg.FailureAttempts,
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
// @Description  Dequeues one or more messages from the specified queue. Supports long polling via wait_seconds. If txn_id is provided, the message is enlisted as a transaction participant.
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
	if err := decodeJSONBody(reqBody, &req, jsonDecodeOptions{
		allowEmpty:       true,
		disallowUnknowns: true,
	}); err != nil {
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
	if err := h.authorizeNamespace(r, resolvedNamespace, nsauth.AccessRead); err != nil {
		return err
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
		TxnID:        req.TxnID,
		Stateful:     false,
		Visibility:   visibility,
		BlockSeconds: blockSeconds,
		PageSize:     pageSize,
	}
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
// @Summary      Fetch queue messages with state sidecars
// @Description  Dequeues messages and includes their associated state blobs in a multipart/related response when available. If txn_id is provided, the message and state sidecar are enlisted as transaction participants.
// @Tags         queue
// @Accept       json
// @Produce      multipart/related
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        queue      query    string  false  "Queue name override when the request body omits it"
// @Param        owner      query    string  false  "Owner override when the request body omits it"
// @Param        request  body      api.DequeueRequest  true  "Dequeue parameters"
// @Success      200      {string}  string  "multipart/related stream: message metadata, payload, optional state sidecar"
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
	if err := decodeJSONBody(reqBody, &req, jsonDecodeOptions{
		allowEmpty:       true,
		disallowUnknowns: true,
	}); err != nil {
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
	if err := h.authorizeNamespace(r, resolvedNamespace, nsauth.AccessRead); err != nil {
		return err
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
		TxnID:        req.TxnID,
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
// @Description  Opens a long-lived multipart/related stream of deliveries for the specified queue owner. Each part contains message metadata and payload. If txn_id is provided, each delivery is enlisted as a transaction participant.
// @Tags         queue
// @Accept       json
// @Produce      multipart/related
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        queue      query    string  false  "Queue name override when the request body omits it"
// @Param        owner      query    string  false  "Owner override when the request body omits it"
// @Param        request  body      api.DequeueRequest  true  "Subscription parameters (queue, owner, wait_seconds, visibility)"
// @Success      200      {string}  string  "multipart/related stream: message metadata + payload"
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
// @Description  Opens a long-lived multipart/related stream where each part contains message metadata, payload, and state snapshot when available. If txn_id is provided, the message and state sidecar are enlisted as transaction participants.
// @Tags         queue
// @Accept       json
// @Produce      multipart/related
// @Param        namespace  query    string  false  "Namespace override when the request body omits it"
// @Param        queue      query    string  false  "Queue name override when the request body omits it"
// @Param        owner      query    string  false  "Owner override when the request body omits it"
// @Param        request  body      api.DequeueRequest  true  "Subscription parameters (queue, owner, wait_seconds, visibility)"
// @Success      200      {string}  string  "multipart/related stream: message metadata, payload, optional state snapshot"
// @Failure      400      {object}  api.ErrorResponse
// @Failure      404      {object}  api.ErrorResponse
// @Failure      409      {object}  api.ErrorResponse
// @Failure      503      {object}  api.ErrorResponse
// @Security     mTLS
// @Router       /v1/queue/subscribeWithState [post]
func (h *Handler) handleQueueSubscribeWithState(w http.ResponseWriter, r *http.Request) error {
	return h.handleQueueSubscribeInternal(w, r, true)
}

type deliverySlice struct {
	items []*core.QueueDelivery
}

var deliverySlicePool sync.Pool

func getSingleDeliverySlice(d *core.QueueDelivery) []*core.QueueDelivery {
	if v, ok := deliverySlicePool.Get().(*deliverySlice); ok {
		if cap(v.items) == 0 {
			v.items = make([]*core.QueueDelivery, 1)
		}
		v.items = v.items[:1]
		v.items[0] = d
		return v.items
	}
	return []*core.QueueDelivery{d}
}

func putSingleDeliverySlice(items []*core.QueueDelivery) {
	if len(items) != 1 {
		return
	}
	items[0] = nil
	deliverySlicePool.Put(&deliverySlice{items: items})
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
	if err := decodeJSONBody(reqBody, &req, jsonDecodeOptions{
		allowEmpty:       true,
		disallowUnknowns: true,
	}); err != nil {
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
	if err := h.authorizeNamespace(r, resolvedNamespace, nsauth.AccessRead); err != nil {
		return err
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
			TxnID:        req.TxnID,
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
			single := getSingleDeliverySlice(delivery)
			writeErr := writeDeliveries(single, nextCursor)
			putSingleDeliverySlice(single)
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

func (h *Handler) lookupQueueTxnID(ctx context.Context, namespace, queueName, messageID string) string {
	key, err := queue.MessageLeaseKey(namespace, queueName, messageID)
	if err != nil {
		return ""
	}
	rel := relativeKey(namespace, key)
	desc, err := h.core.Describe(ctx, core.DescribeCommand{
		Namespace: namespace,
		Key:       rel,
	})
	if err != nil || desc == nil || desc.Meta == nil || desc.Meta.Lease == nil {
		return ""
	}
	return desc.Meta.Lease.TxnID
}

// handleQueueAck godoc
// @Summary      Acknowledge a delivered message
// @Description  Confirms processing of a delivery and deletes the message or its retry lease. If a txn_id is present (or the lease is already enlisted) and TC is enabled, this records a commit decision via the TC.
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
	if err := decodeJSONBody(reqBody, &req, jsonDecodeOptions{disallowUnknowns: true}); err != nil {
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
	if err := h.authorizeNamespace(r, resolvedNamespace, nsauth.AccessRead); err != nil {
		return err
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
	txnID := ""
	queueLogger.Debug("queue.ack.begin")
	res, err := h.core.Ack(ctx, core.QueueAckCommand{
		Namespace:         resolvedNamespace,
		Queue:             req.Queue,
		MessageID:         req.MessageID,
		TxnID:             req.TxnID,
		MetaETag:          req.MetaETag,
		StateETag:         req.StateETag,
		LeaseID:           req.LeaseID,
		StateLeaseID:      req.StateLeaseID,
		Stateful:          req.StateLeaseID != "",
		FencingToken:      req.FencingToken,
		StateFencingToken: req.StateFencingToken,
	})
	if err != nil {
		if txnID == "" {
			txnID = h.lookupQueueTxnID(ctx, resolvedNamespace, req.Queue, req.MessageID)
		}
		queueLogger.With("txn_id", txnID).Warn("queue.ack.error", "error", err)
		return convertCoreError(err)
	}
	if res != nil && res.TxnID != "" {
		txnID = res.TxnID
	}

	if res.LeaseOwner != "" {
		h.clearPendingDelivery(resolvedNamespace, req.Queue, res.LeaseOwner, req.MessageID)
	}

	headers := map[string]string{}
	if res.CorrelationID != "" {
		headers[headerCorrelationID] = res.CorrelationID
	}
	queueLogger = queueLogger.With("txn_id", txnID)
	queueLogger.Debug("queue.ack.success",
		"owner", res.LeaseOwner,
		"correlation", res.CorrelationID,
	)
	h.writeJSON(w, http.StatusOK, api.AckResponse{Acked: true, CorrelationID: res.CorrelationID}, headers)
	return nil
}

// handleQueueNack godoc
// @Summary      Return a message to the queue
// @Description  Requeues the delivery with optional delay and last error metadata (intent=failure only). Set intent=defer to requeue intentionally without consuming max_attempts failure budget. If a txn_id is present (or the lease is already enlisted) and TC is enabled, this records a rollback decision via the TC.
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
	if err := decodeJSONBody(reqBody, &req, jsonDecodeOptions{disallowUnknowns: true}); err != nil {
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
	if err := h.authorizeNamespace(r, resolvedNamespace, nsauth.AccessRead); err != nil {
		return err
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
	if req.Intent != "" {
		queueLogger = queueLogger.With("intent", req.Intent)
	}
	txnID := ""
	queueLogger.Debug("queue.nack.begin")
	delay := time.Duration(req.DelaySeconds) * time.Second
	res, err := h.core.Nack(ctx, core.QueueNackCommand{
		Namespace:         resolvedNamespace,
		Queue:             req.Queue,
		MessageID:         req.MessageID,
		TxnID:             req.TxnID,
		MetaETag:          req.MetaETag,
		LeaseID:           req.LeaseID,
		StateLeaseID:      req.StateLeaseID,
		Stateful:          req.StateLeaseID != "",
		Delay:             delay,
		Intent:            core.QueueNackIntent(req.Intent),
		LastError:         req.LastError,
		FencingToken:      req.FencingToken,
		StateFencingToken: req.StateFencingToken,
	})
	if err != nil {
		if txnID == "" {
			txnID = h.lookupQueueTxnID(ctx, resolvedNamespace, req.Queue, req.MessageID)
		}
		queueLogger.With("txn_id", txnID).Warn("queue.nack.error", "error", err)
		return convertCoreError(err)
	}
	if res != nil && res.TxnID != "" {
		txnID = res.TxnID
	}
	resp := api.NackResponse{
		Requeued:      res.Requeued,
		MetaETag:      res.MetaETag,
		CorrelationID: res.CorrelationID,
	}
	queueLogger = queueLogger.With("txn_id", txnID)
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
// @Description  Extends the visibility timeout and lease window for an in-flight message. txn_id is optional; if omitted the server uses the lease's txn_id (if any).
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
	if err := decodeJSONBody(reqBody, &req, jsonDecodeOptions{disallowUnknowns: true}); err != nil {
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
	if err := h.authorizeNamespace(r, resolvedNamespace, nsauth.AccessRead); err != nil {
		return err
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
	txnID := ""
	queueLogger.Debug("queue.extend.begin")

	extension := time.Duration(req.ExtendBySeconds) * time.Second
	res, err := h.core.Extend(ctx, core.QueueExtendCommand{
		Namespace:         resolvedNamespace,
		Queue:             req.Queue,
		MessageID:         req.MessageID,
		TxnID:             req.TxnID,
		MetaETag:          req.MetaETag,
		LeaseID:           req.LeaseID,
		StateLeaseID:      req.StateLeaseID,
		Stateful:          req.StateLeaseID != "",
		Visibility:        extension,
		FencingToken:      req.FencingToken,
		StateFencingToken: req.StateFencingToken,
	})
	if err != nil {
		if txnID == "" {
			txnID = h.lookupQueueTxnID(ctx, resolvedNamespace, req.Queue, req.MessageID)
		}
		queueLogger.With("txn_id", txnID).Warn("queue.extend.error", "error", err)
		return convertCoreError(err)
	}
	if res != nil && res.TxnID != "" {
		txnID = res.TxnID
	}
	resp := api.ExtendResponse{
		LeaseExpiresAtUnix:       res.LeaseExpiresAtUnix,
		VisibilityTimeoutSeconds: res.VisibilityTimeoutSeconds,
		MetaETag:                 res.MetaETag,
		StateLeaseExpiresAtUnix:  res.StateLeaseExpiresAtUnix,
		CorrelationID:            res.CorrelationID,
	}
	queueLogger = queueLogger.With("txn_id", txnID)
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
			"leader_endpoint", httpErr.LeaderEndpoint,
			"version", httpErr.Version,
			"etag", httpErr.ETag,
			"retry_after", httpErr.RetryAfter,
		)
		resp := api.ErrorResponse{
			ErrorCode:         httpErr.Code,
			Detail:            httpErr.Detail,
			LeaderEndpoint:    httpErr.LeaderEndpoint,
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
	if err := h.authorizeNamespace(r, namespace, nsauth.AccessReadWrite); err != nil {
		return err
	}
	h.observeNamespace(namespace)
	loadRes, err := h.namespaceConfigs.Load(r.Context(), namespace)
	if err != nil {
		return fmt.Errorf("load namespace config: %w", err)
	}
	cfg := loadRes.Config
	etag := loadRes.ETag
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
	if err := decodeJSONBody(body, &payload, jsonDecodeOptions{disallowUnknowns: true}); err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_body", Detail: err.Error()}
	}
	if strings.TrimSpace(payload.Namespace) != "" {
		namespace, err = h.resolveNamespace(payload.Namespace)
		if err != nil {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
		}
	}
	if err := h.authorizeNamespace(r, namespace, nsauth.AccessReadWrite); err != nil {
		return err
	}
	if payload.Query == nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace_request", Detail: "query configuration required"}
	}
	h.observeNamespace(namespace)
	loadRes, err := h.namespaceConfigs.Load(r.Context(), namespace)
	if err != nil {
		return fmt.Errorf("load namespace config: %w", err)
	}
	cfg := loadRes.Config
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

// handleAttachments godoc
// @Summary      Manage state attachments
// @Description  Upload, list, or delete attachments for a key depending on the HTTP method.
// @Tags         lease
// @Accept       octet-stream
// @Produce      json
// @Param        namespace          query   string  false  "Namespace override (defaults to server setting)"
// @Param        key                query   string  true   "Lease key"
// @Param        name               query   string  false  "Attachment name (upload/delete)"
// @Param        id                 query   string  false  "Attachment id (delete)"
// @Param        public             query   bool    false  "Set to true to read without a lease (list)"
// @Param        content_type       query   string  false  "Attachment content type (upload)"
// @Param        max_bytes          query   int64   false  "Maximum attachment size (0 = unlimited)"
// @Param        prevent_overwrite  query   bool    false  "Do not overwrite existing attachments (upload)"
// @Param        X-Lease-ID         header  string false  "Lease identifier (required unless public=1)"
// @Param        X-Txn-ID           header  string false  "Transaction identifier (required for lease operations)"
// @Param        X-Fencing-Token    header  string false  "Optional fencing token proof"
// @Success      200                {object} api.AttachmentListResponse
// @Failure      400                {object} api.ErrorResponse
// @Failure      404                {object} api.ErrorResponse
// @Failure      409                {object} api.ErrorResponse
// @Security     mTLS
// @Router       /v1/attachments [get]
// @Router       /v1/attachments [post]
// @Router       /v1/attachments [delete]
func (h *Handler) handleAttachments(w http.ResponseWriter, r *http.Request) error {
	switch r.Method {
	case http.MethodGet:
		return h.handleAttachmentList(w, r)
	case http.MethodPost:
		return h.handleAttachmentUpload(w, r)
	case http.MethodDelete:
		return h.handleAttachmentDeleteAll(w, r)
	default:
		return httpError{Status: http.StatusMethodNotAllowed, Code: "method_not_allowed", Detail: "unsupported method"}
	}
}

// handleAttachment godoc
// @Summary      Retrieve or delete a state attachment
// @Description  Streams a single attachment (GET) or stages its deletion (DELETE).
// @Tags         lease
// @Accept       json
// @Produce      octet-stream
// @Param        namespace          query   string  false  "Namespace override (defaults to server setting)"
// @Param        key                query   string  true   "Lease key"
// @Param        name               query   string  false  "Attachment name"
// @Param        id                 query   string  false  "Attachment id"
// @Param        public             query   bool    false  "Set to true to read without a lease (GET)"
// @Param        X-Lease-ID         header  string false  "Lease identifier (required unless public=1)"
// @Param        X-Txn-ID           header  string false  "Transaction identifier (required for lease operations)"
// @Param        X-Fencing-Token    header  string false  "Optional fencing token proof"
// @Success      200                {string}  string  "Attachment payload"
// @Failure      400                {object} api.ErrorResponse
// @Failure      404                {object} api.ErrorResponse
// @Failure      409                {object} api.ErrorResponse
// @Security     mTLS
// @Router       /v1/attachment [get]
// @Router       /v1/attachment [delete]
func (h *Handler) handleAttachment(w http.ResponseWriter, r *http.Request) error {
	switch r.Method {
	case http.MethodGet:
		return h.handleAttachmentGet(w, r)
	case http.MethodDelete:
		return h.handleAttachmentDelete(w, r)
	default:
		return httpError{Status: http.StatusMethodNotAllowed, Code: "method_not_allowed", Detail: "unsupported method"}
	}
}

func (h *Handler) handleAttachmentUpload(w http.ResponseWriter, r *http.Request) error {
	key := strings.TrimSpace(r.URL.Query().Get("key"))
	if key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key query required"}
	}
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_attachment", Detail: "attachment name required"}
	}
	namespace, err := h.resolveNamespace(r.URL.Query().Get("namespace"))
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	if err := h.authorizeNamespace(r, namespace, nsauth.AccessWrite); err != nil {
		return err
	}
	h.observeNamespace(namespace)
	storageKey, err := h.namespacedKey(namespace, key)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_key", Detail: err.Error()}
	}
	leaseID := strings.TrimSpace(r.Header.Get("X-Lease-ID"))
	if leaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_lease", Detail: "X-Lease-ID required"}
	}
	txnID := strings.TrimSpace(r.Header.Get("X-Txn-ID"))
	if txnID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_txn", Detail: "X-Txn-ID required"}
	}
	fencingToken, err := parseFencingToken(r)
	if err != nil {
		return err
	}
	maxBytes, maxBytesSet, err := parseMaxBytesQuery(r.URL.Query().Get("max_bytes"))
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_max_bytes", Detail: err.Error()}
	}
	preventOverwrite := parseBoolQuery(r.URL.Query().Get("prevent_overwrite"))
	contentType := strings.TrimSpace(r.URL.Query().Get("content_type"))
	if contentType == "" {
		contentType = strings.TrimSpace(r.Header.Get("Content-Type"))
	}

	res, err := h.core.Attach(r.Context(), core.AttachCommand{
		Namespace:        namespace,
		Key:              key,
		LeaseID:          leaseID,
		FencingToken:     fencingToken,
		TxnID:            txnID,
		Name:             name,
		ContentType:      contentType,
		MaxBytes:         maxBytes,
		MaxBytesSet:      maxBytesSet,
		PreventOverwrite: preventOverwrite,
		Body:             r.Body,
	})
	if err != nil {
		return convertCoreError(err)
	}
	if res.Meta != nil {
		h.cacheLease(leaseID, storageKey, *res.Meta, res.MetaETag)
		if res.Meta.Lease != nil {
			w.Header().Set(headerFencingToken, strconv.FormatInt(res.Meta.Lease.FencingToken, 10))
		}
	}
	if res.Version > 0 {
		w.Header().Set("X-Key-Version", strconv.FormatInt(res.Version, 10))
	}
	if res.Attachment.ID != "" {
		w.Header().Set(headerAttachmentID, res.Attachment.ID)
	}
	h.writeJSON(w, http.StatusOK, api.AttachResponse{
		Attachment: attachmentInfoToAPI(res.Attachment),
		Noop:       res.Noop,
		Version:    res.Version,
	}, nil)
	return nil
}

func (h *Handler) handleAttachmentList(w http.ResponseWriter, r *http.Request) error {
	key := strings.TrimSpace(r.URL.Query().Get("key"))
	if key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key query required"}
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
	namespace, err := h.resolveNamespace(r.URL.Query().Get("namespace"))
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	requiredAccess := nsauth.AccessAny
	if publicRead {
		requiredAccess = nsauth.AccessRead
	}
	if err := h.authorizeNamespace(r, namespace, requiredAccess); err != nil {
		return err
	}
	h.observeNamespace(namespace)
	txnID := strings.TrimSpace(r.Header.Get("X-Txn-ID"))
	if !publicRead && txnID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_txn", Detail: "X-Txn-ID required"}
	}
	res, err := h.core.ListAttachments(r.Context(), core.ListAttachmentsCommand{
		Namespace:    namespace,
		Key:          key,
		LeaseID:      leaseID,
		FencingToken: fencingToken,
		TxnID:        txnID,
		Public:       publicRead,
	})
	if err != nil {
		return convertCoreError(err)
	}
	out := api.AttachmentListResponse{
		Namespace:   namespace,
		Key:         key,
		Attachments: convertAttachmentList(res.Attachments),
	}
	h.writeJSON(w, http.StatusOK, out, nil)
	return nil
}

func (h *Handler) handleAttachmentGet(w http.ResponseWriter, r *http.Request) error {
	key := strings.TrimSpace(r.URL.Query().Get("key"))
	if key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key query required"}
	}
	selector := core.AttachmentSelector{
		Name: strings.TrimSpace(r.URL.Query().Get("name")),
		ID:   strings.TrimSpace(r.URL.Query().Get("id")),
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
	namespace, err := h.resolveNamespace(r.URL.Query().Get("namespace"))
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	requiredAccess := nsauth.AccessAny
	if publicRead {
		requiredAccess = nsauth.AccessRead
	}
	if err := h.authorizeNamespace(r, namespace, requiredAccess); err != nil {
		return err
	}
	h.observeNamespace(namespace)
	txnID := strings.TrimSpace(r.Header.Get("X-Txn-ID"))
	if !publicRead && txnID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_txn", Detail: "X-Txn-ID required"}
	}
	res, err := h.core.RetrieveAttachment(r.Context(), core.RetrieveAttachmentCommand{
		Namespace:    namespace,
		Key:          key,
		LeaseID:      leaseID,
		FencingToken: fencingToken,
		TxnID:        txnID,
		Public:       publicRead,
		Selector:     selector,
	})
	if err != nil {
		return convertCoreError(err)
	}
	defer res.Reader.Close()
	contentType := strings.TrimSpace(res.Attachment.ContentType)
	if contentType == "" {
		contentType = storage.ContentTypeOctetStream
	}
	w.Header().Set("Content-Type", contentType)
	if res.Attachment.Size > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(res.Attachment.Size, 10))
		w.Header().Set(headerAttachmentSize, strconv.FormatInt(res.Attachment.Size, 10))
	}
	if res.Attachment.ID != "" {
		w.Header().Set(headerAttachmentID, res.Attachment.ID)
	}
	if res.Attachment.Name != "" {
		w.Header().Set(headerAttachmentName, res.Attachment.Name)
	}
	if res.Attachment.CreatedAtUnix > 0 {
		w.Header().Set(headerAttachmentCreatedAt, strconv.FormatInt(res.Attachment.CreatedAtUnix, 10))
	}
	if res.Attachment.UpdatedAtUnix > 0 {
		w.Header().Set(headerAttachmentUpdatedAt, strconv.FormatInt(res.Attachment.UpdatedAtUnix, 10))
	}
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, res.Reader)
	return err
}

func (h *Handler) handleAttachmentDelete(w http.ResponseWriter, r *http.Request) error {
	key := strings.TrimSpace(r.URL.Query().Get("key"))
	if key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key query required"}
	}
	selector := core.AttachmentSelector{
		Name: strings.TrimSpace(r.URL.Query().Get("name")),
		ID:   strings.TrimSpace(r.URL.Query().Get("id")),
	}
	namespace, err := h.resolveNamespace(r.URL.Query().Get("namespace"))
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	if err := h.authorizeNamespace(r, namespace, nsauth.AccessWrite); err != nil {
		return err
	}
	h.observeNamespace(namespace)
	storageKey, err := h.namespacedKey(namespace, key)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_key", Detail: err.Error()}
	}
	leaseID := strings.TrimSpace(r.Header.Get("X-Lease-ID"))
	if leaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_lease", Detail: "X-Lease-ID required"}
	}
	txnID := strings.TrimSpace(r.Header.Get("X-Txn-ID"))
	if txnID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_txn", Detail: "X-Txn-ID required"}
	}
	fencingToken, err := parseFencingToken(r)
	if err != nil {
		return err
	}
	res, err := h.core.DeleteAttachment(r.Context(), core.DeleteAttachmentCommand{
		Namespace:    namespace,
		Key:          key,
		LeaseID:      leaseID,
		FencingToken: fencingToken,
		TxnID:        txnID,
		Selector:     selector,
	})
	if err != nil {
		return convertCoreError(err)
	}
	if res.Meta != nil {
		h.cacheLease(leaseID, storageKey, *res.Meta, res.MetaETag)
		if res.Meta.Lease != nil {
			w.Header().Set(headerFencingToken, strconv.FormatInt(res.Meta.Lease.FencingToken, 10))
		}
	}
	if res.Version > 0 {
		w.Header().Set("X-Key-Version", strconv.FormatInt(res.Version, 10))
	}
	h.writeJSON(w, http.StatusOK, api.DeleteAttachmentResponse{Deleted: res.Deleted, Version: res.Version}, nil)
	return nil
}

func (h *Handler) handleAttachmentDeleteAll(w http.ResponseWriter, r *http.Request) error {
	key := strings.TrimSpace(r.URL.Query().Get("key"))
	if key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key query required"}
	}
	namespace, err := h.resolveNamespace(r.URL.Query().Get("namespace"))
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_namespace", Detail: err.Error()}
	}
	if err := h.authorizeNamespace(r, namespace, nsauth.AccessWrite); err != nil {
		return err
	}
	h.observeNamespace(namespace)
	storageKey, err := h.namespacedKey(namespace, key)
	if err != nil {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_key", Detail: err.Error()}
	}
	leaseID := strings.TrimSpace(r.Header.Get("X-Lease-ID"))
	if leaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_lease", Detail: "X-Lease-ID required"}
	}
	txnID := strings.TrimSpace(r.Header.Get("X-Txn-ID"))
	if txnID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_txn", Detail: "X-Txn-ID required"}
	}
	fencingToken, err := parseFencingToken(r)
	if err != nil {
		return err
	}
	res, err := h.core.DeleteAllAttachments(r.Context(), core.DeleteAllAttachmentsCommand{
		Namespace:    namespace,
		Key:          key,
		LeaseID:      leaseID,
		FencingToken: fencingToken,
		TxnID:        txnID,
	})
	if err != nil {
		return convertCoreError(err)
	}
	if res.Meta != nil {
		h.cacheLease(leaseID, storageKey, *res.Meta, res.MetaETag)
		if res.Meta.Lease != nil {
			w.Header().Set(headerFencingToken, strconv.FormatInt(res.Meta.Lease.FencingToken, 10))
		}
	}
	if res.Version > 0 {
		w.Header().Set("X-Key-Version", strconv.FormatInt(res.Version, 10))
	}
	h.writeJSON(w, http.StatusOK, api.DeleteAllAttachmentsResponse{Deleted: res.Deleted, Version: res.Version}, nil)
	return nil
}

func parseMaxBytesQuery(raw string) (int64, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, true, err
	}
	if value < 0 {
		return value, true, fmt.Errorf("max_bytes must be >= 0")
	}
	return value, true, nil
}

func attachmentInfoToAPI(info core.AttachmentInfo) api.AttachmentInfo {
	return api.AttachmentInfo{
		ID:            info.ID,
		Name:          info.Name,
		Size:          info.Size,
		ContentType:   info.ContentType,
		CreatedAtUnix: info.CreatedAtUnix,
		UpdatedAtUnix: info.UpdatedAtUnix,
	}
}

func convertAttachmentList(items []core.AttachmentInfo) []api.AttachmentInfo {
	if len(items) == 0 {
		return nil
	}
	out := make([]api.AttachmentInfo, 0, len(items))
	for _, item := range items {
		out = append(out, attachmentInfoToAPI(item))
	}
	return out
}
