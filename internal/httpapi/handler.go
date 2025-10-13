package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	port "pkt.systems/logport"

	"pkt.systems/lockd/internal/api"
	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/jsonutil"
	"pkt.systems/lockd/internal/storage"
)

const defaultPayloadSpoolMemoryThreshold = 4 << 20 // 4 MiB in-memory, then spill to disk
const headerFencingToken = "X-Fencing-Token"

// Handler wires HTTP endpoints to backend operations.
type Handler struct {
	store                 storage.Backend
	logger                port.ForLogging
	clock                 clock.Clock
	jsonMaxBytes          int64
	compactWriter         func(io.Writer, io.Reader, int64) error
	defaultTTL            time.Duration
	maxTTL                time.Duration
	acquireBlock          time.Duration
	spoolThreshold        int64
	leaseCache            sync.Map
	createLocks           sync.Map
	enforceClientIdentity bool
	forUpdate             forUpdateTracker
	forUpdateMaxHold      time.Duration
}

type forUpdateTracker struct {
	mu           sync.Mutex
	sessions     map[string]*forUpdateSession
	perClient    map[string]int
	global       int
	maxGlobal    int
	maxPerClient int
}

func newForUpdateTracker(maxGlobal, maxPerClient int) forUpdateTracker {
	return forUpdateTracker{
		sessions:     make(map[string]*forUpdateSession),
		perClient:    make(map[string]int),
		maxGlobal:    maxGlobal,
		maxPerClient: maxPerClient,
	}
}

func (t *forUpdateTracker) reserve(clientID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.maxGlobal > 0 && t.global >= t.maxGlobal {
		return fmt.Errorf("for-update capacity reached")
	}
	if t.maxPerClient > 0 && clientID != "" && t.perClient[clientID] >= t.maxPerClient {
		return fmt.Errorf("for-update capacity reached for client")
	}
	t.global++
	if clientID != "" {
		t.perClient[clientID]++
	}
	return nil
}

func (t *forUpdateTracker) releaseSlot(clientID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.global > 0 {
		t.global--
	}
	if clientID != "" {
		if v := t.perClient[clientID]; v > 1 {
			t.perClient[clientID] = v - 1
		} else {
			delete(t.perClient, clientID)
		}
	}
}

func (t *forUpdateTracker) register(session *forUpdateSession) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sessions[session.leaseID] = session
}

func (t *forUpdateTracker) unregister(session *forUpdateSession) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if existing, ok := t.sessions[session.leaseID]; ok && existing == session {
		delete(t.sessions, session.leaseID)
		if t.global > 0 {
			t.global--
		}
		if session.clientID != "" {
			if v := t.perClient[session.clientID]; v > 1 {
				t.perClient[session.clientID] = v - 1
			} else {
				delete(t.perClient, session.clientID)
			}
		}
	}
}

func (t *forUpdateTracker) session(leaseID string) *forUpdateSession {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sessions[leaseID]
}

type forUpdateSession struct {
	handler  *Handler
	key      string
	leaseID  string
	clientID string

	mu       sync.Mutex
	meta     storage.Meta
	metaETag string
	released bool
	done     chan struct{}
}

func newForUpdateSession(h *Handler, key, leaseID, clientID string, meta storage.Meta, metaETag string) *forUpdateSession {
	return &forUpdateSession{
		handler:  h,
		key:      key,
		leaseID:  leaseID,
		clientID: clientID,
		meta:     meta,
		metaETag: metaETag,
		done:     make(chan struct{}),
	}
}

func (s *forUpdateSession) updateMeta(meta storage.Meta, etag string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return
	}
	s.meta = meta
	s.metaETag = etag
}

func (s *forUpdateSession) snapshot() (storage.Meta, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return storage.Meta{}, "", false
	}
	return s.meta, s.metaETag, true
}

func (s *forUpdateSession) wait() {
	<-s.done
}

func (s *forUpdateSession) startMonitors(reqCtx context.Context, maxHold time.Duration) {
	timer := time.NewTimer(maxHold)
	go func() {
		defer timer.Stop()
		select {
		case <-reqCtx.Done():
			releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = s.release(releaseCtx)
			cancel()
		case <-timer.C:
			releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = s.release(releaseCtx)
			cancel()
		case <-s.done:
		}
	}()
}

func (s *forUpdateSession) release(ctx context.Context) error {
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return nil
	}
	meta := s.meta
	expected := s.metaETag
	s.mu.Unlock()

	var newETag string
	for {
		metaCopy := meta
		metaCopy.Lease = nil
		metaCopy.UpdatedAtUnix = s.handler.clock.Now().Unix()
		var err error
		newETag, err = s.handler.store.StoreMeta(ctx, s.key, &metaCopy, expected)
		if err == nil {
			meta = metaCopy
			break
		}
		if errors.Is(err, storage.ErrCASMismatch) {
			latest, etag, loadErr := s.handler.store.LoadMeta(ctx, s.key)
			if loadErr != nil {
				return loadErr
			}
			if latest.Lease == nil || latest.Lease.ID != s.leaseID {
				// already released by someone else
				meta = *latest
				newETag = etag
				break
			}
			meta = *latest
			expected = etag
			continue
		}
		return err
	}

	s.mu.Lock()
	if !s.released {
		s.released = true
		s.meta = meta
		s.metaETag = newETag
		close(s.done)
	}
	s.mu.Unlock()

	s.handler.dropLease(s.leaseID)
	s.handler.forUpdate.unregister(s)
	return nil
}

func (s *forUpdateSession) finish(meta storage.Meta, etag string) {
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return
	}
	s.released = true
	s.meta = meta
	s.metaETag = etag
	close(s.done)
	s.mu.Unlock()

	s.handler.dropLease(s.leaseID)
	s.handler.forUpdate.unregister(s)
}

func (h *Handler) clientKeyFromRequest(r *http.Request) string {
	if id := clientIdentityFromContext(r.Context()); id != "" {
		return id
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// Config groups the dependencies required by Handler.
type Config struct {
	Store                        storage.Backend
	Logger                       port.ForLogging
	Clock                        clock.Clock
	JSONMaxBytes                 int64
	CompactWriter                func(io.Writer, io.Reader, int64) error
	DefaultTTL                   time.Duration
	MaxTTL                       time.Duration
	AcquireBlock                 time.Duration
	SpoolMemoryThreshold         int64
	EnforceClientIdentity        bool
	ForUpdateMaxStreams          int
	ForUpdateMaxStreamsPerClient int
	ForUpdateMaxHold             time.Duration
}

// New constructs a Handler using the supplied configuration.
func New(cfg Config) *Handler {
	logger := cfg.Logger
	if logger == nil {
		logger = port.NoopLogger()
	}
	clk := cfg.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	cw := cfg.CompactWriter
	if cw == nil {
		cw = jsonutil.CompactWriter
	}
	threshold := cfg.SpoolMemoryThreshold
	if threshold <= 0 {
		threshold = defaultPayloadSpoolMemoryThreshold
	}
	maxStreams := cfg.ForUpdateMaxStreams
	if maxStreams <= 0 {
		maxStreams = 100
	}
	maxPerClient := cfg.ForUpdateMaxStreamsPerClient
	if maxPerClient <= 0 {
		maxPerClient = 3
	}
	maxHold := cfg.ForUpdateMaxHold
	if maxHold <= 0 {
		maxHold = 15 * time.Minute
	}
	return &Handler{
		store:                 cfg.Store,
		logger:                logger,
		clock:                 clk,
		jsonMaxBytes:          cfg.JSONMaxBytes,
		compactWriter:         cw,
		defaultTTL:            cfg.DefaultTTL,
		maxTTL:                cfg.MaxTTL,
		acquireBlock:          cfg.AcquireBlock,
		spoolThreshold:        threshold,
		enforceClientIdentity: cfg.EnforceClientIdentity,
		forUpdate:             newForUpdateTracker(maxStreams, maxPerClient),
		forUpdateMaxHold:      maxHold,
	}
}

// Register wires the routes under /v1 and health endpoints.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/v1/acquire", h.wrap(h.handleAcquire))
	mux.HandleFunc("/v1/keepalive", h.wrap(h.handleKeepAlive))
	mux.HandleFunc("/v1/release", h.wrap(h.handleRelease))
	mux.HandleFunc("/v1/get-state", h.wrap(h.handleGetState))
	mux.HandleFunc("/v1/update-state", h.wrap(h.handleUpdateState))
	mux.HandleFunc("/v1/acquire-for-update", h.wrap(h.handleAcquireForUpdate))
	mux.HandleFunc("/v1/describe", h.wrap(h.handleDescribe))
	mux.HandleFunc("/healthz", h.handleHealth)
	mux.HandleFunc("/readyz", h.handleReady)
}

type handlerFunc func(http.ResponseWriter, *http.Request) error

func (h *Handler) wrap(fn handlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if h.enforceClientIdentity {
			if id := clientIdentityFromRequest(r); id != "" {
				ctx = WithClientIdentity(ctx, id)
				r = r.WithContext(ctx)
			}
		}
		if err := fn(w, r.WithContext(ctx)); err != nil {
			h.handleError(w, err)
		}
	}
}

func clientIdentityFromRequest(r *http.Request) string {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return ""
	}
	cert := r.TLS.PeerCertificates[0]
	serial := ""
	if cert.SerialNumber != nil {
		serial = cert.SerialNumber.Text(16)
	}
	return fmt.Sprintf("%s#%s", cert.Subject.String(), serial)
}

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
	if id := clientIdentityFromContext(ctx); id != "" {
		payload.Owner = fmt.Sprintf("%s/%s", payload.Owner, id)
	}
	payload.Idempotency = strings.TrimSpace(r.Header.Get("X-Idempotency-Key"))
	if payload.Key == "" {
		payload.Key = r.URL.Query().Get("key")
	}
	if payload.Key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key is required"}
	}
	if payload.Owner == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_owner", Detail: "owner is required"}
	}
	ttl := h.resolveTTL(payload.TTLSeconds)
	if ttl <= 0 {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_ttl", Detail: "ttl_seconds must be positive"}
	}
	block := h.resolveBlock(payload.BlockSecs)

	clientID := clientIdentityFromContext(r.Context())
	if clientID != "" {
		payload.Owner = fmt.Sprintf("%s/%s", payload.Owner, clientID)
	}

	deadline := h.clock.Now().Add(block)
	leaseID := uuid.NewString()
	for {
		now := h.clock.Now()
		meta, metaETag, err := h.ensureMeta(ctx, payload.Key)
		if err != nil {
			return err
		}
		var creationMu *sync.Mutex
		if metaETag == "" {
			creationMu = h.creationMutex(payload.Key)
			creationMu.Lock()
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			meta.Lease = nil
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix > now.Unix() {
			if creationMu != nil {
				creationMu.Unlock()
			}
			if block > 0 && now.Before(deadline) {
				sleep := minDuration(500*time.Millisecond, time.Until(time.Unix(meta.Lease.ExpiresAtUnix, 0)))
				if sleep <= 0 {
					sleep = 200 * time.Millisecond
				}
				h.clock.Sleep(sleep)
				continue
			}
			retryDur := time.Until(time.Unix(meta.Lease.ExpiresAtUnix, 0))
			if retryDur < 0 {
				retryDur = 0
			}
			retry := int64(math.Ceil(retryDur.Seconds()))
			if retry < 1 {
				retry = 1
			}
			return httpError{
				Status:     http.StatusConflict,
				Code:       "waiting",
				Detail:     "lease already held",
				Version:    meta.Version,
				ETag:       meta.StateETag,
				RetryAfter: retry,
			}
		}
		expiresAt := now.Add(ttl).Unix()
		newFencing := meta.FencingToken + 1
		meta.FencingToken = newFencing
		meta.Lease = &storage.Lease{
			ID:            leaseID,
			Owner:         payload.Owner,
			ExpiresAtUnix: expiresAt,
			FencingToken:  newFencing,
		}
		meta.UpdatedAtUnix = now.Unix()

		newMetaETag, err := h.store.StoreMeta(ctx, payload.Key, meta, metaETag)
		if err != nil {
			if creationMu != nil {
				creationMu.Unlock()
			}
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return fmt.Errorf("store meta: %w", err)
		}
		if creationMu != nil {
			creationMu.Unlock()
		}
		metaETag = newMetaETag
		h.cacheLease(leaseID, payload.Key, *meta, metaETag)

		resp := api.AcquireResponse{
			LeaseID:      leaseID,
			Key:          payload.Key,
			Owner:        payload.Owner,
			ExpiresAt:    expiresAt,
			Version:      meta.Version,
			StateETag:    meta.StateETag,
			FencingToken: newFencing,
		}
		w.Header().Set(headerFencingToken, strconv.FormatInt(newFencing, 10))
		h.writeJSON(w, http.StatusOK, resp, map[string]string{
			"X-Key-Version": strconv.FormatInt(meta.Version, 10),
		})
		return nil
	}
}

func (h *Handler) handleAcquireForUpdate(w http.ResponseWriter, r *http.Request) error {
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
	if payload.Key == "" {
		payload.Key = r.URL.Query().Get("key")
	}
	if id := clientIdentityFromContext(ctx); id != "" {
		payload.Owner = fmt.Sprintf("%s/%s", payload.Owner, id)
	}
	if payload.Key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key is required"}
	}
	if payload.Owner == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_owner", Detail: "owner is required"}
	}
	payload.Idempotency = strings.TrimSpace(r.Header.Get("X-Idempotency-Key"))
	clientIdentity := clientIdentityFromContext(r.Context())
	if clientIdentity != "" {
		payload.Owner = fmt.Sprintf("%s/%s", payload.Owner, clientIdentity)
	}
	clientKey := h.clientKeyFromRequest(r)
	if err := h.forUpdate.reserve(clientKey); err != nil {
		return httpError{Status: http.StatusTooManyRequests, Code: "for_update_busy", Detail: "too many acquire-for-update requests"}
	}
	reserved := true
	defer func() {
		if reserved {
			h.forUpdate.releaseSlot(clientKey)
		}
	}()

	ttl := h.resolveTTL(payload.TTLSeconds)
	if ttl <= 0 {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_ttl", Detail: "ttl_seconds must be positive"}
	}
	block := h.resolveBlock(payload.BlockSecs)

	deadline := h.clock.Now().Add(block)
	leaseID := uuid.NewString()
	var stateReader io.ReadCloser
	var stateInfo *storage.StateInfo
	var meta storage.Meta
	var metaETag string
	for {
		now := h.clock.Now()
		metaPtr, etag, err := h.ensureMeta(ctx, payload.Key)
		if err != nil {
			return err
		}
		var creationMu *sync.Mutex
		if etag == "" {
			creationMu = h.creationMutex(payload.Key)
			creationMu.Lock()
		}
		meta = *metaPtr
		metaETag = etag
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix > now.Unix() {
			if creationMu != nil {
				creationMu.Unlock()
			}
			if block > 0 && now.Before(deadline) {
				sleep := minDuration(500*time.Millisecond, time.Until(time.Unix(meta.Lease.ExpiresAtUnix, 0)))
				if sleep <= 0 {
					sleep = 200 * time.Millisecond
				}
				h.clock.Sleep(sleep)
				continue
			}
			retryDur := time.Until(time.Unix(meta.Lease.ExpiresAtUnix, 0))
			if retryDur < 0 {
				retryDur = 0
			}
			retry := int64(math.Ceil(retryDur.Seconds()))
			if retry < 1 {
				retry = 1
			}
			return httpError{
				Status:     http.StatusConflict,
				Code:       "waiting",
				Detail:     "lease already held",
				Version:    meta.Version,
				ETag:       meta.StateETag,
				RetryAfter: retry,
			}
		}
		expiresAt := now.Add(ttl).Unix()
		newFencing := meta.FencingToken + 1
		meta.FencingToken = newFencing
		meta.Lease = &storage.Lease{
			ID:            leaseID,
			Owner:         payload.Owner,
			ExpiresAtUnix: expiresAt,
			FencingToken:  newFencing,
		}
		meta.UpdatedAtUnix = now.Unix()

		newMetaETag, err := h.store.StoreMeta(ctx, payload.Key, &meta, metaETag)
		if err != nil {
			if creationMu != nil {
				creationMu.Unlock()
			}
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return fmt.Errorf("store meta: %w", err)
		}
		if creationMu != nil {
			creationMu.Unlock()
		}
		metaETag = newMetaETag
		h.cacheLease(leaseID, payload.Key, meta, metaETag)

		stateReader, stateInfo, err = h.store.ReadState(ctx, payload.Key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				stateReader = nil
				stateInfo = nil
				break
			}
			return fmt.Errorf("read state: %w", err)
		}
		break
	}

	session := newForUpdateSession(h, payload.Key, leaseID, clientKey, meta, metaETag)
	h.forUpdate.register(session)
	reserved = false

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Lease-ID", leaseID)
	w.Header().Set("X-Lease-Owner", payload.Owner)
	if meta.Lease != nil {
		w.Header().Set("X-Lease-Expires-At", strconv.FormatInt(meta.Lease.ExpiresAtUnix, 10))
	}
	w.Header().Set(headerFencingToken, strconv.FormatInt(meta.Lease.FencingToken, 10))
	w.Header().Set("X-Key-Version", strconv.FormatInt(meta.Version, 10))
	if meta.StateETag != "" {
		w.Header().Set("ETag", meta.StateETag)
	}
	if stateInfo != nil && stateInfo.Size >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(stateInfo.Size, 10))
	}
	w.WriteHeader(http.StatusOK)
	if stateReader != nil {
		defer stateReader.Close()
		if _, err := io.Copy(w, stateReader); err != nil {
			h.logger.Warn("for-update stream copy failed", "key", payload.Key, "lease", leaseID, "error", err)
		}
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	session.startMonitors(ctx, h.forUpdateMaxHold)
	session.wait()
	return nil
}
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
	if payload.Key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key is required"}
	}
	if payload.LeaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_lease", Detail: "lease_id required"}
	}
	ttl := h.resolveTTL(payload.TTLSeconds)
	if ttl <= 0 {
		return httpError{Status: http.StatusBadRequest, Code: "invalid_ttl", Detail: "ttl_seconds must be positive"}
	}
	key := payload.Key
	for {
		now := h.clock.Now()
		cachedMeta, cachedETag, cachedKey, cached := h.leaseSnapshot(payload.LeaseID)
		var meta storage.Meta
		var metaETag string
		if cached && cachedKey == key {
			meta = cachedMeta
			metaETag = cachedETag
		} else {
			loadedMeta, etag, err := h.ensureMeta(ctx, key)
			if err != nil {
				return err
			}
			meta = *loadedMeta
			metaETag = etag
		}
		if err := validateLease(&meta, payload.LeaseID, token, now); err != nil {
			return err
		}
		meta.Lease.ExpiresAtUnix = now.Add(ttl).Unix()
		meta.UpdatedAtUnix = now.Unix()

		newMetaETag, err := h.store.StoreMeta(ctx, key, &meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				h.dropLease(payload.LeaseID)
				continue
			}
			return fmt.Errorf("store meta: %w", err)
		}
		h.cacheLease(payload.LeaseID, key, meta, newMetaETag)
		resp := api.KeepAliveResponse{ExpiresAt: meta.Lease.ExpiresAtUnix}
		w.Header().Set(headerFencingToken, strconv.FormatInt(meta.Lease.FencingToken, 10))
		h.writeJSON(w, http.StatusOK, resp, nil)
		return nil
	}
}

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
	if payload.Key == "" && r.URL.Query().Get("key") != "" {
		payload.Key = r.URL.Query().Get("key")
	}
	if payload.Key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key is required"}
	}
	if payload.LeaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_lease", Detail: "lease_id required"}
	}
	for {
		cachedMeta, cachedETag, cachedKey, cached := h.leaseSnapshot(payload.LeaseID)
		var meta storage.Meta
		var metaETag string
		if cached && cachedKey == payload.Key {
			meta = cachedMeta
			metaETag = cachedETag
		} else {
			loadedMeta, etag, err := h.ensureMeta(ctx, payload.Key)
			if err != nil {
				return err
			}
			meta = *loadedMeta
			metaETag = etag
		}
		if err := validateLease(&meta, payload.LeaseID, fencingToken, h.clock.Now()); err != nil {
			return err
		}
		meta.Lease = nil
		meta.UpdatedAtUnix = h.clock.Now().Unix()
		released := true
		newMetaETag, err := h.store.StoreMeta(ctx, payload.Key, &meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				h.dropLease(payload.LeaseID)
				continue
			}
			return fmt.Errorf("store meta: %w", err)
		}
		if session := h.forUpdate.session(payload.LeaseID); session != nil {
			session.finish(meta, newMetaETag)
		} else {
			h.dropLease(payload.LeaseID)
		}
		h.writeJSON(w, http.StatusOK, api.ReleaseResponse{Released: released}, nil)
		return nil
	}
}

func (h *Handler) handleGetState(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	key := r.URL.Query().Get("key")
	leaseID := r.Header.Get("X-Lease-ID")
	if key == "" || leaseID == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_params", Detail: "key query and X-Lease-ID required"}
	}
	fencingToken, err := parseFencingToken(r)
	if err != nil {
		return err
	}
	meta, _, err := h.ensureMeta(ctx, key)
	if err != nil {
		return err
	}
	if err := validateLease(meta, leaseID, fencingToken, h.clock.Now()); err != nil {
		return err
	}
	reader, info, err := h.store.ReadState(ctx, key)
	if errors.Is(err, storage.ErrNotFound) {
		w.WriteHeader(http.StatusNoContent)
		return nil
	}
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}
	defer reader.Close()
	if info != nil {
		w.Header().Set("ETag", info.ETag)
		if info.Version > 0 {
			w.Header().Set("X-Key-Version", strconv.FormatInt(info.Version, 10))
		} else {
			w.Header().Set("X-Key-Version", strconv.FormatInt(meta.Version, 10))
		}
	}
	w.Header().Set(headerFencingToken, strconv.FormatInt(meta.Lease.FencingToken, 10))
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, reader)
	return err
}

func (h *Handler) handleUpdateState(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
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
	ifMatch := strings.TrimSpace(r.Header.Get("X-If-State-ETag"))
	ifVersion := strings.TrimSpace(r.Header.Get("X-If-Version"))

	body := http.MaxBytesReader(w, r.Body, h.jsonMaxBytes)
	defer body.Close()

	now := h.clock.Now()
	cachedMeta, cachedETag, cachedKey, cached := h.leaseSnapshot(leaseID)
	var meta storage.Meta
	var metaETag string
	if cached && cachedKey == key {
		meta = cachedMeta
		metaETag = cachedETag
	} else {
		loadedMeta, etag, err := h.ensureMeta(ctx, key)
		if err != nil {
			return err
		}
		meta = *loadedMeta
		metaETag = etag
	}
	if err := validateLease(&meta, leaseID, fencingToken, now); err != nil {
		return err
	}
	if ifVersion != "" {
		want, parseErr := strconv.ParseInt(ifVersion, 10, 64)
		if parseErr != nil {
			return httpError{Status: http.StatusBadRequest, Code: "invalid_version", Detail: parseErr.Error()}
		}
		if meta.Version != want {
			return httpError{
				Status:  http.StatusConflict,
				Code:    "version_conflict",
				Detail:  "state version mismatch",
				Version: meta.Version,
				ETag:    meta.StateETag,
			}
		}
	}

	spool := newPayloadSpool(h.spoolThreshold)
	defer spool.Close()
	if err := h.compactWriter(spool, body, h.jsonMaxBytes); err != nil {
		return err
	}
	payloadReader, err := spool.Reader()
	if err != nil {
		return err
	}

	putRes, err := h.store.WriteState(ctx, key, payloadReader, storage.PutStateOptions{
		ExpectedETag: ifMatch,
	})
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			return httpError{
				Status:  http.StatusConflict,
				Code:    "etag_mismatch",
				Detail:  "state etag mismatch",
				Version: meta.Version,
				ETag:    meta.StateETag,
			}
		}
		return fmt.Errorf("write state: %w", err)
	}

	meta.Version++
	meta.StateETag = putRes.NewETag
	meta.UpdatedAtUnix = h.clock.Now().Unix()
	newMetaETag, err := h.store.StoreMeta(ctx, key, &meta, metaETag)
	if err != nil {
		if errors.Is(err, storage.ErrCASMismatch) {
			return httpError{
				Status:  http.StatusConflict,
				Code:    "meta_conflict",
				Detail:  "state metadata changed concurrently",
				Version: meta.Version,
				ETag:    meta.StateETag,
			}
		}
		return fmt.Errorf("store meta: %w", err)
	}
	h.cacheLease(leaseID, key, meta, newMetaETag)
	resp := map[string]any{
		"new_version":    meta.Version,
		"new_state_etag": meta.StateETag,
		"bytes":          putRes.BytesWritten,
	}
	headers := map[string]string{
		"X-Key-Version":    strconv.FormatInt(meta.Version, 10),
		"ETag":             meta.StateETag,
		headerFencingToken: strconv.FormatInt(meta.Lease.FencingToken, 10),
	}
	h.writeJSON(w, http.StatusOK, resp, headers)
	return nil
}

func (h *Handler) handleDescribe(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	key := r.URL.Query().Get("key")
	if key == "" {
		return httpError{Status: http.StatusBadRequest, Code: "missing_key", Detail: "key query required"}
	}
	meta, _, err := h.ensureMeta(ctx, key)
	if err != nil {
		return err
	}
	resp := api.DescribeResponse{
		Key:       key,
		Version:   meta.Version,
		StateETag: meta.StateETag,
		UpdatedAt: meta.UpdatedAtUnix,
	}
	if meta.Lease != nil {
		resp.LeaseID = meta.Lease.ID
		resp.Owner = meta.Lease.Owner
		resp.ExpiresAt = meta.Lease.ExpiresAtUnix
	}
	h.writeJSON(w, http.StatusOK, resp, nil)
	return nil
}

func (h *Handler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleReady(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleError(w http.ResponseWriter, err error) {
	var httpErr httpError
	if errors.As(err, &httpErr) {
		resp := api.ErrorResponse{
			ErrorCode:         httpErr.Code,
			Detail:            httpErr.Detail,
			CurrentVersion:    httpErr.Version,
			CurrentETag:       httpErr.ETag,
			RetryAfterSeconds: httpErr.RetryAfter,
		}
		h.writeJSON(w, httpErr.Status, resp, nil)
		return
	}
	h.logger.Error("handler error", "error", err)
	resp := api.ErrorResponse{
		ErrorCode: "internal_error",
		Detail:    "internal server error",
	}
	h.writeJSON(w, http.StatusInternalServerError, resp, nil)
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, payload any, headers map[string]string) {
	w.Header().Set("Content-Type", "application/json")
	for k, v := range headers {
		w.Header().Set(k, v)
	}
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	enc := json.NewEncoder(w)
	_ = enc.Encode(payload)
}

func (h *Handler) ensureMeta(ctx context.Context, key string) (*storage.Meta, string, error) {
	meta, etag, err := h.store.LoadMeta(ctx, key)
	if errors.Is(err, storage.ErrNotFound) {
		return &storage.Meta{}, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("load meta: %w", err)
	}
	return meta, etag, nil
}

func (h *Handler) resolveTTL(requested int64) time.Duration {
	if requested <= 0 {
		return h.defaultTTL
	}
	ttl := time.Duration(requested) * time.Second
	if ttl > h.maxTTL {
		return h.maxTTL
	}
	return ttl
}

func (h *Handler) resolveBlock(requested int64) time.Duration {
	if requested <= 0 {
		return 0
	}
	block := time.Duration(requested) * time.Second
	if block > h.acquireBlock {
		return h.acquireBlock
	}
	return block
}

func validateLease(meta *storage.Meta, leaseID string, fencingToken int64, now time.Time) error {
	if meta.Lease == nil || meta.Lease.ID != leaseID {
		return httpError{
			Status:  http.StatusForbidden,
			Code:    "lease_required",
			Detail:  "active lease required",
			Version: meta.Version,
			ETag:    meta.StateETag,
		}
	}
	if meta.Lease.ExpiresAtUnix < now.Unix() {
		return httpError{
			Status:  http.StatusForbidden,
			Code:    "lease_expired",
			Detail:  "lease expired",
			Version: meta.Version,
			ETag:    meta.StateETag,
		}
	}
	if meta.Lease.FencingToken != fencingToken {
		return httpError{
			Status:  http.StatusForbidden,
			Code:    "fencing_mismatch",
			Detail:  "fencing token mismatch",
			Version: meta.Version,
			ETag:    meta.StateETag,
		}
	}
	return nil
}

func parseFencingToken(r *http.Request) (int64, error) {
	value := strings.TrimSpace(r.Header.Get(headerFencingToken))
	if value == "" {
		return 0, httpError{Status: http.StatusBadRequest, Code: "missing_fencing_token", Detail: "X-Fencing-Token header required"}
	}
	token, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, httpError{Status: http.StatusBadRequest, Code: "invalid_fencing_token", Detail: "invalid fencing token"}
	}
	return token, nil
}

type payloadSpool struct {
	threshold int64
	buf       []byte
	file      *os.File
	pooled    bool
}

var payloadBufferPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, defaultPayloadSpoolMemoryThreshold)
	},
}

func newPayloadSpool(threshold int64) *payloadSpool {
	ps := &payloadSpool{threshold: threshold}
	if threshold <= 0 {
		return ps
	}
	maxInt := int64(^uint(0) >> 1)
	if threshold > maxInt {
		threshold = maxInt
	}
	bufCap := int(threshold)
	if threshold == defaultPayloadSpoolMemoryThreshold {
		if buf, ok := payloadBufferPool.Get().([]byte); ok {
			if cap(buf) < bufCap {
				buf = make([]byte, 0, bufCap)
			} else {
				buf = buf[:0]
			}
			ps.buf = buf
			ps.pooled = true
			return ps
		}
	}
	if bufCap > 0 {
		ps.buf = make([]byte, 0, bufCap)
	}
	return ps
}

func (p *payloadSpool) Write(data []byte) (int, error) {
	if p.file != nil {
		return p.file.Write(data)
	}
	if int64(len(p.buf))+int64(len(data)) <= p.threshold {
		p.buf = append(p.buf, data...)
		return len(data), nil
	}
	f, err := os.CreateTemp("", "lockd-json-")
	if err != nil {
		return 0, err
	}
	if len(p.buf) > 0 {
		if _, err := f.Write(p.buf); err != nil {
			f.Close()
			os.Remove(f.Name())
			return 0, err
		}
	}
	if p.pooled && p.buf != nil {
		payloadBufferPool.Put(p.buf[:0])
		p.pooled = false
	}
	n, err := f.Write(data)
	if err != nil {
		f.Close()
		os.Remove(f.Name())
		return n, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		os.Remove(f.Name())
		return n, err
	}
	// ensure subsequent writes append to file
	p.file = f
	p.buf = nil
	return n, nil
}

func (p *payloadSpool) Reader() (io.ReadSeeker, error) {
	if p.file != nil {
		if _, err := p.file.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		return p.file, nil
	}
	return bytes.NewReader(p.buf), nil
}

func (p *payloadSpool) Close() error {
	if p.file != nil {
		name := p.file.Name()
		err := p.file.Close()
		_ = os.Remove(name)
		p.file = nil
		return err
	}
	if p.pooled && p.buf != nil {
		payloadBufferPool.Put(p.buf[:0])
		p.pooled = false
	}
	p.buf = nil
	return nil
}

type leaseCacheEntry struct {
	key  string
	meta storage.Meta
	etag string
}

func (h *Handler) cacheLease(leaseID, key string, meta storage.Meta, etag string) {
	h.leaseCache.Store(leaseID, &leaseCacheEntry{key: key, meta: meta, etag: etag})
	if session := h.forUpdate.session(leaseID); session != nil {
		session.updateMeta(meta, etag)
	}
}

func (h *Handler) leaseSnapshot(leaseID string) (storage.Meta, string, string, bool) {
	if v, ok := h.leaseCache.Load(leaseID); ok {
		entry := v.(*leaseCacheEntry)
		return entry.meta, entry.etag, entry.key, true
	}
	return storage.Meta{}, "", "", false
}

func (h *Handler) creationMutex(key string) *sync.Mutex {
	mu, _ := h.createLocks.LoadOrStore(key, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

func (h *Handler) dropLease(leaseID string) {
	h.leaseCache.Delete(leaseID)
}

type httpError struct {
	Status     int
	Code       string
	Detail     string
	Version    int64
	ETag       string
	RetryAfter int64
}

func (h httpError) Error() string {
	if h.Detail != "" {
		return fmt.Sprintf("%s: %s", h.Code, h.Detail)
	}
	return h.Code
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= b {
		return a
	}
	return b
}
