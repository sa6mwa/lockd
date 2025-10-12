package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
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

// Handler wires HTTP endpoints to backend operations.
type Handler struct {
	store          storage.Backend
	logger         port.ForLogging
	clock          clock.Clock
	jsonMaxBytes   int64
	compactWriter  func(io.Writer, io.Reader, int64) error
	defaultTTL     time.Duration
	maxTTL         time.Duration
	acquireBlock   time.Duration
	spoolThreshold int64
	leaseCache     sync.Map
}

// Config groups the dependencies required by Handler.
type Config struct {
	Store                storage.Backend
	Logger               port.ForLogging
	Clock                clock.Clock
	JSONMaxBytes         int64
	CompactWriter        func(io.Writer, io.Reader, int64) error
	DefaultTTL           time.Duration
	MaxTTL               time.Duration
	AcquireBlock         time.Duration
	SpoolMemoryThreshold int64
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
	return &Handler{
		store:          cfg.Store,
		logger:         logger,
		clock:          clk,
		jsonMaxBytes:   cfg.JSONMaxBytes,
		compactWriter:  cw,
		defaultTTL:     cfg.DefaultTTL,
		maxTTL:         cfg.MaxTTL,
		acquireBlock:   cfg.AcquireBlock,
		spoolThreshold: threshold,
	}
}

// Register wires the routes under /v1 and health endpoints.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/v1/acquire", h.wrap(h.handleAcquire))
	mux.HandleFunc("/v1/keepalive", h.wrap(h.handleKeepAlive))
	mux.HandleFunc("/v1/release", h.wrap(h.handleRelease))
	mux.HandleFunc("/v1/get_state", h.wrap(h.handleGetState))
	mux.HandleFunc("/v1/update_state", h.wrap(h.handleUpdateState))
	mux.HandleFunc("/v1/describe", h.wrap(h.handleDescribe))
	mux.HandleFunc("/healthz", h.handleHealth)
	mux.HandleFunc("/readyz", h.handleReady)
}

type handlerFunc func(http.ResponseWriter, *http.Request) error

func (h *Handler) wrap(fn handlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			h.handleError(w, err)
		}
	}
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

	deadline := h.clock.Now().Add(block)
	leaseID := uuid.NewString()
	for {
		now := h.clock.Now()
		meta, metaETag, err := h.ensureMeta(ctx, payload.Key)
		if err != nil {
			return err
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix <= now.Unix() {
			meta.Lease = nil
		}
		if meta.Lease != nil && meta.Lease.ExpiresAtUnix > now.Unix() {
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
		meta.Lease = &storage.Lease{
			ID:            leaseID,
			Owner:         payload.Owner,
			ExpiresAtUnix: expiresAt,
		}
		meta.UpdatedAtUnix = now.Unix()

		newMetaETag, err := h.store.StoreMeta(ctx, payload.Key, meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return fmt.Errorf("store meta: %w", err)
		}
		meta.StateETag = newMetaETag
		h.cacheLease(leaseID, payload.Key, *meta, newMetaETag)

		resp := api.AcquireResponse{
			LeaseID:   leaseID,
			Key:       payload.Key,
			Owner:     payload.Owner,
			ExpiresAt: expiresAt,
			Version:   meta.Version,
			StateETag: meta.StateETag,
		}
		h.writeJSON(w, http.StatusOK, resp, map[string]string{
			"X-Key-Version": strconv.FormatInt(meta.Version, 10),
		})
		return nil
	}
}

func (h *Handler) handleKeepAlive(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
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
		if meta.Lease == nil || meta.Lease.ID != payload.LeaseID {
			return httpError{
				Status:  http.StatusConflict,
				Code:    "lease_conflict",
				Detail:  "lease not held",
				Version: meta.Version,
				ETag:    meta.StateETag,
			}
		}
		if meta.Lease.ExpiresAtUnix < now.Unix() {
			return httpError{
				Status:  http.StatusConflict,
				Code:    "lease_expired",
				Detail:  "lease expired",
				Version: meta.Version,
				ETag:    meta.StateETag,
			}
		}
		meta.Lease.ExpiresAtUnix = now.Add(ttl).Unix()
		meta.UpdatedAtUnix = now.Unix()

		newMetaETag, err := h.store.StoreMeta(ctx, key, &meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return fmt.Errorf("store meta: %w", err)
		}
		meta.StateETag = newMetaETag
		h.cacheLease(payload.LeaseID, key, meta, newMetaETag)
		resp := api.KeepAliveResponse{ExpiresAt: meta.Lease.ExpiresAtUnix}
		h.writeJSON(w, http.StatusOK, resp, nil)
		return nil
	}
}

func (h *Handler) handleRelease(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
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
		released := false
		if meta.Lease != nil && meta.Lease.ID == payload.LeaseID {
			meta.Lease = nil
			meta.UpdatedAtUnix = h.clock.Now().Unix()
			released = true
		}
		newMetaETag, err := h.store.StoreMeta(ctx, payload.Key, &meta, metaETag)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			return fmt.Errorf("store meta: %w", err)
		}
		meta.StateETag = newMetaETag
		if released {
			h.dropLease(payload.LeaseID)
		} else {
			h.cacheLease(payload.LeaseID, payload.Key, meta, newMetaETag)
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
	meta, _, err := h.ensureMeta(ctx, key)
	if err != nil {
		return err
	}
	if err := validateLease(meta, leaseID, h.clock.Now()); err != nil {
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
	if err := validateLease(&meta, leaseID, now); err != nil {
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
	meta.StateETag = newMetaETag
	h.cacheLease(leaseID, key, meta, newMetaETag)
	resp := map[string]any{
		"new_version":    meta.Version,
		"new_state_etag": meta.StateETag,
		"bytes":          putRes.BytesWritten,
	}
	headers := map[string]string{
		"X-Key-Version": strconv.FormatInt(meta.Version, 10),
		"ETag":          meta.StateETag,
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

func validateLease(meta *storage.Meta, leaseID string, now time.Time) error {
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
	return nil
}

type payloadSpool struct {
	threshold int64
	buf       []byte
	file      *os.File
}

func newPayloadSpool(threshold int64) *payloadSpool {
	return &payloadSpool{threshold: threshold}
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
}

func (h *Handler) leaseSnapshot(leaseID string) (storage.Meta, string, string, bool) {
	if v, ok := h.leaseCache.Load(leaseID); ok {
		entry := v.(*leaseCacheEntry)
		return entry.meta, entry.etag, entry.key, true
	}
	return storage.Meta{}, "", "", false
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
