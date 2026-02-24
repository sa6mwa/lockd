package mcp

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"pkt.systems/pslog"
)

const (
	defaultTransferCapabilityTTL = 10 * time.Minute
	transferIDEntropyBytes       = 24
)

var (
	errTransferCapabilityNotFound = errors.New("transfer capability not found")
	errTransferCapabilityExpired  = errors.New("transfer capability expired")
)

type transferMethodNotAllowedError struct {
	Allowed string
}

func (e *transferMethodNotAllowedError) Error() string {
	if e == nil {
		return "transfer capability method not allowed"
	}
	return fmt.Sprintf("transfer capability method not allowed: use %s", e.Allowed)
}

type transferMode string

const (
	transferModeUpload   transferMode = "upload"
	transferModeDownload transferMode = "download"
)

type transferRegistration struct {
	ID            string
	Method        string
	ExpiresAtUnix int64
}

type writeStreamUploadCapabilityStatus struct {
	Available     bool
	ExpiresAtUnix int64
}

type transferDownloadRequest struct {
	ContentType   string
	ContentLength int64
	Filename      string
	Headers       map[string]string
	Cleanup       func()
}

type transferCapability struct {
	id          string
	mode        transferMode
	method      string
	sessionID   string
	streamKey   string
	expiresAt   time.Time
	contentType string
	contentLen  int64
	filename    string
	headers     map[string]string
	uploadFn    func(context.Context, io.Reader) (int64, error)
	reader      io.ReadCloser
	cleanup     func()
	closeOnce   sync.Once
}

func (c *transferCapability) close() {
	if c == nil {
		return
	}
	c.closeOnce.Do(func() {
		if c.cleanup != nil {
			c.cleanup()
		}
	})
}

type transferManager struct {
	logger pslog.Logger

	rootCtx context.Context
	cancel  context.CancelFunc

	mu             sync.Mutex
	byID           map[string]*transferCapability
	bySession      map[string]map[string]struct{}
	byStream       map[string]string
	sessionWaiters map[string]struct{}
}

func newTransferManager(logger pslog.Logger) *transferManager {
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &transferManager{
		logger:         logger,
		rootCtx:        ctx,
		cancel:         cancel,
		byID:           make(map[string]*transferCapability),
		bySession:      make(map[string]map[string]struct{}),
		byStream:       make(map[string]string),
		sessionWaiters: make(map[string]struct{}),
	}
}

func (m *transferManager) Close() {
	m.cancel()
	m.mu.Lock()
	ids := make([]string, 0, len(m.byID))
	for id := range m.byID {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	for _, id := range ids {
		m.Revoke(id)
	}
}

func (m *transferManager) RegisterUpload(session *mcpsdk.ServerSession, streamID string, uploadFn func(context.Context, io.Reader) (int64, error)) (transferRegistration, error) {
	if session == nil {
		return transferRegistration{}, fmt.Errorf("active MCP session required")
	}
	streamID = strings.TrimSpace(streamID)
	if streamID == "" {
		return transferRegistration{}, fmt.Errorf("stream_id is required")
	}
	if uploadFn == nil {
		return transferRegistration{}, fmt.Errorf("upload function required")
	}
	id, err := newTransferID()
	if err != nil {
		return transferRegistration{}, err
	}
	expiresAt := time.Now().UTC().Add(defaultTransferCapabilityTTL)
	sid := normalizeMCPServerSessionID(session)
	streamKey := makeTransferStreamKey(sid, streamID)

	capability := &transferCapability{
		id:        id,
		mode:      transferModeUpload,
		method:    http.MethodPut,
		sessionID: sid,
		streamKey: streamKey,
		expiresAt: expiresAt,
		uploadFn:  uploadFn,
	}

	startWaiter := false
	var replaced *transferCapability
	m.mu.Lock()
	if existingID, ok := m.byStream[streamKey]; ok {
		replaced = m.removeLocked(existingID)
	}
	m.byID[id] = capability
	if m.bySession[sid] == nil {
		m.bySession[sid] = make(map[string]struct{})
	}
	m.bySession[sid][id] = struct{}{}
	m.byStream[streamKey] = id
	if _, ok := m.sessionWaiters[sid]; !ok {
		m.sessionWaiters[sid] = struct{}{}
		startWaiter = true
	}
	m.mu.Unlock()
	if replaced != nil {
		replaced.close()
	}
	if startWaiter {
		go m.waitSession(session, sid)
	}
	m.scheduleExpiry(id, expiresAt)
	return transferRegistration{
		ID:            id,
		Method:        http.MethodPut,
		ExpiresAtUnix: expiresAt.Unix(),
	}, nil
}

func (m *transferManager) RegisterDownload(session *mcpsdk.ServerSession, reader io.ReadCloser, req transferDownloadRequest) (transferRegistration, error) {
	if session == nil {
		if reader != nil {
			_ = reader.Close()
		}
		return transferRegistration{}, fmt.Errorf("active MCP session required")
	}
	if reader == nil {
		return transferRegistration{}, fmt.Errorf("download reader required")
	}
	id, err := newTransferID()
	if err != nil {
		_ = reader.Close()
		return transferRegistration{}, err
	}
	expiresAt := time.Now().UTC().Add(defaultTransferCapabilityTTL)
	sid := normalizeMCPServerSessionID(session)
	cleanup := func() {
		_ = reader.Close()
		if req.Cleanup != nil {
			req.Cleanup()
		}
	}
	capability := &transferCapability{
		id:          id,
		mode:        transferModeDownload,
		method:      http.MethodGet,
		sessionID:   sid,
		expiresAt:   expiresAt,
		contentType: strings.TrimSpace(req.ContentType),
		contentLen:  req.ContentLength,
		filename:    strings.TrimSpace(req.Filename),
		headers:     cloneStringMap(req.Headers),
		reader:      reader,
		cleanup:     cleanup,
	}

	startWaiter := false
	m.mu.Lock()
	m.byID[id] = capability
	if m.bySession[sid] == nil {
		m.bySession[sid] = make(map[string]struct{})
	}
	m.bySession[sid][id] = struct{}{}
	if _, ok := m.sessionWaiters[sid]; !ok {
		m.sessionWaiters[sid] = struct{}{}
		startWaiter = true
	}
	m.mu.Unlock()
	if startWaiter {
		go m.waitSession(session, sid)
	}
	m.scheduleExpiry(id, expiresAt)
	return transferRegistration{
		ID:            id,
		Method:        http.MethodGet,
		ExpiresAtUnix: expiresAt.Unix(),
	}, nil
}

func (m *transferManager) Revoke(id string) {
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	m.mu.Lock()
	capability := m.removeLocked(id)
	m.mu.Unlock()
	if capability != nil {
		capability.close()
	}
}

func (m *transferManager) RevokeWriteStream(session *mcpsdk.ServerSession, streamID string) {
	if session == nil {
		return
	}
	streamID = strings.TrimSpace(streamID)
	if streamID == "" {
		return
	}
	sid := normalizeMCPServerSessionID(session)
	streamKey := makeTransferStreamKey(sid, streamID)

	m.mu.Lock()
	id := m.byStream[streamKey]
	capability := m.removeLocked(id)
	m.mu.Unlock()
	if capability != nil {
		capability.close()
	}
}

func (m *transferManager) WriteStreamUploadStatus(session *mcpsdk.ServerSession, streamID string) writeStreamUploadCapabilityStatus {
	if session == nil {
		return writeStreamUploadCapabilityStatus{}
	}
	streamID = strings.TrimSpace(streamID)
	if streamID == "" {
		return writeStreamUploadCapabilityStatus{}
	}
	sid := normalizeMCPServerSessionID(session)
	streamKey := makeTransferStreamKey(sid, streamID)
	now := time.Now().UTC()

	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.byStream[streamKey]
	if id == "" {
		return writeStreamUploadCapabilityStatus{}
	}
	capability := m.byID[id]
	if capability == nil {
		return writeStreamUploadCapabilityStatus{}
	}
	if !capability.expiresAt.IsZero() && (now.After(capability.expiresAt) || now.Equal(capability.expiresAt)) {
		return writeStreamUploadCapabilityStatus{}
	}
	return writeStreamUploadCapabilityStatus{
		Available:     true,
		ExpiresAtUnix: capability.expiresAt.Unix(),
	}
}

func (m *transferManager) Take(id, method string, now time.Time) (*transferCapability, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, errTransferCapabilityNotFound
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		method = http.MethodGet
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	capability := m.byID[id]
	if capability == nil {
		return nil, errTransferCapabilityNotFound
	}
	if !capability.expiresAt.IsZero() && (now.After(capability.expiresAt) || now.Equal(capability.expiresAt)) {
		expired := m.removeLocked(id)
		if expired != nil {
			go expired.close()
		}
		return nil, errTransferCapabilityExpired
	}
	if method != capability.method {
		return nil, &transferMethodNotAllowedError{Allowed: capability.method}
	}
	return m.removeLocked(id), nil
}

func (m *transferManager) waitSession(session *mcpsdk.ServerSession, sid string) {
	if session == nil {
		return
	}
	_ = session.Wait()
	m.revokeSession(sid)
}

func (m *transferManager) revokeSession(sid string) {
	m.mu.Lock()
	byID := m.bySession[sid]
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	delete(m.bySession, sid)
	delete(m.sessionWaiters, sid)
	for _, id := range ids {
		capability := m.removeLocked(id)
		if capability != nil {
			go capability.close()
		}
	}
	m.mu.Unlock()
}

func (m *transferManager) scheduleExpiry(id string, expiresAt time.Time) {
	delay := time.Until(expiresAt)
	if delay <= 0 {
		m.Revoke(id)
		return
	}
	timer := time.NewTimer(delay)
	go func() {
		defer timer.Stop()
		select {
		case <-m.rootCtx.Done():
			return
		case <-timer.C:
			m.Revoke(id)
		}
	}()
}

func (m *transferManager) removeLocked(id string) *transferCapability {
	if strings.TrimSpace(id) == "" {
		return nil
	}
	capability := m.byID[id]
	if capability == nil {
		return nil
	}
	delete(m.byID, id)
	if capability.sessionID != "" {
		if bySession := m.bySession[capability.sessionID]; bySession != nil {
			delete(bySession, id)
			if len(bySession) == 0 {
				delete(m.bySession, capability.sessionID)
				delete(m.sessionWaiters, capability.sessionID)
			}
		}
	}
	if capability.streamKey != "" {
		delete(m.byStream, capability.streamKey)
	}
	return capability
}

func makeTransferStreamKey(sessionID, streamID string) string {
	return strings.TrimSpace(sessionID) + "|" + strings.TrimSpace(streamID)
}

func newTransferID() (string, error) {
	buf := make([]byte, transferIDEntropyBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate transfer capability id: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func (s *server) ensureTransferManager() *transferManager {
	if s.transfers != nil {
		return s.transfers
	}
	s.transfers = newTransferManager(s.transferLog)
	return s.transfers
}

func (s *server) transferURL(capabilityID string) string {
	if s.baseURL == nil {
		return ""
	}
	u := *s.baseURL
	u.Path = path.Join(s.baseURL.Path, s.mcpHTTPPath, "transfer", strings.TrimSpace(capabilityID))
	u.RawQuery = ""
	u.Fragment = ""
	u.RawFragment = ""
	return u.String()
}

func (s *server) handleTransfer(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.ensureTransferManager() == nil {
		http.Error(w, "transfer subsystem unavailable", http.StatusServiceUnavailable)
		return
	}
	pathPrefix := s.transferPath + "/"
	if !strings.HasPrefix(r.URL.Path, pathPrefix) {
		http.NotFound(w, r)
		return
	}
	token := strings.TrimSpace(strings.TrimPrefix(r.URL.Path, pathPrefix))
	if token == "" || strings.Contains(token, "/") {
		http.NotFound(w, r)
		return
	}

	capability, err := s.transfers.Take(token, r.Method, time.Now().UTC())
	if err != nil {
		switch {
		case errors.Is(err, errTransferCapabilityNotFound):
			http.NotFound(w, r)
		case errors.Is(err, errTransferCapabilityExpired):
			http.Error(w, "transfer capability expired", http.StatusGone)
		default:
			var methodErr *transferMethodNotAllowedError
			if errors.As(err, &methodErr) {
				w.Header().Set("Allow", methodErr.Allowed)
				http.Error(w, methodErr.Error(), http.StatusMethodNotAllowed)
				return
			}
			http.Error(w, "transfer capability unavailable", http.StatusNotFound)
		}
		return
	}
	defer capability.close()

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Lockd-Transfer-ID", capability.id)
	switch capability.mode {
	case transferModeUpload:
		if capability.uploadFn == nil {
			http.Error(w, "upload capability misconfigured", http.StatusInternalServerError)
			return
		}
		if r.Body == nil {
			http.Error(w, "upload body required", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		written, uploadErr := capability.uploadFn(r.Context(), r.Body)
		if uploadErr != nil {
			s.transferLog.Warn("mcp.transfer.upload.error", "transfer_id", capability.id, "stream_key", capability.streamKey, "error", uploadErr)
			http.Error(w, uploadErr.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("X-Lockd-Transfer-Bytes", strconv.FormatInt(written, 10))
		w.WriteHeader(http.StatusNoContent)
	case transferModeDownload:
		reader := capability.reader
		if reader == nil {
			http.Error(w, "download capability misconfigured", http.StatusInternalServerError)
			return
		}
		if capability.contentType != "" {
			w.Header().Set("Content-Type", capability.contentType)
		}
		if capability.contentLen > 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(capability.contentLen, 10))
		}
		if capability.filename != "" {
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", capability.filename))
		}
		for key, value := range capability.headers {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			w.Header().Set(key, value)
		}
		if _, copyErr := io.Copy(w, reader); copyErr != nil && !errors.Is(copyErr, context.Canceled) {
			s.transferLog.Warn("mcp.transfer.download.error", "transfer_id", capability.id, "error", copyErr)
		}
	default:
		http.Error(w, "unsupported transfer capability", http.StatusInternalServerError)
	}
}
