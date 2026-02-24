package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"pkt.systems/pslog"
)

type writeStreamKind string

const (
	writeStreamKindState      writeStreamKind = "state"
	writeStreamKindQueue      writeStreamKind = "queue"
	writeStreamKindAttachment writeStreamKind = "attachment"
)

type writeStreamResult struct {
	output any
	err    error
}

type writeStreamSession struct {
	id        string
	sessionID string
	kind      writeStreamKind
	startedAt time.Time

	writer *io.PipeWriter
	cancel context.CancelFunc
	done   chan writeStreamResult

	mu          sync.Mutex
	bytes       int64
	payloadSHA  string
	closed      bool
	committing  bool
	aborted     bool
	ingesting   bool
	transferred bool
}

type writeStreamStatus struct {
	BytesReceived    int64
	PayloadSHA256    string
	UploadCompleted  bool
	UploadInProgress bool
	Committing       bool
	Aborted          bool
}

func (s *writeStreamSession) commit() (writeStreamResult, int64, error) {
	s.mu.Lock()
	if s.aborted {
		bytes := s.bytes
		s.mu.Unlock()
		return writeStreamResult{}, bytes, fmt.Errorf("write stream %s already aborted", s.id)
	}
	if s.ingesting {
		bytes := s.bytes
		s.mu.Unlock()
		return writeStreamResult{}, bytes, fmt.Errorf("write stream %s upload in progress", s.id)
	}
	if s.committing {
		bytes := s.bytes
		s.mu.Unlock()
		return writeStreamResult{}, bytes, fmt.Errorf("write stream %s already committing", s.id)
	}
	s.committing = true
	bytes := s.bytes
	err := s.writer.Close()
	s.mu.Unlock()
	if err != nil {
		return writeStreamResult{}, bytes, err
	}
	result := <-s.done
	if result.err != nil {
		return writeStreamResult{}, bytes, result.err
	}
	return result, bytes, nil
}

func (s *writeStreamSession) abort(reason string) (int64, error) {
	s.mu.Lock()
	if s.committing {
		bytes := s.bytes
		s.mu.Unlock()
		return bytes, fmt.Errorf("write stream %s already committing", s.id)
	}
	if s.aborted {
		bytes := s.bytes
		s.mu.Unlock()
		return bytes, nil
	}
	s.aborted = true
	s.closed = true
	bytes := s.bytes
	err := s.writer.CloseWithError(fmt.Errorf("write stream aborted: %s", strings.TrimSpace(reason)))
	s.mu.Unlock()
	s.cancel()
	<-s.done
	return bytes, err
}

func (s *writeStreamSession) uploadFrom(reader io.Reader) (int64, int64, error) {
	if reader == nil {
		return 0, 0, fmt.Errorf("upload reader required")
	}
	s.mu.Lock()
	if s.closed || s.committing || s.aborted {
		total := s.bytes
		s.mu.Unlock()
		return 0, total, fmt.Errorf("write stream %s is not writable", s.id)
	}
	if s.ingesting {
		total := s.bytes
		s.mu.Unlock()
		return 0, total, fmt.Errorf("write stream %s upload already in progress", s.id)
	}
	if s.transferred {
		total := s.bytes
		s.mu.Unlock()
		return 0, total, fmt.Errorf("write stream %s already received transfer upload data", s.id)
	}
	s.ingesting = true
	s.mu.Unlock()

	hasher := sha256.New()
	written, err := io.Copy(s.writer, io.TeeReader(reader, hasher))

	s.mu.Lock()
	defer s.mu.Unlock()
	s.ingesting = false
	if written > 0 {
		s.bytes += written
	}
	total := s.bytes
	if err != nil {
		s.closed = true
		_ = s.writer.CloseWithError(err)
		return written, total, err
	}
	s.payloadSHA = hex.EncodeToString(hasher.Sum(nil))
	s.transferred = true
	return written, total, nil
}

func (s *writeStreamSession) status() writeStreamStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	payloadSHA := strings.TrimSpace(s.payloadSHA)
	if payloadSHA == "" && s.bytes == 0 {
		empty := sha256.Sum256(nil)
		payloadSHA = hex.EncodeToString(empty[:])
	}
	return writeStreamStatus{
		BytesReceived:    s.bytes,
		PayloadSHA256:    payloadSHA,
		UploadCompleted:  s.transferred,
		UploadInProgress: s.ingesting,
		Committing:       s.committing,
		Aborted:          s.aborted,
	}
}

type writeStreamManager struct {
	logger pslog.Logger

	mu             sync.Mutex
	streamsBySID   map[string]map[string]*writeStreamSession
	sessionWaiters map[string]struct{}
	nextID         uint64
}

func newWriteStreamManager(logger pslog.Logger) *writeStreamManager {
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	return &writeStreamManager{
		logger:         logger,
		streamsBySID:   make(map[string]map[string]*writeStreamSession),
		sessionWaiters: make(map[string]struct{}),
	}
}

func (m *writeStreamManager) Begin(session *mcpsdk.ServerSession, kind writeStreamKind, run func(context.Context, io.Reader) (any, error)) (string, error) {
	if session == nil {
		return "", fmt.Errorf("active MCP session required")
	}
	if run == nil {
		return "", fmt.Errorf("write stream runner required")
	}
	sessionID := normalizeMCPServerSessionID(session)
	streamID := m.nextStreamID(kind)
	reader, writer := io.Pipe()
	runCtx, cancel := context.WithCancel(context.Background())
	ws := &writeStreamSession{
		id:        streamID,
		sessionID: sessionID,
		kind:      kind,
		startedAt: time.Now().UTC(),
		writer:    writer,
		cancel:    cancel,
		done:      make(chan writeStreamResult, 1),
	}

	m.mu.Lock()
	if m.streamsBySID[sessionID] == nil {
		m.streamsBySID[sessionID] = make(map[string]*writeStreamSession)
	}
	m.streamsBySID[sessionID][streamID] = ws
	if _, ok := m.sessionWaiters[sessionID]; !ok {
		m.sessionWaiters[sessionID] = struct{}{}
		go m.waitSession(session, sessionID)
	}
	m.mu.Unlock()

	go func() {
		defer reader.Close()
		output, err := run(runCtx, reader)
		ws.done <- writeStreamResult{output: output, err: err}
	}()

	m.logger.Debug("mcp.write_stream.begin", "session_id", sessionID, "stream_id", streamID, "kind", string(kind))
	return streamID, nil
}

func (m *writeStreamManager) Upload(session *mcpsdk.ServerSession, streamID string, kind writeStreamKind, reader io.Reader) (int64, int64, error) {
	ws, err := m.lookup(session, streamID)
	if err != nil {
		return 0, 0, err
	}
	if ws.kind != kind {
		return 0, 0, fmt.Errorf("write stream %s kind mismatch: expected %s, got %s", ws.id, kind, ws.kind)
	}
	return ws.uploadFrom(reader)
}

func (m *writeStreamManager) Commit(session *mcpsdk.ServerSession, streamID string, kind writeStreamKind) (any, writeStreamStatus, error) {
	ws, err := m.take(session, streamID, kind)
	if err != nil {
		return nil, writeStreamStatus{}, err
	}
	result, _, err := ws.commit()
	if err != nil {
		return nil, ws.status(), err
	}
	status := ws.status()
	m.logger.Debug("mcp.write_stream.commit", "session_id", ws.sessionID, "stream_id", ws.id, "kind", string(ws.kind), "bytes", status.BytesReceived)
	return result.output, status, nil
}

func (m *writeStreamManager) Abort(session *mcpsdk.ServerSession, streamID string, kind writeStreamKind, reason string) (int64, error) {
	ws, err := m.take(session, streamID, kind)
	if err != nil {
		return 0, err
	}
	bytes, abortErr := ws.abort(reason)
	if abortErr != nil {
		return bytes, abortErr
	}
	m.logger.Debug("mcp.write_stream.abort", "session_id", ws.sessionID, "stream_id", ws.id, "kind", string(ws.kind), "bytes", bytes, "reason", strings.TrimSpace(reason))
	return bytes, nil
}

func (m *writeStreamManager) Status(session *mcpsdk.ServerSession, streamID string, kind writeStreamKind) (writeStreamStatus, error) {
	ws, err := m.lookup(session, streamID)
	if err != nil {
		return writeStreamStatus{}, err
	}
	if ws.kind != kind {
		return writeStreamStatus{}, fmt.Errorf("write stream %s kind mismatch: expected %s, got %s", ws.id, kind, ws.kind)
	}
	return ws.status(), nil
}

func (m *writeStreamManager) Close() {
	m.mu.Lock()
	sessions := make([]string, 0, len(m.streamsBySID))
	for sid := range m.streamsBySID {
		sessions = append(sessions, sid)
	}
	m.mu.Unlock()
	for _, sid := range sessions {
		m.abortSession(sid, "server closing")
	}
}

func (m *writeStreamManager) waitSession(session *mcpsdk.ServerSession, sessionID string) {
	if session == nil {
		return
	}
	_ = session.Wait()
	m.abortSession(sessionID, "mcp session closed")
}

func (m *writeStreamManager) abortSession(sessionID string, reason string) {
	m.mu.Lock()
	streams := m.streamsBySID[sessionID]
	delete(m.streamsBySID, sessionID)
	delete(m.sessionWaiters, sessionID)
	m.mu.Unlock()
	if len(streams) == 0 {
		return
	}
	for _, ws := range streams {
		_, _ = ws.abort(reason)
	}
	m.logger.Info("mcp.write_stream.session_abort", "session_id", sessionID, "streams", len(streams), "reason", strings.TrimSpace(reason))
}

func (m *writeStreamManager) lookup(session *mcpsdk.ServerSession, streamID string) (*writeStreamSession, error) {
	sid, normalizedStreamID, err := normalizeSessionAndStreamID(session, streamID)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	byStream := m.streamsBySID[sid]
	if byStream == nil {
		return nil, fmt.Errorf("write stream %s not found", normalizedStreamID)
	}
	ws := byStream[normalizedStreamID]
	if ws == nil {
		return nil, fmt.Errorf("write stream %s not found", normalizedStreamID)
	}
	return ws, nil
}

func (m *writeStreamManager) take(session *mcpsdk.ServerSession, streamID string, kind writeStreamKind) (*writeStreamSession, error) {
	sid, normalizedStreamID, err := normalizeSessionAndStreamID(session, streamID)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	byStream := m.streamsBySID[sid]
	if byStream == nil {
		return nil, fmt.Errorf("write stream %s not found", normalizedStreamID)
	}
	ws := byStream[normalizedStreamID]
	if ws == nil {
		return nil, fmt.Errorf("write stream %s not found", normalizedStreamID)
	}
	if ws.kind != kind {
		return nil, fmt.Errorf("write stream %s kind mismatch: expected %s, got %s", ws.id, kind, ws.kind)
	}
	delete(byStream, normalizedStreamID)
	if len(byStream) == 0 {
		delete(m.streamsBySID, sid)
	}
	return ws, nil
}

func (m *writeStreamManager) nextStreamID(kind writeStreamKind) string {
	id := atomic.AddUint64(&m.nextID, 1)
	kindValue := strings.TrimSpace(string(kind))
	if kindValue == "" {
		kindValue = "stream"
	}
	return fmt.Sprintf("lockd-%s-stream-%d", kindValue, id)
}

func normalizeSessionAndStreamID(session *mcpsdk.ServerSession, streamID string) (string, string, error) {
	if session == nil {
		return "", "", fmt.Errorf("active MCP session required")
	}
	normalizedStreamID := strings.TrimSpace(streamID)
	if normalizedStreamID == "" {
		return "", "", fmt.Errorf("stream_id is required")
	}
	return normalizeMCPServerSessionID(session), normalizedStreamID, nil
}

func normalizeMCPServerSessionID(session *mcpsdk.ServerSession) string {
	if session == nil {
		return ""
	}
	sid := strings.TrimSpace(session.ID())
	if sid != "" {
		return sid
	}
	return fmt.Sprintf("session-%p", session)
}
