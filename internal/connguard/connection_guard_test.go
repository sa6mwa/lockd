package connguard

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"pkt.systems/pslog"
)

func TestConnectionGuardClassifiesAndBlocks(t *testing.T) {
	now := time.Now()
	g := NewConnectionGuard(ConnectionGuardConfig{
		Enabled:          true,
		FailureThreshold: 3,
		FailureWindow:    time.Second,
		BlockDuration:    500 * time.Millisecond,
		ProbeTimeout:     50 * time.Millisecond,
	}, pslog.NoopLogger())
	g.now = func() time.Time { return now }

	remote := "127.0.0.1:5555"
	if g.classifyFailure(remote, "zero_connect") {
		t.Fatalf("first event should not block")
	}
	now = now.Add(50 * time.Millisecond)
	if g.classifyFailure(remote, "zero_connect") {
		t.Fatalf("second event should not block")
	}
	now = now.Add(50 * time.Millisecond)
	if !g.classifyFailure(remote, "zero_connect") {
		t.Fatalf("third event should block")
	}

	now = now.Add(100 * time.Millisecond)
	if !g.isBlocked(remote) {
		t.Fatalf("expected remote to remain blocked")
	}
	now = now.Add(600 * time.Millisecond)
	if g.isBlocked(remote) {
		t.Fatalf("expected block to expire")
	}

	if g.classifyFailure(remote, "zero_connect") {
		t.Fatalf("post-expiry event should not block immediately")
	}
}

func TestConnectionGuardLogsEngagementLifecycle(t *testing.T) {
	now := time.Now()
	logger := newCaptureLogger()
	g := NewConnectionGuard(ConnectionGuardConfig{
		Enabled:          true,
		FailureThreshold: 2,
		FailureWindow:    time.Second,
		BlockDuration:    500 * time.Millisecond,
		ProbeTimeout:     50 * time.Millisecond,
	}, logger)
	g.now = func() time.Time { return now }

	remote := "127.0.0.1:5555"
	if g.classifyFailure(remote, "zero_connect") {
		t.Fatalf("first event should not block")
	}
	now = now.Add(50 * time.Millisecond)
	if !g.classifyFailure(remote, "zero_connect") {
		t.Fatalf("second event should block")
	}

	if _, ok := logger.find("lockd.connguard.engaged"); !ok {
		t.Fatalf("expected lockd.connguard.engaged log; logs=%v", logger.snapshot())
	}
	if _, ok := logger.find("lockd.connguard.blocked"); !ok {
		t.Fatalf("expected lockd.connguard.blocked log; logs=%v", logger.snapshot())
	}

	now = now.Add(600 * time.Millisecond)
	if g.isBlocked(remote) {
		t.Fatalf("expected block to expire")
	}
	if _, ok := logger.find("lockd.connguard.disengaged"); !ok {
		t.Fatalf("expected lockd.connguard.disengaged log; logs=%v", logger.snapshot())
	}
}

func TestConnectionGuardPrefixedConnPreservesByte(t *testing.T) {
	server, client := net.Pipe()
	defer func() {
		_ = server.Close()
		_ = client.Close()
	}()

	go func() {
		_, _ = client.Write([]byte("bc"))
		_ = client.Close()
	}()

	pc := &prefixedConn{
		Conn:   server,
		prefix: []byte("a"),
	}
	out := make([]byte, 4)
	n, err := pc.Read(out)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read: %v", err)
	}
	if string(out[:n]) != "abc" {
		t.Fatalf("expected abc, got %q", string(out[:n]))
	}
}

func TestConnectionGuardTLSTimeoutDoesNotBlock(t *testing.T) {
	logger := newCaptureLogger()
	g := NewConnectionGuard(ConnectionGuardConfig{
		Enabled:          true,
		FailureThreshold: 2,
		FailureWindow:    time.Second,
		BlockDuration:    time.Minute,
		ProbeTimeout:     25 * time.Millisecond,
	}, logger)
	listener := &guardedListener{
		guard:     g,
		tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	remote := "127.0.0.1:7777"

	for i := 0; i < 3; i++ {
		conn := &failingConn{
			remote:  remote,
			readErr: timeoutReadError{},
		}
		accepted, rejected, err := listener.wrapTLSConnection(conn, remote)
		if err == nil {
			t.Fatalf("expected tls handshake timeout")
		}
		if !rejected {
			t.Fatalf("expected timeout connection to be rejected")
		}
		if accepted != nil {
			_ = accepted.Close()
		}
	}

	if g.isBlocked(remote) {
		t.Fatalf("timeout handshakes must not trigger hard block")
	}
	if _, ok := logger.find("lockd.connguard.engaged"); ok {
		t.Fatalf("timeout handshakes must not emit engaged log")
	}
}

func TestConnectionGuardTLSHandshakeErrorStillBlocks(t *testing.T) {
	g := NewConnectionGuard(ConnectionGuardConfig{
		Enabled:          true,
		FailureThreshold: 2,
		FailureWindow:    time.Second,
		BlockDuration:    time.Minute,
		ProbeTimeout:     25 * time.Millisecond,
	}, pslog.NoopLogger())
	listener := &guardedListener{
		guard:     g,
		tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	remote := "127.0.0.1:8888"

	for i := 0; i < 2; i++ {
		conn := &failingConn{
			remote:  remote,
			readErr: io.EOF,
		}
		accepted, rejected, err := listener.wrapTLSConnection(conn, remote)
		if err == nil {
			t.Fatalf("expected tls handshake failure")
		}
		if !rejected {
			t.Fatalf("expected malformed connection to be rejected")
		}
		if accepted != nil {
			_ = accepted.Close()
		}
	}

	if !g.isBlocked(remote) {
		t.Fatalf("non-timeout handshake failures must still block after threshold")
	}
}

type captureEntry struct {
	level  string
	msg    string
	fields []any
}

type captureLogger struct {
	fields  []any
	entries *[]captureEntry
}

func newCaptureLogger() *captureLogger {
	entries := make([]captureEntry, 0, 8)
	return &captureLogger{entries: &entries}
}

func (l *captureLogger) cloneWith(args ...any) *captureLogger {
	combined := append([]any{}, l.fields...)
	combined = append(combined, args...)
	return &captureLogger{fields: combined, entries: l.entries}
}

func (l *captureLogger) find(msg string) (captureEntry, bool) {
	if l == nil || l.entries == nil {
		return captureEntry{}, false
	}
	for _, entry := range *l.entries {
		if entry.msg == msg {
			return entry, true
		}
	}
	return captureEntry{}, false
}

func (l *captureLogger) snapshot() []captureEntry {
	if l == nil || l.entries == nil {
		return nil
	}
	out := make([]captureEntry, len(*l.entries))
	copy(out, *l.entries)
	return out
}

func (l *captureLogger) record(level, msg string, args ...any) {
	fields := append([]any{}, l.fields...)
	fields = append(fields, args...)
	*l.entries = append(*l.entries, captureEntry{level: level, msg: msg, fields: fields})
}

func (l *captureLogger) Trace(msg string, args ...any) { l.record("trace", msg, args...) }
func (l *captureLogger) Debug(msg string, args ...any) { l.record("debug", msg, args...) }
func (l *captureLogger) Info(msg string, args ...any)  { l.record("info", msg, args...) }
func (l *captureLogger) Warn(msg string, args ...any)  { l.record("warn", msg, args...) }
func (l *captureLogger) Error(msg string, args ...any) { l.record("error", msg, args...) }
func (l *captureLogger) Fatal(msg string, args ...any) { l.record("fatal", msg, args...) }
func (l *captureLogger) Panic(msg string, args ...any) { l.record("panic", msg, args...) }
func (l *captureLogger) Log(level pslog.Level, msg string, args ...any) {
	l.record(pslog.LevelString(level), msg, args...)
}
func (l *captureLogger) With(args ...any) pslog.Logger { return l.cloneWith(args...) }
func (l *captureLogger) WithLogLevel() pslog.Logger    { return l }
func (l *captureLogger) LogLevel(pslog.Level) pslog.Logger {
	return l
}
func (l *captureLogger) LogLevelFromEnv(string) pslog.Logger { return l }

type timeoutReadError struct{}

func (timeoutReadError) Error() string   { return "i/o timeout" }
func (timeoutReadError) Timeout() bool   { return true }
func (timeoutReadError) Temporary() bool { return true }

type fakeAddr string

func (a fakeAddr) Network() string { return "tcp" }
func (a fakeAddr) String() string  { return string(a) }

type failingConn struct {
	remote      string
	readErr     error
	readInvoked bool
}

func (c *failingConn) Read(p []byte) (int, error) {
	c.readInvoked = true
	if c.readErr != nil {
		return 0, c.readErr
	}
	return 0, io.EOF
}

func (c *failingConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *failingConn) Close() error                { return nil }
func (c *failingConn) LocalAddr() net.Addr         { return fakeAddr("127.0.0.1:0") }
func (c *failingConn) RemoteAddr() net.Addr        { return fakeAddr(c.remote) }
func (c *failingConn) SetDeadline(time.Time) error { return nil }
func (c *failingConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *failingConn) SetWriteDeadline(time.Time) error {
	return nil
}
