package connguard

import (
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

type captureEntry struct {
	level  string
	msg    string
	fields []any
}

func (e captureEntry) toMap() map[string]any {
	out := make(map[string]any)
	for i := 0; i+1 < len(e.fields); i += 2 {
		key, ok := e.fields[i].(string)
		if !ok {
			continue
		}
		out[key] = e.fields[i+1]
	}
	return out
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
