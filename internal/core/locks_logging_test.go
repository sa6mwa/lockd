package core

import (
	"context"
	"sync"
	"testing"

	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

func newTestServiceWithLogger(t testing.TB, logger pslog.Logger) *Service {
	t.Helper()
	mem := memory.New()
	return New(Config{
		Store:            mem,
		BackendHash:      "test-backend",
		DefaultNamespace: "default",
		Logger:           logger,
	})
}

type captureLevelLogger struct {
	mu      sync.Mutex
	entries map[string]map[string]int
}

func newCaptureLevelLogger() *captureLevelLogger {
	return &captureLevelLogger{
		entries: make(map[string]map[string]int),
	}
}

func (l *captureLevelLogger) record(level, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	byMsg := l.entries[level]
	if byMsg == nil {
		byMsg = make(map[string]int)
		l.entries[level] = byMsg
	}
	byMsg[msg]++
}

func (l *captureLevelLogger) seen(level, msg string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.entries[level][msg] > 0
}

func (l *captureLevelLogger) Trace(msg string, _ ...any) { l.record("trace", msg) }
func (l *captureLevelLogger) Debug(msg string, _ ...any) { l.record("debug", msg) }
func (l *captureLevelLogger) Info(msg string, _ ...any)  { l.record("info", msg) }
func (l *captureLevelLogger) Warn(msg string, _ ...any)  { l.record("warn", msg) }
func (l *captureLevelLogger) Error(msg string, _ ...any) { l.record("error", msg) }
func (l *captureLevelLogger) Fatal(msg string, _ ...any) { l.record("fatal", msg) }
func (l *captureLevelLogger) Panic(msg string, _ ...any) { l.record("panic", msg) }
func (l *captureLevelLogger) Log(level pslog.Level, msg string, _ ...any) {
	switch level {
	case pslog.TraceLevel:
		l.record("trace", msg)
	case pslog.DebugLevel:
		l.record("debug", msg)
	case pslog.InfoLevel:
		l.record("info", msg)
	case pslog.WarnLevel:
		l.record("warn", msg)
	case pslog.ErrorLevel:
		l.record("error", msg)
	case pslog.FatalLevel:
		l.record("fatal", msg)
	case pslog.PanicLevel:
		l.record("panic", msg)
	default:
		l.record("other", msg)
	}
}
func (l *captureLevelLogger) With(_ ...any) pslog.Logger { return l }
func (l *captureLevelLogger) WithLogLevel() pslog.Logger { return l }
func (l *captureLevelLogger) LogLevel(_ pslog.Level) pslog.Logger {
	return l
}
func (l *captureLevelLogger) LogLevelFromEnv(_ string) pslog.Logger { return l }

func TestAcquireLogsAtDebugLevel(t *testing.T) {
	ctx := context.Background()
	cmd := AcquireCommand{
		Key:        "logging-key",
		Owner:      "tester",
		TTLSeconds: 30,
	}

	logger := newCaptureLevelLogger()
	svc := newTestServiceWithLogger(t, logger)
	ctx = pslog.ContextWithLogger(ctx, logger)
	if _, err := svc.Acquire(ctx, cmd); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	if logger.seen("info", "lease.acquire.begin") || logger.seen("info", "lease.acquire.success") {
		t.Fatalf("expected acquire hot-path logs to avoid info level")
	}
	if !logger.seen("debug", "lease.acquire.begin") {
		t.Fatalf("expected debug logs to include lease.acquire.begin")
	}
	if !logger.seen("debug", "lease.acquire.success") {
		t.Fatalf("expected debug logs to include lease.acquire.success")
	}
}
