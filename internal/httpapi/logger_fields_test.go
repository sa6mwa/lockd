package httpapi

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"pkt.systems/lockd/internal/correlation"
	"pkt.systems/pslog"
)

type loggerFieldEntry struct {
	msg  string
	args []any
}

type loggerFieldCapture struct {
	entries  []loggerFieldEntry
	withCall atomic.Int32
}

func (l *loggerFieldCapture) append(msg string, args ...any) {
	if l == nil {
		return
	}
	copied := append([]any(nil), args...)
	l.entries = append(l.entries, loggerFieldEntry{msg: msg, args: copied})
}

func (l *loggerFieldCapture) Trace(msg string, args ...any) { l.append(msg, args...) }
func (l *loggerFieldCapture) Debug(msg string, args ...any) { l.append(msg, args...) }
func (l *loggerFieldCapture) Info(msg string, args ...any)  { l.append(msg, args...) }
func (l *loggerFieldCapture) Warn(msg string, args ...any)  { l.append(msg, args...) }
func (l *loggerFieldCapture) Error(msg string, args ...any) { l.append(msg, args...) }
func (l *loggerFieldCapture) Fatal(msg string, args ...any) { l.append(msg, args...) }
func (l *loggerFieldCapture) Panic(msg string, args ...any) { l.append(msg, args...) }
func (l *loggerFieldCapture) Log(level pslog.Level, msg string, args ...any) {
	l.append(msg, append([]any{"level", level}, args...)...)
}
func (l *loggerFieldCapture) With(args ...any) pslog.Logger {
	l.withCall.Add(1)
	return withLoggerFields(l, args...)
}
func (l *loggerFieldCapture) WithLogLevel() pslog.Logger          { return l }
func (l *loggerFieldCapture) LogLevel(pslog.Level) pslog.Logger   { return l }
func (l *loggerFieldCapture) LogLevelFromEnv(string) pslog.Logger { return l }

func TestWithLoggerFieldsPrefixesWithoutCallingBaseWith(t *testing.T) {
	base := &loggerFieldCapture{}
	logger := withLoggerFields(base, "cid", "abc")
	logger = logger.With("client_identity", "id-1")
	logger.Info("event", "k", "v")

	if got := base.withCall.Load(); got != 0 {
		t.Fatalf("expected base With not to be called, got %d", got)
	}
	if len(base.entries) != 1 {
		t.Fatalf("expected one log entry, got %d", len(base.entries))
	}
	want := []any{"cid", "abc", "client_identity", "id-1", "k", "v"}
	if !reflect.DeepEqual(base.entries[0].args, want) {
		t.Fatalf("unexpected args: got=%v want=%v", base.entries[0].args, want)
	}
}

func TestApplyCorrelationUsesPrefixedLoggerWithoutWith(t *testing.T) {
	base := &loggerFieldCapture{}
	ctx := correlation.Set(context.Background(), "cid-123")

	ctx, logger := applyCorrelation(ctx, base, nil)
	logger.Info("event", "k", "v")

	if got := base.withCall.Load(); got != 0 {
		t.Fatalf("expected applyCorrelation to avoid base With, got %d", got)
	}
	if len(base.entries) != 1 {
		t.Fatalf("expected one log entry, got %d", len(base.entries))
	}
	want := []any{"cid", "cid-123", "k", "v"}
	if !reflect.DeepEqual(base.entries[0].args, want) {
		t.Fatalf("unexpected args: got=%v want=%v", base.entries[0].args, want)
	}
	if gotLogger := pslog.LoggerFromContext(ctx); gotLogger == nil {
		t.Fatalf("expected logger stored in context")
	}
}
