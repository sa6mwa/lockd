package loggingutil

import (
	"pkt.systems/pslog"
)

// noopLogger is a minimal pslog.Logger implementation that drops everything
// without allocating.
type noopLogger struct{}

func (noopLogger) Trace(string, ...any)            {}
func (noopLogger) Debug(string, ...any)            {}
func (noopLogger) Info(string, ...any)             {}
func (noopLogger) Warn(string, ...any)             {}
func (noopLogger) Error(string, ...any)            {}
func (noopLogger) Fatal(string, ...any)            {}
func (noopLogger) Panic(string, ...any)            {}
func (noopLogger) Log(pslog.Level, string, ...any) {}
func (n noopLogger) With(...any) pslog.Logger      { return n }
func (n noopLogger) WithLogLevel() pslog.Logger    { return n }
func (n noopLogger) LogLevel(pslog.Level) pslog.Logger {
	return n
}
func (n noopLogger) LogLevelFromEnv(string) pslog.Logger { return n }

// NoopLogger returns a disabled pslog.Logger that discards all entries.
func NoopLogger() pslog.Logger {
	return noopLogger{}
}

// IsNoop reports whether the provided logger is a noopLogger (alloc-free dropper).
func IsNoop(logger pslog.Logger) bool {
	_, ok := logger.(noopLogger)
	return ok
}

// EnsureLogger returns l when non-nil, otherwise it returns a disabled logger.
func EnsureLogger(l pslog.Logger) pslog.Logger {
	if l != nil {
		return l
	}
	return NoopLogger()
}

// NoopBase returns a disabled pslog.Base that discards all entries.
func NoopBase() pslog.Base {
	return noopLogger{}
}

// EnsureBase returns b when non-nil, otherwise it returns a disabled logger.
func EnsureBase(b pslog.Base) pslog.Base {
	if b != nil {
		return b
	}
	return NoopBase()
}
