package loggingutil

import (
	"fmt"
	"strings"

	"pkt.systems/pslog"
)

type subsystemLogger struct {
	base      pslog.Logger
	subsystem string
	keyvals   []any
}

// Subsystem builds a dot-delimited subsystem path from the supplied parts while
// skipping empty fragments.
func Subsystem(parts ...string) string {
	if len(parts) == 0 {
		return ""
	}
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(part, ". ")
		if part == "" {
			continue
		}
		filtered = append(filtered, part)
	}
	if len(filtered) == 0 {
		return ""
	}
	return strings.Join(filtered, ".")
}

// WithSubsystem returns a logger that automatically attaches the provided
// subsystem path to every log entry. When logger already carries subsystem
// metadata, the new value replaces the previous one while preserving other
// contextual fields.
func WithSubsystem(logger pslog.Logger, subsystem string) pslog.Logger {
	if subsystem == "" {
		return EnsureLogger(logger)
	}
	switch existing := logger.(type) {
	case *subsystemLogger:
		return &subsystemLogger{
			base:      ensureBase(existing.base),
			subsystem: subsystem,
			keyvals:   cloneKeyvals(existing.keyvals),
		}
	default:
		return &subsystemLogger{
			base:      ensureBase(logger),
			subsystem: subsystem,
			keyvals:   nil,
		}
	}
}

func ensureBase(logger pslog.Logger) pslog.Logger {
	if logger != nil {
		return logger
	}
	return NoopLogger()
}

func cloneKeyvals(src []any) []any {
	if len(src) == 0 {
		return nil
	}
	dst := make([]any, len(src))
	copy(dst, src)
	return dst
}

func (l *subsystemLogger) Trace(msg string, keyvals ...any) {
	l.base.Trace(msg, l.merged(keyvals)...)
}

func (l *subsystemLogger) Debug(msg string, keyvals ...any) {
	l.base.Debug(msg, l.merged(keyvals)...)
}

func (l *subsystemLogger) Info(msg string, keyvals ...any) {
	l.base.Info(msg, l.merged(keyvals)...)
}

func (l *subsystemLogger) Warn(msg string, keyvals ...any) {
	l.base.Warn(msg, l.merged(keyvals)...)
}

func (l *subsystemLogger) Error(msg string, keyvals ...any) {
	l.base.Error(msg, l.merged(keyvals)...)
}

func (l *subsystemLogger) Fatal(msg string, keyvals ...any) {
	l.base.Fatal(msg, l.merged(keyvals)...)
}

func (l *subsystemLogger) Panic(msg string, keyvals ...any) {
	l.base.Panic(msg, l.merged(keyvals)...)
}

func (l *subsystemLogger) Log(level pslog.Level, msg string, keyvals ...any) {
	l.base.Log(level, msg, l.merged(keyvals)...)
}

func (l *subsystemLogger) With(keyvals ...any) pslog.Logger {
	if len(keyvals) == 0 {
		return &subsystemLogger{
			base:      ensureBase(l.base),
			subsystem: l.subsystem,
			keyvals:   cloneKeyvals(l.keyvals),
		}
	}
	newLogger := &subsystemLogger{
		base:      ensureBase(l.base),
		subsystem: l.subsystem,
		keyvals:   cloneKeyvals(l.keyvals),
	}
	extra := make([]any, 0, len(keyvals))
	newSubsystem := l.subsystem
	for i := 0; i < len(keyvals); {
		key := keyvals[i]
		name, ok := keyName(key)
		if ok && name == "sys" && i+1 < len(keyvals) {
			newSubsystem = fmt.Sprint(keyvals[i+1])
			i += 2
			continue
		}
		extra = append(extra, key)
		i++
		if i < len(keyvals) {
			extra = append(extra, keyvals[i])
			i++
		}
	}
	newLogger.subsystem = newSubsystem
	if len(extra) > 0 {
		newLogger.keyvals = append(newLogger.keyvals, extra...)
	}
	return newLogger
}

func (l *subsystemLogger) WithLogLevel() pslog.Logger {
	return &subsystemLogger{
		base:      ensureBase(l.base).WithLogLevel(),
		subsystem: l.subsystem,
		keyvals:   cloneKeyvals(l.keyvals),
	}
}

func (l *subsystemLogger) LogLevel(level pslog.Level) pslog.Logger {
	return &subsystemLogger{
		base:      ensureBase(l.base).LogLevel(level),
		subsystem: l.subsystem,
		keyvals:   cloneKeyvals(l.keyvals),
	}
}

func (l *subsystemLogger) LogLevelFromEnv(key string) pslog.Logger {
	return &subsystemLogger{
		base:      ensureBase(l.base).LogLevelFromEnv(key),
		subsystem: l.subsystem,
		keyvals:   cloneKeyvals(l.keyvals),
	}
}

func (l *subsystemLogger) merged(extra []any) []any {
	total := 2 + len(l.keyvals) + len(extra)
	result := make([]any, 0, total)
	result = append(result, pslog.TrustedString("sys"), l.subsystem)
	if len(l.keyvals) > 0 {
		result = append(result, l.keyvals...)
	}
	if len(extra) > 0 {
		result = append(result, extra...)
	}
	return result
}

func keyName(key any) (string, bool) {
	switch v := key.(type) {
	case string:
		return v, true
	case pslog.TrustedString:
		return string(v), true
	default:
		return "", false
	}
}
