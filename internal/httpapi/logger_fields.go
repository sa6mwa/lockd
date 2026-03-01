package httpapi

import "pkt.systems/pslog"

type prefixedLogger struct {
	base   pslog.Logger
	fields []any
}

func withLoggerFields(logger pslog.Logger, keyvals ...any) pslog.Logger {
	if logger == nil || len(keyvals) == 0 {
		return logger
	}
	prefix := append([]any(nil), keyvals...)
	if existing, ok := logger.(*prefixedLogger); ok {
		combined := append(append([]any(nil), existing.fields...), prefix...)
		return &prefixedLogger{base: existing.base, fields: combined}
	}
	return &prefixedLogger{base: logger, fields: prefix}
}

func (l *prefixedLogger) appendFields(args ...any) []any {
	if l == nil || len(l.fields) == 0 {
		return args
	}
	if len(args) == 0 {
		return append([]any(nil), l.fields...)
	}
	out := make([]any, 0, len(l.fields)+len(args))
	out = append(out, l.fields...)
	out = append(out, args...)
	return out
}

func (l *prefixedLogger) Trace(msg string, args ...any) {
	if l == nil || l.base == nil {
		return
	}
	l.base.Trace(msg, l.appendFields(args...)...)
}

func (l *prefixedLogger) Debug(msg string, args ...any) {
	if l == nil || l.base == nil {
		return
	}
	l.base.Debug(msg, l.appendFields(args...)...)
}

func (l *prefixedLogger) Info(msg string, args ...any) {
	if l == nil || l.base == nil {
		return
	}
	l.base.Info(msg, l.appendFields(args...)...)
}

func (l *prefixedLogger) Warn(msg string, args ...any) {
	if l == nil || l.base == nil {
		return
	}
	l.base.Warn(msg, l.appendFields(args...)...)
}

func (l *prefixedLogger) Error(msg string, args ...any) {
	if l == nil || l.base == nil {
		return
	}
	l.base.Error(msg, l.appendFields(args...)...)
}

func (l *prefixedLogger) Fatal(msg string, args ...any) {
	if l == nil || l.base == nil {
		return
	}
	l.base.Fatal(msg, l.appendFields(args...)...)
}

func (l *prefixedLogger) Panic(msg string, args ...any) {
	if l == nil || l.base == nil {
		return
	}
	l.base.Panic(msg, l.appendFields(args...)...)
}

func (l *prefixedLogger) Log(level pslog.Level, msg string, args ...any) {
	if l == nil || l.base == nil {
		return
	}
	l.base.Log(level, msg, l.appendFields(args...)...)
}

func (l *prefixedLogger) With(args ...any) pslog.Logger {
	return withLoggerFields(l, args...)
}

func (l *prefixedLogger) WithLogLevel() pslog.Logger {
	if l == nil || l.base == nil {
		return l
	}
	return withLoggerFields(l.base.WithLogLevel(), l.fields...)
}

func (l *prefixedLogger) LogLevel(level pslog.Level) pslog.Logger {
	if l == nil || l.base == nil {
		return l
	}
	return withLoggerFields(l.base.LogLevel(level), l.fields...)
}

func (l *prefixedLogger) LogLevelFromEnv(key string) pslog.Logger {
	if l == nil || l.base == nil {
		return l
	}
	return withLoggerFields(l.base.LogLevelFromEnv(key), l.fields...)
}
