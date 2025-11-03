package loggingutil

import (
	"io"
	"sync"

	"pkt.systems/pslog"
)

var (
	noOnce     sync.Once
	noLogger   pslog.Logger
	noBaseOnce sync.Once
	noBase     pslog.Base
)

// NoopLogger returns a disabled pslog.Logger that discards all entries.
func NoopLogger() pslog.Logger {
	noOnce.Do(func() {
		noLogger = pslog.NewWithOptions(io.Discard, pslog.Options{
			Mode:     pslog.ModeStructured,
			MinLevel: pslog.Disabled,
		})
	})
	return noLogger
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
	noBaseOnce.Do(func() {
		noBase = pslog.NewBaseLoggerWithOptions(io.Discard, pslog.Options{
			Mode:     pslog.ModeStructured,
			MinLevel: pslog.Disabled,
		})
	})
	return noBase
}

// EnsureBase returns b when non-nil, otherwise it returns a disabled logger.
func EnsureBase(b pslog.Base) pslog.Base {
	if b != nil {
		return b
	}
	return NoopBase()
}
