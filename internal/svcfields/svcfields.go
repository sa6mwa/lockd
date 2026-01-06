package svcfields

import (
	"strings"

	"pkt.systems/pslog"
)

// SubsystemKey is the canonical key for subsystem tags.
const SubsystemKey = pslog.TrustedString("sys")

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

// WithSubsystem attaches a subsystem tag to every log entry.
func WithSubsystem(logger pslog.Logger, subsystem string) pslog.Logger {
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	subsystem = strings.Trim(subsystem, ". ")
	if subsystem == "" {
		return logger
	}
	return logger.With(SubsystemKey, subsystem)
}
