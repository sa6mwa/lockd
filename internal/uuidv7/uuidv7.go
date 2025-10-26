package uuidv7

import (
	"time"

	"github.com/google/uuid"
)

// New returns a UUIDv7 value (time-ordered) or panics if generation fails.
func New() uuid.UUID {
	return uuid.Must(uuid.NewV7())
}

// NewString returns a string representation of a UUIDv7.
func NewString() string {
	return New().String()
}

// Time extracts the Unix timestamp encoded in a UUIDv7. It returns false when
// the UUID is not version 7 or does not use the RFC4122 variant.
func Time(u uuid.UUID) (time.Time, bool) {
	if u.Version() != 7 || u.Variant() != uuid.RFC4122 {
		return time.Time{}, false
	}
	timestamp := uint64(u[0])<<40 |
		uint64(u[1])<<32 |
		uint64(u[2])<<24 |
		uint64(u[3])<<16 |
		uint64(u[4])<<8 |
		uint64(u[5])
	return time.Unix(0, int64(timestamp)*int64(time.Millisecond)), true
}

// ParseTime parses a UUID string and extracts the encoded timestamp when the
// UUID is version 7. Returns false when parsing fails or the UUID is not v7.
func ParseTime(s string) (time.Time, bool) {
	id, err := uuid.Parse(s)
	if err != nil {
		return time.Time{}, false
	}
	return Time(id)
}
