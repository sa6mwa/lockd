package uuidv7

import "github.com/google/uuid"

// New returns a UUIDv7 value (time-ordered) or panics if generation fails.
func New() uuid.UUID {
	return uuid.Must(uuid.NewV7())
}

// NewString returns a string representation of a UUIDv7.
func NewString() string {
	return New().String()
}
