package uuidv7_test

import (
	"testing"

	"github.com/google/uuid"

	"pkt.systems/lockd/internal/uuidv7"
)

func TestNewReturnsUUIDv7(t *testing.T) {
	t.Parallel()

	id := uuidv7.New()
	if id.Version() != 7 {
		t.Fatalf("expected version 7 UUID, got %d", id.Version())
	}
	other := uuidv7.New()
	if id == other {
		t.Fatal("expected unique UUIDs on subsequent calls")
	}
}

func TestNewStringParsesAsUUIDv7(t *testing.T) {
	t.Parallel()

	raw := uuidv7.NewString()
	parsed, err := uuid.Parse(raw)
	if err != nil {
		t.Fatalf("uuid.Parse: %v", err)
	}
	if parsed.Version() != 7 {
		t.Fatalf("expected version 7 from string, got %d", parsed.Version())
	}
}
