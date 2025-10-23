package correlation

import (
	"context"
	"strings"
	"sync"

	"pkt.systems/lockd/internal/uuidv7"
)

// MaxIDLength defines the maximum number of characters accepted for correlation identifiers.
const MaxIDLength = 128

type contextKey struct{}

type state struct {
	mu sync.RWMutex
	id string
}

// Ensure attaches correlation state to ctx if not already present.
func Ensure(ctx context.Context) context.Context {
	if ctx == nil {
		return context.WithValue(context.Background(), contextKey{}, &state{})
	}
	if _, ok := ctx.Value(contextKey{}).(*state); ok {
		return ctx
	}
	return context.WithValue(ctx, contextKey{}, &state{})
}

// Set records the correlation ID on ctx and returns the context carrying the state.
func Set(ctx context.Context, id string) context.Context {
	if normalized, ok := Normalize(id); ok {
		ctx = Ensure(ctx)
		st, _ := ctx.Value(contextKey{}).(*state)
		st.mu.Lock()
		st.id = normalized
		st.mu.Unlock()
		return ctx
	}
	return ctx
}

// ID retrieves the correlation ID stored on ctx, if any.
func ID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if st, ok := ctx.Value(contextKey{}).(*state); ok && st != nil {
		st.mu.RLock()
		id := st.id
		st.mu.RUnlock()
		return id
	}
	return ""
}

// Has reports whether ctx carries a correlation ID.
func Has(ctx context.Context) bool {
	return ID(ctx) != ""
}

// Normalize validates and canonicalizes an external correlation identifier.
// It returns the normalized ID and true if the input is acceptable.
func Normalize(id string) (string, bool) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", false
	}
	if len(id) > MaxIDLength {
		return "", false
	}
	for _, r := range id {
		if r < 0x20 || r > 0x7e {
			return "", false
		}
	}
	return id, true
}

// Generate produces a new random correlation identifier.
func Generate() string {
	return uuidv7.NewString()
}
