package client

import (
	"context"
	"strings"

	"pkt.systems/lockd/internal/uuidv7"
)

// MaxCorrelationIDLength bounds the length of client-supplied correlation identifiers.
const MaxCorrelationIDLength = 128

type correlationContextKey struct{}

// NormalizeCorrelationID trims and validates an identifier.
func NormalizeCorrelationID(id string) (string, bool) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", false
	}
	if len(id) > MaxCorrelationIDLength {
		return "", false
	}
	for _, r := range id {
		if r < 0x20 || r > 0x7e {
			return "", false
		}
	}
	return id, true
}

// WithCorrelationID annotates ctx with a correlation identifier to be sent with subsequent requests.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	normalized, ok := NormalizeCorrelationID(id)
	if !ok {
		return ctx
	}
	return context.WithValue(ctx, correlationContextKey{}, normalized)
}

// CorrelationIDFromContext extracts the correlation identifier carried by ctx, if present.
func CorrelationIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(correlationContextKey{}).(string); ok {
		return v
	}
	return ""
}

// GenerateCorrelationID creates a new random correlation identifier.
func GenerateCorrelationID() string {
	return uuidv7.NewString()
}
