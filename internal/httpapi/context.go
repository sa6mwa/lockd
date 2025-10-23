package httpapi

import (
	"context"
)

// contextKey is a private type to avoid collisions with external context keys.
type contextKey string

const clientIDKey contextKey = "httpapi-client-id"

// WithClientIdentity returns a new context carrying the TLS client identity
// extracted during request handling. The expectation is that middleware sets
// this based on the verified certificate's subject/serial.
func WithClientIdentity(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, clientIDKey, id)
}

// clientIdentityFromContext retrieves the client identity, if any.
func clientIdentityFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(clientIDKey).(string); ok {
		return v
	}
	return ""
}
