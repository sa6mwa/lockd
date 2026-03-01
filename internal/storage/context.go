package storage

import "context"

type contextKey string

const (
	stateDescriptorKey     contextKey = "storage.state_descriptor"
	stateReadHintsKey      contextKey = "storage.state_read_hints"
	objectDescriptorKey    contextKey = "storage.object_descriptor"
	statePlaintextSizeKey  contextKey = "storage.state_plaintext_size"
	objectPlaintextSizeKey contextKey = "storage.object_plaintext_size"
	stateObjectContextKey  contextKey = "storage.state_object_context"
	noSyncKey              contextKey = "storage.no_sync"
)

type stateReadHints struct {
	descriptor    []byte
	plaintextSize int64
}

// ContextWithStateDescriptor attaches a state descriptor to ctx for use by storage backends.
func ContextWithStateDescriptor(ctx context.Context, descriptor []byte) context.Context {
	return contextWithStateReadHints(ctx, descriptor, 0, true)
}

// ContextWithStateReadHints attaches both descriptor and plaintext size for state reads.
func ContextWithStateReadHints(ctx context.Context, descriptor []byte, plaintextSize int64) context.Context {
	return contextWithStateReadHints(ctx, descriptor, plaintextSize, true)
}

// ContextWithStateReadHintsBorrowed attaches state read hints without copying descriptor.
// Callers must treat descriptor as immutable for the lifetime of the context usage.
func ContextWithStateReadHintsBorrowed(ctx context.Context, descriptor []byte, plaintextSize int64) context.Context {
	return contextWithStateReadHints(ctx, descriptor, plaintextSize, false)
}

func contextWithStateReadHints(ctx context.Context, descriptor []byte, plaintextSize int64, copyDescriptor bool) context.Context {
	if len(descriptor) == 0 && plaintextSize <= 0 {
		return ctx
	}
	hints := stateReadHints{
		plaintextSize: plaintextSize,
	}
	if len(descriptor) > 0 {
		if copyDescriptor {
			hints.descriptor = append([]byte(nil), descriptor...)
		} else {
			hints.descriptor = descriptor
		}
	}
	return context.WithValue(ctx, stateReadHintsKey, hints)
}

// StateDescriptorFromContext retrieves a state descriptor previously attached to ctx.
func StateDescriptorFromContext(ctx context.Context) ([]byte, bool) {
	hintsValue := ctx.Value(stateReadHintsKey)
	hints, hintsOK := hintsValue.(stateReadHints)
	if hintsOK && len(hints.descriptor) > 0 {
		return hints.descriptor, true
	}
	value := ctx.Value(stateDescriptorKey)
	descriptor, ok := value.([]byte)
	if !ok || len(descriptor) == 0 {
		return nil, false
	}
	return descriptor, true
}

// ContextWithObjectDescriptor attaches a generic object descriptor (e.g., queue payload) to ctx.
func ContextWithObjectDescriptor(ctx context.Context, descriptor []byte) context.Context {
	if len(descriptor) == 0 {
		return ctx
	}
	buf := append([]byte(nil), descriptor...)
	return context.WithValue(ctx, objectDescriptorKey, buf)
}

// ObjectDescriptorFromContext returns an object descriptor attached to ctx, if present.
func ObjectDescriptorFromContext(ctx context.Context) ([]byte, bool) {
	value := ctx.Value(objectDescriptorKey)
	if value == nil {
		return nil, false
	}
	descriptor, ok := value.([]byte)
	if !ok {
		return nil, false
	}
	return descriptor, true
}

// ContextWithObjectPlaintextSize attaches a plaintext size hint for generic objects.
func ContextWithObjectPlaintextSize(ctx context.Context, size int64) context.Context {
	if size <= 0 {
		return ctx
	}
	return context.WithValue(ctx, objectPlaintextSizeKey, size)
}

// ObjectPlaintextSizeFromContext retrieves a plaintext size hint for objects.
func ObjectPlaintextSizeFromContext(ctx context.Context) (int64, bool) {
	value := ctx.Value(objectPlaintextSizeKey)
	if value == nil {
		return 0, false
	}
	size, ok := value.(int64)
	if !ok {
		return 0, false
	}
	return size, true
}

// ContextWithStatePlaintextSize attaches the plaintext size of a state object to ctx.
func ContextWithStatePlaintextSize(ctx context.Context, size int64) context.Context {
	if size <= 0 {
		return ctx
	}
	return context.WithValue(ctx, statePlaintextSizeKey, size)
}

// ContextWithStateObjectContext overrides the crypto context used for state encryption/decryption.
func ContextWithStateObjectContext(ctx context.Context, objectCtx string) context.Context {
	if objectCtx == "" {
		return ctx
	}
	return context.WithValue(ctx, stateObjectContextKey, objectCtx)
}

// StateObjectContextFromContext returns an override context, falling back to defaultCtx.
func StateObjectContextFromContext(ctx context.Context, defaultCtx string) string {
	value := ctx.Value(stateObjectContextKey)
	if value == nil {
		return defaultCtx
	}
	overridden, ok := value.(string)
	if !ok || overridden == "" {
		return defaultCtx
	}
	return overridden
}

// StatePlaintextSizeFromContext retrieves a previously attached plaintext size.
func StatePlaintextSizeFromContext(ctx context.Context) (int64, bool) {
	hintsValue := ctx.Value(stateReadHintsKey)
	hints, hintsOK := hintsValue.(stateReadHints)
	if hintsOK && hints.plaintextSize > 0 {
		return hints.plaintextSize, true
	}
	value := ctx.Value(statePlaintextSizeKey)
	if value == nil {
		return 0, false
	}
	size, ok := value.(int64)
	if !ok {
		return 0, false
	}
	return size, true
}

// ContextWithNoSync marks storage writes as best-effort (no fsync) for disk/NFS.
func ContextWithNoSync(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, noSyncKey, true)
}

// NoSyncFromContext reports whether the storage write should skip fsync.
func NoSyncFromContext(ctx context.Context) bool {
	value := ctx.Value(noSyncKey)
	if value == nil {
		return false
	}
	enabled, ok := value.(bool)
	return ok && enabled
}
