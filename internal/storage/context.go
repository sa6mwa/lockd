package storage

import "context"

type contextKey string

const (
	stateDescriptorKey     contextKey = "storage.state_descriptor"
	objectDescriptorKey    contextKey = "storage.object_descriptor"
	statePlaintextSizeKey  contextKey = "storage.state_plaintext_size"
	objectPlaintextSizeKey contextKey = "storage.object_plaintext_size"
)

// ContextWithStateDescriptor attaches a state descriptor to ctx for use by storage backends.
func ContextWithStateDescriptor(ctx context.Context, descriptor []byte) context.Context {
	if len(descriptor) == 0 {
		return ctx
	}
	buf := append([]byte(nil), descriptor...)
	return context.WithValue(ctx, stateDescriptorKey, buf)
}

// StateDescriptorFromContext retrieves a state descriptor previously attached to ctx.
func StateDescriptorFromContext(ctx context.Context) ([]byte, bool) {
	value := ctx.Value(stateDescriptorKey)
	if value == nil {
		return nil, false
	}
	descriptor, ok := value.([]byte)
	if !ok {
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

// StatePlaintextSizeFromContext retrieves a previously attached plaintext size.
func StatePlaintextSizeFromContext(ctx context.Context) (int64, bool) {
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
