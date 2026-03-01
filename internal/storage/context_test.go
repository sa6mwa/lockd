package storage

import (
	"context"
	"testing"
)

func TestContextWithStateReadHintsCopiesDescriptor(t *testing.T) {
	src := []byte("descriptor")
	ctx := ContextWithStateReadHints(context.Background(), src, 123)
	src[0] = 'X'

	descriptor, ok := StateDescriptorFromContext(ctx)
	if !ok {
		t.Fatalf("expected descriptor to be present")
	}
	if string(descriptor) != "descriptor" {
		t.Fatalf("unexpected descriptor: %q", descriptor)
	}
	size, ok := StatePlaintextSizeFromContext(ctx)
	if !ok {
		t.Fatalf("expected plaintext size to be present")
	}
	if size != 123 {
		t.Fatalf("unexpected plaintext size: %d", size)
	}
}

func TestContextWithStateReadHintsBorrowedDescriptor(t *testing.T) {
	src := []byte("descriptor")
	ctx := ContextWithStateReadHintsBorrowed(context.Background(), src, 456)
	src[0] = 'X'

	descriptor, ok := StateDescriptorFromContext(ctx)
	if !ok {
		t.Fatalf("expected descriptor to be present")
	}
	if string(descriptor) != "Xescriptor" {
		t.Fatalf("expected borrowed descriptor view, got %q", descriptor)
	}
	size, ok := StatePlaintextSizeFromContext(ctx)
	if !ok {
		t.Fatalf("expected plaintext size to be present")
	}
	if size != 456 {
		t.Fatalf("unexpected plaintext size: %d", size)
	}
}

func TestContextWithStateDescriptorRetainsCopySemantics(t *testing.T) {
	src := []byte("descriptor")
	ctx := ContextWithStateDescriptor(context.Background(), src)
	src[0] = 'X'

	descriptor, ok := StateDescriptorFromContext(ctx)
	if !ok {
		t.Fatalf("expected descriptor to be present")
	}
	if string(descriptor) != "descriptor" {
		t.Fatalf("unexpected descriptor: %q", descriptor)
	}
}
