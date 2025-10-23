package storage_test

import (
	"errors"
	"testing"

	"pkt.systems/lockd/internal/storage"
)

func TestNewTransientErrorWraps(t *testing.T) {
	t.Parallel()

	err := errors.New("boom")
	wrapped := storage.NewTransientError(err)
	if wrapped == nil {
		t.Fatal("expected wrapped error")
	}
	if !errors.Is(wrapped, err) {
		t.Fatal("wrapped error should contain original")
	}
	if !storage.IsTransient(wrapped) {
		t.Fatal("expected IsTransient to detect wrapped error")
	}
	if storage.IsTransient(err) {
		t.Fatal("plain error should not be transient")
	}
}

func TestNewTransientErrorHandlesNil(t *testing.T) {
	t.Parallel()

	if storage.NewTransientError(nil) != nil {
		t.Fatal("nil input should return nil")
	}
}
