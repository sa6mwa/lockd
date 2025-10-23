package correlation

import (
	"context"
	"strings"
	"testing"
)

func TestNormalize(t *testing.T) {
	valid := "abc-123"
	if got, ok := Normalize(valid); !ok || got != valid {
		t.Fatalf("expected %q to normalize, got %q ok=%v", valid, got, ok)
	}
	trimmed := "  xyz  "
	if got, ok := Normalize(trimmed); !ok || got != "xyz" {
		t.Fatalf("expected trimmed normalize to xyz, got %q ok=%v", got, ok)
	}
	if _, ok := Normalize(""); ok {
		t.Fatal("empty id should be invalid")
	}
	if _, ok := Normalize(strings.Repeat("a", MaxIDLength+1)); ok {
		t.Fatal("overlong id should be invalid")
	}
	if _, ok := Normalize("bad\x01suffix"); ok {
		t.Fatal("non-printable should be invalid")
	}
}

func TestSetAndGet(t *testing.T) {
	ctx := context.Background()
	if Has(ctx) {
		t.Fatalf("expected empty context to have no correlation id")
	}
	ctx = Set(ctx, "")
	if Has(ctx) {
		t.Fatalf("expected invalid set to be ignored")
	}
	ctx = Set(ctx, "foo")
	if !Has(ctx) {
		t.Fatalf("expected correlation id to be present")
	}
	if got := ID(ctx); got != "foo" {
		t.Fatalf("expected foo, got %q", got)
	}
	ctx2 := Ensure(context.Background())
	if Has(ctx2) {
		t.Fatalf("ensure should not set id")
	}
}

func TestGenerate(t *testing.T) {
	id := Generate()
	if id == "" {
		t.Fatalf("expected generated id")
	}
	if len(id) > MaxIDLength {
		t.Fatalf("generated id length %d exceeds limit", len(id))
	}
	if _, ok := Normalize(id); !ok {
		t.Fatalf("generated id should be valid, got %q", id)
	}
}
