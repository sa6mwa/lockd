package s3

import "testing"

func TestByteBudget(t *testing.T) {
	budget := newByteBudget(10)
	if budget == nil {
		t.Fatalf("expected budget")
	}
	releaseA, ok := budget.tryAcquire(4)
	if !ok {
		t.Fatalf("expected acquire for 4")
	}
	releaseB, ok := budget.tryAcquire(6)
	if !ok {
		t.Fatalf("expected acquire for 6")
	}
	if _, ok := budget.tryAcquire(1); ok {
		t.Fatalf("expected budget exhaustion")
	}
	releaseB()
	if _, ok := budget.tryAcquire(5); !ok {
		t.Fatalf("expected acquire after release")
	}
	releaseA()
}
