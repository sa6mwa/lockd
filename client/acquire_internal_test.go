package client

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestAcquireRetryDelayCapsWithoutJitter(t *testing.T) {
	cfg := defaultAcquireConfig()
	cfg.Jitter = 0
	cfg.randInt63n = func(n int64) int64 { return 0 }
	normalizeAcquireConfig(&cfg)

	wait := acquireRetryDelay(10*time.Second, cfg)
	if wait != cfg.MaxDelay {
		t.Fatalf("expected wait=%s, got %s", cfg.MaxDelay, wait)
	}
}

func TestAcquireRetryDelayAppliesPositiveJitter(t *testing.T) {
	cfg := defaultAcquireConfig()
	cfg.Jitter = 500 * time.Millisecond
	cfg.randInt63n = func(n int64) int64 {
		// return max-1 to get +jitter offset
		return n - 1
	}
	normalizeAcquireConfig(&cfg)

	wait := acquireRetryDelay(10*time.Second, cfg)
	expected := cfg.MaxDelay + cfg.Jitter
	if wait != expected {
		t.Fatalf("expected wait=%s, got %s", expected, wait)
	}
}

func TestAcquireRetryDelayAppliesNegativeJitter(t *testing.T) {
	cfg := defaultAcquireConfig()
	cfg.Jitter = 500 * time.Millisecond
	cfg.randInt63n = func(int64) int64 {
		// return zero to get -jitter offset
		return 0
	}
	normalizeAcquireConfig(&cfg)

	wait := acquireRetryDelay(10*time.Second, cfg)
	expected := cfg.MaxDelay - cfg.Jitter
	if wait != expected {
		t.Fatalf("expected wait=%s, got %s", expected, wait)
	}
	if wait < 0 {
		t.Fatalf("wait must be non-negative, got %s", wait)
	}
}

func TestShouldRetryForUpdateUnexpectedEOF(t *testing.T) {
	if !shouldRetryForUpdate(io.ErrUnexpectedEOF) {
		t.Fatalf("expected unexpected EOF to be retryable")
	}
	if !shouldRetryForUpdate(io.EOF) {
		t.Fatalf("expected EOF to be retryable")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if shouldRetryForUpdate(ctx.Err()) {
		t.Fatalf("context cancellation should not be retryable")
	}
}
