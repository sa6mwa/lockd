package client

import (
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
