package clock_test

import (
	"testing"
	"time"

	"pkt.systems/lockd/internal/clock"
)

func TestRealNowUsesUTC(t *testing.T) {
	t.Parallel()

	now := clock.Real{}.Now()
	if loc := now.Location(); loc != time.UTC {
		t.Fatalf("expected UTC location, got %v", loc)
	}
	if delta := time.Since(now); delta < 0 || delta > time.Second {
		t.Fatalf("unexpected Now delta: %v", delta)
	}
}

func TestRealAfterDeliversOnce(t *testing.T) {
	t.Parallel()

	ch := clock.Real{}.After(10 * time.Millisecond)
	select {
	case <-ch:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("After did not trigger within timeout")
	}
}

func TestRealSleepSleepsAtLeastDuration(t *testing.T) {
	t.Parallel()

	start := time.Now()
	clock.Real{}.Sleep(5 * time.Millisecond)
	if elapsed := time.Since(start); elapsed < 5*time.Millisecond {
		t.Fatalf("sleep duration too short: %v", elapsed)
	}
}
