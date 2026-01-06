package clock_test

import (
	"runtime"
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

func TestManualAfterAndAdvance(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	clk := clock.NewManual(start)
	ch := clk.After(3 * time.Second)
	select {
	case <-ch:
		t.Fatal("timer fired before advance")
	default:
	}

	clk.Advance(3 * time.Second)
	select {
	case got := <-ch:
		if got != clk.Now() {
			t.Fatalf("expected timer to fire at %v, got %v", clk.Now(), got)
		}
	default:
		t.Fatal("expected timer to fire after advance")
	}
}

func TestManualSleep(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	clk := clock.NewManual(start)
	done := make(chan struct{})
	go func() {
		clk.Sleep(2 * time.Second)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("sleep returned before advance")
	default:
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for clk.Pending() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("sleep did not schedule timer")
		}
		runtime.Gosched()
	}

	clk.Advance(2 * time.Second)
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("sleep did not return after advance")
	}
}

func TestManualNowUsesUTC(t *testing.T) {
	t.Parallel()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.FixedZone("local", -5*60*60))
	clk := clock.NewManual(start)
	if loc := clk.Now().Location(); loc != time.UTC {
		t.Fatalf("expected UTC location, got %v", loc)
	}
}
