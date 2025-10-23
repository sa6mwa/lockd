package lockd

import (
	"context"
	"sync"
	"testing"
	"time"

	"pkt.systems/logport"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
)

type sweeperClock struct {
	mu  sync.Mutex
	now time.Time
}

func newSweeperClock(start time.Time) clock.Clock {
	return &sweeperClock{now: start}
}

func (c *sweeperClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *sweeperClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	c.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- now
	return ch
}

func (c *sweeperClock) Sleep(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func TestSweeperClearsExpiredLeases(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	start := time.Unix(1_700_000_000, 0)
	expired := start.Add(-time.Minute)
	meta := &storage.Meta{
		Lease: &storage.Lease{
			ID:            "L-1",
			Owner:         "worker",
			ExpiresAtUnix: expired.Unix(),
		},
		Version: 1,
	}
	if _, err := store.StoreMeta(ctx, "alpha", meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}
	cfg := Config{
		Store:           "mem://",
		SweeperInterval: time.Second,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}
	srv, err := NewServer(cfg,
		WithBackend(store),
		WithClock(newSweeperClock(start)),
		WithLogger(logport.NoopLogger()),
	)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.startSweeper()
	defer srv.stopSweeper()
	// Allow sweeper goroutine to run at least once.
	time.Sleep(10 * time.Millisecond)
	updated, _, err := store.LoadMeta(ctx, "alpha")
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	if updated.Lease != nil {
		t.Fatalf("expected sweeper to clear lease, still present: %+v", updated.Lease)
	}
}
