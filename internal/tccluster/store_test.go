package tccluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

func TestStoreAnnounceActiveAndExpire(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewManual(start)
	store := memory.New()
	cluster := NewStore(store, pslog.NoopLogger(), clk)

	record, err := cluster.Announce(ctx, "id-a", "http://a/", 10*time.Second)
	if err != nil {
		t.Fatalf("announce: %v", err)
	}
	if record.Endpoint != "http://a" {
		t.Fatalf("expected normalized endpoint, got %q", record.Endpoint)
	}

	active, err := cluster.Active(ctx)
	if err != nil {
		t.Fatalf("active: %v", err)
	}
	if len(active.Endpoints) != 1 || active.Endpoints[0] != "http://a" {
		t.Fatalf("expected endpoint http://a, got %+v", active.Endpoints)
	}
	if active.UpdatedAtUnix != record.UpdatedAtUnix {
		t.Fatalf("expected updated_at %d, got %d", record.UpdatedAtUnix, active.UpdatedAtUnix)
	}

	clk.Advance(11 * time.Second)
	active, err = cluster.Active(ctx)
	if err != nil {
		t.Fatalf("active after advance: %v", err)
	}
	if len(active.Endpoints) != 0 {
		t.Fatalf("expected endpoints expired, got %+v", active.Endpoints)
	}
}

func TestStoreLeaveRemovesLease(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewManual(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	store := memory.New()
	cluster := NewStore(store, pslog.NoopLogger(), clk)

	if _, err := cluster.Announce(ctx, "id-a", "http://a", 10*time.Second); err != nil {
		t.Fatalf("announce: %v", err)
	}
	if err := cluster.Leave(ctx, "id-a"); err != nil {
		t.Fatalf("leave: %v", err)
	}
	active, err := cluster.Active(ctx)
	if err != nil {
		t.Fatalf("active after leave: %v", err)
	}
	if len(active.Endpoints) != 0 {
		t.Fatalf("expected empty endpoints, got %+v", active.Endpoints)
	}
}

func TestStorePauseResume(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewManual(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	store := memory.New()
	cluster := NewStore(store, pslog.NoopLogger(), clk)

	cluster.Pause("id-a")
	if !cluster.IsPaused("id-a") {
		t.Fatalf("expected paused identity")
	}
	if _, err := cluster.Announce(ctx, "id-a", "http://a", 5*time.Second); err != nil {
		t.Fatalf("announce: %v", err)
	}
	if cluster.IsPaused("id-a") {
		t.Fatalf("expected announce to resume identity")
	}
}

func TestStoreAnnounceIfNotPaused(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewManual(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	store := memory.New()
	cluster := NewStore(store, pslog.NoopLogger(), clk)

	cluster.Pause("id-a")
	if _, err := cluster.AnnounceIfNotPaused(ctx, "id-a", "http://a", 5*time.Second); !errors.Is(err, ErrPaused) {
		t.Fatalf("expected ErrPaused, got %v", err)
	}
	if !cluster.IsPaused("id-a") {
		t.Fatalf("expected identity to remain paused")
	}
	active, err := cluster.Active(ctx)
	if err != nil {
		t.Fatalf("active after paused announce: %v", err)
	}
	if len(active.Endpoints) != 0 {
		t.Fatalf("expected no endpoints, got %+v", active.Endpoints)
	}
}
