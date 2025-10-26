package inprocess_test

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client/inprocess"
)

func TestNewRejectsNonUnixSockets(t *testing.T) {
	t.Parallel()

	cfg := lockd.Config{
		ListenProto: "tcp",
		Store:       "mem://",
	}
	cli, err := inprocess.New(context.Background(), cfg)
	if err == nil {
		_ = cli.Close(context.Background())
		t.Fatal("expected error when ListenProto is not unix")
	}
}

func TestNewRunsServerAndCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := lockd.Config{
		Store:       "mem://",
		DisableMTLS: false, // should be disabled automatically.
	}
	inproc, err := inprocess.New(ctx, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() {
		if err := inproc.Close(ctx); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	req := api.AcquireRequest{
		Owner:      "unit-test",
		TTLSeconds: int64((5 * time.Second).Seconds()),
	}
	lease, err := inproc.Acquire(ctx, req)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease.Key == "" {
		t.Fatal("expected generated key")
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// Close twice to ensure idempotency.
	if err := inproc.Close(ctx); err != nil {
		t.Fatalf("Close first call: %v", err)
	}
	if err := inproc.Close(ctx); err != nil {
		t.Fatalf("Close second call: %v", err)
	}
}
