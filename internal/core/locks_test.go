package core

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"pkt.systems/lockd/api"
)

func TestResolveBlockNoWait(t *testing.T) {
	svc := &Service{acquireBlock: 5 * time.Second}

	block, waitForever := svc.resolveBlock(api.BlockNoWait)
	if block != 0 {
		t.Fatalf("expected block=0 for BlockNoWait, got %s", block)
	}
	if waitForever {
		t.Fatalf("expected waitForever=false for BlockNoWait")
	}
}

func TestResolveBlockDefaults(t *testing.T) {
	svc := &Service{acquireBlock: 7 * time.Second}

	block, waitForever := svc.resolveBlock(0)
	if block != 7*time.Second {
		t.Fatalf("expected block=%s for default, got %s", 7*time.Second, block)
	}
	if !waitForever {
		t.Fatalf("expected waitForever=true for default block seconds")
	}

	block, waitForever = svc.resolveBlock(2)
	if block != 2*time.Second {
		t.Fatalf("expected block=2s, got %s", block)
	}
	if waitForever {
		t.Fatalf("expected waitForever=false for explicit block seconds")
	}
}

func TestAcquireIfNotExistsRejectsExistingKey(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

	first, err := svc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          "orders/if-not-exists",
		Owner:        "owner-a",
		TTLSeconds:   30,
		BlockSeconds: api.BlockNoWait,
		IfNotExists:  true,
	})
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}

	released, err := svc.Release(ctx, ReleaseCommand{
		Namespace:    "default",
		Key:          first.Key,
		LeaseID:      first.LeaseID,
		TxnID:        first.TxnID,
		FencingToken: first.FencingToken,
	})
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if released == nil || !released.Released {
		t.Fatalf("expected release success, got %+v", released)
	}

	_, err = svc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          first.Key,
		Owner:        "owner-b",
		TTLSeconds:   30,
		BlockSeconds: 5,
		IfNotExists:  true,
	})
	if err == nil {
		t.Fatal("expected already_exists failure")
	}
	var failure Failure
	if !errors.As(err, &failure) {
		t.Fatalf("expected core failure, got %T: %v", err, err)
	}
	if failure.Code != "already_exists" {
		t.Fatalf("expected already_exists code, got %q", failure.Code)
	}
	if failure.HTTPStatus != http.StatusConflict {
		t.Fatalf("expected HTTP 409, got %d", failure.HTTPStatus)
	}
}
