package core

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd/api"
)

func TestKeepAliveStaleKnownMetaReloadsAfterCASMismatch(t *testing.T) {
	svc := newTestService(t)
	acq, err := svc.Acquire(context.Background(), AcquireCommand{
		Namespace:    "default",
		Key:          "known-meta-keepalive",
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: api.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if acq.Meta == nil {
		t.Fatalf("expected acquire meta")
	}
	staleMeta := cloneMeta(*acq.Meta)
	staleETag := acq.MetaETag

	if _, err := svc.KeepAlive(context.Background(), KeepAliveCommand{
		Namespace:    "default",
		Key:          acq.Key,
		LeaseID:      acq.LeaseID,
		FencingToken: acq.FencingToken,
		TxnID:        acq.TxnID,
		TTLSeconds:   31,
	}); err != nil {
		t.Fatalf("initial keepalive: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := svc.KeepAlive(ctx, KeepAliveCommand{
		Namespace:     "default",
		Key:           acq.Key,
		LeaseID:       acq.LeaseID,
		FencingToken:  acq.FencingToken,
		TxnID:         acq.TxnID,
		TTLSeconds:    32,
		KnownMeta:     &staleMeta,
		KnownMetaETag: staleETag,
	})
	if err != nil {
		t.Fatalf("keepalive with stale known meta: %v", err)
	}
	if res == nil || res.ExpiresAt == 0 {
		t.Fatalf("expected keepalive result with expiry, got %+v", res)
	}
}

func TestReleaseStaleKnownMetaReloadsAfterCASMismatch(t *testing.T) {
	svc := newTestService(t)
	acq, err := svc.Acquire(context.Background(), AcquireCommand{
		Namespace:    "default",
		Key:          "known-meta-release",
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: api.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if acq.Meta == nil {
		t.Fatalf("expected acquire meta")
	}
	staleMeta := cloneMeta(*acq.Meta)
	staleETag := acq.MetaETag

	if _, err := svc.KeepAlive(context.Background(), KeepAliveCommand{
		Namespace:    "default",
		Key:          acq.Key,
		LeaseID:      acq.LeaseID,
		FencingToken: acq.FencingToken,
		TxnID:        acq.TxnID,
		TTLSeconds:   31,
	}); err != nil {
		t.Fatalf("initial keepalive: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := svc.Release(ctx, ReleaseCommand{
		Namespace:     "default",
		Key:           acq.Key,
		LeaseID:       acq.LeaseID,
		FencingToken:  acq.FencingToken,
		TxnID:         acq.TxnID,
		KnownMeta:     &staleMeta,
		KnownMetaETag: staleETag,
	})
	if err != nil {
		t.Fatalf("release with stale known meta: %v", err)
	}
	if res == nil || !res.Released {
		t.Fatalf("expected release success, got %+v", res)
	}
}
