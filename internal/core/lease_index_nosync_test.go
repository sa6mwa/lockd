package core

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
)

type leaseIndexNoSyncRecord struct {
	key    string
	noSync bool
}

type leaseIndexNoSyncBackend struct {
	storage.Backend
	mu      sync.Mutex
	records []leaseIndexNoSyncRecord
}

func (b *leaseIndexNoSyncBackend) PutObject(ctx context.Context, namespace, key string, body io.Reader, opts storage.PutObjectOptions) (*storage.ObjectInfo, error) {
	if strings.HasPrefix(key, leaseIndexPrefix+"/") {
		b.mu.Lock()
		b.records = append(b.records, leaseIndexNoSyncRecord{
			key:    key,
			noSync: storage.NoSyncFromContext(ctx),
		})
		b.mu.Unlock()
	}
	return b.Backend.PutObject(ctx, namespace, key, body, opts)
}

func (b *leaseIndexNoSyncBackend) assertLeaseIndexNoSync(t *testing.T, want bool) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.records) == 0 {
		t.Fatalf("expected lease index writes, got none")
	}
	for _, record := range b.records {
		if record.noSync != want {
			t.Fatalf("expected lease index write %q no_sync=%v, got %v", record.key, want, record.noSync)
		}
	}
}

func TestLeaseIndexWritesUseNoSyncInFailoverMode(t *testing.T) {
	backend := &leaseIndexNoSyncBackend{Backend: memory.New()}
	svc := New(Config{
		Store:            backend,
		DefaultNamespace: "default",
		BackendHash:      "test-backend",
		HAMode:           "failover",
		HALeaseTTL:       0,
	})

	_, err := svc.Acquire(context.Background(), AcquireCommand{
		Namespace:    "default",
		Key:          "orders/failover-nosync",
		Owner:        "owner-a",
		TTLSeconds:   30,
		BlockSeconds: api.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	backend.assertLeaseIndexNoSync(t, true)
}

func TestLeaseIndexWritesRemainSyncInConcurrentMode(t *testing.T) {
	backend := &leaseIndexNoSyncBackend{Backend: memory.New()}
	svc := New(Config{
		Store:            backend,
		DefaultNamespace: "default",
		BackendHash:      "test-backend",
		HAMode:           "concurrent",
	})

	_, err := svc.Acquire(context.Background(), AcquireCommand{
		Namespace:    "default",
		Key:          "orders/concurrent-sync",
		Owner:        "owner-a",
		TTLSeconds:   30,
		BlockSeconds: api.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	backend.assertLeaseIndexNoSync(t, false)
}
