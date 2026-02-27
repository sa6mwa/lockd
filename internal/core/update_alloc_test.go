package core

import (
	"bytes"
	"context"
	"runtime"
	"strings"
	"testing"

	"pkt.systems/lockd/internal/jsonutil"
	"pkt.systems/lockd/internal/storage/disk"
	"pkt.systems/pslog"
)

func TestUpdateLargePayloadTotalAllocBounded(t *testing.T) {
	ctx := context.Background()
	store, err := disk.New(disk.Config{
		Root:       t.TempDir(),
		QueueWatch: false,
	})
	if err != nil {
		t.Fatalf("disk store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	svc := New(Config{
		Store:            store,
		BackendHash:      "test-backend",
		DefaultNamespace: "default",
		JSONMaxBytes:     80 << 20,
		SpoolThreshold:   1 << 20,
		StateCacheBytes:  0,
		Logger:           pslog.NoopLogger(),
	})

	const payloadBytes = 50 << 20
	payload := buildLargeJSONObjectPayload(payloadBytes)
	if len(payload) < payloadBytes {
		t.Fatalf("payload too small: got=%d want>=%d", len(payload), payloadBytes)
	}

	// Warm once to remove one-time runtime/setup noise from the alloc gate.
	if err := updateLargePayloadOnce(ctx, svc, "update-alloc-warm", payload); err != nil {
		t.Fatalf("warm update: %v", err)
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	if err := updateLargePayloadOnce(ctx, svc, "update-alloc-measure", payload); err != nil {
		t.Fatalf("update: %v", err)
	}

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	delta := after.TotalAlloc - before.TotalAlloc

	const allocCap = uint64(24 << 20) // Must remain far below full-document buffering.
	if delta > allocCap {
		t.Fatalf("update total alloc too large: got=%d cap=%d payload=%d", delta, allocCap, len(payload))
	}
}

func updateLargePayloadOnce(ctx context.Context, svc *Service, key string, payload []byte) error {
	acq, err := svc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          key,
		Owner:        "alloc-gate",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		return err
	}
	_, err = svc.Update(ctx, UpdateCommand{
		Namespace:      "default",
		Key:            key,
		LeaseID:        acq.LeaseID,
		FencingToken:   acq.FencingToken,
		TxnID:          acq.TxnID,
		Body:           bytes.NewReader(payload),
		CompactWriter:  jsonutil.CompactWriter,
		MaxBytes:       int64(len(payload)) + (1 << 20),
		SpoolThreshold: 1 << 20,
	})
	_, relErr := svc.Release(ctx, ReleaseCommand{
		Namespace:    "default",
		Key:          key,
		LeaseID:      acq.LeaseID,
		TxnID:        acq.TxnID,
		FencingToken: acq.FencingToken,
	})
	if err != nil {
		return err
	}
	return relErr
}

func buildLargeJSONObjectPayload(target int) []byte {
	var buf bytes.Buffer
	buf.Grow(target + 1024)
	buf.WriteString(`{ "message" : "`)
	for buf.Len() < target-64 {
		buf.WriteString(strings.Repeat("timeout payload ", 256))
	}
	buf.WriteString(`", "kind": "alloc-gate" }`)
	return buf.Bytes()
}
