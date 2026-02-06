package core

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
)

type casRetryStore struct {
	storage.Backend

	mu                   sync.Mutex
	stagedCASFailures    int
	stagedStoreMetaCalls int
}

func (s *casRetryStore) StoreMeta(ctx context.Context, namespace, key string, meta *storage.Meta, expectedETag string) (string, error) {
	if meta != nil && meta.StagedTxnID != "" {
		s.mu.Lock()
		s.stagedStoreMetaCalls++
		if s.stagedCASFailures > 0 {
			s.stagedCASFailures--
			s.mu.Unlock()
			return "", storage.ErrCASMismatch
		}
		s.mu.Unlock()
	}
	return s.Backend.StoreMeta(ctx, namespace, key, meta, expectedETag)
}

func (s *casRetryStore) stagedMetaCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stagedStoreMetaCalls
}

func TestUpdateCASRetryRestagesFullPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := &casRetryStore{
		Backend:           memory.New(),
		stagedCASFailures: 1,
	}
	svc := New(Config{
		Store:            store,
		BackendHash:      "test-backend",
		DefaultNamespace: "default",
	})

	acq, err := svc.Acquire(ctx, AcquireCommand{
		Namespace:    "default",
		Key:          "update-cas-retry",
		Owner:        "worker",
		TTLSeconds:   30,
		BlockSeconds: apiBlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	payload := `{"value":"cas-retry","count":123}`
	res, err := svc.Update(ctx, UpdateCommand{
		Namespace:    "default",
		Key:          "update-cas-retry",
		LeaseID:      acq.LeaseID,
		FencingToken: acq.FencingToken,
		TxnID:        acq.TxnID,
		Body:         strings.NewReader(payload),
		CompactWriter: func(w io.Writer, r io.Reader, _ int64) error {
			_, err := io.Copy(w, r)
			return err
		},
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	if store.stagedMetaCallCount() < 2 {
		t.Fatalf("expected staged meta CAS retry, got %d StoreMeta attempts", store.stagedMetaCallCount())
	}
	if got, want := res.Bytes, int64(len(payload)); got != want {
		t.Fatalf("unexpected staged bytes: got %d want %d", got, want)
	}
	if res.Meta == nil {
		t.Fatalf("expected result meta")
	}
	if got, want := res.Meta.StagedStatePlaintextBytes, int64(len(payload)); got != want {
		t.Fatalf("unexpected staged plaintext bytes: got %d want %d", got, want)
	}
	if res.Meta.StagedStateETag != res.NewStateETag {
		t.Fatalf("staged etag mismatch: meta=%q result=%q", res.Meta.StagedStateETag, res.NewStateETag)
	}

	staged, err := svc.staging.LoadStagedState(ctx, "default", "update-cas-retry", acq.TxnID)
	if err != nil {
		t.Fatalf("load staged state: %v", err)
	}
	defer staged.Reader.Close()
	gotPayload, err := io.ReadAll(staged.Reader)
	if err != nil {
		t.Fatalf("read staged state: %v", err)
	}
	if string(gotPayload) != payload {
		t.Fatalf("unexpected staged payload: got %q want %q", string(gotPayload), payload)
	}
}
