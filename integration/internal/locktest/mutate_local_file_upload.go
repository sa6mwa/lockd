package locktest

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
)

// MutateLocalFileUploadConfig configures a backend integration scenario that
// writes a local fixture file into state via LeaseSession.MutateLocal.
type MutateLocalFileUploadConfig struct {
	Client *lockdclient.Client

	KeyPrefix   string
	OwnerPrefix string
	Namespace   string
	TTLSeconds  int64

	FixtureName  string
	FixtureBytes []byte
	Mutations    []string

	ExpectedFields map[string]any
	CleanupKey     func(string)
}

// RunMutateLocalFileUpload verifies that file-backed local mutations are visible
// in both staged and committed state for a backend-specific client.
func RunMutateLocalFileUpload(t *testing.T, cfg MutateLocalFileUploadConfig) {
	t.Helper()

	if cfg.Client == nil {
		t.Fatal("locktest: client is required")
	}
	if len(cfg.Mutations) == 0 {
		t.Fatal("locktest: mutations are required")
	}
	if len(cfg.ExpectedFields) == 0 {
		t.Fatal("locktest: expected fields are required")
	}

	namespace := cfg.Namespace
	if namespace == "" {
		namespace = namespaces.Default
	}
	keyPrefix := cfg.KeyPrefix
	if keyPrefix == "" {
		keyPrefix = "local-mutate"
	}
	ownerPrefix := cfg.OwnerPrefix
	if ownerPrefix == "" {
		ownerPrefix = keyPrefix
	}
	ttl := cfg.TTLSeconds
	if ttl <= 0 {
		ttl = 30
	}
	fixtureName := cfg.FixtureName
	if fixtureName == "" {
		fixtureName = "blob.txt"
	}

	key := keyPrefix + "-" + uuidv7.NewString()
	if cfg.CleanupKey != nil {
		t.Cleanup(func() { cfg.CleanupKey(key) })
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	lease, err := cfg.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespace,
		Key:        key,
		Owner:      ownerPrefix,
		TTLSeconds: ttl,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}

	tempDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tempDir, fixtureName), cfg.FixtureBytes, 0o600); err != nil {
		t.Fatalf("write fixture file: %v", err)
	}

	if _, err := lease.MutateLocal(ctx, cfg.Mutations, lockdclient.MutateLocalOptions{
		FileValueBaseDir: tempDir,
	}); err != nil {
		t.Fatalf("MutateLocal: %v", err)
	}

	staged := decodeLeaseBody(ctx, t, lease)
	assertFields(t, "staged", staged, cfg.ExpectedFields)

	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release lease: %v", err)
	}

	verifyLease, err := cfg.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespace,
		Key:        key,
		Owner:      ownerPrefix + "-verify",
		TTLSeconds: ttl,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("reacquire committed key: %v", err)
	}
	defer func() {
		if err := verifyLease.Release(ctx); err != nil {
			t.Fatalf("release verify lease: %v", err)
		}
	}()

	committed := decodeLeaseBody(ctx, t, verifyLease)
	assertFields(t, "committed", committed, cfg.ExpectedFields)
}

func decodeLeaseBody(ctx context.Context, t testing.TB, lease *lockdclient.LeaseSession) map[string]any {
	t.Helper()

	snap, err := lease.Get(ctx)
	if err != nil {
		t.Fatalf("get lease snapshot: %v", err)
	}
	if snap == nil || !snap.HasState {
		t.Fatal("expected state snapshot")
	}
	defer snap.Close()

	var body map[string]any
	if err := snap.Decode(&body); err != nil {
		t.Fatalf("decode state snapshot: %v", err)
	}
	return body
}

func assertFields(t testing.TB, label string, got map[string]any, want map[string]any) {
	t.Helper()

	for field, expected := range want {
		if actual, ok := got[field]; !ok {
			t.Fatalf("missing %s field %q in %#v", label, field, got)
		} else if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("unexpected %s field %q: got %#v want %#v", label, field, actual, expected)
		}
	}
}
