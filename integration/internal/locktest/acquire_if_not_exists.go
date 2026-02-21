package locktest

import (
	"context"
	"errors"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/uuidv7"
)

// AcquireIfNotExistsConfig configures the create-only acquire scenario.
type AcquireIfNotExistsConfig struct {
	Client      *lockdclient.Client
	KeyPrefix   string
	OwnerPrefix string
	TTLSeconds  int64
	CleanupKey  func(string)
}

const fastFailureUpperBound = 12 * time.Second

// RunAcquireIfNotExistsReturnsAlreadyExists verifies create-only acquire behavior
// and related failure/control paths on a single key.
func RunAcquireIfNotExistsReturnsAlreadyExists(t *testing.T, cfg AcquireIfNotExistsConfig) {
	t.Helper()
	if cfg.Client == nil {
		t.Fatal("locktest: client is required")
	}
	prefix := cfg.KeyPrefix
	if prefix == "" {
		prefix = "if-not-exists"
	}
	ownerPrefix := cfg.OwnerPrefix
	if ownerPrefix == "" {
		ownerPrefix = prefix
	}
	ttl := cfg.TTLSeconds
	if ttl <= 0 {
		ttl = 30
	}

	key := prefix + "-" + uuidv7.NewString()
	if cfg.CleanupKey != nil {
		t.Cleanup(func() { cfg.CleanupKey(key) })
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	first, err := cfg.Client.Acquire(ctx, api.AcquireRequest{
		Key:         key,
		Owner:       ownerPrefix + "-create",
		TTLSeconds:  ttl,
		BlockSecs:   lockdclient.BlockWaitForever,
		IfNotExists: true,
	})
	if err != nil {
		t.Fatalf("initial if_not_exists acquire failed: %v", err)
	}

	t.Run("CreateOnlyWhileLeaseHeldFailsAlreadyExists", func(t *testing.T) {
		t.Helper()
		start := time.Now()
		_, err := cfg.Client.Acquire(ctx, api.AcquireRequest{
			Key:         key,
			Owner:       ownerPrefix + "-exists-held",
			TTLSeconds:  ttl,
			BlockSecs:   lockdclient.BlockWaitForever,
			IfNotExists: true,
		})
		assertAlreadyExists(t, err)
		assertFastFailure(t, start, "create-only while held")
	})

	t.Run("NormalAcquireWhileLeaseHeldNowaitReturnsWaiting", func(t *testing.T) {
		t.Helper()
		start := time.Now()
		_, err := cfg.Client.Acquire(ctx, api.AcquireRequest{
			Key:        key,
			Owner:      ownerPrefix + "-normal-held",
			TTLSeconds: ttl,
			BlockSecs:  lockdclient.BlockNoWait,
		})
		assertWaiting(t, err)
		assertFastFailure(t, start, "normal acquire while held")
	})

	if err := first.Release(ctx); err != nil {
		t.Fatalf("release after initial acquire failed: %v", err)
	}

	t.Run("CreateOnlyAfterReleaseFailsAlreadyExists", func(t *testing.T) {
		t.Helper()
		start := time.Now()
		_, err := cfg.Client.Acquire(ctx, api.AcquireRequest{
			Key:         key,
			Owner:       ownerPrefix + "-exists-released",
			TTLSeconds:  ttl,
			BlockSecs:   lockdclient.BlockWaitForever,
			IfNotExists: true,
		})
		assertAlreadyExists(t, err)
		assertFastFailure(t, start, "create-only after release")
	})

	t.Run("NormalAcquireAfterReleaseSucceeds", func(t *testing.T) {
		t.Helper()
		lease, err := cfg.Client.Acquire(ctx, api.AcquireRequest{
			Key:        key,
			Owner:      ownerPrefix + "-normal-release",
			TTLSeconds: ttl,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("expected normal acquire to succeed after release: %v", err)
		}
		if err := lease.Release(ctx); err != nil {
			t.Fatalf("release after normal acquire failed: %v", err)
		}
	})

	t.Run("AcquireForUpdateCreateOnlyExistingFailsAlreadyExists", func(t *testing.T) {
		t.Helper()
		handlerCalled := false
		start := time.Now()
		err := cfg.Client.AcquireForUpdate(ctx, api.AcquireRequest{
			Key:         key,
			Owner:       ownerPrefix + "-afu-existing",
			TTLSeconds:  ttl,
			BlockSecs:   lockdclient.BlockWaitForever,
			IfNotExists: true,
		}, func(context.Context, *lockdclient.AcquireForUpdateContext) error {
			handlerCalled = true
			return nil
		})
		assertAlreadyExists(t, err)
		assertFastFailure(t, start, "acquire-for-update existing key")
		if handlerCalled {
			t.Fatal("acquire-for-update handler should not run for already_exists")
		}
	})
}

func assertAlreadyExists(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected already_exists error")
	}
	if !lockdclient.IsAlreadyExists(err) {
		t.Fatalf("expected IsAlreadyExists=true, got %v", err)
	}
	if !errors.Is(err, lockdclient.ErrAlreadyExists) {
		t.Fatalf("expected errors.Is(err, ErrAlreadyExists)=true, got %v", err)
	}
	var apiErr *lockdclient.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	if apiErr.Response.ErrorCode != "already_exists" {
		t.Fatalf("expected already_exists code, got %q", apiErr.Response.ErrorCode)
	}
	if apiErr.Status != 409 {
		t.Fatalf("expected already_exists HTTP status 409, got %d", apiErr.Status)
	}
}

func assertWaiting(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected waiting error")
	}
	var apiErr *lockdclient.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	if apiErr.Response.ErrorCode != "waiting" {
		t.Fatalf("expected waiting code, got %q", apiErr.Response.ErrorCode)
	}
	if apiErr.Status != 409 {
		t.Fatalf("expected waiting HTTP status 409, got %d", apiErr.Status)
	}
}

func assertFastFailure(t *testing.T, start time.Time, label string) {
	t.Helper()
	if elapsed := time.Since(start); elapsed > fastFailureUpperBound {
		t.Fatalf("expected %s to fail quickly, took %s", label, elapsed)
	}
}
