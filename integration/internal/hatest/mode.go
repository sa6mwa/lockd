package hatest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/uuidv7"
)

const (
	haNamespace      = ".ha"
	haLeaseKey       = "activelease"
	haMemberPrefix   = "members/"
	haMemberModeAttr = "ha_mode"
)

// RequireNoHALease asserts that no active HA lease metadata has been written.
func RequireNoHALease(t testing.TB, backend storage.Backend) {
	t.Helper()
	if backend == nil {
		t.Fatalf("require no ha lease: nil backend")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := backend.LoadMeta(ctx, haNamespace, haLeaseKey)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected no %s/%s lease record, got %v", haNamespace, haLeaseKey, err)
	}
}

// RequireNoHAMembers asserts that no HA membership records have been written.
func RequireNoHAMembers(t testing.TB, backend storage.Backend) {
	t.Helper()
	if backend == nil {
		t.Fatalf("require no ha members: nil backend")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	keys, err := backend.ListMetaKeys(ctx, haNamespace)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("list ha members: %v", err)
	}
	for _, key := range keys {
		if strings.HasPrefix(key, haMemberPrefix) {
			t.Fatalf("expected no %s membership records, found %q", haNamespace, key)
		}
	}
}

// WaitForHAMemberMode blocks until any live HA membership record advertises the requested mode.
func WaitForHAMemberMode(t testing.TB, backend storage.Backend, mode string, timeout time.Duration) *storage.Meta {
	t.Helper()
	if backend == nil {
		t.Fatalf("wait for ha member mode: nil backend")
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		keys, err := backend.ListMetaKeys(ctx, haNamespace)
		cancel()
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("list ha members: %v", err)
		}
		for _, key := range keys {
			if !strings.HasPrefix(key, haMemberPrefix) {
				continue
			}
			ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
			res, err := backend.LoadMeta(ctx, haNamespace, key)
			cancel()
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			if err != nil {
				t.Fatalf("load ha member %q: %v", key, err)
			}
			if res.Meta == nil || res.Meta.Lease == nil || res.Meta.Lease.ExpiresAtUnix <= time.Now().Unix() {
				continue
			}
			got, ok := res.Meta.GetAttribute(haMemberModeAttr)
			if ok && strings.EqualFold(strings.TrimSpace(got), mode) {
				return res.Meta
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for live %s membership mode %q", haNamespace, mode)
	return nil
}

// RequireNoLiveHAMemberMode asserts that no live HA membership record currently advertises the requested mode.
func RequireNoLiveHAMemberMode(t testing.TB, backend storage.Backend, mode string) {
	t.Helper()
	if backend == nil {
		t.Fatalf("require no live ha member mode: nil backend")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	keys, err := backend.ListMetaKeys(ctx, haNamespace)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("list ha members: %v", err)
	}
	nowUnix := time.Now().Unix()
	for _, key := range keys {
		if !strings.HasPrefix(key, haMemberPrefix) {
			continue
		}
		res, err := backend.LoadMeta(ctx, haNamespace, key)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			t.Fatalf("load ha member %q: %v", key, err)
		}
		if res.Meta == nil || res.Meta.Lease == nil || res.Meta.Lease.ExpiresAtUnix <= nowUnix {
			continue
		}
		got, ok := res.Meta.GetAttribute(haMemberModeAttr)
		if ok && strings.EqualFold(strings.TrimSpace(got), mode) {
			t.Fatalf("expected no live %s membership mode %q, found %q", haNamespace, mode, key)
		}
	}
}

// WaitForHALease blocks until a live HA lease is present.
func WaitForHALease(t testing.TB, backend storage.Backend, timeout time.Duration) *storage.Meta {
	t.Helper()
	if backend == nil {
		t.Fatalf("wait for ha lease: nil backend")
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, err := backend.LoadMeta(ctx, haNamespace, haLeaseKey)
		cancel()
		if err == nil && res.Meta != nil && res.Meta.Lease != nil && res.Meta.Lease.ExpiresAtUnix > time.Now().Unix() {
			return res.Meta
		}
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("wait for ha lease: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s/%s lease record", haNamespace, haLeaseKey)
	return nil
}

// RequireAcquireRelease verifies that the provided client can acquire and release a probe key.
func RequireAcquireRelease(t testing.TB, cli *lockdclient.Client, prefix string) {
	t.Helper()
	if cli == nil {
		t.Fatalf("require acquire/release: nil client")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        prefix + "-" + uuidv7.NewString(),
		Owner:      "ha-mode-probe",
		TTLSeconds: 10,
		BlockSecs:  lockdclient.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("probe acquire: %v", err)
	}
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer releaseCancel()
	if err := lease.Release(releaseCtx); err != nil {
		t.Fatalf("probe release: %v", err)
	}
}

// RequirePassiveNode verifies that the server rejects writes as a passive failover node.
func RequirePassiveNode(t testing.TB, ts *lockd.TestServer) {
	t.Helper()
	if err := passiveNodeProbe(ts); err != nil {
		t.Fatal(err)
	}
}

// WaitForPassiveNode blocks until the server rejects writes as a passive failover node.
func WaitForPassiveNode(t testing.TB, ts *lockd.TestServer, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		err := passiveNodeProbe(ts)
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("timed out waiting for passive node")
	}
	t.Fatal(lastErr)
}

func passiveNodeProbe(ts *lockd.TestServer) error {
	if ts == nil {
		return fmt.Errorf("require passive node: nil server")
	}
	cli, err := ts.NewClient(lockdclient.WithEndpointShuffle(false))
	if err != nil {
		return fmt.Errorf("require passive node client: %w", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        "ha-passive-" + uuidv7.NewString(),
		Owner:      "ha-passive-probe",
		TTLSeconds: 10,
		BlockSecs:  lockdclient.BlockNoWait,
	})
	if err == nil {
		_ = lease.Release(ctx)
		return fmt.Errorf("expected passive node to reject acquire")
	}
	if !IsNodePassive(err) {
		return fmt.Errorf("expected passive-node error, got %v", err)
	}
	return nil
}
