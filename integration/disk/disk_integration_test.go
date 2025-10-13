//go:build integration && disk

package diskintegration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/disk"
	port "pkt.systems/logport"
)

func TestDiskLockLifecycle(t *testing.T) {
	t.Setenv("LOCKD_DISK_ROOT", filepath.Join(t.TempDir(), "disk-root"))
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx := context.Background()
	key := "disk-lifecycle-" + uuid.NewString()
	runLifecycleTest(t, ctx, cli, key, "disk-worker")
}

func TestDiskConcurrency(t *testing.T) {
	t.Setenv("LOCKD_DISK_ROOT", filepath.Join(t.TempDir(), "disk-root"))
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx := context.Background()
	key := "disk-concurrency-" + uuid.NewString()
	workers := 6
	iterations := 5
	ttl := int64(10)

	var updates atomic.Int64

	var wg sync.WaitGroup
	wg.Add(workers)
	for id := 0; id < workers; id++ {
		go func(workerID int) {
			defer wg.Done()
			owner := fmt.Sprintf("worker-%d", workerID)
			for iter := 0; iter < iterations; {
				lease := acquireWithRetry(t, ctx, cli, key, owner, ttl, 5)
				state, etag, version, err := getStateJSON(ctx, cli, key, lease.LeaseID)
				if err != nil {
					_ = releaseLease(t, ctx, cli, key, lease.LeaseID)
					continue
				}
				if version == "" {
					version = strconv.FormatInt(lease.Version, 10)
				}
				var counter float64
				if state != nil {
					if v, ok := state["counter"]; ok {
						counter, _ = v.(float64)
					}
				}
				counter++
				body, _ := json.Marshal(map[string]any{"counter": counter, "last": owner})
				_, err = cli.UpdateStateBytes(ctx, key, lease.LeaseID, body, lockdclient.UpdateStateOptions{IfETag: etag, IfVersion: version})
				if err != nil {
					var apiErr *lockdclient.APIError
					if errors.As(err, &apiErr) {
						code := apiErr.Response.ErrorCode
						if code == "meta_conflict" || code == "version_conflict" || code == "etag_mismatch" {
							_ = releaseLease(t, ctx, cli, key, lease.LeaseID)
							continue
						}
					}
					t.Fatalf("update state: %v", err)
				}
				updates.Add(1)
				_ = releaseLease(t, ctx, cli, key, lease.LeaseID)
				iter++
			}
		}(id)
	}
	wg.Wait()

	verifier := acquireWithRetry(t, ctx, cli, key, "verifier", ttl, 5)
	finalState, _, _, err := getStateJSON(ctx, cli, key, verifier.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !releaseLease(t, ctx, cli, key, verifier.LeaseID) {
		t.Fatalf("expected release success")
	}

	expected := float64(workers * iterations)
	if got := float64(updates.Load()); got != expected {
		t.Fatalf("expected %f successful updates, got %f", expected, got)
	}
	if finalState == nil {
		t.Fatalf("expected final state, got nil")
	}
	if value, ok := finalState["counter"].(float64); !ok || value != expected {
		t.Fatalf("expected counter %.0f, got %v", expected, finalState["counter"])
	}
}

func TestDiskAcquireForUpdateConcurrency(t *testing.T) {
	t.Setenv("LOCKD_DISK_ROOT", filepath.Join(t.TempDir(), "disk-root"))
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	key := "disk-for-update-concurrency-" + uuid.NewString()
	const workers = 6
	const iterations = 6

	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker)))
			for iter := 0; iter < iterations; iter++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				lease, err := cli.AcquireForUpdate(ctx, lockdclient.AcquireRequest{
					Key:        key,
					Owner:      fmt.Sprintf("for-update-worker-%d", worker),
					TTLSeconds: 10,
					BlockSecs:  2,
				})
				if err != nil {
					t.Errorf("worker %d acquire-for-update: %v", worker, err)
					return
				}
				if lease.Body != nil {
					_, _ = io.Copy(io.Discard, lease.Body)
					_ = lease.Body.Close()
				}
				payload := map[string]any{
					"worker":    worker,
					"iteration": iter,
					"nonce":     uuid.NewString(),
					"random":    rng.Int63(),
				}
				body, err := json.Marshal(payload)
				if err != nil {
					t.Errorf("worker %d marshal: %v", worker, err)
					return
				}
				opts := lockdclient.UpdateStateOptions{}
				if lease.StateETag != "" {
					opts.IfETag = lease.StateETag
				}
				if lease.Version > 0 {
					opts.IfVersion = strconv.FormatInt(lease.Version, 10)
				}
				if lease.FencingToken != "" {
					opts.FencingToken = lease.FencingToken
				}
				if _, err := cli.UpdateStateBytes(ctx, key, lease.LeaseID, body, opts); err != nil {
					t.Errorf("worker %d update: %v", worker, err)
					return
				}
				if _, err := cli.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
					t.Errorf("worker %d release: %v", worker, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	verifier := acquireWithRetry(t, ctx, cli, key, "verify", 10, 2)
	state, _, _, err := getStateJSON(ctx, cli, key, verifier.LeaseID)
	if err != nil {
		t.Fatalf("verify get state: %v", err)
	}
	if state == nil {
		t.Fatalf("expected state after concurrency test")
	}
	_ = releaseLease(t, ctx, cli, key, verifier.LeaseID)
}

func TestDiskAcquireForUpdateConnectionDrop(t *testing.T) {
	t.Setenv("LOCKD_DISK_ROOT", filepath.Join(t.TempDir(), "disk-root"))
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "disk-for-update-drop-" + uuid.NewString()

	lease, err := cli.AcquireForUpdate(ctx, lockdclient.AcquireRequest{
		Key:        key,
		Owner:      "dropper",
		TTLSeconds: 8,
		BlockSecs:  1,
	})
	if err != nil {
		t.Fatalf("acquire-for-update: %v", err)
	}
	if lease.Body != nil {
		_ = lease.Body.Close()
	}

	// give the server time to clean up the lease automatically
	time.Sleep(200 * time.Millisecond)

	second := acquireWithRetry(t, ctx, cli, key, "re-acquire", 8, 1)
	if second.LeaseID == lease.LeaseID {
		t.Fatalf("expected new lease after connection drop")
	}
	_ = releaseLease(t, ctx, cli, key, second.LeaseID)
}

func TestDiskAcquireForUpdateRandomPayloads(t *testing.T) {
	t.Setenv("LOCKD_DISK_ROOT", filepath.Join(t.TempDir(), "disk-root"))
	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key := "disk-for-update-random-" + uuid.NewString()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 40; i++ {
		payload := map[string]any{
			"iteration": i,
			"value":     rng.Int63(),
			"text":      randomJSONSafeString(rng),
			"time":      time.Now().UTC().Format(time.RFC3339Nano),
		}
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}

		lease, err := cli.AcquireForUpdate(ctx, lockdclient.AcquireRequest{
			Key:        key,
			Owner:      "random",
			TTLSeconds: 10,
			BlockSecs:  2,
		})
		if err != nil {
			t.Fatalf("acquire-for-update: %v", err)
		}
		if lease.Body != nil {
			_, _ = io.Copy(io.Discard, lease.Body)
			_ = lease.Body.Close()
		}

		opts := lockdclient.UpdateStateOptions{}
		if lease.StateETag != "" {
			opts.IfETag = lease.StateETag
		}
		if lease.Version > 0 {
			opts.IfVersion = strconv.FormatInt(lease.Version, 10)
		}
		if lease.FencingToken != "" {
			opts.FencingToken = lease.FencingToken
		}
		if _, err := cli.UpdateStateBytes(ctx, key, lease.LeaseID, body, opts); err != nil {
			t.Fatalf("update state: %v", err)
		}
		if _, err := cli.Release(ctx, lockdclient.ReleaseRequest{Key: key, LeaseID: lease.LeaseID}); err != nil {
			t.Fatalf("release: %v", err)
		}
	}
}

func TestDiskRetentionSweep(t *testing.T) {
	t.Setenv("LOCKD_DISK_ROOT", filepath.Join(t.TempDir(), "disk-root"))
	root := prepareDiskRoot(t, "")
	store, err := disk.New(disk.Config{
		Root:            root,
		Retention:       time.Second,
		JanitorInterval: 0,
		Now:             func() time.Time { return time.Unix(10, 0) },
	})
	if err != nil {
		t.Fatalf("new disk store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	key := "retention-" + uuid.NewString()

	meta := storage.Meta{Version: 1, UpdatedAtUnix: 1}
	if _, err := store.StoreMeta(ctx, key, &meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}
	if _, err := store.WriteState(ctx, key, strings.NewReader(`{"old":true}`), storage.PutStateOptions{}); err != nil {
		t.Fatalf("write state: %v", err)
	}

	store.SweepOnceForTests()

	if _, _, err := store.LoadMeta(ctx, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected meta cleanup, got %v", err)
	}
	if _, _, err := store.ReadState(ctx, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected state cleanup, got %v", err)
	}
}

func TestDiskMultiReplica(t *testing.T) {
	t.Setenv("LOCKD_DISK_ROOT", filepath.Join(t.TempDir(), "disk-root"))
	root := prepareDiskRoot(t, "")

	cfg1 := buildDiskConfig(t, root, 0)
	cli1 := startDiskServer(t, cfg1)

	cfg2 := buildDiskConfig(t, root, 0)
	cli2 := startDiskServer(t, cfg2)

	clients := []*lockdclient.Client{cli1, cli2}
	owners := []string{"replica-1", "replica-2"}
	ctx := context.Background()
	key := "disk-multi-" + uuid.NewString()
	iterations := 5

	var wg sync.WaitGroup
	for i, cli := range clients {
		owner := owners[i]
		wg.Add(1)
		go func(cli *lockdclient.Client, owner string) {
			defer wg.Done()
			for n := 0; n < iterations; n++ {
				for {
					lease := acquireWithRetry(t, ctx, cli, key, owner, 45, 10)
					state, etag, version, err := getStateJSON(ctx, cli, key, lease.LeaseID)
					if err != nil {
						_ = releaseLease(t, ctx, cli, key, lease.LeaseID)
						continue
					}
					if version == "" {
						version = strconv.FormatInt(lease.Version, 10)
					}
					var counter float64
					if state != nil {
						if v, ok := state["counter"].(float64); ok {
							counter = v
						}
					}
					counter++
					body, _ := json.Marshal(map[string]any{"counter": counter, "owner": owner})
					_, err = cli.UpdateStateBytes(ctx, key, lease.LeaseID, body, lockdclient.UpdateStateOptions{IfETag: etag, IfVersion: version})
					if err != nil {
						var apiErr *lockdclient.APIError
						if errors.As(err, &apiErr) {
							code := apiErr.Response.ErrorCode
							if code == "meta_conflict" || code == "etag_mismatch" || code == "version_conflict" {
								_ = releaseLease(t, ctx, cli, key, lease.LeaseID)
								continue
							}
						}
						t.Fatalf("update state: %v", err)
					}
					if !releaseLease(t, ctx, cli, key, lease.LeaseID) {
						t.Fatalf("release failed for owner %s", owner)
					}
					break
				}
			}
		}(cli, owner)
	}
	wg.Wait()

	verifier := acquireWithRetry(t, ctx, cli1, key, "verifier", 45, 5)
	finalState, _, _, err := getStateJSON(ctx, cli1, key, verifier.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if !releaseLease(t, ctx, cli1, key, verifier.LeaseID) {
		t.Fatalf("release verifier failed")
	}
	expected := float64(iterations * len(clients))
	if finalState == nil {
		t.Fatalf("expected final state, got nil")
	}
	if v, ok := finalState["counter"].(float64); !ok || v != expected {
		t.Fatalf("expected counter %.0f, got %v", expected, finalState["counter"])
	}
}

func TestDiskOnNFS(t *testing.T) {
	base := nfsBasePath()
	if base == "" {
		t.Skip("nfs root unavailable")
	}
	root := prepareDiskRoot(t, base)
	cfg := buildDiskConfig(t, root, 0)
	cli := startDiskServer(t, cfg)

	ctx := context.Background()
	key := "disk-nfs-" + uuid.NewString()
	runLifecycleTest(t, ctx, cli, key, "nfs-worker")
}

func runLifecycleTest(t *testing.T, ctx context.Context, cli *lockdclient.Client, key, owner string) {
	lease := acquireWithRetry(t, ctx, cli, key, owner, 45, 10)

	state, etag, version, err := getStateJSON(ctx, cli, key, lease.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if state != nil {
		t.Fatalf("expected nil state, got %v", state)
	}

	payload, _ := json.Marshal(map[string]any{"cursor": 42, "backend": "disk"})
	opts := lockdclient.UpdateStateOptions{IfVersion: version, IfETag: etag}
	if opts.IfVersion == "" {
		opts.IfVersion = strconv.FormatInt(lease.Version, 10)
	}
	if _, err := cli.UpdateStateBytes(ctx, key, lease.LeaseID, payload, opts); err != nil {
		t.Fatalf("update state: %v", err)
	}

	state, _, _, err = getStateJSON(ctx, cli, key, lease.LeaseID)
	if err != nil {
		t.Fatalf("get_state: %v", err)
	}
	if cursor, ok := state["cursor"].(float64); !ok || cursor != 42 {
		t.Fatalf("expected cursor 42, got %v", state["cursor"])
	}

	if !releaseLease(t, ctx, cli, key, lease.LeaseID) {
		t.Fatalf("expected release success")
	}
}

func prepareDiskRoot(tb testing.TB, base string) string {
	tb.Helper()
	var root string
	if base != "" {
		if _, err := os.Stat(base); err != nil {
			tb.Logf("disk base %q unavailable: %v", base, err)
			return ""
		}
		root = filepath.Join(base, "lockd-"+uuid.NewString())
	} else if env := os.Getenv("LOCKD_DISK_ROOT"); env != "" {
		root = filepath.Join(env, "lockd-"+uuid.NewString())
	} else {
		tb.Fatalf("LOCKD_DISK_ROOT must be set (source .env.disk before running disk integration/benchmarks)")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		tb.Fatalf("mkdir disk root: %v", err)
	}
	tb.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func nfsBasePath() string {
	if env := os.Getenv("LOCKD_DISK_NFS_ROOT"); env != "" {
		return env
	}
	candidates := []string{"/mnt/nfs4-lockd", "/mnt/nfs-lockd"}
	for _, path := range candidates {
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			return path
		}
	}
	return ""
}

func buildDiskConfig(tb testing.TB, root string, retention time.Duration) lockd.Config {
	storeURL := diskStoreURL(root)
	addr := pickPort(tb)
	cfg := lockd.Config{
		Store:           storeURL,
		MTLS:            false,
		Listen:          addr,
		ListenProto:     "tcp",
		DefaultTTL:      20 * time.Second,
		MaxTTL:          time.Minute,
		AcquireBlock:    5 * time.Second,
		SweeperInterval: 2 * time.Second,
		DiskRetention:   retention,
	}
	if err := cfg.Validate(); err != nil {
		tb.Fatalf("config validation failed: %v", err)
	}
	return cfg
}

func diskStoreURL(root string) string {
	if root == "" {
		root = "/tmp/lockd-disk"
	}
	if !strings.HasPrefix(root, "/") {
		root = "/" + root
	}
	return (&url.URL{Scheme: "disk", Path: root}).String()
}

func pickPort(tb testing.TB) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("pick port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func startDiskServer(tb testing.TB, cfg lockd.Config) *lockdclient.Client {
	tb.Helper()

	logger := port.NoopLogger()
	ctx := context.Background()
	_, stop, err := lockd.StartServer(ctx, cfg, lockd.WithLogger(logger))
	if err != nil {
		tb.Fatalf("start server: %v", err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = stop(ctx)
	})

	httpClient := &http.Client{Timeout: 15 * time.Second}
	baseURL := "http://" + cfg.Listen
	cli, err := lockdclient.New(baseURL, lockdclient.WithHTTPClient(httpClient))
	if err != nil {
		tb.Fatalf("client: %v", err)
	}
	return cli
}

func randomJSONSafeString(r *rand.Rand) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
	length := 8 + r.Intn(16)
	b := make([]byte, length)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return string(b)
}

func acquireWithRetry(tb testing.TB, ctx context.Context, cli *lockdclient.Client, key, owner string, ttl, block int64) *lockdclient.AcquireResponse {
	tb.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		lease, err := cli.Acquire(ctx, lockdclient.AcquireRequest{
			Key:        key,
			Owner:      owner,
			TTLSeconds: ttl,
			BlockSecs:  block,
		})
		if err == nil {
			return lease
		}
		if time.Now().After(deadline) {
			tb.Fatalf("acquire failed: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func getStateJSON(ctx context.Context, cli *lockdclient.Client, key, leaseID string) (map[string]any, string, string, error) {
	reader, etag, version, err := cli.GetState(ctx, key, leaseID)
	if err != nil {
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) {
			if apiErr.Status == http.StatusNoContent || apiErr.Status == http.StatusNotFound {
				return nil, "", "", nil
			}
		}
		return nil, "", "", err
	}
	if reader == nil {
		return nil, etag, version, nil
	}
	defer reader.Close()
	var payload map[string]any
	if err := json.NewDecoder(reader).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
		return nil, "", "", err
	}
	return payload, etag, version, nil
}

func releaseLease(tb testing.TB, ctx context.Context, cli *lockdclient.Client, key, leaseID string) bool {
	tb.Helper()
	resp, err := cli.Release(ctx, lockdclient.ReleaseRequest{
		Key:     key,
		LeaseID: leaseID,
	})
	if err != nil {
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.Response.ErrorCode {
			case "lease_required", "lease_expired":
				return false
			}
		}
		tb.Fatalf("release: %v", err)
	}
	return resp.Released
}
