//go:build integration && aws

package awsintegration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	api "pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	querydata "pkt.systems/lockd/integration/query/querydata"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

var (
	awsStoreVerifyOnce sync.Once
	awsStoreVerifyErr  error
)

func loadAWSConfig(tb testing.TB) lockd.Config {
	store := os.Getenv("LOCKD_STORE")
	if store == "" {
		tb.Fatalf("LOCKD_STORE must be set to an aws:// URI for AWS integration tests")
	}
	if !strings.HasPrefix(store, "aws://") {
		tb.Fatalf("LOCKD_STORE must reference an aws:// URI, got %q", store)
	}
	cfg := lockd.Config{
		Store:         store,
		AWSRegion:     os.Getenv("LOCKD_AWS_REGION"),
		AWSKMSKeyID:   os.Getenv("LOCKD_AWS_KMS_KEY_ID"),
		S3SSE:         os.Getenv("LOCKD_S3_SSE"),
		S3KMSKeyID:    os.Getenv("LOCKD_S3_KMS_KEY_ID"),
		S3MaxPartSize: 16 << 20,
	}
	if cfg.AWSRegion == "" {
		cfg.AWSRegion = os.Getenv("AWS_REGION")
	}
	if cfg.AWSRegion == "" {
		cfg.AWSRegion = os.Getenv("AWS_DEFAULT_REGION")
	}
	cryptotest.MaybeEnableStorageEncryption(tb, &cfg)
	if err := cfg.Validate(); err != nil {
		tb.Fatalf("config validation: %v", err)
	}
	return cfg
}

func ensureStoreReady(tb testing.TB, ctx context.Context, cfg lockd.Config) {
	ResetAWSBucketForCrypto(tb, cfg)
	awsStoreVerifyOnce.Do(func() {
		res, err := storagecheck.VerifyStore(ctx, cfg)
		if err != nil {
			awsStoreVerifyErr = err
			return
		}
		if !res.Passed() {
			awsStoreVerifyErr = fmt.Errorf("store verification failed: %+v", res)
		}
	})
	if awsStoreVerifyErr != nil {
		tb.Fatalf("store verification failed: %v", awsStoreVerifyErr)
	}
}

func startLockdServer(t *testing.T, cfg lockd.Config) *lockdclient.Client {
	ts := startAWSTestServer(t, cfg)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}
	return cli
}

func startAWSTestServer(t testing.TB, cfg lockd.Config, opts ...lockd.TestServerOption) *lockd.TestServer {
	t.Helper()
	cfgCopy := cfg
	if cfgCopy.JSONMaxBytes == 0 {
		cfgCopy.JSONMaxBytes = 100 << 20
	}
	if cfgCopy.DefaultTTL == 0 {
		cfgCopy.DefaultTTL = 30 * time.Second
	}
	if cfgCopy.MaxTTL == 0 {
		cfgCopy.MaxTTL = 2 * time.Minute
	}
	if cfgCopy.AcquireBlock == 0 {
		cfgCopy.AcquireBlock = 5 * time.Second
	}
	if cfgCopy.SweeperInterval == 0 {
		cfgCopy.SweeperInterval = 5 * time.Second
	}
	if cfgCopy.StorageRetryMaxAttempts < 12 {
		cfgCopy.StorageRetryMaxAttempts = 12
	}
	if cfgCopy.StorageRetryBaseDelay < 500*time.Millisecond {
		cfgCopy.StorageRetryBaseDelay = 500 * time.Millisecond
	}
	if cfgCopy.StorageRetryMaxDelay < 15*time.Second {
		cfgCopy.StorageRetryMaxDelay = 15 * time.Second
	}

	options := []lockd.TestServerOption{
		lockd.WithTestConfig(cfgCopy),
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestListener("tcp", "127.0.0.1:0"),
		lockd.WithTestClientOptions(
			lockdclient.WithHTTPTimeout(2*time.Minute),
			lockdclient.WithCloseTimeout(2*time.Minute),
			lockdclient.WithKeepAliveTimeout(2*time.Minute),
			lockdclient.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
		),
		lockd.WithTestCloseDefaults(
			lockd.WithDrainLeases(-1),
			lockd.WithShutdownTimeout(10*time.Second),
		),
	}
	options = append(options, opts...)
	options = append(options, cryptotest.SharedMTLSOptions(t)...)
	t.Cleanup(func() {
		CleanupNamespaceIndexes(t, cfg, namespaces.Default)
	})
	return lockd.StartTestServer(t, options...)
}

func acquireWithRetry(t *testing.T, ctx context.Context, cli *lockdclient.Client, key, owner string, ttl, block int64) *lockdclient.LeaseSession {
	for attempt := 0; attempt < 60; attempt++ {
		resp, err := cli.Acquire(ctx, api.AcquireRequest{Key: key, Owner: owner, TTLSeconds: ttl, BlockSecs: block})
		if err == nil {
			return resp
		}
		if apiErr := (*lockdclient.APIError)(nil); errors.As(err, &apiErr) {
			if apiErr.Status == http.StatusConflict {
				time.Sleep(200 * time.Millisecond)
				continue
			}
		}
		t.Fatalf("acquire failed: %v", err)
	}
	t.Fatal("acquire retry limit exceeded")
	return nil
}

func getStateJSON(ctx context.Context, cli *lockdclient.Client, key, leaseID string) (map[string]any, string, string, error) {
	resp, err := cli.Get(ctx, key,
		lockdclient.WithGetLeaseID(leaseID),
		lockdclient.WithGetPublicDisabled(true),
	)
	if err != nil {
		return nil, "", "", err
	}
	if resp == nil {
		return nil, "", "", nil
	}
	defer resp.Close()
	if !resp.HasState {
		return nil, resp.ETag, resp.Version, nil
	}
	data, err := resp.Bytes()
	if err != nil {
		return nil, "", "", err
	}
	if len(data) == 0 {
		return nil, resp.ETag, resp.Version, nil
	}
	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, "", "", err
	}
	return payload, resp.ETag, resp.Version, nil
}

func releaseLease(t *testing.T, ctx context.Context, cli *lockdclient.Client, key, leaseID string) bool {
	resp, err := cli.Release(ctx, api.ReleaseRequest{Key: key, LeaseID: leaseID})
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if !resp.Released {
		return false
	}
	return true
}

func cleanupS3(t *testing.T, cfg lockd.Config, key string) {
	if err := cleanupAWSKey(t, cfg, namespaces.Default, key); err != nil {
		t.Logf("aws cleanup %s: %v", key, err)
	}
}

// ResetAWSBucketForCrypto clears cryptotest buckets when storage encryption is enabled.
func ResetAWSBucketForCrypto(tb testing.TB, cfg lockd.Config) {
	if os.Getenv(cryptotest.EnvVar) != "1" {
		return
	}
	s3cfg, _, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		tb.Fatalf("build aws config: %v", err)
	}
	store, err := s3.New(s3cfg)
	if err != nil {
		tb.Fatalf("new aws store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client := store.Client()
	listPrefix := strings.Trim(s3cfg.Prefix, "/")
	opts := minio.ListObjectsOptions{Recursive: true}
	if listPrefix != "" {
		opts.Prefix = listPrefix
	}
	for obj := range client.ListObjects(ctx, s3cfg.Bucket, opts) {
		if obj.Err != nil {
			tb.Logf("aws cleanup list error: %v", obj.Err)
			continue
		}
		if err := client.RemoveObject(ctx, s3cfg.Bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			tb.Logf("aws cleanup remove %q: %v", obj.Key, err)
		}
	}
	if keys, err := store.ListMetaKeys(ctx, namespaces.Default); err == nil {
		for _, key := range keys {
			if err := store.Remove(ctx, namespaces.Default, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("aws cleanup state %q: %v", key, err)
			}
			if err := store.DeleteMeta(ctx, namespaces.Default, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("aws cleanup meta %q: %v", key, err)
			}
		}
	} else {
		tb.Logf("aws cleanup list meta: %v", err)
	}
}

// CleanupQueryNamespaces removes previously seeded query fixtures across all namespaces.
func CleanupQueryNamespaces(tb testing.TB, cfg lockd.Config) {
	tb.Helper()
	names := querydata.QueryNamespaces()
	if len(names) == 0 {
		return
	}
	s3cfg, _, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		tb.Fatalf("build aws config: %v", err)
	}
	store, err := s3.New(s3cfg)
	if err != nil {
		tb.Fatalf("new aws store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, ns := range names {
		cleanupNamespaceKeys(tb, store, ctx, ns)
	}
}

func cleanupNamespaceKeys(tb testing.TB, store storage.Backend, ctx context.Context, namespace string) {
	keys, err := store.ListMetaKeys(ctx, namespace)
	if err != nil {
		tb.Logf("aws cleanup list meta (%s): %v", namespace, err)
		return
	}
	for _, key := range keys {
		if err := store.Remove(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			tb.Logf("aws cleanup remove %s/%s: %v", namespace, key, err)
		}
		if err := store.DeleteMeta(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			tb.Logf("aws cleanup delete meta %s/%s: %v", namespace, key, err)
		}
	}
	cleanupIndexArtifacts(tb, store, ctx, namespace)
}

func cleanupIndexArtifacts(tb testing.TB, store storage.Backend, ctx context.Context, namespace string) {
	listOpts := storage.ListOptions{Prefix: "index/", Limit: 1000}
	for {
		res, err := store.ListObjects(ctx, namespace, listOpts)
		if err != nil {
			tb.Logf("aws cleanup list index (%s): %v", namespace, err)
			return
		}
		for _, obj := range res.Objects {
			if err := store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{}); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("aws cleanup index %s/%s: %v", namespace, obj.Key, err)
			}
		}
		if !res.Truncated || res.NextStartAfter == "" {
			break
		}
		listOpts.StartAfter = res.NextStartAfter
	}
}

// CleanupQueue removes queue blobs and metadata for the given queue.
func CleanupQueue(tb testing.TB, cfg lockd.Config, namespace, queue string) {
	tb.Helper()
	s3cfg, _, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		tb.Fatalf("build aws config: %v", err)
	}
	store, err := s3.New(s3cfg)
	if err != nil {
		tb.Fatalf("new aws store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	prefix := path.Join("q", queue) + "/"
	listOpts := storage.ListOptions{Prefix: prefix, Limit: 1000}
	for {
		res, err := store.ListObjects(ctx, namespace, listOpts)
		if err != nil {
			tb.Fatalf("list aws queue objects: %v", err)
		}
		for _, obj := range res.Objects {
			if err := store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{}); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Fatalf("delete aws queue object %s: %v", obj.Key, err)
			}
		}
		if !res.Truncated || res.NextStartAfter == "" {
			break
		}
		listOpts.StartAfter = res.NextStartAfter
	}

	keys, err := store.ListMetaKeys(ctx, namespace)
	if err != nil {
		tb.Fatalf("list aws queue meta keys: %v", err)
	}
	metaPrefix := path.Join("q", queue)
	for _, key := range keys {
		if !strings.HasPrefix(key, metaPrefix) {
			continue
		}
		if err := store.Remove(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			tb.Fatalf("cleanup aws queue state %s: %v", key, err)
		}
		if err := store.DeleteMeta(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			tb.Fatalf("cleanup aws queue meta %s: %v", key, err)
		}
	}
}

// CleanupKey removes state and metadata for a lock key.
func CleanupKey(tb testing.TB, cfg lockd.Config, namespace, key string) {
	tb.Helper()
	if err := cleanupAWSKey(tb, cfg, namespace, key); err != nil {
		tb.Fatalf("cleanup aws key %s/%s: %v", namespace, key, err)
	}
}

func cleanupAWSKey(tb testing.TB, cfg lockd.Config, namespace, key string) error {
	tb.Helper()
	s3cfg, _, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		return fmt.Errorf("build aws config: %w", err)
	}
	store, err := s3.New(s3cfg)
	if err != nil {
		return fmt.Errorf("new aws store: %w", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := store.Remove(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("remove state %s/%s: %w", namespace, key, err)
	}
	if err := store.DeleteMeta(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("delete meta %s/%s: %w", namespace, key, err)
	}
	if err := cleanupNamespaceIndexesIfEmpty(tb, store, ctx, namespace); err != nil {
		return err
	}
	return nil
}

func cleanupNamespaceIndexesIfEmpty(tb testing.TB, store storage.Backend, ctx context.Context, namespace string) error {
	tb.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		keys, err := store.ListMetaKeys(ctx, namespace)
		if err != nil {
			return fmt.Errorf("list meta (%s): %w", namespace, err)
		}
		if len(keys) == 0 {
			cleanupIndexArtifacts(tb, store, ctx, namespace)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("namespace %s still has %d meta keys (e.g. %v)", namespace, len(keys), sampleKeys(keys))
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for namespace %s drain: %w", namespace, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func sampleKeys(keys []string) []string {
	if len(keys) <= 3 {
		return keys
	}
	out := make([]string, 3)
	copy(out, keys[:3])
	return out
}

// CleanupNamespaceIndexes removes index manifests and segments for the provided namespaces.
func CleanupNamespaceIndexes(tb testing.TB, cfg lockd.Config, nsList ...string) {
	tb.Helper()
	if len(nsList) == 0 {
		nsList = []string{namespaces.Default}
	}
	s3cfg, _, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		tb.Fatalf("build aws config: %v", err)
	}
	store, err := s3.New(s3cfg)
	if err != nil {
		tb.Fatalf("new aws store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, ns := range nsList {
		tb.Logf("aws cleanup: removing index artifacts for namespace %s", ns)
		cleanupIndexArtifacts(tb, store, ctx, ns)
	}
}
