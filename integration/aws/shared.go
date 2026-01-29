//go:build integration && aws

package awsintegration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd"
	api "pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/integration/internal/storepath"
	querydata "pkt.systems/lockd/integration/query/querydata"
	"pkt.systems/lockd/internal/diagnostics/storagecheck"
	"pkt.systems/lockd/internal/storage"
	awsstore "pkt.systems/lockd/internal/storage/aws"
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
	store = storepath.Scoped(tb, store, "aws")
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

func appendStorePath(tb testing.TB, store, suffix string) string {
	return storepath.Append(tb, store, suffix)
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
	cryptotest.ConfigureTCAuth(t, &cfgCopy)

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

func releaseLease(t *testing.T, ctx context.Context, lease *lockdclient.LeaseSession) bool {
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	return true
}

func runAttachmentTest(t *testing.T, ctx context.Context, cli *lockdclient.Client, key string) {
	t.Helper()
	lease := acquireWithRetry(t, ctx, cli, key, "attach-worker", 45, lockdclient.BlockWaitForever)
	alpha := []byte("alpha")
	bravo := []byte("bravo")
	if _, err := lease.Attach(ctx, lockdclient.AttachRequest{
		Name: "alpha.bin",
		Body: bytes.NewReader(alpha),
	}); err != nil {
		t.Fatalf("attach alpha: %v", err)
	}
	if _, err := lease.Attach(ctx, lockdclient.AttachRequest{
		Name:        "bravo.bin",
		Body:        bytes.NewReader(bravo),
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("attach bravo: %v", err)
	}
	list, err := lease.ListAttachments(ctx)
	if err != nil {
		t.Fatalf("list attachments: %v", err)
	}
	if list == nil || len(list.Attachments) != 2 {
		t.Fatalf("expected 2 attachments, got %+v", list)
	}
	att, err := lease.RetrieveAttachment(ctx, lockdclient.AttachmentSelector{Name: "alpha.bin"})
	if err != nil {
		t.Fatalf("retrieve alpha: %v", err)
	}
	data, err := io.ReadAll(att)
	att.Close()
	if err != nil {
		t.Fatalf("read alpha: %v", err)
	}
	if !bytes.Equal(data, alpha) {
		t.Fatalf("unexpected alpha payload: %q", data)
	}
	if !releaseLease(t, ctx, lease) {
		t.Fatalf("expected release success")
	}

	resp, err := cli.Get(ctx, key)
	if err != nil {
		t.Fatalf("public get: %v", err)
	}
	publicList, err := resp.ListAttachments(ctx)
	if err != nil {
		resp.Close()
		t.Fatalf("public list: %v", err)
	}
	if len(publicList.Attachments) != 2 {
		resp.Close()
		t.Fatalf("expected 2 public attachments, got %+v", publicList.Attachments)
	}
	publicAtt, err := resp.RetrieveAttachment(ctx, lockdclient.AttachmentSelector{Name: "bravo.bin"})
	if err != nil {
		resp.Close()
		t.Fatalf("public retrieve bravo: %v", err)
	}
	publicData, err := io.ReadAll(publicAtt)
	publicAtt.Close()
	resp.Close()
	if err != nil {
		t.Fatalf("read public bravo: %v", err)
	}
	if !bytes.Equal(publicData, bravo) {
		t.Fatalf("unexpected bravo payload: %q", publicData)
	}

	lease2 := acquireWithRetry(t, ctx, cli, key, "attach-delete", 45, lockdclient.BlockWaitForever)
	if _, err := lease2.DeleteAttachment(ctx, lockdclient.AttachmentSelector{Name: "alpha.bin"}); err != nil {
		t.Fatalf("delete alpha: %v", err)
	}
	if !releaseLease(t, ctx, lease2) {
		t.Fatalf("expected release success")
	}
	listAfter, err := cli.ListAttachments(ctx, lockdclient.ListAttachmentsRequest{Key: key, Public: true})
	if err != nil {
		t.Fatalf("list after delete: %v", err)
	}
	if len(listAfter.Attachments) != 1 || listAfter.Attachments[0].Name != "bravo.bin" {
		t.Fatalf("expected bravo only, got %+v", listAfter.Attachments)
	}

	lease3 := acquireWithRetry(t, ctx, cli, key, "attach-clear", 45, lockdclient.BlockWaitForever)
	if _, err := lease3.DeleteAllAttachments(ctx); err != nil {
		t.Fatalf("delete all: %v", err)
	}
	if !releaseLease(t, ctx, lease3) {
		t.Fatalf("expected release success")
	}
	finalList, err := cli.ListAttachments(ctx, lockdclient.ListAttachmentsRequest{Key: key, Public: true})
	if err != nil {
		t.Fatalf("final list: %v", err)
	}
	if len(finalList.Attachments) != 0 {
		t.Fatalf("expected no attachments, got %+v", finalList.Attachments)
	}
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
	awsResult, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		tb.Fatalf("build aws config: %v", err)
	}
	awscfg := awsResult.Config
	store, err := awsstore.New(awscfg)
	if err != nil {
		tb.Fatalf("new aws store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	opts := storage.ListOptions{Prefix: "", Limit: 1000}
	for {
		res, err := store.ListObjects(ctx, "", opts)
		if err != nil {
			tb.Logf("aws cleanup list error: %v", err)
			break
		}
		for _, obj := range res.Objects {
			if err := store.DeleteObject(ctx, "", obj.Key, storage.DeleteObjectOptions{IgnoreNotFound: true}); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("aws cleanup remove %q: %v", obj.Key, err)
			}
		}
		if !res.Truncated || res.NextStartAfter == "" {
			break
		}
		opts.StartAfter = res.NextStartAfter
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
	awsResult, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		tb.Fatalf("build aws config: %v", err)
	}
	awscfg := awsResult.Config
	store, err := awsstore.New(awscfg)
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
	removePrefix(tb, store, ctx, namespace, "index/")
	removePrefix(tb, store, ctx, namespace, ".staging/")
}

// removePrefix best-effort deletes all objects under the prefix within a namespace.
func removePrefix(tb testing.TB, store storage.Backend, ctx context.Context, namespace, prefix string) {
	tb.Helper()
	listOpts := storage.ListOptions{Prefix: prefix, Limit: 1000}
	for {
		res, err := store.ListObjects(ctx, namespace, listOpts)
		if err != nil {
			tb.Logf("aws cleanup list prefix (%s/%s): %v", namespace, prefix, err)
			return
		}
		for _, obj := range res.Objects {
			if err := store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{}); err != nil && !errors.Is(err, storage.ErrNotFound) {
				tb.Logf("aws cleanup delete %s/%s: %v", namespace, obj.Key, err)
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
	awsResult, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		tb.Fatalf("build aws config: %v", err)
	}
	awscfg := awsResult.Config
	store, err := awsstore.New(awscfg)
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
	awsResult, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		return fmt.Errorf("build aws config: %w", err)
	}
	awscfg := awsResult.Config
	store, err := awsstore.New(awscfg)
	if err != nil {
		return fmt.Errorf("new aws store: %w", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := store.Remove(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("remove state %s/%s: %w", namespace, key, err)
	}
	if err := cleanupPrefix(store, ctx, namespace, path.Join("state", key, ".staging")+"/"); err != nil {
		return fmt.Errorf("cleanup staging %s/%s: %w", namespace, key, err)
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
	keys, err := store.ListMetaKeys(ctx, namespace)
	if err != nil {
		return fmt.Errorf("list meta (%s): %w", namespace, err)
	}
	if len(keys) == 0 {
		cleanupIndexArtifacts(tb, store, ctx, namespace)
		return nil
	}
	tb.Logf("aws cleanup: namespace %s still has %d meta keys (e.g. %v); skipping index cleanup", namespace, len(keys), sampleKeys(keys))
	return nil
}

func sampleKeys(keys []string) []string {
	if len(keys) <= 3 {
		return keys
	}
	out := make([]string, 3)
	copy(out, keys[:3])
	return out
}

// cleanupPrefix deletes all objects under the provided prefix, returning the first error encountered.
func cleanupPrefix(store storage.Backend, ctx context.Context, namespace, prefix string) error {
	listOpts := storage.ListOptions{Prefix: prefix, Limit: 1000}
	for {
		res, err := store.ListObjects(ctx, namespace, listOpts)
		if err != nil {
			return fmt.Errorf("list objects %s/%s: %w", namespace, prefix, err)
		}
		for _, obj := range res.Objects {
			if err := store.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{}); err != nil && !errors.Is(err, storage.ErrNotFound) {
				return fmt.Errorf("delete object %s/%s: %w", namespace, obj.Key, err)
			}
		}
		if !res.Truncated || res.NextStartAfter == "" {
			return nil
		}
		listOpts.StartAfter = res.NextStartAfter
	}
}

// CleanupNamespaceIndexes removes index manifests and segments for the provided namespaces.
func CleanupNamespaceIndexes(tb testing.TB, cfg lockd.Config, nsList ...string) {
	tb.Helper()
	if len(nsList) == 0 {
		nsList = []string{namespaces.Default}
	}
	awsResult, err := lockd.BuildAWSConfig(cfg)
	if err != nil {
		tb.Fatalf("build aws config: %v", err)
	}
	awscfg := awsResult.Config
	store, err := awsstore.New(awscfg)
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
