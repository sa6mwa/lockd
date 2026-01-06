//go:build integration && mixed

package mixedintegration

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/integration/internal/cryptotest"
	"pkt.systems/lockd/internal/storage/s3"
	"pkt.systems/lockd/namespaces"
)

func TestMixedBackendTxnCommitRollback(t *testing.T) {
	t.Run("Commit", func(t *testing.T) {
		runMixedBackendTxn(t, true)
	})
	t.Run("Rollback", func(t *testing.T) {
		runMixedBackendTxn(t, false)
	})
}

func runMixedBackendTxn(t *testing.T, commit bool) {
	diskCfg := loadDiskConfig(t)
	minioCfg := loadMinioConfig(t)
	cryptotest.ConfigureTCAuth(t, &diskCfg)
	cryptotest.ConfigureTCAuth(t, &minioCfg)
	ensureMinioBucket(t, minioCfg)

	serverOpts := cryptotest.SharedMTLSOptions(t)
	bundlePath := cryptotest.SharedTCClientBundlePath(t)

	rm := lockd.StartTestServer(t, append(serverOpts, lockd.WithTestConfig(minioCfg))...)
	tcCfg := diskCfg
	if bundlePath != "" {
		tcCfg.TCClientBundlePath = bundlePath
	} else {
		tcCfg.DisableMTLS = true
	}
	tc := lockd.StartTestServer(t, append(serverOpts, lockd.WithTestConfig(tcCfg))...)

	stop := func(ts *lockd.TestServer) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = ts.Stop(ctx)
	}
	t.Cleanup(func() { stop(tc) })
	t.Cleanup(func() { stop(rm) })

	cryptotest.RegisterRM(t, tc, rm)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	keyA := fmt.Sprintf("mixed-%d-a", time.Now().UnixNano())
	keyB := fmt.Sprintf("mixed-%d-b", time.Now().UnixNano())
	tcTxnClient := cryptotest.RequireTCClient(t, tc)

	leaseA, err := tc.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyA,
		Owner:      "mixed-tc",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire disk key: %v", err)
	}
	if err := leaseA.Save(ctx, map[string]any{"value": "disk"}); err != nil {
		t.Fatalf("save disk key: %v", err)
	}

	leaseB, err := rm.Client.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespaces.Default,
		Key:        keyB,
		Owner:      "mixed-rm",
		TTLSeconds: 30,
		TxnID:      leaseA.TxnID,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("acquire minio key: %v", err)
	}
	if err := leaseB.Save(ctx, map[string]any{"value": "minio"}); err != nil {
		t.Fatalf("save minio key: %v", err)
	}

	diskHash, err := tc.Backend().BackendHash(ctx)
	if err != nil {
		t.Fatalf("disk backend hash: %v", err)
	}
	minioHash, err := rm.Backend().BackendHash(ctx)
	if err != nil {
		t.Fatalf("minio backend hash: %v", err)
	}

	req := api.TxnDecisionRequest{
		TxnID: leaseA.TxnID,
		Participants: []api.TxnParticipant{
			{Namespace: namespaces.Default, Key: keyA, BackendHash: diskHash},
			{Namespace: namespaces.Default, Key: keyB, BackendHash: minioHash},
		},
	}
	if commit {
		if _, err := tcTxnClient.TxnCommit(ctx, req); err != nil {
			t.Fatalf("txn commit: %v", err)
		}
	} else {
		if _, err := tcTxnClient.TxnRollback(ctx, req); err != nil {
			t.Fatalf("txn rollback: %v", err)
		}
	}

	waitForState(t, tc.Client, keyA, commit)
	waitForState(t, rm.Client, keyB, commit)
}

func waitForState(t testing.TB, cli *lockdclient.Client, key string, expect bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := cli.Get(ctx, key)
		cancel()
		if err == nil && resp != nil {
			hasState := resp.HasState
			_ = resp.Close()
			if hasState == expect {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("expected state=%v for key %s", expect, key)
}

func loadDiskConfig(tb testing.TB) lockd.Config {
	tb.Helper()
	root := tb.TempDir()
	cfg := lockd.Config{
		Store:           diskStoreURL(root),
		SweeperInterval: time.Second,
		TCFanoutTimeout: 15 * time.Second,
	}
	cryptotest.MaybeEnableStorageEncryption(tb, &cfg)
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

func loadMinioConfig(tb testing.TB) lockd.Config {
	tb.Helper()
	ensureMinioCredentials(tb)
	store := firstEnv("LOCKD_MINIO_STORE", "LOCKD_STORE")
	if store == "" {
		tb.Fatalf("LOCKD_MINIO_STORE must be set to an s3:// URI for MinIO integration tests")
	}
	if !strings.HasPrefix(store, "s3://") {
		tb.Fatalf("LOCKD_MINIO_STORE must reference an s3:// URI for MinIO integration tests, got %q", store)
	}
	accessKey := firstEnv("LOCKD_MINIO_S3_ACCESS_KEY_ID", "LOCKD_S3_ACCESS_KEY_ID")
	secretKey := firstEnv("LOCKD_MINIO_S3_SECRET_ACCESS_KEY", "LOCKD_S3_SECRET_ACCESS_KEY")
	if accessKey == "" || secretKey == "" {
		accessKey = firstEnv("LOCKD_MINIO_S3_ROOT_USER", "LOCKD_S3_ROOT_USER")
		secretKey = firstEnv("LOCKD_MINIO_S3_ROOT_PASSWORD", "LOCKD_S3_ROOT_PASSWORD")
	}
	sessionToken := firstEnv("LOCKD_MINIO_S3_SESSION_TOKEN", "LOCKD_S3_SESSION_TOKEN")
	cfg := lockd.Config{
		Store:           store,
		S3MaxPartSize:   8 << 20,
		SweeperInterval: time.Second,
		TCFanoutTimeout: 15 * time.Second,
	}
	if accessKey != "" || secretKey != "" || sessionToken != "" {
		cfg.S3AccessKeyID = accessKey
		cfg.S3SecretAccessKey = secretKey
		cfg.S3SessionToken = sessionToken
	}
	cryptotest.MaybeEnableStorageEncryption(tb, &cfg)
	return cfg
}

func ensureMinioCredentials(tb testing.TB) {
	accessKey := strings.TrimSpace(firstEnv("LOCKD_MINIO_S3_ACCESS_KEY_ID", "LOCKD_S3_ACCESS_KEY_ID"))
	secretKey := strings.TrimSpace(firstEnv("LOCKD_MINIO_S3_SECRET_ACCESS_KEY", "LOCKD_S3_SECRET_ACCESS_KEY"))
	rootUser := strings.TrimSpace(firstEnv("LOCKD_MINIO_S3_ROOT_USER", "LOCKD_S3_ROOT_USER"))
	rootPass := strings.TrimSpace(firstEnv("LOCKD_MINIO_S3_ROOT_PASSWORD", "LOCKD_S3_ROOT_PASSWORD"))
	if (accessKey == "" || secretKey == "") && (rootUser == "" || rootPass == "") {
		tb.Fatalf("MinIO integration tests require either LOCKD_MINIO_S3_ACCESS_KEY_ID/LOCKD_MINIO_S3_SECRET_ACCESS_KEY or LOCKD_MINIO_S3_ROOT_USER/LOCKD_MINIO_S3_ROOT_PASSWORD to be set")
	}
}

func firstEnv(keys ...string) string {
	for _, key := range keys {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return val
		}
	}
	return ""
}

func ensureMinioBucket(tb testing.TB, cfg lockd.Config) {
	minioResult, err := lockd.BuildGenericS3Config(cfg)
	if err != nil {
		tb.Fatalf("build s3 config: %v", err)
	}
	minioCfg := minioResult.Config
	store, err := s3.New(minioCfg)
	if err != nil {
		tb.Fatalf("new minio store: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exists, err := store.Client().BucketExists(ctx, minioCfg.Bucket)
	if err != nil {
		tb.Fatalf("bucket exists: %v", err)
	}
	if !exists {
		if err := store.Client().MakeBucket(ctx, minioCfg.Bucket, minio.MakeBucketOptions{Region: minioCfg.Region}); err != nil {
			tb.Fatalf("make bucket: %v", err)
		}
	}
	_ = store.Close()
}
