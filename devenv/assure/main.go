package main

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
	"time"

	"github.com/google/uuid"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/logport"
)

func main() {
	ctx := context.Background()
	cfg := loadConfig()
	if err := run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "devenv assurance failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("devenv assurance succeeded")
}

type envConfig struct {
	MinioEndpoint string
	MinioAccess   string
	MinioSecret   string
	MinioBucket   string
	MinioPrefix   string
	MinioSecure   bool
	OTLPGRPC      string
	JaegerURL     string
}

func loadConfig() envConfig {
	return envConfig{
		MinioEndpoint: "localhost:9000",
		MinioAccess:   "lockddev",
		MinioSecret:   "lockddevpass",
		MinioBucket:   "lockd-assure",
		MinioPrefix:   "assure",
		MinioSecure:   false,
		OTLPGRPC:      "localhost:4317",
		JaegerURL:     "http://localhost:16686",
	}
}

func run(ctx context.Context, cfg envConfig) error {
	minioClient, err := newMinioClient(cfg)
	if err != nil {
		return fmt.Errorf("connect to minio: %w", err)
	}
	if err := ensureBucket(ctx, minioClient, cfg.MinioBucket); err != nil {
		return fmt.Errorf("ensure minio bucket: %w", err)
	}
	if err := probeMinioIO(ctx, minioClient, cfg); err != nil {
		return fmt.Errorf("minio IO check failed: %w", err)
	}

	server, err := startLockdServer(ctx, cfg)
	if err != nil {
		return fmt.Errorf("start lockd server: %w", err)
	}
	defer server.Close()

	key := "assure-" + uuid.NewString()
	traceStart := time.Now()
	metaObj, stateObj, err := exerciseLockd(ctx, cfg, server.baseURL, key)
	if err != nil {
		return fmt.Errorf("lockd workflow failed: %w", err)
	}

	if err := verifyObjects(ctx, minioClient, cfg.MinioBucket, []string{metaObj, stateObj}); err != nil {
		return fmt.Errorf("verify lockd objects: %w", err)
	}
	if err := cleanupPrefix(ctx, minioClient, cfg.MinioBucket, cfg.MinioPrefix); err != nil {
		return fmt.Errorf("cleanup prefix: %w", err)
	}

	if err := waitForJaegerTrace(ctx, cfg.JaegerURL, traceStart.Add(-2*time.Second)); err != nil {
		return fmt.Errorf("jaeger trace check failed: %w", err)
	}

	return nil
}

func newMinioClient(cfg envConfig) (*minio.Client, error) {
	endpoint := strings.TrimSpace(cfg.MinioEndpoint)
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")
	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccess, cfg.MinioSecret, ""),
		Secure: cfg.MinioSecure,
	}
	return minio.New(endpoint, opts)
}

func ensureBucket(ctx context.Context, mc *minio.Client, bucket string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	exists, err := mc.BucketExists(timeoutCtx, bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return mc.MakeBucket(timeoutCtx, bucket, minio.MakeBucketOptions{})
}

func probeMinioIO(ctx context.Context, mc *minio.Client, cfg envConfig) error {
	object := path.Join(strings.Trim(cfg.MinioPrefix, "/"), "probe-"+uuid.NewString()+".txt")
	data := []byte("lockd devenv assure")
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := mc.PutObject(timeoutCtx, cfg.MinioBucket, object, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "text/plain"})
	if err != nil {
		return err
	}
	defer mc.RemoveObject(context.Background(), cfg.MinioBucket, object, minio.RemoveObjectOptions{})
	timeoutCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	obj, err := mc.GetObject(timeoutCtx, cfg.MinioBucket, object, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer obj.Close()
	content, err := io.ReadAll(obj)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read probe object: %w", err)
	}
	if string(content) != string(data) {
		return fmt.Errorf("unexpected probe payload: %q", string(content))
	}
	return nil
}

func startLockdServer(ctx context.Context, cfg envConfig) (*lockdServerHandle, error) {
	prefix := strings.Trim(cfg.MinioPrefix, "/")
	storePath := cfg.MinioBucket
	if prefix != "" {
		storePath = storePath + "/" + prefix
	}
	insecureParam := "1"
	if cfg.MinioSecure {
		insecureParam = "0"
	}
	storeURL := fmt.Sprintf("s3://%s/%s?insecure=%s&path-style=1", cfg.MinioEndpoint, storePath, insecureParam)

	serverCfg := lockd.Config{
		Store:             storeURL,
		ListenProto:       "tcp",
		Listen:            "127.0.0.1:0",
		DisableMTLS:       true,
		JSONUtil:          lockd.JSONUtilJSONV2,
		OTLPEndpoint:      cfg.OTLPGRPC,
		S3AccessKeyID:     cfg.MinioAccess,
		S3SecretAccessKey: cfg.MinioSecret,
	}
	if err := serverCfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate lockd config: %w", err)
	}
	srv, stop, err := lockd.StartServer(ctx, serverCfg, lockd.WithLogger(logport.NoopLogger()))
	if err != nil {
		return nil, err
	}
	addr := srv.ListenerAddr()
	if addr == nil {
		_ = stop(context.Background())
		return nil, fmt.Errorf("lockd listener not initialized")
	}
	baseURL := fmt.Sprintf("http://%s", addr.String())
	return &lockdServerHandle{baseURL: baseURL, stop: stop}, nil
}

type lockdServerHandle struct {
	baseURL string
	stop    func(context.Context, ...lockd.CloseOption) error
}

func (h *lockdServerHandle) Close() {
	if h == nil || h.stop == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = h.stop(ctx)
}

func exerciseLockd(ctx context.Context, cfg envConfig, baseURL, key string) (string, string, error) {
	cli, err := lockdclient.New(baseURL,
		lockdclient.WithDisableMTLS(true),
		lockdclient.WithHTTPTimeout(15*time.Second),
		lockdclient.WithLogger(logport.NoopLogger()),
	)
	if err != nil {
		return "", "", fmt.Errorf("new client: %w", err)
	}
	defer cli.Close()

	acquireCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	lease, err := cli.Acquire(acquireCtx, api.AcquireRequest{
		Key:        key,
		Owner:      "assure",
		TTLSeconds: 60,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	cancel()
	if err != nil {
		return "", "", fmt.Errorf("initial acquire: %w", err)
	}

	payload := map[string]any{"value": "created", "count": 1}
	if err := lease.Save(ctx, payload); err != nil {
		return "", "", fmt.Errorf("initial save: %w", err)
	}
	if err := lease.Release(ctx); err != nil {
		return "", "", fmt.Errorf("initial release: %w", err)
	}

	err = cli.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        key,
		Owner:      "assure-updater",
		TTLSeconds: 45,
		BlockSecs:  lockdclient.BlockWaitForever,
	}, func(handlerCtx context.Context, af *lockdclient.AcquireForUpdateContext) error {
		if af.State == nil || !af.State.HasState {
			return fmt.Errorf("expected state snapshot")
		}
		var snapshot map[string]any
		if err := af.State.Decode(&snapshot); err != nil {
			return fmt.Errorf("decode snapshot: %w", err)
		}
		snapshot["value"] = "updated"
		if count, ok := snapshot["count"].(float64); ok {
			snapshot["count"] = count + 1
		} else {
			snapshot["count"] = 2
		}
		snapshot["owner"] = "assure-updater"
		return af.Save(handlerCtx, snapshot)
	})
	if err != nil {
		return "", "", fmt.Errorf("acquire-for-update: %w", err)
	}

	verifyCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	lease2, err := cli.Acquire(verifyCtx, api.AcquireRequest{
		Key:        key,
		Owner:      "assure-verifier",
		TTLSeconds: 30,
		BlockSecs:  lockdclient.BlockWaitForever,
	})
	if err != nil {
		return "", "", fmt.Errorf("verification acquire: %w", err)
	}
	stateBytes, err := lease2.GetBytes(ctx)
	if err != nil {
		return "", "", fmt.Errorf("load state bytes: %w", err)
	}
	if err := lease2.Release(ctx); err != nil {
		return "", "", fmt.Errorf("verification release: %w", err)
	}
	var state map[string]any
	if err := json.Unmarshal(stateBytes, &state); err != nil {
		return "", "", fmt.Errorf("decode verification payload: %w", err)
	}
	if state["value"] != "updated" {
		return "", "", fmt.Errorf("unexpected final value: %v", state["value"])
	}

	prefix := strings.Trim(cfg.MinioPrefix, "/")
	metaObject := path.Join(prefix, "meta", key+".pb")
	stateObject := path.Join(prefix, "state", key+".json")
	if prefix == "" {
		metaObject = path.Join("meta", key+".pb")
		stateObject = path.Join("state", key+".json")
	}
	return metaObject, stateObject, nil
}

func verifyObjects(ctx context.Context, mc *minio.Client, bucket string, objects []string) error {
	for _, obj := range objects {
		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := mc.StatObject(timeoutCtx, bucket, obj, minio.StatObjectOptions{})
		cancel()
		if err != nil {
			return fmt.Errorf("stat %s: %w", obj, err)
		}
	}
	return nil
}

func cleanupPrefix(ctx context.Context, mc *minio.Client, bucket, prefix string) error {
	normalized := strings.Trim(prefix, "/")
	if normalized != "" {
		normalized += "/"
	}
	opts := minio.ListObjectsOptions{
		Prefix:    normalized,
		Recursive: true,
	}
	for object := range mc.ListObjects(ctx, bucket, opts) {
		if object.Err != nil {
			return fmt.Errorf("list %s: %w", object.Key, object.Err)
		}
		removeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := mc.RemoveObject(removeCtx, bucket, object.Key, minio.RemoveObjectOptions{})
		cancel()
		if err != nil {
			return fmt.Errorf("remove %s: %w", object.Key, err)
		}
	}
	return nil
}

type jaegerResponse struct {
	Data []struct {
		TraceID string `json:"traceID"`
		Spans   []struct {
			OperationName string `json:"operationName"`
			StartTime     int64  `json:"startTime"`
		} `json:"spans"`
	} `json:"data"`
}

func waitForJaegerTrace(ctx context.Context, baseURL string, since time.Time) error {
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := fetchJaeger(ctx, baseURL)
		if err != nil {
			lastErr = err
		} else if hasRecentSpan(resp, since) {
			return nil
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return lastErr
			}
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("no jaeger traces newer than %s", since.Format(time.RFC3339))
}

func fetchJaeger(ctx context.Context, baseURL string) (jaegerResponse, error) {
	var result jaegerResponse
	client := &http.Client{Timeout: 5 * time.Second}
	endpoint := strings.TrimRight(baseURL, "/") + "/api/traces?service=lockd&lookback=1h&limit=20"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return result, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("jaeger query status %s", resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return result, err
	}
	return result, nil
}

func hasRecentSpan(resp jaegerResponse, since time.Time) bool {
	threshold := since.UnixMicro()
	for _, trace := range resp.Data {
		for _, span := range trace.Spans {
			if span.StartTime >= threshold {
				return true
			}
		}
	}
	return false
}
