//go:build integration && disk && otlp && external && !lq

package diskintegration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/uuidv7"
)

func TestDiskOTLPExternalTraceEmission(t *testing.T) {
	endpoint := os.Getenv("LOCKD_OTLP_ENDPOINT")
	if endpoint == "" {
		t.Skip("LOCKD_OTLP_ENDPOINT is not set; skipping external OTLP test")
	}

	logDir := filepath.Join("logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("create logs dir: %v", err)
	}
	logFile := filepath.Join(logDir, fmt.Sprintf("disk-otlp-external-%s.log", time.Now().Format("20060102T150405.000000000")))
	t.Setenv("LOCKD_BENCH_LOG_PATH", logFile)
	t.Setenv("LOCKD_BENCH_LOG_LEVEL", "trace")
	if f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
		fmt.Fprintf(f, "=== disk external trace test %s ===\n", time.Now().Format(time.RFC3339Nano))
		_ = f.Close()
	} else {
		t.Fatalf("open log file: %v", err)
	}

	t.Setenv("LOCKD_OTLP_ENDPOINT", endpoint)
	ensureDiskRootEnv(t)

	root := prepareDiskRoot(t, "")
	cfg := buildDiskConfig(t, root, 0)
	cfg.OTLPEndpoint = endpoint

	cli := startDiskServer(t, cfg)

	ctx := context.Background()
	key := "disk-otlp-external-" + uuidv7.NewString()
	lease := acquireWithRetry(t, ctx, cli, key, "otlp-external-worker", 30, lockdclient.BlockWaitForever)
	if !releaseLease(t, ctx, lease) {
		t.Fatalf("release failed")
	}

	// allow asynchronous exporters to flush
	time.Sleep(2 * time.Second)
}
