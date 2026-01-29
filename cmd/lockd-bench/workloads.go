package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/pslog"
)

type benchKey struct {
	key string
	idx int
}

type benchClient struct {
	client      *lockdclient.Client
	stop        func(context.Context) error
	cleanup     func()
	canMutateNS bool
	server      *lockd.Server
}

func runWorkloadBench(cfg benchConfig) {
	ctx := context.Background()
	logger, closeLogger := newBenchLogger(cfg)
	defer closeLogger()
	root, cleanup := prepareRoot(cfg.root, cfg.keepRoot)
	defer cleanup()

	runID := time.Now().UTC().Format("20060102T150405.000000000Z")
	totalRuns := cfg.warmupRuns + cfg.runs
	results := make([]workloadRun, 0, cfg.runs)
	namespacesUsed := make(map[string]struct{})

	var cpuProfileFile *os.File
	if cfg.cpuProfile != "" {
		f, err := os.Create(cfg.cpuProfile)
		if err != nil {
			die("cpu profile: %v", err)
		}
		cpuProfileFile = f
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			die("cpu profile start: %v", err)
		}
	}
	defer func() {
		if cpuProfileFile == nil {
			return
		}
		pprof.StopCPUProfile()
		_ = cpuProfileFile.Close()
	}()

	bench, err := startBenchClient(ctx, cfg, root, logger)
	if err != nil {
		die("bench client: %v", err)
	}
	cleanupBackend, cleanupClose, err := openCleanupBackend(cfg, root)
	if err != nil {
		die("bench cleanup backend: %v", err)
	}
	defer func() {
		if cleanupClose != nil {
			cleanupClose()
		}
		if bench.stop != nil {
			_ = bench.stop(context.Background())
		}
		if bench.cleanup != nil {
			bench.cleanup()
		}
	}()
	if bench.server != nil {
		fmt.Printf("bench qrf config: cpu_soft=%.1f cpu_hard=%.1f mem_soft=%.1f mem_hard=%.1f mem_headroom=%.1f mem_soft_bytes=%d mem_hard_bytes=%d swap_soft=%.1f swap_hard=%.1f soft_delay=%s engaged_delay=%s recovery_delay=%s max_wait=%s\n",
			cfg.qrfCPUSoft,
			cfg.qrfCPUHard,
			cfg.qrfMemSoftPct,
			cfg.qrfMemHardPct,
			cfg.qrfMemHeadroom,
			cfg.qrfMemSoftBytes,
			cfg.qrfMemHardBytes,
			cfg.qrfSwapSoft,
			cfg.qrfSwapHard,
			cfg.qrfSoftDelay,
			cfg.qrfEngagedDelay,
			cfg.qrfRecoveryDelay,
			cfg.qrfMaxWait)
	}

	for i := 0; i < totalRuns; i++ {
		warmup := i < cfg.warmupRuns
		runLabel := fmt.Sprintf("run=%d/%d warmup=%t", i+1, totalRuns, warmup)
		prefix := fmt.Sprintf("bench/%s/run-%d", runID, i)
		runCfg := cfg
		runCfg.namespace = runNamespace(cfg, runID, i)
		namespacesUsed[runCfg.namespace] = struct{}{}
		run, err := runWorkloadOnce(ctx, bench, runCfg, prefix)
		if err != nil {
			die("workload: %v", err)
		}
		printWorkloadRun(runCfg, runLabel, run)
		if bench.server != nil {
			status := bench.server.QRFStatus()
			fmt.Printf("bench qrf status: state=%s reason=%s cpu=%.1f mem=%.1f swap=%.1f load1=%.2f load1x=%.2f inflight_lock=%d inflight_queue=%d inflight_query=%d\n",
				status.State.String(),
				status.Reason,
				status.Snapshot.SystemCPUPercent,
				status.Snapshot.SystemMemoryUsedPercent,
				status.Snapshot.SystemSwapUsedPercent,
				status.Snapshot.SystemLoad1,
				status.Snapshot.Load1Multiplier,
				status.Snapshot.LockInflight,
				status.Snapshot.QueueProducerInflight+status.Snapshot.QueueConsumerInflight+status.Snapshot.QueueAckInflight,
				status.Snapshot.QueryInflight,
			)
		}
		if !warmup {
			results = append(results, run)
		}
	}
	if len(results) > 1 {
		fmt.Printf("bench summary: workload=%s runs=%d warmup=%d aggregation=median\n", cfg.workload, cfg.runs, cfg.warmupRuns)
		printWorkloadSummary(cfg, results)
	}
	if !cfg.keepNamespace {
		if cfg.endpoint != "" {
			fmt.Printf("bench cleanup: skipped (endpoint provided)\n")
		} else if shouldSkipDiskCleanup(cfg, root) {
			fmt.Printf("bench cleanup: skipped (temp root)\n")
		} else if cleanupBackend == nil {
			fmt.Printf("bench cleanup: skipped (backend unavailable)\n")
		} else {
			if bench.stop != nil {
				_ = bench.stop(context.Background())
				bench.stop = nil
			}
			for ns := range namespacesUsed {
				stats, err := cleanupNamespace(ctx, cleanupBackend, ns)
				if err != nil {
					die("cleanup namespace %s: %v", ns, err)
				}
				fmt.Printf("bench cleanup: namespace=%s objects=%d meta=%d\n", ns, stats.objects, stats.meta)
			}
		}
	}
	if cfg.memProfile != "" {
		f, err := os.Create(cfg.memProfile)
		if err != nil {
			die("mem profile: %v", err)
		}
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			_ = f.Close()
			die("mem profile write: %v", err)
		}
		_ = f.Close()
	}
}

func startBenchClient(ctx context.Context, cfg benchConfig, root string, logger pslog.Logger) (*benchClient, error) {
	endpoint := strings.TrimSpace(cfg.endpoint)
	if endpoint != "" {
		opts := []lockdclient.Option{lockdclient.WithDisableMTLS(cfg.disableMTLS)}
		if cfg.httpTimeout > 0 {
			opts = append(opts, lockdclient.WithHTTPTimeout(cfg.httpTimeout))
		}
		if logger != nil {
			opts = append(opts, lockdclient.WithLogger(logger))
		}
		cli, err := lockdclient.New(endpoint, opts...)
		if err != nil {
			return nil, err
		}
		return &benchClient{client: cli, canMutateNS: false}, nil
	}
	socketDir := createBenchTempDir("socket-")
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	socketPath := filepath.Join(socketDir, "lockd.sock")
	cleanup := func() { _ = os.RemoveAll(socketDir) }

	storeURL, err := benchStoreURL(cfg, root)
	if err != nil {
		cleanup()
		return nil, err
	}
	srvCfg := lockdConfigForStore(storeURL, cfg)
	srvCfg.ListenProto = "unix"
	srvCfg.Listen = socketPath
	srvCfg.DisableMTLS = true
	srvCfg.HAMode = cfg.haMode
	srvCfg.HALeaseTTL = cfg.haLeaseTTL
	srvCfg.DefaultNamespace = cfg.namespace
	srvCfg.LogstoreCommitMaxOps = cfg.commitMaxOps
	srvCfg.LogstoreSegmentSize = cfg.segmentSize
	srvCfg.QRFDisabled = cfg.qrfDisabled
	srvCfg.QRFCPUPercentSoftLimit = cfg.qrfCPUSoft
	srvCfg.QRFCPUPercentHardLimit = cfg.qrfCPUHard
	srvCfg.QRFCPUPercentSoftLimitSet = cfg.qrfCPUSoft > 0
	srvCfg.QRFCPUPercentHardLimitSet = cfg.qrfCPUHard > 0
	srvCfg.QRFMemorySoftLimitPercent = cfg.qrfMemSoftPct
	srvCfg.QRFMemoryHardLimitPercent = cfg.qrfMemHardPct
	srvCfg.QRFMemoryStrictHeadroomPercent = cfg.qrfMemHeadroom
	srvCfg.QRFMemorySoftLimitBytes = cfg.qrfMemSoftBytes
	srvCfg.QRFMemoryHardLimitBytes = cfg.qrfMemHardBytes
	srvCfg.QRFSwapSoftLimitPercent = cfg.qrfSwapSoft
	srvCfg.QRFSwapHardLimitPercent = cfg.qrfSwapHard
	srvCfg.QRFSoftDelay = cfg.qrfSoftDelay
	srvCfg.QRFEngagedDelay = cfg.qrfEngagedDelay
	srvCfg.QRFRecoveryDelay = cfg.qrfRecoveryDelay
	srvCfg.QRFMaxWait = cfg.qrfMaxWait
	srvCfg.QueryDocPrefetch = cfg.queryDocPrefetch
	if err := srvCfg.Validate(); err != nil {
		cleanup()
		return nil, err
	}
	handle, err := lockd.StartServer(ctx, srvCfg, lockd.WithLogger(logger))
	if err != nil {
		cleanup()
		return nil, err
	}
	opts := []lockdclient.Option{lockdclient.WithLogger(logger)}
	if cfg.httpTimeout > 0 {
		opts = append(opts, lockdclient.WithHTTPTimeout(cfg.httpTimeout))
	}
	cli, err := lockdclient.New("unix://"+socketPath, opts...)
	if err != nil {
		_ = handle.Stop(context.Background())
		cleanup()
		return nil, err
	}
	stop := func(ctx context.Context) error {
		return handle.Stop(ctx)
	}
	return &benchClient{client: cli, stop: stop, cleanup: cleanup, canMutateNS: true, server: handle.Server}, nil
}

func benchStoreURL(cfg benchConfig, root string) (string, error) {
	if cfg.storeURL != "" {
		return cfg.storeURL, nil
	}
	switch cfg.backend {
	case "mem":
		return "mem://", nil
	case "disk":
		if root == "" {
			return "", fmt.Errorf("disk backend requires root")
		}
		return "disk://" + root, nil
	case "minio", "aws", "azure":
		storeURL := strings.TrimSpace(os.Getenv("LOCKD_STORE"))
		if storeURL == "" {
			return "", fmt.Errorf("%s backend requires --store or LOCKD_STORE", cfg.backend)
		}
		return storeURL, nil
	default:
		return "", fmt.Errorf("unsupported backend %q", cfg.backend)
	}
}

type workloadRun struct {
	elapsed       time.Duration
	indexFlush    time.Duration
	phases        map[string]benchStats
	errCounts     map[string]int64
	firstErr      error
	firstErrPhase string
}

func runWorkloadOnce(ctx context.Context, bench *benchClient, cfg benchConfig, prefix string) (workloadRun, error) {
	if bench == nil || bench.client == nil {
		return workloadRun{}, fmt.Errorf("client required")
	}
	cli := bench.client
	keyCount := cfg.keyCount
	if keyCount <= 0 {
		keyCount = cfg.ops
	}
	if keyCount < cfg.concurrency {
		keyCount = cfg.concurrency
	}
	workload := strings.ToLower(strings.TrimSpace(cfg.workload))
	switch workload {
	case "lock", "lock-core", "core":
		keys := buildBenchKeys(prefix, keyCount)
		return runLockWorkload(ctx, cli, cfg, keys)
	case "attachments", "attachment":
		keys := buildBenchKeys(prefix, keyCount)
		return runAttachmentWorkload(ctx, cli, cfg, keys)
	case "attachments-public", "public-attachments", "attachment-public":
		keys := buildBenchKeys(prefix, keyCount)
		return runAttachmentPublicWorkload(ctx, cli, cfg, keys)
	case "queue":
		return runQueueWorkload(ctx, cli, cfg)
	case "public-read", "public", "read":
		keys := buildBenchKeys(prefix, keyCount)
		if err := seedStateKeys(ctx, cli, cfg, keys, buildStatePayload); err != nil {
			return workloadRun{}, err
		}
		return runPublicReadWorkload(ctx, cli, cfg, keys)
	case "query-index":
		if err := ensureNamespaceQueryConfig(ctx, cli, cfg, "index", "none", bench.canMutateNS); err != nil {
			return workloadRun{}, err
		}
		if cfg.querySeed {
			keys := buildBenchKeys(prefix, keyCount)
			if err := seedStateKeys(ctx, cli, cfg, keys, buildQueryPayload); err != nil {
				return workloadRun{}, err
			}
		}
		indexFlushStart := time.Now()
		if cfg.queryFlush {
			if err := waitForIndex(ctx, cli, cfg); err != nil {
				return workloadRun{}, err
			}
		}
		run, err := runQueryWorkload(ctx, cli, cfg, "index")
		if err != nil {
			return workloadRun{}, err
		}
		if cfg.queryFlush {
			run.indexFlush = time.Since(indexFlushStart)
		}
		return run, nil
	case "query-scan":
		if err := ensureNamespaceQueryConfig(ctx, cli, cfg, "scan", "none", bench.canMutateNS); err != nil {
			return workloadRun{}, err
		}
		if cfg.querySeed {
			keys := buildBenchKeys(prefix, keyCount)
			if err := seedStateKeys(ctx, cli, cfg, keys, buildQueryPayload); err != nil {
				return workloadRun{}, err
			}
		}
		return runQueryWorkload(ctx, cli, cfg, "scan")
	case "mixed":
		if err := ensureNamespaceQueryConfig(ctx, cli, cfg, "index", "scan", bench.canMutateNS); err != nil {
			return workloadRun{}, err
		}
		keys := buildBenchKeys(prefix, keyCount)
		if cfg.querySeed {
			if err := seedStateKeys(ctx, cli, cfg, keys, buildQueryPayload); err != nil {
				return workloadRun{}, err
			}
		}
		indexFlushStart := time.Now()
		if cfg.queryFlush {
			if err := waitForIndex(ctx, cli, cfg); err != nil {
				return workloadRun{}, err
			}
		}
		run, err := runMixedWorkload(ctx, cli, cfg, keys)
		if err != nil {
			return workloadRun{}, err
		}
		if cfg.queryFlush {
			run.indexFlush = time.Since(indexFlushStart)
		}
		return run, nil
	case "xa-commit":
		keys := buildBenchKeys(prefix, keyCount)
		return runXAWorkload(ctx, cli, cfg, keys, true)
	case "xa-rollback":
		keys := buildBenchKeys(prefix, keyCount)
		return runXAWorkload(ctx, cli, cfg, keys, false)
	default:
		return workloadRun{}, fmt.Errorf("unknown workload %q", cfg.workload)
	}
}

func ensureNamespaceQueryConfig(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, preferred, fallback string, allow bool) error {
	if cli == nil {
		return fmt.Errorf("client required")
	}
	if !allow {
		return nil
	}
	req := api.NamespaceConfigRequest{
		Namespace: cfg.namespace,
		Query: &api.NamespaceQueryConfig{
			PreferredEngine: preferred,
			FallbackEngine:  fallback,
		},
	}
	_, err := cli.UpdateNamespaceConfig(ctx, req, lockdclient.NamespaceConfigOptions{})
	return err
}

func buildBenchKeys(prefix string, count int) []benchKey {
	if count <= 0 {
		return nil
	}
	keys := make([]benchKey, count)
	for i := 0; i < count; i++ {
		keys[i] = benchKey{key: fmt.Sprintf("%s/key-%d", prefix, i), idx: i}
	}
	return keys
}

func seedStateKeys(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, keys []benchKey, payloadFn func(benchKey, benchConfig) []byte) error {
	for _, key := range keys {
		lease, err := cli.Acquire(ctx, api.AcquireRequest{
			Namespace:  cfg.namespace,
			Key:        key.key,
			Owner:      "bench-seed",
			TTLSeconds: 30,
		})
		if err != nil {
			return err
		}
		payload := payloadFn(key, cfg)
		if _, err := lease.UpdateBytes(ctx, payload); err != nil {
			_ = lease.Release(ctx)
			return err
		}
		if err := lease.Release(ctx); err != nil {
			return err
		}
	}
	return nil
}

func runLockWorkload(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, keys []benchKey) (workloadRun, error) {
	payload := buildStatePayload(benchKey{}, cfg)
	return runWorkloadOps(ctx, cfg, keys, []string{"acquire", "update", "release"}, func(ctx context.Context, key benchKey) opResult {
		start := time.Now()
		acqStart := time.Now()
		lease, err := cli.Acquire(ctx, api.AcquireRequest{
			Namespace:  cfg.namespace,
			Key:        key.key,
			Owner:      "bench",
			TTLSeconds: 30,
		})
		acqDur := time.Since(acqStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "acquire", phases: map[string]time.Duration{"acquire": acqDur}}
		}
		updStart := time.Now()
		_, err = lease.UpdateBytes(ctx, payload)
		updDur := time.Since(updStart)
		if err != nil {
			_ = lease.Release(ctx)
			return opResult{total: time.Since(start), err: err, errPhase: "update", phases: map[string]time.Duration{"acquire": acqDur, "update": updDur}}
		}
		relStart := time.Now()
		err = lease.Release(ctx)
		relDur := time.Since(relStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "release", phases: map[string]time.Duration{"acquire": acqDur, "update": updDur, "release": relDur}}
		}
		return opResult{total: time.Since(start), phases: map[string]time.Duration{"acquire": acqDur, "update": updDur, "release": relDur}}
	})
}

func runAttachmentWorkload(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, keys []benchKey) (workloadRun, error) {
	payload := buildBinaryPayload(cfg.attachBytes)
	return runWorkloadOps(ctx, cfg, keys, []string{"acquire", "attach", "get", "delete", "release"}, func(ctx context.Context, key benchKey) opResult {
		start := time.Now()
		acqStart := time.Now()
		lease, err := cli.Acquire(ctx, api.AcquireRequest{
			Namespace:  cfg.namespace,
			Key:        key.key,
			Owner:      "bench",
			TTLSeconds: 30,
		})
		acqDur := time.Since(acqStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "acquire", phases: map[string]time.Duration{"acquire": acqDur}}
		}
		name := fmt.Sprintf("bench-%d", key.idx)
		attStart := time.Now()
		_, err = lease.Attach(ctx, lockdclient.AttachRequest{
			Name:        name,
			Body:        bytes.NewReader(payload),
			ContentType: "application/octet-stream",
		})
		attDur := time.Since(attStart)
		if err != nil {
			_ = lease.Release(ctx)
			return opResult{total: time.Since(start), err: err, errPhase: "attach", phases: map[string]time.Duration{"acquire": acqDur, "attach": attDur}}
		}
		getStart := time.Now()
		attachment, err := lease.RetrieveAttachment(ctx, lockdclient.AttachmentSelector{Name: name})
		getDur := time.Since(getStart)
		if err != nil {
			_ = lease.Release(ctx)
			return opResult{total: time.Since(start), err: err, errPhase: "get", phases: map[string]time.Duration{"acquire": acqDur, "attach": attDur, "get": getDur}}
		}
		if attachment != nil {
			_, _ = io.Copy(io.Discard, attachment)
			_ = attachment.Close()
		}
		delStart := time.Now()
		_, err = lease.DeleteAttachment(ctx, lockdclient.AttachmentSelector{Name: name})
		delDur := time.Since(delStart)
		if err != nil {
			_ = lease.Release(ctx)
			return opResult{total: time.Since(start), err: err, errPhase: "delete", phases: map[string]time.Duration{"acquire": acqDur, "attach": attDur, "get": getDur, "delete": delDur}}
		}
		relStart := time.Now()
		err = lease.Release(ctx)
		relDur := time.Since(relStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "release", phases: map[string]time.Duration{"acquire": acqDur, "attach": attDur, "get": getDur, "delete": delDur, "release": relDur}}
		}
		return opResult{total: time.Since(start), phases: map[string]time.Duration{"acquire": acqDur, "attach": attDur, "get": getDur, "delete": delDur, "release": relDur}}
	})
}

func runAttachmentPublicWorkload(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, keys []benchKey) (workloadRun, error) {
	payload := buildBinaryPayload(cfg.attachBytes)
	phaseOrder := []string{"acquire", "attach", "release", "public-list", "public-get", "cleanup-acquire", "cleanup-delete", "cleanup-release"}
	return runWorkloadOps(ctx, cfg, keys, phaseOrder, func(ctx context.Context, key benchKey) opResult {
		start := time.Now()
		phases := make(map[string]time.Duration, len(phaseOrder))

		acqStart := time.Now()
		lease, err := cli.Acquire(ctx, api.AcquireRequest{
			Namespace:  cfg.namespace,
			Key:        key.key,
			Owner:      "bench",
			TTLSeconds: 30,
		})
		phases["acquire"] = time.Since(acqStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "acquire", phases: phases}
		}

		name := fmt.Sprintf("bench-%d", key.idx)
		attStart := time.Now()
		_, err = lease.Attach(ctx, lockdclient.AttachRequest{
			Name:        name,
			Body:        bytes.NewReader(payload),
			ContentType: "application/octet-stream",
		})
		phases["attach"] = time.Since(attStart)
		if err != nil {
			_ = lease.Release(ctx)
			return opResult{total: time.Since(start), err: err, errPhase: "attach", phases: phases}
		}

		relStart := time.Now()
		err = lease.Release(ctx)
		phases["release"] = time.Since(relStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "release", phases: phases}
		}

		listStart := time.Now()
		if err := waitForPublicAttachment(ctx, cli, cfg, key.key, name); err != nil {
			phases["public-list"] = time.Since(listStart)
			return opResult{total: time.Since(start), err: err, errPhase: "public-list", phases: phases}
		}
		phases["public-list"] = time.Since(listStart)

		getStart := time.Now()
		attachment, err := cli.GetAttachment(ctx, lockdclient.GetAttachmentRequest{
			Namespace: cfg.namespace,
			Key:       key.key,
			Public:    true,
			Selector:  lockdclient.AttachmentSelector{Name: name},
		})
		phases["public-get"] = time.Since(getStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "public-get", phases: phases}
		}
		if attachment != nil {
			_, _ = io.Copy(io.Discard, attachment)
			_ = attachment.Close()
		}

		cleanupAcqStart := time.Now()
		cleanupLease, err := cli.Acquire(ctx, api.AcquireRequest{
			Namespace:  cfg.namespace,
			Key:        key.key,
			Owner:      "bench-cleanup",
			TTLSeconds: 30,
		})
		phases["cleanup-acquire"] = time.Since(cleanupAcqStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "cleanup-acquire", phases: phases}
		}

		cleanupDelStart := time.Now()
		_, err = cleanupLease.DeleteAttachment(ctx, lockdclient.AttachmentSelector{Name: name})
		phases["cleanup-delete"] = time.Since(cleanupDelStart)
		if err != nil {
			_ = cleanupLease.Release(ctx)
			return opResult{total: time.Since(start), err: err, errPhase: "cleanup-delete", phases: phases}
		}

		cleanupRelStart := time.Now()
		err = cleanupLease.Release(ctx)
		phases["cleanup-release"] = time.Since(cleanupRelStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "cleanup-release", phases: phases}
		}

		return opResult{total: time.Since(start), phases: phases}
	})
}

func runQueueWorkload(ctx context.Context, cli *lockdclient.Client, cfg benchConfig) (workloadRun, error) {
	queue := cfg.queueName
	payload := buildBinaryPayload(cfg.queueBytes)
	return runWorkloadOps(ctx, cfg, nil, []string{"enqueue", "dequeue", "payload", "ack"}, func(ctx context.Context, _ benchKey) opResult {
		start := time.Now()
		enqStart := time.Now()
		_, err := cli.Enqueue(ctx, queue, bytes.NewReader(payload), lockdclient.EnqueueOptions{
			Namespace:   cfg.namespace,
			ContentType: "application/octet-stream",
		})
		enqDur := time.Since(enqStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "enqueue", phases: map[string]time.Duration{"enqueue": enqDur}}
		}
		deqStart := time.Now()
		msg, err := cli.Dequeue(ctx, queue, lockdclient.DequeueOptions{
			Namespace:    cfg.namespace,
			Owner:        "bench",
			BlockSeconds: lockdclient.BlockNoWait,
			PageSize:     1,
		})
		deqDur := time.Since(deqStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "dequeue", phases: map[string]time.Duration{"enqueue": enqDur, "dequeue": deqDur}}
		}
		if msg == nil {
			return opResult{total: time.Since(start), err: errors.New("queue empty"), errPhase: "dequeue", phases: map[string]time.Duration{"enqueue": enqDur, "dequeue": deqDur}}
		}
		payloadStart := time.Now()
		_, _ = msg.WritePayloadTo(io.Discard)
		payloadDur := time.Since(payloadStart)
		ackStart := time.Now()
		err = msg.Ack(ctx)
		ackDur := time.Since(ackStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "ack", phases: map[string]time.Duration{"enqueue": enqDur, "dequeue": deqDur, "payload": payloadDur, "ack": ackDur}}
		}
		return opResult{total: time.Since(start), phases: map[string]time.Duration{"enqueue": enqDur, "dequeue": deqDur, "payload": payloadDur, "ack": ackDur}}
	})
}

func runPublicReadWorkload(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, keys []benchKey) (workloadRun, error) {
	return runWorkloadOps(ctx, cfg, keys, []string{"get"}, func(ctx context.Context, key benchKey) opResult {
		start := time.Now()
		getStart := time.Now()
		resp, err := cli.Get(ctx, key.key, lockdclient.WithGetNamespace(cfg.namespace))
		getDur := time.Since(getStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "get", phases: map[string]time.Duration{"get": getDur}}
		}
		if resp != nil {
			reader := resp.Reader()
			if reader != nil {
				_, _ = io.Copy(io.Discard, reader)
				_ = reader.Close()
			}
			_ = resp.Close()
		}
		return opResult{total: time.Since(start), phases: map[string]time.Duration{"get": getDur}}
	})
}

func runXAWorkload(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, keys []benchKey, commit bool) (workloadRun, error) {
	payload := buildStatePayload(benchKey{}, cfg)
	phaseOrder := []string{"acquire-a", "update-a", "acquire-b", "update-b", "decide"}
	return runWorkloadOps(ctx, cfg, keys, phaseOrder, func(ctx context.Context, key benchKey) opResult {
		start := time.Now()
		phases := make(map[string]time.Duration, len(phaseOrder))

		keyA := key.key + "/a"
		keyB := key.key + "/b"

		acqAStart := time.Now()
		leaseA, err := cli.Acquire(ctx, api.AcquireRequest{
			Namespace:  cfg.namespace,
			Key:        keyA,
			Owner:      "bench-xa",
			TTLSeconds: 30,
			BlockSecs:  api.BlockNoWait,
		})
		phases["acquire-a"] = time.Since(acqAStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "acquire-a", phases: phases}
		}

		updAStart := time.Now()
		if _, err := leaseA.UpdateBytes(ctx, payload); err != nil {
			_ = leaseA.Release(ctx)
			phases["update-a"] = time.Since(updAStart)
			return opResult{total: time.Since(start), err: err, errPhase: "update-a", phases: phases}
		}
		phases["update-a"] = time.Since(updAStart)

		acqBStart := time.Now()
		leaseB, err := cli.Acquire(ctx, api.AcquireRequest{
			Namespace:  cfg.namespace,
			Key:        keyB,
			Owner:      "bench-xa",
			TTLSeconds: 30,
			TxnID:      leaseA.TxnID,
			BlockSecs:  api.BlockNoWait,
		})
		phases["acquire-b"] = time.Since(acqBStart)
		if err != nil {
			_ = leaseA.Release(ctx)
			return opResult{total: time.Since(start), err: err, errPhase: "acquire-b", phases: phases}
		}

		updBStart := time.Now()
		if _, err := leaseB.UpdateBytes(ctx, payload); err != nil {
			_ = leaseA.Release(ctx)
			_ = leaseB.Release(ctx)
			phases["update-b"] = time.Since(updBStart)
			return opResult{total: time.Since(start), err: err, errPhase: "update-b", phases: phases}
		}
		phases["update-b"] = time.Since(updBStart)

		decideStart := time.Now()
		req := api.TxnDecisionRequest{
			TxnID: leaseA.TxnID,
			Participants: []api.TxnParticipant{
				{Namespace: cfg.namespace, Key: keyA},
				{Namespace: cfg.namespace, Key: keyB},
			},
		}
		if commit {
			_, err = cli.TxnCommit(ctx, req)
		} else {
			_, err = cli.TxnRollback(ctx, req)
		}
		phases["decide"] = time.Since(decideStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "decide", phases: phases}
		}
		return opResult{total: time.Since(start), phases: phases}
	})
}

func runQueryWorkload(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, engine string) (workloadRun, error) {
	return runWorkloadOps(ctx, cfg, nil, []string{"query"}, func(ctx context.Context, _ benchKey) opResult {
		start := time.Now()
		queryStart := time.Now()
		resp, err := cli.Query(ctx,
			lockdclient.WithQueryNamespace(cfg.namespace),
			lockdclient.WithQuery(cfg.queryExpr),
			lockdclient.WithQueryLimit(cfg.queryLimit),
			lockdclient.WithQueryEngine(engine),
			withQueryReturn(cfg.queryReturn),
		)
		queryDur := time.Since(queryStart)
		if err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "query", phases: map[string]time.Duration{"query": queryDur}}
		}
		if resp != nil {
			_ = resp.ForEach(func(row lockdclient.QueryRow) error {
				if row.HasDocument() {
					reader, err := row.DocumentReader()
					if err == nil && reader != nil {
						_, _ = io.Copy(io.Discard, reader)
						_ = reader.Close()
					}
				}
				return nil
			})
			_ = resp.Close()
		}
		return opResult{total: time.Since(start), phases: map[string]time.Duration{"query": queryDur}}
	})
}

func runMixedWorkload(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, keys []benchKey) (workloadRun, error) {
	selector, err := parseMix(cfg.workloadMix)
	if err != nil {
		return workloadRun{}, err
	}
	payload := buildQueryPayload(benchKey{}, cfg)
	attachPayload := buildBinaryPayload(cfg.attachBytes)
	queuePayload := buildBinaryPayload(cfg.queueBytes)
	return runWorkloadOps(ctx, cfg, keys, selector.names(), func(ctx context.Context, key benchKey) opResult {
		op := selector.pick()
		switch op {
		case "lock":
			return opLock(ctx, cli, cfg, key, payload)
		case "read":
			return opRead(ctx, cli, cfg, key)
		case "attach":
			return opAttach(ctx, cli, cfg, key, attachPayload)
		case "queue":
			return opQueue(ctx, cli, cfg, queuePayload)
		case "query-index":
			return opQuery(ctx, cli, cfg, "index")
		case "query-scan":
			return opQuery(ctx, cli, cfg, "scan")
		default:
			return opResult{total: 0, err: fmt.Errorf("unknown op %s", op), errPhase: op}
		}
	})
}

type opResult struct {
	total    time.Duration
	phases   map[string]time.Duration
	err      error
	errPhase string
}

func runWorkloadOps(ctx context.Context, cfg benchConfig, keys []benchKey, phaseOrder []string, fn func(context.Context, benchKey) opResult) (workloadRun, error) {
	if fn == nil {
		return workloadRun{}, fmt.Errorf("nil workload op")
	}
	var (
		totalLat   []time.Duration
		phaseLat   = make(map[string][]time.Duration)
		errCounts  = make(map[string]int64)
		mu         sync.Mutex
		firstErr   error
		firstPhase string
		errOnce    sync.Once
		opsDone    atomic.Uint64
	)

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(cfg.concurrency)
	for w := 0; w < cfg.concurrency; w++ {
		go func(worker int) {
			defer wg.Done()
			localTotal := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			localPhases := make(map[string][]time.Duration)
			localErrs := make(map[string]int64)
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker)))
			workerKeys := keys
			if len(keys) >= cfg.concurrency {
				workerKeys = make([]benchKey, 0, (len(keys)+cfg.concurrency-1)/cfg.concurrency)
				for i := worker; i < len(keys); i += cfg.concurrency {
					workerKeys = append(workerKeys, keys[i])
				}
			}
			for {
				idx := opsDone.Add(1) - 1
				if int(idx) >= cfg.ops {
					break
				}
				var key benchKey
				if len(workerKeys) > 0 {
					key = workerKeys[int(idx)%len(workerKeys)]
					if len(workerKeys) > 1 {
						key = workerKeys[rng.Intn(len(workerKeys))]
					}
				}
				out := fn(ctx, key)
				localTotal = append(localTotal, out.total)
				for phase, dur := range out.phases {
					localPhases[phase] = append(localPhases[phase], dur)
				}
				if out.err != nil {
					localErrs["total"]++
					if out.errPhase != "" {
						localErrs[out.errPhase]++
					}
					errOnce.Do(func() {
						firstErr = out.err
						firstPhase = out.errPhase
					})
				}
			}
			mu.Lock()
			totalLat = append(totalLat, localTotal...)
			for phase, samples := range localPhases {
				phaseLat[phase] = append(phaseLat[phase], samples...)
			}
			for phase, count := range localErrs {
				errCounts[phase] += count
			}
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	phases := make(map[string]benchStats)
	phases["total"] = buildStats("total", elapsed, totalLat, errCounts["total"])
	for _, phase := range phaseOrder {
		samples := phaseLat[phase]
		phases[phase] = buildStats(phase, elapsed, samples, errCounts[phase])
	}
	return workloadRun{
		elapsed:       elapsed,
		phases:        phases,
		errCounts:     errCounts,
		firstErr:      firstErr,
		firstErrPhase: firstPhase,
	}, nil
}

func printWorkloadRun(cfg benchConfig, runLabel string, run workloadRun) {
	fmt.Printf("bench workload=%s %s ops=%d concurrency=%d payload_bytes=%d namespace=%s\n",
		cfg.workload, runLabel, cfg.ops, cfg.concurrency, cfg.payloadBytes, cfg.namespace)
	if run.firstErr != nil {
		fmt.Printf("first_error_phase=%s err=%v\n", run.firstErrPhase, run.firstErr)
	}
	if run.indexFlush > 0 {
		fmt.Printf("index_flush=%s\n", run.indexFlush)
	}
	phases := orderedPhaseKeys(run.phases)
	for _, phase := range phases {
		printStats(run.phases[phase])
	}
}

func printWorkloadSummary(cfg benchConfig, runs []workloadRun) {
	if len(runs) == 0 {
		return
	}
	phaseKeys := orderedPhaseKeys(runs[0].phases)
	for _, phase := range phaseKeys {
		stats := make([]benchStats, 0, len(runs))
		for _, run := range runs {
			stats = append(stats, run.phases[phase])
		}
		printStats(medianStats(phase, stats))
	}
}

func orderedPhaseKeys(phases map[string]benchStats) []string {
	if len(phases) == 0 {
		return nil
	}
	keys := make([]string, 0, len(phases))
	for k := range phases {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i] == "total" {
			return true
		}
		if keys[j] == "total" {
			return false
		}
		return keys[i] < keys[j]
	})
	return keys
}

func buildStatePayload(_ benchKey, cfg benchConfig) []byte {
	return buildPayload(cfg.payloadBytes)
}

func buildQueryPayload(key benchKey, _ benchConfig) []byte {
	status := "open"
	if key.idx%2 == 0 {
		status = "closed"
	}
	doc := map[string]any{
		"status":  status,
		"counter": key.idx,
	}
	payload, err := json.Marshal(doc)
	if err != nil {
		return []byte(`{"status":"open","counter":0}`)
	}
	return payload
}

func buildBinaryPayload(size int) []byte {
	if size <= 0 {
		return nil
	}
	return bytes.Repeat([]byte("x"), size)
}

func waitForPublicAttachment(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, key, name string) error {
	if cli == nil {
		return fmt.Errorf("client required")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		list, err := cli.ListAttachments(ctx, lockdclient.ListAttachmentsRequest{
			Namespace: cfg.namespace,
			Key:       key,
			Public:    true,
		})
		if err == nil && list != nil {
			for _, att := range list.Attachments {
				if strings.EqualFold(att.Name, name) {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return err
			}
			return fmt.Errorf("public attachment %s not visible", name)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitForIndex(ctx context.Context, cli *lockdclient.Client, cfg benchConfig) error {
	if cli == nil {
		return nil
	}
	_, err := cli.FlushIndex(ctx, cfg.namespace, lockdclient.WithFlushModeWait())
	return err
}

func withQueryReturn(mode string) lockdclient.QueryOption {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "keys":
		return lockdclient.WithQueryReturnKeys()
	default:
		return lockdclient.WithQueryReturnDocuments()
	}
}

func opLock(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, key benchKey, payload []byte) opResult {
	start := time.Now()
	acqStart := time.Now()
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  cfg.namespace,
		Key:        key.key,
		Owner:      "bench",
		TTLSeconds: 30,
	})
	acqDur := time.Since(acqStart)
	if err != nil {
		return opResult{total: time.Since(start), err: err, errPhase: "lock", phases: map[string]time.Duration{"lock": acqDur}}
	}
	updStart := time.Now()
	_, err = lease.UpdateBytes(ctx, payload)
	updDur := time.Since(updStart)
	if err != nil {
		_ = lease.Release(ctx)
		return opResult{total: time.Since(start), err: err, errPhase: "lock", phases: map[string]time.Duration{"lock": acqDur + updDur}}
	}
	relStart := time.Now()
	err = lease.Release(ctx)
	relDur := time.Since(relStart)
	if err != nil {
		return opResult{total: time.Since(start), err: err, errPhase: "lock", phases: map[string]time.Duration{"lock": acqDur + updDur + relDur}}
	}
	return opResult{total: time.Since(start), phases: map[string]time.Duration{"lock": acqDur + updDur + relDur}}
}

func opRead(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, key benchKey) opResult {
	start := time.Now()
	resp, err := cli.Get(ctx, key.key, lockdclient.WithGetNamespace(cfg.namespace))
	if err != nil {
		return opResult{total: time.Since(start), err: err, errPhase: "read", phases: map[string]time.Duration{"read": time.Since(start)}}
	}
	if resp != nil {
		reader := resp.Reader()
		if reader != nil {
			_, _ = io.Copy(io.Discard, reader)
			_ = reader.Close()
		}
		_ = resp.Close()
	}
	return opResult{total: time.Since(start), phases: map[string]time.Duration{"read": time.Since(start)}}
}

func opAttach(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, key benchKey, payload []byte) opResult {
	start := time.Now()
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  cfg.namespace,
		Key:        key.key,
		Owner:      "bench",
		TTLSeconds: 30,
	})
	if err != nil {
		return opResult{total: time.Since(start), err: err, errPhase: "attach", phases: map[string]time.Duration{"attach": time.Since(start)}}
	}
	name := fmt.Sprintf("bench-%d", key.idx)
	_, err = lease.Attach(ctx, lockdclient.AttachRequest{
		Name:        name,
		Body:        bytes.NewReader(payload),
		ContentType: "application/octet-stream",
	})
	if err != nil {
		_ = lease.Release(ctx)
		return opResult{total: time.Since(start), err: err, errPhase: "attach", phases: map[string]time.Duration{"attach": time.Since(start)}}
	}
	attachment, err := lease.RetrieveAttachment(ctx, lockdclient.AttachmentSelector{Name: name})
	if err == nil && attachment != nil {
		_, _ = io.Copy(io.Discard, attachment)
		_ = attachment.Close()
	}
	_, err = lease.DeleteAttachment(ctx, lockdclient.AttachmentSelector{Name: name})
	if err != nil {
		_ = lease.Release(ctx)
		return opResult{total: time.Since(start), err: err, errPhase: "attach", phases: map[string]time.Duration{"attach": time.Since(start)}}
	}
	if err := lease.Release(ctx); err != nil {
		return opResult{total: time.Since(start), err: err, errPhase: "attach", phases: map[string]time.Duration{"attach": time.Since(start)}}
	}
	return opResult{total: time.Since(start), phases: map[string]time.Duration{"attach": time.Since(start)}}
}

func opQueue(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, payload []byte) opResult {
	start := time.Now()
	_, err := cli.Enqueue(ctx, cfg.queueName, bytes.NewReader(payload), lockdclient.EnqueueOptions{
		Namespace:   cfg.namespace,
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return opResult{total: time.Since(start), err: err, errPhase: "queue", phases: map[string]time.Duration{"queue": time.Since(start)}}
	}
	msg, err := cli.Dequeue(ctx, cfg.queueName, lockdclient.DequeueOptions{
		Namespace:    cfg.namespace,
		Owner:        "bench",
		BlockSeconds: lockdclient.BlockNoWait,
		PageSize:     1,
	})
	if err != nil {
		return opResult{total: time.Since(start), err: err, errPhase: "queue", phases: map[string]time.Duration{"queue": time.Since(start)}}
	}
	if msg != nil {
		_, _ = msg.WritePayloadTo(io.Discard)
		if err := msg.Ack(ctx); err != nil {
			return opResult{total: time.Since(start), err: err, errPhase: "queue", phases: map[string]time.Duration{"queue": time.Since(start)}}
		}
	}
	return opResult{total: time.Since(start), phases: map[string]time.Duration{"queue": time.Since(start)}}
}

func opQuery(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, engine string) opResult {
	start := time.Now()
	resp, err := cli.Query(ctx,
		lockdclient.WithQueryNamespace(cfg.namespace),
		lockdclient.WithQuery(cfg.queryExpr),
		lockdclient.WithQueryLimit(cfg.queryLimit),
		lockdclient.WithQueryEngine(engine),
		withQueryReturn(cfg.queryReturn),
	)
	if err != nil {
		return opResult{total: time.Since(start), err: err, errPhase: "query", phases: map[string]time.Duration{"query": time.Since(start)}}
	}
	if resp != nil {
		_ = resp.ForEach(func(row lockdclient.QueryRow) error {
			if row.HasDocument() {
				reader, err := row.DocumentReader()
				if err == nil && reader != nil {
					_, _ = io.Copy(io.Discard, reader)
					_ = reader.Close()
				}
			}
			return nil
		})
		_ = resp.Close()
	}
	return opResult{total: time.Since(start), phases: map[string]time.Duration{"query": time.Since(start)}}
}

type mixSelector struct {
	entries []mixEntry
	total   int
}

type mixEntry struct {
	name   string
	weight int
}

func parseMix(raw string) (*mixSelector, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("workload mix empty")
	}
	parts := strings.Split(raw, ",")
	selector := &mixSelector{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid mix entry %q", part)
		}
		name := strings.TrimSpace(kv[0])
		if name == "" {
			return nil, fmt.Errorf("invalid mix entry %q", part)
		}
		weight, err := parsePositiveInt(kv[1])
		if err != nil {
			return nil, fmt.Errorf("invalid mix weight %q: %w", part, err)
		}
		selector.entries = append(selector.entries, mixEntry{name: name, weight: weight})
		selector.total += weight
	}
	if selector.total == 0 {
		return nil, fmt.Errorf("mix weights must be > 0")
	}
	return selector, nil
}

func (m *mixSelector) pick() string {
	if m == nil || len(m.entries) == 0 {
		return ""
	}
	n := rand.Intn(m.total)
	acc := 0
	for _, entry := range m.entries {
		acc += entry.weight
		if n < acc {
			return entry.name
		}
	}
	return m.entries[len(m.entries)-1].name
}

func (m *mixSelector) names() []string {
	if m == nil {
		return nil
	}
	names := make([]string, 0, len(m.entries))
	for _, entry := range m.entries {
		names = append(names, entry.name)
	}
	sort.Strings(names)
	return names
}

func parsePositiveInt(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty")
	}
	var n int
	_, err := fmt.Sscanf(raw, "%d", &n)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("expected positive integer")
	}
	return n, nil
}
