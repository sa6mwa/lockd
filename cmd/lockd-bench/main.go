package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/kryptograf"
	lockd "pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/core"
	"pkt.systems/lockd/internal/jsonutil"
	indexer "pkt.systems/lockd/internal/search/index"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/disk"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"
)

type benchConfig struct {
	mode             string
	ops              int
	concurrency      int
	payloadBytes     int
	root             string
	keepRoot         bool
	logLevel         string
	logPath          string
	enableCrypto     bool
	enableSnappy     bool
	haMode           string
	haLeaseTTL       time.Duration
	commitMaxOps     int
	segmentSize      int64
	gomaxprocs       int
	cpuProfile       string
	memProfile       string
	warmupRuns       int
	runs             int
	backend          string
	envFile          string
	storeURL         string
	workload         string
	workloadMix      string
	endpoint         string
	disableMTLS      bool
	clientBundle     string
	namespace        string
	queueName        string
	keyCount         int
	attachBytes      int
	queueBytes       int
	queryExpr        string
	queryLimit       int
	queryReturn      string
	querySeed        bool
	queryFlush       bool
	qrfDisabled      bool
	qrfCPUSoft       float64
	qrfCPUHard       float64
	qrfMemSoftPct    float64
	qrfMemHardPct    float64
	qrfMemHeadroom   float64
	qrfMemSoftBytes  uint64
	qrfMemHardBytes  uint64
	qrfSwapSoft      float64
	qrfSwapHard      float64
	qrfSoftDelay     time.Duration
	qrfEngagedDelay  time.Duration
	qrfRecoveryDelay time.Duration
	qrfMaxWait       time.Duration
	queryDocPrefetch int
	httpTimeout      time.Duration
	keepNamespace    bool
	namespaceSet     bool
}

type stringFlag struct {
	value string
	set   bool
}

func (s *stringFlag) String() string {
	return s.value
}

func (s *stringFlag) Set(v string) error {
	s.value = v
	s.set = true
	return nil
}

type benchSummary struct {
	count int
	avg   time.Duration
	min   time.Duration
	max   time.Duration
	p50   time.Duration
	p90   time.Duration
	p95   time.Duration
	p99   time.Duration
	p999  time.Duration
}

func main() {
	cfg := benchConfig{
		mode:             "load",
		ops:              10000,
		concurrency:      8,
		payloadBytes:     1000,
		enableCrypto:     true,
		enableSnappy:     false,
		logLevel:         "error",
		haMode:           "concurrent",
		haLeaseTTL:       5 * time.Second,
		commitMaxOps:     1024,
		segmentSize:      0,
		warmupRuns:       1,
		runs:             3,
		backend:          "disk",
		namespace:        "default",
		queueName:        "bench",
		attachBytes:      1024,
		queueBytes:       512,
		queryExpr:        "eq{field=/status,value=open}",
		queryLimit:       50,
		queryReturn:      "documents",
		querySeed:        true,
		queryFlush:       true,
		workloadMix:      "lock=30,read=20,attach=15,queue=15,query-index=10,query-scan=10",
		queryDocPrefetch: lockd.DefaultQueryDocPrefetch,
		qrfCPUSoft:       lockd.DefaultQRFCPUPercentSoftLimit,
		qrfCPUHard:       lockd.DefaultQRFCPUPercentHardLimit,
		qrfMemSoftPct:    75.0,
		qrfMemHardPct:    85.0,
		qrfMemHeadroom:   lockd.DefaultQRFMemoryStrictHeadroomPercent,
		qrfSwapSoft:      lockd.DefaultQRFSwapSoftLimitPercent,
		qrfSwapHard:      lockd.DefaultQRFSwapHardLimitPercent,
		qrfSoftDelay:     lockd.DefaultQRFSoftDelay,
		qrfEngagedDelay:  lockd.DefaultQRFEngagedDelay,
		qrfRecoveryDelay: lockd.DefaultQRFRecoveryDelay,
		qrfMaxWait:       lockd.DefaultQRFMaxWait,
		httpTimeout:      lockdclient.DefaultHTTPTimeout,
	}
	flag.StringVar(&cfg.mode, "mode", cfg.mode, "bench mode: load, update, acquire")
	flag.IntVar(&cfg.ops, "ops", cfg.ops, "number of operations to run")
	flag.IntVar(&cfg.concurrency, "concurrency", cfg.concurrency, "number of concurrent workers")
	flag.IntVar(&cfg.payloadBytes, "payload-bytes", cfg.payloadBytes, "payload size in bytes (total across YCSB-style fields for load/update)")
	flag.StringVar(&cfg.root, "root", "", "storage root (empty creates a temp directory)")
	flag.BoolVar(&cfg.keepRoot, "keep-root", false, "keep the storage root after completion")
	flag.StringVar(&cfg.logLevel, "log-level", cfg.logLevel, "log level (trace,debug,info,warn,error,disabled)")
	flag.StringVar(&cfg.logPath, "log-path", cfg.logPath, "log output path (default stderr)")
	flag.BoolVar(&cfg.enableCrypto, "crypto", cfg.enableCrypto, "enable storage encryption")
	flag.BoolVar(&cfg.enableSnappy, "snappy", cfg.enableSnappy, "enable snappy compression for crypto")
	flag.StringVar(&cfg.haMode, "ha", cfg.haMode, "HA mode (concurrent or failover)")
	flag.DurationVar(&cfg.haLeaseTTL, "ha-lease-ttl", cfg.haLeaseTTL, "HA lease TTL (failover mode only)")
	flag.IntVar(&cfg.commitMaxOps, "commit-max-ops", cfg.commitMaxOps, "max ops per logstore fsync batch")
	flag.Int64Var(&cfg.segmentSize, "segment-size", cfg.segmentSize, "logstore segment size (0 uses default)")
	flag.IntVar(&cfg.gomaxprocs, "gomaxprocs", 0, "override GOMAXPROCS (0 uses default)")
	flag.StringVar(&cfg.cpuProfile, "cpuprofile", "", "write CPU profile to file")
	flag.StringVar(&cfg.memProfile, "memprofile", "", "write heap profile to file")
	flag.IntVar(&cfg.warmupRuns, "warmup", cfg.warmupRuns, "number of warmup runs (excluded from summary)")
	flag.IntVar(&cfg.runs, "runs", cfg.runs, "number of measured runs (summary is median)")
	flag.StringVar(&cfg.backend, "backend", cfg.backend, "storage backend (disk, minio, aws, azure, mem)")
	flag.StringVar(&cfg.envFile, "env-file", "", "env file to load (defaults to .env.minio for minio backend)")
	flag.StringVar(&cfg.storeURL, "store", "", "storage backend URL override (defaults to LOCKD_STORE)")
	flag.StringVar(&cfg.workload, "workload", "", "workload to run (lock, attachments, attachments-public, queue, public-read, query-index, query-scan, xa-commit, xa-rollback, mixed)")
	flag.StringVar(&cfg.workloadMix, "workload-mix", cfg.workloadMix, "mixed workload weights (e.g. lock=30,read=20,attach=10,queue=10,query-index=20,query-scan=10)")
	flag.StringVar(&cfg.endpoint, "endpoint", "", "lockd endpoint (when set, uses existing server instead of in-process)")
	flag.BoolVar(&cfg.disableMTLS, "disable-mtls", false, "disable mTLS expectations for client endpoints")
	flag.StringVar(&cfg.clientBundle, "bundle", "", "path to client bundle PEM (default auto-discover under $HOME/.lockd)")
	nsFlag := &stringFlag{value: cfg.namespace}
	flag.Var(nsFlag, "namespace", "namespace used for workload keys (auto-generated when omitted)")
	flag.BoolVar(&cfg.keepNamespace, "keep-namespace", false, "keep namespace data after benchmark runs (embedded server only)")
	flag.StringVar(&cfg.queueName, "queue", cfg.queueName, "queue name for queue workloads")
	flag.IntVar(&cfg.keyCount, "key-count", 0, "number of keys to pre-seed for read/query workloads (0 uses ops)")
	flag.IntVar(&cfg.attachBytes, "attachment-bytes", cfg.attachBytes, "attachment payload size in bytes")
	flag.IntVar(&cfg.queueBytes, "queue-bytes", cfg.queueBytes, "queue payload size in bytes")
	flag.StringVar(&cfg.queryExpr, "query-expr", cfg.queryExpr, "query selector expression")
	flag.IntVar(&cfg.queryLimit, "query-limit", cfg.queryLimit, "query page size")
	flag.StringVar(&cfg.queryReturn, "query-return", cfg.queryReturn, "query return mode (keys or documents)")
	flag.BoolVar(&cfg.querySeed, "query-seed", cfg.querySeed, "seed data before query workloads")
	flag.BoolVar(&cfg.queryFlush, "query-flush", cfg.queryFlush, "flush index before query workloads")
	flag.IntVar(&cfg.queryDocPrefetch, "query-doc-prefetch", cfg.queryDocPrefetch, "prefetch depth for query return=documents (1 disables parallelism)")
	flag.BoolVar(&cfg.qrfDisabled, "qrf-disabled", false, "disable QRF pacing in embedded server")
	flag.Float64Var(&cfg.qrfCPUSoft, "qrf-cpu-soft-limit", cfg.qrfCPUSoft, "QRF CPU soft limit percent (embedded server only)")
	flag.Float64Var(&cfg.qrfCPUHard, "qrf-cpu-hard-limit", cfg.qrfCPUHard, "QRF CPU hard limit percent (embedded server only)")
	flag.Float64Var(&cfg.qrfMemSoftPct, "qrf-memory-soft-limit-percent", cfg.qrfMemSoftPct, "QRF memory soft limit percent (embedded server only)")
	flag.Float64Var(&cfg.qrfMemHardPct, "qrf-memory-hard-limit-percent", cfg.qrfMemHardPct, "QRF memory hard limit percent (embedded server only)")
	flag.Float64Var(&cfg.qrfMemHeadroom, "qrf-memory-strict-headroom-percent", cfg.qrfMemHeadroom, "QRF memory headroom percent when reclaimable cache is unknown (embedded server only)")
	flag.Uint64Var(&cfg.qrfMemSoftBytes, "qrf-memory-soft-limit-bytes", cfg.qrfMemSoftBytes, "QRF memory soft limit bytes for process RSS (embedded server only, 0 disables)")
	flag.Uint64Var(&cfg.qrfMemHardBytes, "qrf-memory-hard-limit-bytes", cfg.qrfMemHardBytes, "QRF memory hard limit bytes for process RSS (embedded server only, 0 disables)")
	flag.Float64Var(&cfg.qrfSwapSoft, "qrf-swap-soft-limit-percent", cfg.qrfSwapSoft, "QRF swap soft limit percent (embedded server only)")
	flag.Float64Var(&cfg.qrfSwapHard, "qrf-swap-hard-limit-percent", cfg.qrfSwapHard, "QRF swap hard limit percent (embedded server only)")
	flag.DurationVar(&cfg.qrfSoftDelay, "qrf-soft-delay", cfg.qrfSoftDelay, "QRF soft-arm delay (embedded server only)")
	flag.DurationVar(&cfg.qrfEngagedDelay, "qrf-engaged-delay", cfg.qrfEngagedDelay, "QRF engaged delay (embedded server only)")
	flag.DurationVar(&cfg.qrfRecoveryDelay, "qrf-recovery-delay", cfg.qrfRecoveryDelay, "QRF recovery delay (embedded server only)")
	flag.DurationVar(&cfg.qrfMaxWait, "qrf-max-wait", cfg.qrfMaxWait, "QRF max wait before throttling (embedded server only)")
	flag.DurationVar(&cfg.httpTimeout, "http-timeout", cfg.httpTimeout, "HTTP client timeout (0 uses client default)")
	flag.Parse()
	cfg.namespace = nsFlag.value
	cfg.namespaceSet = nsFlag.set

	if cfg.ops <= 0 {
		die("ops must be > 0")
	}
	if cfg.concurrency <= 0 {
		die("concurrency must be > 0")
	}
	if cfg.runs <= 0 {
		die("runs must be > 0")
	}
	if cfg.warmupRuns < 0 {
		die("warmup must be >= 0")
	}
	cfg.backend = strings.ToLower(strings.TrimSpace(cfg.backend))
	if cfg.backend == "" {
		cfg.backend = "disk"
	}
	switch cfg.backend {
	case "disk", "minio", "aws", "azure", "mem":
	default:
		die("unsupported backend %q (expected disk, minio, aws, azure, or mem)", cfg.backend)
	}
	if strings.TrimSpace(cfg.envFile) == "" {
		switch cfg.backend {
		case "minio":
			cfg.envFile = ".env.minio"
		case "aws":
			cfg.envFile = ".env.aws"
		case "azure":
			cfg.envFile = ".env.azure"
		}
	}
	if cfg.envFile != "" {
		if cfg.backend == "minio" || cfg.backend == "aws" || cfg.backend == "azure" {
			if _, err := os.Stat(cfg.envFile); err != nil {
				if os.IsNotExist(err) {
					die("%s backend requires -env-file or %s (missing)", cfg.backend, cfg.envFile)
				}
				die("%s env-file: %v", cfg.backend, err)
			}
		}
		if err := loadEnvFile(cfg.envFile); err != nil {
			die("load env file: %v", err)
		}
	}
	normalizeBenchEnv()
	if cfg.workload != "" {
		runWorkloadBench(cfg)
		return
	}
	logger, closeLogger := newBenchLogger(cfg)
	defer closeLogger()
	if cfg.gomaxprocs > 0 {
		runtime.GOMAXPROCS(cfg.gomaxprocs)
	}
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

	root, cleanup := prepareRoot(cfg.root, cfg.keepRoot)
	defer cleanup()

	if strings.TrimSpace(cfg.endpoint) == "" && strings.EqualFold(cfg.backend, "disk") && strings.EqualFold(cfg.haMode, "concurrent") {
		die("ha=concurrent is not supported for disk/nfs backends; use ha=failover")
	}

	ctx := context.Background()
	payload := buildPayload(cfg.payloadBytes)

	if strings.TrimSpace(cfg.endpoint) != "" {
		cli, err := newBenchClient(cfg, logger)
		if err != nil {
			die("endpoint client: %v", err)
		}
		defer cli.Close()

		runID := time.Now().UTC().Format("20060102T150405.000000000Z")
		totalRuns := cfg.warmupRuns + cfg.runs
		results := make([]benchRun, 0, cfg.runs)
		namespacesUsed := make(map[string]struct{})

		switch cfg.mode {
		case "load":
			for i := 0; i < totalRuns; i++ {
				warmup := i < cfg.warmupRuns
				runLabel := fmt.Sprintf("run=%d/%d warmup=%t", i+1, totalRuns, warmup)
				prefix := fmt.Sprintf("bench/%s/run-%d", runID, i)
				runCfg := cfg
				runCfg.namespace = runNamespace(cfg, runID, i)
				namespacesUsed[runCfg.namespace] = struct{}{}
				run := runBenchOnceClient(ctx, cli, payload, runCfg, newKeyGeneratorWithPrefix(prefix))
				printBenchRun(runCfg, runLabel, run, true)
				if !warmup {
					results = append(results, run)
				}
			}
		case "update":
			for i := 0; i < totalRuns; i++ {
				warmup := i < cfg.warmupRuns
				runLabel := fmt.Sprintf("run=%d/%d warmup=%t", i+1, totalRuns, warmup)
				prefix := fmt.Sprintf("bench/%s/run-%d", runID, i)
				runCfg := cfg
				runCfg.namespace = runNamespace(cfg, runID, i)
				namespacesUsed[runCfg.namespace] = struct{}{}
				keys := seedKeysClient(ctx, cli, payload, runCfg, prefix)
				run := runBenchOnceClient(ctx, cli, payload, runCfg, reuseKeyGenerator(keys))
				printBenchRun(runCfg, runLabel, run, true)
				if !warmup {
					results = append(results, run)
				}
			}
		case "acquire":
			for i := 0; i < totalRuns; i++ {
				warmup := i < cfg.warmupRuns
				runLabel := fmt.Sprintf("run=%d/%d warmup=%t", i+1, totalRuns, warmup)
				prefix := fmt.Sprintf("bench/%s/run-%d", runID, i)
				runCfg := cfg
				runCfg.namespace = runNamespace(cfg, runID, i)
				namespacesUsed[runCfg.namespace] = struct{}{}
				run := runAcquireBenchOnceClient(ctx, cli, runCfg, newKeyGeneratorWithPrefix(prefix))
				printBenchRun(runCfg, runLabel, run, false)
				if !warmup {
					results = append(results, run)
				}
			}
		default:
			die("unsupported mode %q", cfg.mode)
		}

		if len(results) > 1 {
			fmt.Printf("bench summary: mode=%s runs=%d warmup=%d aggregation=median\n", cfg.mode, cfg.runs, cfg.warmupRuns)
			printBenchSummary(cfg, results)
		}
		if strings.EqualFold(cfg.haMode, "failover") {
			fmt.Printf("ha stats: refreshes=0 errors=0 transitions=0\n")
		}
		return
	}

	crypto, err := maybeCrypto(cfg.enableCrypto, cfg.enableSnappy)
	if err != nil {
		die("crypto init: %v", err)
	}
	store, indexStore := openBenchStore(cfg, crypto, root)
	flushDocs, flushInterval := indexerDefaults(store)
	indexManager := indexer.NewManager(indexStore, indexer.WriterOptions{
		FlushDocs:     flushDocs,
		FlushInterval: flushInterval,
		Logger:        logger,
	})

	svc := core.New(core.Config{
		Store:            store,
		Crypto:           crypto,
		HAMode:           cfg.haMode,
		HALeaseTTL:       cfg.haLeaseTTL,
		DefaultNamespace: "default",
		DefaultTTL:       30 * time.Second,
		IndexManager:     indexManager,
		StateCacheBytes:  lockd.DefaultStateCacheBytes,
		Logger:           logger,
	})
	if strings.EqualFold(cfg.haMode, "failover") {
		if err := waitForNodeActive(ctx, svc, cfg.haLeaseTTL); err != nil {
			die("ha node active: %v", err)
		}
	}

	runID := time.Now().UTC().Format("20060102T150405.000000000Z")
	totalRuns := cfg.warmupRuns + cfg.runs
	results := make([]benchRun, 0, cfg.runs)
	namespacesUsed := make(map[string]struct{})
	switch cfg.mode {
	case "load":
		for i := 0; i < totalRuns; i++ {
			warmup := i < cfg.warmupRuns
			runLabel := fmt.Sprintf("run=%d/%d warmup=%t", i+1, totalRuns, warmup)
			prefix := fmt.Sprintf("bench/%s/run-%d", runID, i)
			runCfg := cfg
			runCfg.namespace = runNamespace(cfg, runID, i)
			namespacesUsed[runCfg.namespace] = struct{}{}
			statsBefore := fsyncStats(store)
			run := runBenchOnce(ctx, svc, payload, runCfg, newKeyGeneratorWithPrefix(prefix))
			statsAfter := fsyncStats(store)
			printBenchRun(runCfg, runLabel, run, true)
			printFsyncStatsFrom(diffFsyncStats(statsBefore, statsAfter))
			if !warmup {
				results = append(results, run)
			}
		}
	case "update":
		for i := 0; i < totalRuns; i++ {
			warmup := i < cfg.warmupRuns
			runLabel := fmt.Sprintf("run=%d/%d warmup=%t", i+1, totalRuns, warmup)
			prefix := fmt.Sprintf("bench/%s/run-%d", runID, i)
			runCfg := cfg
			runCfg.namespace = runNamespace(cfg, runID, i)
			namespacesUsed[runCfg.namespace] = struct{}{}
			keys := seedKeys(ctx, svc, payload, runCfg, prefix)
			statsBefore := fsyncStats(store)
			run := runBenchOnce(ctx, svc, payload, runCfg, reuseKeyGenerator(keys))
			statsAfter := fsyncStats(store)
			printBenchRun(runCfg, runLabel, run, true)
			printFsyncStatsFrom(diffFsyncStats(statsBefore, statsAfter))
			if !warmup {
				results = append(results, run)
			}
		}
	case "acquire":
		for i := 0; i < totalRuns; i++ {
			warmup := i < cfg.warmupRuns
			runLabel := fmt.Sprintf("run=%d/%d warmup=%t", i+1, totalRuns, warmup)
			prefix := fmt.Sprintf("bench/%s/run-%d", runID, i)
			runCfg := cfg
			runCfg.namespace = runNamespace(cfg, runID, i)
			namespacesUsed[runCfg.namespace] = struct{}{}
			statsBefore := fsyncStats(store)
			run := runAcquireBenchOnce(ctx, svc, runCfg, newKeyGeneratorWithPrefix(prefix))
			statsAfter := fsyncStats(store)
			printBenchRun(runCfg, runLabel, run, false)
			printFsyncStatsFrom(diffFsyncStats(statsBefore, statsAfter))
			if !warmup {
				results = append(results, run)
			}
		}
	default:
		die("unsupported mode %q", cfg.mode)
	}
	if len(results) > 1 {
		fmt.Printf("bench summary: mode=%s runs=%d warmup=%d aggregation=median\n", cfg.mode, cfg.runs, cfg.warmupRuns)
		printBenchSummary(cfg, results)
	}
	indexManager.Close(ctx)
	if !cfg.keepNamespace {
		if shouldSkipDiskCleanup(cfg, root) {
			fmt.Printf("bench cleanup: skipped (temp root)\n")
		} else {
			for ns := range namespacesUsed {
				stats, err := cleanupNamespace(ctx, store, ns)
				if err != nil {
					die("cleanup namespace %s: %v", ns, err)
				}
				fmt.Printf("bench cleanup: namespace=%s objects=%d meta=%d\n", ns, stats.objects, stats.meta)
			}
		}
	}
	if strings.EqualFold(cfg.haMode, "failover") {
		stats := svc.HAStats()
		fmt.Printf("ha stats: refreshes=%d errors=%d transitions=%d\n", stats.Refreshes, stats.Errors, stats.Transitions)
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

func prepareRoot(root string, keep bool) (string, func()) {
	if strings.TrimSpace(root) == "" {
		dir := createBenchTempDir("root-")
		return dir, func() {
			if keep {
				fmt.Printf("bench root: %s (kept)\n", dir)
				return
			}
			_ = os.RemoveAll(dir)
		}
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		die("root path: %v", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		die("root mkdir: %v", err)
	}
	return abs, func() {
		if keep {
			fmt.Printf("bench root: %s (kept)\n", abs)
		}
	}
}

func benchTempBase() string {
	return filepath.Join(os.TempDir(), "lockd-bench")
}

func shouldSkipDiskCleanup(cfg benchConfig, root string) bool {
	if cfg.keepRoot {
		return false
	}
	if !strings.EqualFold(cfg.backend, "disk") {
		return false
	}
	return isBenchTempRoot(root)
}

func isBenchTempRoot(root string) bool {
	root = strings.TrimSpace(root)
	if root == "" {
		return false
	}
	base, err := filepath.Abs(benchTempBase())
	if err != nil {
		return false
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	base = filepath.Clean(base)
	absRoot = filepath.Clean(absRoot)
	if absRoot == base {
		return true
	}
	if !strings.HasSuffix(base, string(os.PathSeparator)) {
		base += string(os.PathSeparator)
	}
	return strings.HasPrefix(absRoot, base)
}

func createBenchTempDir(prefix string) string {
	base := benchTempBase()
	if err := os.MkdirAll(base, 0o755); err != nil {
		die("temp dir: %v", err)
	}
	dir, err := os.MkdirTemp(base, prefix)
	if err != nil {
		die("temp dir: %v", err)
	}
	return dir
}

func runNamespace(cfg benchConfig, runID string, idx int) string {
	if cfg.namespaceSet {
		return cfg.namespace
	}
	base := strings.ToLower(runID)
	return fmt.Sprintf("bench-%s-%d", base, idx)
}

type cleanupStats struct {
	objects int
	meta    int
}

func cleanupNamespace(ctx context.Context, backend storage.Backend, namespace string) (cleanupStats, error) {
	stats := cleanupStats{}
	if backend == nil {
		return stats, nil
	}
	startAfter := ""
	for {
		list, err := backend.ListObjects(ctx, namespace, storage.ListOptions{
			StartAfter: startAfter,
			Limit:      1000,
		})
		if err != nil {
			return stats, err
		}
		for _, obj := range list.Objects {
			if err := backend.DeleteObject(ctx, namespace, obj.Key, storage.DeleteObjectOptions{IgnoreNotFound: true}); err != nil && !errors.Is(err, storage.ErrNotFound) {
				return stats, err
			}
			stats.objects++
			startAfter = obj.Key
		}
		if !list.Truncated {
			break
		}
		if list.NextStartAfter != "" {
			startAfter = list.NextStartAfter
		}
	}
	metaKeys, err := backend.ListMetaKeys(ctx, namespace)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return stats, err
	}
	for _, key := range metaKeys {
		if err := backend.Remove(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return stats, err
		}
		if err := backend.DeleteMeta(ctx, namespace, key, ""); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return stats, err
		}
		stats.meta++
	}
	return stats, nil
}

func newBenchLogger(cfg benchConfig) (pslog.Logger, func()) {
	levelStr := strings.TrimSpace(cfg.logLevel)
	if levelStr == "" {
		return pslog.NoopLogger(), func() {}
	}
	level, ok := pslog.ParseLevel(levelStr)
	if !ok {
		die("log-level: invalid value %q", levelStr)
	}
	if level == pslog.Disabled || level == pslog.NoLevel {
		return pslog.NoopLogger(), func() {}
	}
	var (
		writer  = os.Stderr
		cleanup = func() {}
	)
	if strings.TrimSpace(cfg.logPath) != "" {
		path, err := filepath.Abs(cfg.logPath)
		if err != nil {
			die("log-path: %v", err)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			die("log-path mkdir: %v", err)
		}
		f, err := os.Create(path)
		if err != nil {
			die("log-path create: %v", err)
		}
		writer = f
		cleanup = func() { _ = f.Close() }
	}
	logger := pslog.NewStructured(writer).LogLevel(level)
	return logger, cleanup
}

func maybeCrypto(enable bool, snappy bool) (*storage.Crypto, error) {
	if !enable {
		return nil, nil
	}
	root := kryptograf.MustGenerateRootKey()
	kg := kryptograf.New(root)
	mat, err := kg.MintDEK([]byte("bench-meta-context"))
	if err != nil {
		return nil, err
	}
	metaDescriptor := mat.Descriptor
	mat.Zero()
	return storage.NewCrypto(storage.CryptoConfig{
		Enabled:            true,
		RootKey:            root,
		MetadataDescriptor: metaDescriptor,
		MetadataContext:    []byte("bench-meta-context"),
		Snappy:             snappy,
	})
}

func waitForNodeActive(ctx context.Context, svc *core.Service, ttl time.Duration) error {
	if svc == nil {
		return fmt.Errorf("nil service")
	}
	timeout := ttl
	if timeout <= 0 || timeout > 2*time.Second {
		timeout = 2 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		if err := svc.RequireNodeActive(); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for HA node active")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

type keyGenerator interface {
	Next() (string, bool)
}

type sequentialKeys struct {
	seq    uint64
	limit  uint64
	prefix string
}

func newKeyGeneratorWithPrefix(prefix string) *sequentialKeys {
	return &sequentialKeys{prefix: prefix}
}

func (k *sequentialKeys) Next() (string, bool) {
	next := atomic.AddUint64(&k.seq, 1) - 1
	if k.limit > 0 && next >= k.limit {
		return "", false
	}
	prefix := k.prefix
	if prefix == "" {
		prefix = "bench"
	}
	return fmt.Sprintf("%s/%d", prefix, next), true
}

type reuseKeys struct {
	keys []string
	seq  uint64
}

func reuseKeyGenerator(keys []string) *reuseKeys {
	return &reuseKeys{keys: keys}
}

func (k *reuseKeys) Next() (string, bool) {
	if len(k.keys) == 0 {
		return "", false
	}
	next := atomic.AddUint64(&k.seq, 1) - 1
	return k.keys[next%uint64(len(k.keys))], true
}

func seedKeys(ctx context.Context, svc *core.Service, payload []byte, cfg benchConfig, prefix string) []string {
	keys := make([]string, cfg.ops)
	for i := 0; i < cfg.ops; i++ {
		keyPrefix := prefix
		if keyPrefix == "" {
			keyPrefix = "bench"
		}
		key := fmt.Sprintf("%s/%d", keyPrefix, i)
		keys[i] = key
		runSingle(ctx, svc, cfg.namespace, key, payload)
	}
	return keys
}

func seedKeysClient(ctx context.Context, cli *lockdclient.Client, payload []byte, cfg benchConfig, prefix string) []string {
	keys := make([]string, cfg.ops)
	for i := 0; i < cfg.ops; i++ {
		keyPrefix := prefix
		if keyPrefix == "" {
			keyPrefix = "bench"
		}
		key := fmt.Sprintf("%s/%d", keyPrefix, i)
		keys[i] = key
		runSingleClient(ctx, cli, cfg.namespace, key, payload)
	}
	return keys
}

type benchStats struct {
	label     string
	ops       int
	opsPerSec float64
	avg       time.Duration
	min       time.Duration
	max       time.Duration
	p50       time.Duration
	p90       time.Duration
	p95       time.Duration
	p99       time.Duration
	p999      time.Duration
	errs      int64
}

type benchRun struct {
	elapsed       time.Duration
	total         benchStats
	acquire       benchStats
	update        benchStats
	release       benchStats
	errs          int64
	firstErr      error
	firstErrPhase string
}

func runBenchOnce(ctx context.Context, svc *core.Service, payload []byte, cfg benchConfig, keys keyGenerator) benchRun {
	var (
		totalLat      []time.Duration
		acqLat        []time.Duration
		updLat        []time.Duration
		relLat        []time.Duration
		errs          atomic.Int64
		acqErrs       atomic.Int64
		updErrs       atomic.Int64
		relErrs       atomic.Int64
		opsDone       atomic.Uint64
		mu            sync.Mutex
		firstErr      error
		firstErrPhase string
		errOnce       sync.Once
	)

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(cfg.concurrency)
	for w := 0; w < cfg.concurrency; w++ {
		go func() {
			defer wg.Done()
			localTotal := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			localAcq := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			localUpd := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			localRel := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			for {
				idx := opsDone.Add(1) - 1
				if int(idx) >= cfg.ops {
					break
				}
				key, ok := keys.Next()
				if !ok {
					errs.Add(1)
					break
				}
				t0 := time.Now()
				acqStart := time.Now()
				acq, err := svc.Acquire(ctx, core.AcquireCommand{
					Namespace:  cfg.namespace,
					Key:        key,
					Owner:      "bench",
					TTLSeconds: 30,
				})
				acqDur := time.Since(acqStart)
				if err != nil {
					errOnce.Do(func() {
						firstErr = err
						firstErrPhase = "acquire"
					})
					errs.Add(1)
					acqErrs.Add(1)
					continue
				}
				knownMeta := acq.Meta
				knownETag := acq.MetaETag
				updStart := time.Now()
				updRes, err := svc.Update(ctx, core.UpdateCommand{
					Namespace:      cfg.namespace,
					Key:            key,
					LeaseID:        acq.LeaseID,
					FencingToken:   acq.FencingToken,
					TxnID:          acq.TxnID,
					Body:           bytes.NewReader(payload),
					CompactWriter:  jsonutil.CompactWriter,
					SpoolThreshold: 4 << 20,
					KnownMeta:      knownMeta,
					KnownMetaETag:  knownETag,
				})
				updDur := time.Since(updStart)
				if err != nil {
					_, _ = svc.Release(ctx, core.ReleaseCommand{
						Namespace:    cfg.namespace,
						Key:          key,
						LeaseID:      acq.LeaseID,
						FencingToken: acq.FencingToken,
						TxnID:        acq.TxnID,
						Rollback:     true,
					})
					errOnce.Do(func() {
						firstErr = err
						firstErrPhase = "update"
					})
					errs.Add(1)
					updErrs.Add(1)
					continue
				}
				if updRes != nil {
					knownMeta = updRes.Meta
					knownETag = updRes.MetaETag
				}
				relStart := time.Now()
				_, err = svc.Release(ctx, core.ReleaseCommand{
					Namespace:     cfg.namespace,
					Key:           key,
					LeaseID:       acq.LeaseID,
					FencingToken:  acq.FencingToken,
					TxnID:         acq.TxnID,
					KnownMeta:     knownMeta,
					KnownMetaETag: knownETag,
				})
				relDur := time.Since(relStart)
				if err != nil {
					errOnce.Do(func() {
						firstErr = err
						firstErrPhase = "release"
					})
					errs.Add(1)
					relErrs.Add(1)
					continue
				}
				localTotal = append(localTotal, time.Since(t0))
				localAcq = append(localAcq, acqDur)
				localUpd = append(localUpd, updDur)
				localRel = append(localRel, relDur)
			}
			mu.Lock()
			totalLat = append(totalLat, localTotal...)
			acqLat = append(acqLat, localAcq...)
			updLat = append(updLat, localUpd...)
			relLat = append(relLat, localRel...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	totalStats := buildStats("total", elapsed, totalLat, errs.Load())
	acqStats := buildStats("acquire", elapsed, acqLat, acqErrs.Load())
	updStats := buildStats("update", elapsed, updLat, updErrs.Load())
	relStats := buildStats("release", elapsed, relLat, relErrs.Load())
	return benchRun{
		elapsed:       elapsed,
		total:         totalStats,
		acquire:       acqStats,
		update:        updStats,
		release:       relStats,
		errs:          errs.Load(),
		firstErr:      firstErr,
		firstErrPhase: firstErrPhase,
	}
}

func runBenchOnceClient(ctx context.Context, cli *lockdclient.Client, payload []byte, cfg benchConfig, keys keyGenerator) benchRun {
	var (
		totalLat      []time.Duration
		acqLat        []time.Duration
		updLat        []time.Duration
		relLat        []time.Duration
		errs          atomic.Int64
		acqErrs       atomic.Int64
		updErrs       atomic.Int64
		relErrs       atomic.Int64
		opsDone       atomic.Uint64
		mu            sync.Mutex
		firstErr      error
		firstErrPhase string
		errOnce       sync.Once
	)

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(cfg.concurrency)
	for w := 0; w < cfg.concurrency; w++ {
		go func() {
			defer wg.Done()
			localTotal := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			localAcq := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			localUpd := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			localRel := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			for {
				idx := opsDone.Add(1) - 1
				if int(idx) >= cfg.ops {
					break
				}
				key, ok := keys.Next()
				if !ok {
					errs.Add(1)
					break
				}
				t0 := time.Now()
				acqStart := time.Now()
				acq, err := cli.Acquire(ctx, api.AcquireRequest{
					Namespace:  cfg.namespace,
					Key:        key,
					Owner:      "bench",
					TTLSeconds: 30,
				})
				acqDur := time.Since(acqStart)
				if err != nil {
					errOnce.Do(func() {
						firstErr = err
						firstErrPhase = "acquire"
					})
					errs.Add(1)
					acqErrs.Add(1)
					continue
				}
				updStart := time.Now()
				if _, err := acq.UpdateBytes(ctx, payload); err != nil {
					_ = acq.ReleaseWithOptions(ctx, lockdclient.ReleaseOptions{Rollback: true})
					errOnce.Do(func() {
						firstErr = err
						firstErrPhase = "update"
					})
					errs.Add(1)
					updErrs.Add(1)
					continue
				}
				updDur := time.Since(updStart)
				relStart := time.Now()
				if err := acq.Release(ctx); err != nil {
					errOnce.Do(func() {
						firstErr = err
						firstErrPhase = "release"
					})
					errs.Add(1)
					relErrs.Add(1)
					continue
				}
				relDur := time.Since(relStart)
				localTotal = append(localTotal, time.Since(t0))
				localAcq = append(localAcq, acqDur)
				localUpd = append(localUpd, updDur)
				localRel = append(localRel, relDur)
			}
			mu.Lock()
			totalLat = append(totalLat, localTotal...)
			acqLat = append(acqLat, localAcq...)
			updLat = append(updLat, localUpd...)
			relLat = append(relLat, localRel...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	totalStats := buildStats("total", elapsed, totalLat, errs.Load())
	acqStats := buildStats("acquire", elapsed, acqLat, acqErrs.Load())
	updStats := buildStats("update", elapsed, updLat, updErrs.Load())
	relStats := buildStats("release", elapsed, relLat, relErrs.Load())
	return benchRun{
		elapsed:       elapsed,
		total:         totalStats,
		acquire:       acqStats,
		update:        updStats,
		release:       relStats,
		errs:          errs.Load(),
		firstErr:      firstErr,
		firstErrPhase: firstErrPhase,
	}
}

func runAcquireBenchOnce(ctx context.Context, svc *core.Service, cfg benchConfig, keys keyGenerator) benchRun {
	var (
		totalLat      []time.Duration
		acqLat        []time.Duration
		relLat        []time.Duration
		errs          atomic.Int64
		acqErrs       atomic.Int64
		relErrs       atomic.Int64
		opsDone       atomic.Uint64
		mu            sync.Mutex
		firstErr      error
		firstErrPhase string
		errOnce       sync.Once
	)

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(cfg.concurrency)
	for w := 0; w < cfg.concurrency; w++ {
		go func() {
			defer wg.Done()
			localTotal := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			localAcq := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			localRel := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			for {
				idx := opsDone.Add(1) - 1
				if int(idx) >= cfg.ops {
					break
				}
				key, ok := keys.Next()
				if !ok {
					errs.Add(1)
					break
				}
				t0 := time.Now()
				acqStart := time.Now()
				acq, err := svc.Acquire(ctx, core.AcquireCommand{
					Namespace:  cfg.namespace,
					Key:        key,
					Owner:      "bench",
					TTLSeconds: 30,
				})
				acqDur := time.Since(acqStart)
				if err != nil {
					errOnce.Do(func() {
						firstErr = err
						firstErrPhase = "acquire"
					})
					errs.Add(1)
					acqErrs.Add(1)
					continue
				}
				knownMeta := acq.Meta
				knownETag := acq.MetaETag
				relStart := time.Now()
				_, err = svc.Release(ctx, core.ReleaseCommand{
					Namespace:     cfg.namespace,
					Key:           key,
					LeaseID:       acq.LeaseID,
					FencingToken:  acq.FencingToken,
					TxnID:         acq.TxnID,
					KnownMeta:     knownMeta,
					KnownMetaETag: knownETag,
				})
				relDur := time.Since(relStart)
				if err != nil {
					errOnce.Do(func() {
						firstErr = err
						firstErrPhase = "release"
					})
					errs.Add(1)
					relErrs.Add(1)
					continue
				}
				localTotal = append(localTotal, time.Since(t0))
				localAcq = append(localAcq, acqDur)
				localRel = append(localRel, relDur)
			}
			mu.Lock()
			totalLat = append(totalLat, localTotal...)
			acqLat = append(acqLat, localAcq...)
			relLat = append(relLat, localRel...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	totalStats := buildStats("total", elapsed, totalLat, errs.Load())
	acqStats := buildStats("acquire", elapsed, acqLat, acqErrs.Load())
	relStats := buildStats("release", elapsed, relLat, relErrs.Load())
	return benchRun{
		elapsed:       elapsed,
		total:         totalStats,
		acquire:       acqStats,
		release:       relStats,
		errs:          errs.Load(),
		firstErr:      firstErr,
		firstErrPhase: firstErrPhase,
	}
}

func runAcquireBenchOnceClient(ctx context.Context, cli *lockdclient.Client, cfg benchConfig, keys keyGenerator) benchRun {
	var (
		totalLat      []time.Duration
		acqLat        []time.Duration
		relLat        []time.Duration
		errs          atomic.Int64
		acqErrs       atomic.Int64
		relErrs       atomic.Int64
		opsDone       atomic.Uint64
		mu            sync.Mutex
		firstErr      error
		firstErrPhase string
		errOnce       sync.Once
	)

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(cfg.concurrency)
	for w := 0; w < cfg.concurrency; w++ {
		go func() {
			defer wg.Done()
			localTotal := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			localAcq := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			localRel := make([]time.Duration, 0, cfg.ops/cfg.concurrency+1)
			for {
				idx := opsDone.Add(1) - 1
				if int(idx) >= cfg.ops {
					break
				}
				key, ok := keys.Next()
				if !ok {
					errs.Add(1)
					break
				}
				t0 := time.Now()
				acqStart := time.Now()
				acq, err := cli.Acquire(ctx, api.AcquireRequest{
					Namespace:  cfg.namespace,
					Key:        key,
					Owner:      "bench",
					TTLSeconds: 30,
				})
				acqDur := time.Since(acqStart)
				if err != nil {
					errOnce.Do(func() {
						firstErr = err
						firstErrPhase = "acquire"
					})
					errs.Add(1)
					acqErrs.Add(1)
					continue
				}
				relStart := time.Now()
				if err := acq.Release(ctx); err != nil {
					errOnce.Do(func() {
						firstErr = err
						firstErrPhase = "release"
					})
					errs.Add(1)
					relErrs.Add(1)
					continue
				}
				relDur := time.Since(relStart)
				localTotal = append(localTotal, time.Since(t0))
				localAcq = append(localAcq, acqDur)
				localRel = append(localRel, relDur)
			}
			mu.Lock()
			totalLat = append(totalLat, localTotal...)
			acqLat = append(acqLat, localAcq...)
			relLat = append(relLat, localRel...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	totalStats := buildStats("total", elapsed, totalLat, errs.Load())
	acqStats := buildStats("acquire", elapsed, acqLat, acqErrs.Load())
	relStats := buildStats("release", elapsed, relLat, relErrs.Load())
	return benchRun{
		elapsed:       elapsed,
		total:         totalStats,
		acquire:       acqStats,
		release:       relStats,
		errs:          errs.Load(),
		firstErr:      firstErr,
		firstErrPhase: firstErrPhase,
	}
}

func buildStats(label string, elapsed time.Duration, samples []time.Duration, errs int64) benchStats {
	summary := summarize(samples)
	ops := summary.count
	opsPerSec := 0.0
	if elapsed > 0 {
		opsPerSec = float64(ops) / elapsed.Seconds()
	}
	return benchStats{
		label:     label,
		ops:       ops,
		opsPerSec: opsPerSec,
		avg:       summary.avg,
		min:       summary.min,
		max:       summary.max,
		p50:       summary.p50,
		p90:       summary.p90,
		p95:       summary.p95,
		p99:       summary.p99,
		p999:      summary.p999,
		errs:      errs,
	}
}

func printBenchRun(cfg benchConfig, runLabel string, run benchRun, includeUpdate bool) {
	fmt.Printf("bench mode=%s %s ops=%d concurrency=%d payload_bytes=%d ha=%s ha_lease_ttl=%s commit_max_ops=%d segment_size=%d crypto=%t snappy=%t\n",
		cfg.mode, runLabel, cfg.ops, cfg.concurrency, cfg.payloadBytes, cfg.haMode, cfg.haLeaseTTL, cfg.commitMaxOps, cfg.segmentSize, cfg.enableCrypto, cfg.enableSnappy)
	if run.firstErr != nil {
		fmt.Printf("first_error_phase=%s err=%v\n", run.firstErrPhase, run.firstErr)
	}
	printStats(run.total)
	printStats(run.acquire)
	if includeUpdate {
		printStats(run.update)
	}
	printStats(run.release)
}

func printBenchSummary(cfg benchConfig, runs []benchRun) {
	total := make([]benchStats, 0, len(runs))
	acq := make([]benchStats, 0, len(runs))
	upd := make([]benchStats, 0, len(runs))
	rel := make([]benchStats, 0, len(runs))
	for _, run := range runs {
		total = append(total, run.total)
		acq = append(acq, run.acquire)
		if run.update.ops > 0 || cfg.mode != "acquire" {
			upd = append(upd, run.update)
		}
		rel = append(rel, run.release)
	}
	printStats(medianStats("total", total))
	printStats(medianStats("acquire", acq))
	if cfg.mode != "acquire" {
		printStats(medianStats("update", upd))
	}
	printStats(medianStats("release", rel))
}

func printStats(stats benchStats) {
	fmt.Printf("%s: ops=%d ops/s=%.1f avg=%s p50=%s p90=%s p95=%s p99=%s p99.9=%s min=%s max=%s errors=%d\n",
		stats.label, stats.ops, stats.opsPerSec, stats.avg, stats.p50, stats.p90, stats.p95, stats.p99, stats.p999, stats.min, stats.max, stats.errs)
}

func printFsyncStatsFrom(stats disk.FsyncBatchStats) {
	if stats.TotalBatches == 0 {
		return
	}
	avgBatch := float64(stats.TotalRequests) / float64(stats.TotalBatches)
	avgSyncMs := 0.0
	maxSyncMs := 0.0
	if stats.TotalBatches > 0 {
		avgSyncMs = float64(stats.TotalSyncNs) / float64(stats.TotalBatches) / 1e6
	}
	if stats.MaxSyncNs > 0 {
		maxSyncMs = float64(stats.MaxSyncNs) / 1e6
	}
	p50 := batchPercentile(stats, 50)
	p90 := batchPercentile(stats, 90)
	p99 := batchPercentile(stats, 99)
	fmt.Printf("fsync: batches=%d requests=%d avg_batch=%.2f avg_sync_ms=%.2f max_sync_ms=%.2f p50=%d p90=%d p99=%d max=%d\n",
		stats.TotalBatches, stats.TotalRequests, avgBatch, avgSyncMs, maxSyncMs, p50, p90, p99, stats.MaxBatchSize)
}

func diffFsyncStats(before, after disk.FsyncBatchStats) disk.FsyncBatchStats {
	if after.TotalBatches == 0 {
		return after
	}
	delta := disk.FsyncBatchStats{
		TotalBatches:  subUint64(after.TotalBatches, before.TotalBatches),
		TotalRequests: subUint64(after.TotalRequests, before.TotalRequests),
		MaxBatchSize:  after.MaxBatchSize,
		TotalSyncNs:   subUint64(after.TotalSyncNs, before.TotalSyncNs),
		MaxSyncNs:     after.MaxSyncNs,
		Bounds:        append([]int(nil), after.Bounds...),
	}
	if len(after.Counts) > 0 {
		delta.Counts = make([]uint64, len(after.Counts))
		for i := range after.Counts {
			var prev uint64
			if i < len(before.Counts) {
				prev = before.Counts[i]
			}
			delta.Counts[i] = subUint64(after.Counts[i], prev)
		}
	}
	return delta
}

func subUint64(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}

func batchPercentile(stats disk.FsyncBatchStats, pct float64) int {
	if stats.TotalBatches == 0 {
		return 0
	}
	if pct <= 0 {
		return stats.Bounds[0]
	}
	if pct >= 100 {
		if stats.MaxBatchSize == 0 {
			return stats.Bounds[len(stats.Bounds)-1]
		}
		return int(stats.MaxBatchSize)
	}
	target := uint64(math.Ceil((pct / 100.0) * float64(stats.TotalBatches)))
	if target == 0 {
		target = 1
	}
	var acc uint64
	for i, count := range stats.Counts {
		acc += count
		if acc >= target {
			if i < len(stats.Bounds) {
				return stats.Bounds[i]
			}
			if stats.MaxBatchSize > 0 {
				return int(stats.MaxBatchSize)
			}
			return stats.Bounds[len(stats.Bounds)-1]
		}
	}
	if stats.MaxBatchSize > 0 {
		return int(stats.MaxBatchSize)
	}
	return stats.Bounds[len(stats.Bounds)-1]
}

func summarize(samples []time.Duration) benchSummary {
	if len(samples) == 0 {
		return benchSummary{}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	total := time.Duration(0)
	for _, d := range samples {
		total += d
	}
	return benchSummary{
		count: len(samples),
		avg:   time.Duration(int64(total) / int64(len(samples))),
		min:   samples[0],
		max:   samples[len(samples)-1],
		p50:   percentile(samples, 50),
		p90:   percentile(samples, 90),
		p95:   percentile(samples, 95),
		p99:   percentile(samples, 99),
		p999:  percentile(samples, 99.9),
	}
}

func medianStats(label string, stats []benchStats) benchStats {
	if len(stats) == 0 {
		return benchStats{label: label}
	}
	ops := medianInt(extractInt(stats, func(s benchStats) int { return s.ops }))
	opsPerSec := medianFloat(extractFloat(stats, func(s benchStats) float64 { return s.opsPerSec }))
	avg := medianDuration(extractDuration(stats, func(s benchStats) time.Duration { return s.avg }))
	min := medianDuration(extractDuration(stats, func(s benchStats) time.Duration { return s.min }))
	max := medianDuration(extractDuration(stats, func(s benchStats) time.Duration { return s.max }))
	p50 := medianDuration(extractDuration(stats, func(s benchStats) time.Duration { return s.p50 }))
	p90 := medianDuration(extractDuration(stats, func(s benchStats) time.Duration { return s.p90 }))
	p95 := medianDuration(extractDuration(stats, func(s benchStats) time.Duration { return s.p95 }))
	p99 := medianDuration(extractDuration(stats, func(s benchStats) time.Duration { return s.p99 }))
	p999 := medianDuration(extractDuration(stats, func(s benchStats) time.Duration { return s.p999 }))
	var errs int64
	for _, s := range stats {
		errs += s.errs
	}
	return benchStats{
		label:     label,
		ops:       ops,
		opsPerSec: opsPerSec,
		avg:       avg,
		min:       min,
		max:       max,
		p50:       p50,
		p90:       p90,
		p95:       p95,
		p99:       p99,
		p999:      p999,
		errs:      errs,
	}
}

func extractInt(stats []benchStats, sel func(benchStats) int) []int {
	out := make([]int, 0, len(stats))
	for _, s := range stats {
		out = append(out, sel(s))
	}
	return out
}

func extractFloat(stats []benchStats, sel func(benchStats) float64) []float64 {
	out := make([]float64, 0, len(stats))
	for _, s := range stats {
		out = append(out, sel(s))
	}
	return out
}

func extractDuration(stats []benchStats, sel func(benchStats) time.Duration) []time.Duration {
	out := make([]time.Duration, 0, len(stats))
	for _, s := range stats {
		out = append(out, sel(s))
	}
	return out
}

func medianInt(values []int) int {
	if len(values) == 0 {
		return 0
	}
	sort.Ints(values)
	return values[len(values)/2]
}

func medianFloat(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	return values[len(values)/2]
}

func medianDuration(values []time.Duration) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values[len(values)/2]
}

func percentile(samples []time.Duration, pct float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	if pct <= 0 {
		return samples[0]
	}
	if pct >= 100 {
		return samples[len(samples)-1]
	}
	pos := (pct / 100.0) * float64(len(samples)-1)
	idx := int(math.Round(pos))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(samples) {
		idx = len(samples) - 1
	}
	return samples[idx]
}

func buildPayload(size int) []byte {
	if size < 0 {
		size = 0
	}
	const fieldCount = 10
	fields := make(map[string]string, fieldCount)
	base := 0
	if fieldCount > 0 {
		base = size / fieldCount
	}
	remainder := size - base*fieldCount
	for i := 0; i < fieldCount; i++ {
		fieldSize := base
		if remainder > 0 {
			fieldSize++
			remainder--
		}
		fields[fmt.Sprintf("field%d", i)] = string(bytes.Repeat([]byte("x"), fieldSize))
	}
	doc := map[string]any{"data": fields}
	payload, err := json.Marshal(doc)
	if err != nil {
		return []byte(`{"data":{}}`)
	}
	return payload
}

func runSingle(ctx context.Context, svc *core.Service, namespace, key string, payload []byte) {
	acq, err := svc.Acquire(ctx, core.AcquireCommand{
		Namespace:  namespace,
		Key:        key,
		Owner:      "bench-seed",
		TTLSeconds: 30,
	})
	if err != nil {
		die("seed acquire: %v", err)
	}
	if _, err := svc.Update(ctx, core.UpdateCommand{
		Namespace:      namespace,
		Key:            key,
		LeaseID:        acq.LeaseID,
		FencingToken:   acq.FencingToken,
		TxnID:          acq.TxnID,
		Body:           bytes.NewReader(payload),
		CompactWriter:  jsonutil.CompactWriter,
		SpoolThreshold: 4 << 20,
	}); err != nil {
		_, _ = svc.Release(ctx, core.ReleaseCommand{
			Namespace:    namespace,
			Key:          key,
			LeaseID:      acq.LeaseID,
			FencingToken: acq.FencingToken,
			TxnID:        acq.TxnID,
			Rollback:     true,
		})
		die("seed update: %v", err)
	}
	if _, err := svc.Release(ctx, core.ReleaseCommand{
		Namespace:    namespace,
		Key:          key,
		LeaseID:      acq.LeaseID,
		FencingToken: acq.FencingToken,
		TxnID:        acq.TxnID,
	}); err != nil {
		die("seed release: %v", err)
	}
}

func runSingleClient(ctx context.Context, cli *lockdclient.Client, namespace, key string, payload []byte) {
	acq, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespace,
		Key:        key,
		Owner:      "bench-seed",
		TTLSeconds: 30,
	})
	if err != nil {
		die("seed acquire: %v", err)
	}
	if _, err := acq.UpdateBytes(ctx, payload); err != nil {
		_ = acq.ReleaseWithOptions(ctx, lockdclient.ReleaseOptions{Rollback: true})
		die("seed update: %v", err)
	}
	if err := acq.Release(ctx); err != nil {
		die("seed release: %v", err)
	}
}

func newBenchClient(cfg benchConfig, logger pslog.Logger) (*lockdclient.Client, error) {
	httpClient, err := buildBenchHTTPClient(cfg)
	if err != nil {
		return nil, err
	}
	opts := []lockdclient.Option{
		lockdclient.WithDisableMTLS(cfg.disableMTLS),
		lockdclient.WithHTTPClient(httpClient),
		lockdclient.WithHTTPTimeout(cfg.httpTimeout),
	}
	if logger != nil {
		opts = append(opts, lockdclient.WithLogger(logger))
	}
	endpoints, err := lockdclient.ParseEndpoints(cfg.endpoint, cfg.disableMTLS)
	if err != nil {
		return nil, err
	}
	return lockdclient.NewWithEndpoints(endpoints, opts...)
}

func buildBenchHTTPClient(cfg benchConfig) (*http.Client, error) {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("bench: unexpected default transport type")
	}
	tr := transport.Clone()
	if cfg.disableMTLS {
		tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		return &http.Client{Transport: tr}, nil
	}
	bundlePath, err := lockd.ResolveClientBundlePath(lockd.ClientBundleRoleSDK, cfg.clientBundle)
	if err != nil {
		return nil, err
	}
	bundle, err := tlsutil.LoadClientBundle(bundlePath)
	if err != nil {
		return nil, err
	}
	tr.TLSClientConfig = buildBenchClientTLS(bundle)
	return &http.Client{Transport: tr}, nil
}

func buildBenchClientTLS(bundle *tlsutil.ClientBundle) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		Certificates:       []tls.Certificate{bundle.Certificate},
		RootCAs:            bundle.CAPool,
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return verifyBenchServerCertificate(rawCerts, bundle.CAPool)
		},
	}
}

func verifyBenchServerCertificate(rawCerts [][]byte, roots *x509.CertPool) error {
	if len(rawCerts) == 0 {
		return errors.New("mtls: missing server certificate")
	}
	certs := make([]*x509.Certificate, 0, len(rawCerts))
	for _, raw := range rawCerts {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return fmt.Errorf("mtls: parse server certificate: %w", err)
		}
		certs = append(certs, cert)
	}
	leaf := certs[0]
	opts := x509.VerifyOptions{
		Roots:         roots,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		Intermediates: x509.NewCertPool(),
		CurrentTime:   time.Now(),
	}
	for _, cert := range certs[1:] {
		opts.Intermediates.AddCert(cert)
	}
	if _, err := leaf.Verify(opts); err != nil {
		return fmt.Errorf("mtls: verify server certificate: %w", err)
	}
	return nil
}

func openBenchStore(cfg benchConfig, crypto *storage.Crypto, root string) (storage.Backend, *indexer.Store) {
	switch cfg.backend {
	case "disk":
		store, err := disk.New(disk.Config{
			Root:                 root,
			Retention:            0,
			JanitorInterval:      0,
			QueueWatch:           false,
			Crypto:               crypto,
			LogstoreCommitMaxOps: cfg.commitMaxOps,
			LogstoreSegmentSize:  cfg.segmentSize,
		})
		if err != nil {
			die("disk store init: %v", err)
		}
		return store, indexer.NewStore(store, crypto)
	case "minio", "aws", "azure", "mem":
		storeURL, err := benchStoreURL(cfg, root)
		if err != nil {
			die("bench store: %v", err)
		}
		lockdCfg := lockdConfigForStore(storeURL, cfg)
		store, err := lockd.OpenBackend(lockdCfg, crypto)
		if err != nil {
			die("bench store init: %v", err)
		}
		return store, indexer.NewStore(store, crypto)
	default:
		die("unsupported backend %q", cfg.backend)
		return nil, nil
	}
}

type indexerDefaultsProvider interface {
	IndexerFlushDefaults() (int, time.Duration)
}

func indexerDefaults(store storage.Backend) (int, time.Duration) {
	if provider, ok := store.(indexerDefaultsProvider); ok && provider != nil {
		return provider.IndexerFlushDefaults()
	}
	return indexer.DefaultFlushDocs, indexer.DefaultFlushInterval
}

func lockdConfigForStore(storeURL string, cfg benchConfig) lockd.Config {
	return lockd.Config{
		Store:                      storeURL,
		S3AccessKeyID:              os.Getenv("LOCKD_S3_ACCESS_KEY_ID"),
		S3SecretAccessKey:          os.Getenv("LOCKD_S3_SECRET_ACCESS_KEY"),
		S3SessionToken:             os.Getenv("LOCKD_S3_SESSION_TOKEN"),
		S3KMSKeyID:                 os.Getenv("LOCKD_S3_KMS_KEY_ID"),
		AWSKMSKeyID:                os.Getenv("LOCKD_AWS_KMS_KEY_ID"),
		S3SSE:                      os.Getenv("LOCKD_S3_SSE"),
		S3MaxPartSize:              lockd.DefaultS3MaxPartSize,
		S3SmallEncryptBufferBudget: lockd.DefaultS3SmallEncryptBufferBudget,
		AWSRegion:                  firstEnv("LOCKD_AWS_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"),
		AzureAccount:               os.Getenv("LOCKD_AZURE_ACCOUNT"),
		AzureAccountKey:            os.Getenv("LOCKD_AZURE_ACCOUNT_KEY"),
		AzureEndpoint:              os.Getenv("LOCKD_AZURE_ENDPOINT"),
		AzureSASToken:              os.Getenv("LOCKD_AZURE_SAS_TOKEN"),
		StorageEncryptionSnappy:    cfg.enableSnappy,
		DisableStorageEncryption:   !cfg.enableCrypto,
	}
}

func openCleanupBackend(cfg benchConfig, root string) (storage.Backend, func(), error) {
	if strings.TrimSpace(cfg.endpoint) != "" {
		return nil, nil, nil
	}
	if cfg.keepNamespace {
		return nil, nil, nil
	}
	if strings.EqualFold(cfg.backend, "mem") {
		return nil, nil, nil
	}
	storeURL, err := benchStoreURL(cfg, root)
	if err != nil {
		return nil, nil, err
	}
	lockdCfg := lockdConfigForStore(storeURL, cfg)
	lockdCfg.DisableStorageEncryption = true
	lockdCfg.StorageEncryptionSnappy = false
	lockdCfg.DisableMTLS = true
	backend, err := lockd.OpenBackend(lockdCfg, nil)
	if err != nil {
		return nil, nil, err
	}
	return backend, func() { _ = backend.Close() }, nil
}

func fsyncStats(store storage.Backend) disk.FsyncBatchStats {
	if diskStore, ok := store.(*disk.Store); ok && diskStore != nil {
		return diskStore.FsyncStats()
	}
	return disk.FsyncBatchStats{}
}

func loadEnvFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		if key == "" {
			continue
		}
		val := strings.TrimSpace(parts[1])
		val = strings.Trim(val, `"'`)
		if err := os.Setenv(key, val); err != nil {
			return err
		}
	}
	return nil
}

func normalizeBenchEnv() {
	if os.Getenv("LOCKD_AWS_REGION") == "" {
		if v := os.Getenv("AWS_REGION"); v != "" {
			_ = os.Setenv("LOCKD_AWS_REGION", v)
		} else if v := os.Getenv("AWS_DEFAULT_REGION"); v != "" {
			_ = os.Setenv("LOCKD_AWS_REGION", v)
		}
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

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
