package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/svcfields"
	"pkt.systems/pslog"
)

func submain(ctx context.Context) int {
	baseLogger := pslog.LoggerFromEnv(context.Background(),
		pslog.WithEnvPrefix("LOCKD_LOG_"),
		pslog.WithEnvOptions(pslog.Options{Mode: pslog.ModeStructured, MinLevel: pslog.InfoLevel}),
		pslog.WithEnvWriter(os.Stderr),
	).With("app", "lockd")
	cmd := newRootCommand(baseLogger)
	rootInvocation := invocationTargetsRootCommand(cmd, os.Args[1:])
	ctx = withSignalCancel(ctx)
	if _, err := cmd.ExecuteContextC(ctx); err != nil {
		if err != context.Canceled {
			if rootInvocation {
				svcfields.WithSubsystem(baseLogger, "cli.root").Error("command failed", "error", err)
			} else {
				fmt.Fprintf(os.Stderr, "%s\n", err)
			}
		}
		return 1
	}
	return 0
}

func invocationTargetsRootCommand(root *cobra.Command, args []string) bool {
	if len(args) == 0 {
		return true
	}
	lookupLong := func(name string) *pflag.Flag {
		flag := root.Flags().Lookup(name)
		if flag == nil {
			flag = root.PersistentFlags().Lookup(name)
		}
		return flag
	}
	lookupShort := func(shorthand string) *pflag.Flag {
		flag := root.Flags().ShorthandLookup(shorthand)
		if flag == nil {
			flag = root.PersistentFlags().ShorthandLookup(shorthand)
		}
		return flag
	}
	remainingHasSubcommand := func(rest []string) bool {
		for _, tok := range rest {
			if !isSubcommandToken(root, tok) {
				continue
			}
			return true
		}
		return false
	}
	for i := 0; i < len(args); {
		arg := args[i]
		if arg == "--" {
			return true
		}
		if strings.HasPrefix(arg, "--") && arg != "--" {
			if eq := strings.IndexByte(arg, '='); eq >= 0 {
				i++
				continue
			}
			name := strings.TrimPrefix(arg, "--")
			flag := lookupLong(name)
			if flag == nil {
				return !remainingHasSubcommand(args[i+1:])
			}
			i++
			if flag.NoOptDefVal == "" && i < len(args) {
				i++
			}
			continue
		}
		if strings.HasPrefix(arg, "-") && arg != "-" {
			sh := strings.TrimPrefix(arg, "-")
			consumeNext := false
			for idx, ch := range sh {
				flag := lookupShort(string(ch))
				if flag == nil {
					return !remainingHasSubcommand(args[i+1:])
				}
				if flag.NoOptDefVal == "" {
					if idx == len(sh)-1 {
						consumeNext = true
					}
					break
				}
			}
			i++
			if consumeNext && i < len(args) {
				i++
			}
			continue
		}
		return !isSubcommandToken(root, arg)
	}
	return true
}

func isSubcommandToken(root *cobra.Command, token string) bool {
	for _, sub := range root.Commands() {
		if token == sub.Name() {
			return true
		}
		for _, alias := range sub.Aliases {
			if token == alias {
				return true
			}
		}
	}
	return false
}

func humanizeBytes(n int64) string {
	return strings.ReplaceAll(humanize.Bytes(uint64(n)), " ", "")
}

func loadConfigFile() (string, error) {
	cfgPath := strings.TrimSpace(viper.GetString("config"))
	explicit := cfgPath != ""

	if cfgPath == "" {
		if dir, err := lockd.DefaultConfigDir(); err == nil {
			candidate := filepath.Join(dir, lockd.DefaultConfigFileName)
			if _, err := os.Stat(candidate); err == nil {
				cfgPath = candidate
			}
		}
	}

	if cfgPath == "" {
		return "", nil
	}

	expanded, err := expandPath(cfgPath)
	if err != nil {
		return "", fmt.Errorf("expand config path %q: %w", cfgPath, err)
	}
	info, err := os.Stat(expanded)
	if err != nil {
		if os.IsNotExist(err) && !explicit {
			return "", nil
		}
		return "", fmt.Errorf("config file %q: %w", expanded, err)
	}
	if info.IsDir() {
		return "", fmt.Errorf("config file %q is a directory", expanded)
	}

	viper.SetConfigFile(expanded)
	if err := viper.ReadInConfig(); err != nil {
		return "", fmt.Errorf("read config file %q: %w", expanded, err)
	}
	return expanded, nil
}

func expandPath(p string) (string, error) {
	if p == "" {
		return "", nil
	}
	if strings.HasPrefix(p, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if len(p) == 1 {
			p = home
		} else if p[1] == '/' || p[1] == '\\' {
			p = filepath.Join(home, p[2:])
		}
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	return abs, nil
}

func newRootCommand(baseLogger pslog.Logger) *cobra.Command {
	var cfg lockd.Config
	var bootstrapDir string
	var bootstrapRan bool
	runBootstrap := func(baseLogger pslog.Logger) error {
		if bootstrapDir == "" || bootstrapRan {
			return nil
		}
		bootstrapRan = true
		abs, err := filepath.Abs(bootstrapDir)
		if err != nil {
			return fmt.Errorf("resolve --bootstrap path: %w", err)
		}
		if os.Getenv("LOCKD_CONFIG_DIR") == "" {
			if err := os.Setenv("LOCKD_CONFIG_DIR", abs); err != nil {
				return fmt.Errorf("set LOCKD_CONFIG_DIR: %w", err)
			}
		}
		logger := svcfields.WithSubsystem(baseLogger, "cli.bootstrap")
		return bootstrapConfigDir(abs, logger)
	}

	cmd := &cobra.Command{
		Use:           "lockd",
		Short:         "lockd is a single-binary coordination service with exclusive leases, atomic JSON state, and an at-least-once queue",
		SilenceErrors: true,
		Example: `
  # AWS S3 backend (expects AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
  LOCKD_STORE=aws://my-bucket/prefix LOCKD_AWS_REGION=us-west-2 lockd

  # MinIO backend (TLS on by default; append ?insecure=1 for HTTP)
  LOCKD_STORE=s3://localhost:9000/lockd-data?insecure=1 LOCKD_S3_ACCESS_KEY_ID=minioadmin LOCKD_S3_SECRET_ACCESS_KEY=minioadmin lockd

  # SSD-optimised disk backend rooted at /var/lib/lockd-data
  lockd --store disk:///var/lib/lockd-data --disk-retention=0

  # In-memory storage (tests/dev only)
  lockd --store mem://

  # Prefer stdlib JSON compaction for tiny payloads
  lockd --store mem:// --json-util stdlib
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := baseLogger
			cliLogger := svcfields.WithSubsystem(logger, "cli.root")
			ctx := cmd.Context()
			cmd.SilenceUsage = true
			svcfields.WithSubsystem(logger, "server.lifecycle.init").WithLogLevel().Info(
				"welcome to lockd",
				"app", "lockd",
				"pid", os.Getpid(),
				"uid", os.Getuid(),
				"gid", os.Getgid(),
			)
			if err := runBootstrap(baseLogger); err != nil {
				return err
			}

			configFile, err := loadConfigFile()
			if err != nil {
				return err
			}
			if configFile != "" {
				cliLogger.Info("loaded config file", "path", configFile)
			}

			if err := bindConfig(&cfg); err != nil {
				return err
			}
			flagChanged := func(name string) bool {
				if f := cmd.Flags().Lookup(name); f != nil {
					return f.Changed
				}
				return false
			}
			envSet := func(key string) bool {
				_, ok := os.LookupEnv(key)
				return ok
			}
			cfg.IndexerFlushDocsSet = flagChanged("indexer-flush-docs") || viper.InConfig("indexer-flush-docs") || envSet("LOCKD_INDEXER_FLUSH_DOCS")
			cfg.IndexerFlushIntervalSet = flagChanged("indexer-flush-interval") || viper.InConfig("indexer-flush-interval") || envSet("LOCKD_INDEXER_FLUSH_INTERVAL")
			logLevel := strings.TrimSpace(viper.GetString("log-level"))
			if logLevel == "" {
				logLevel = "info"
			}

			level, ok := pslog.ParseLevel(logLevel)
			if ok {
				logger = logger.LogLevel(level)
				cliLogger = svcfields.WithSubsystem(logger, "cli.root")
			}

			server, err := lockd.NewServer(cfg, lockd.WithLogger(logger))
			if err != nil {
				return err
			}

			defer func() {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = server.Shutdown(shutdownCtx)
			}()

			go func() {
				<-ctx.Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := server.Shutdown(shutdownCtx); err != nil {
					cliLogger.Error("shutdown failed", "error", err)
				}
			}()

			err = server.Start()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		},
	}

	persistentFlags := cmd.PersistentFlags()
	persistentFlags.StringP("config", "c", "", "path to YAML config file (defaults to $HOME/.lockd/"+lockd.DefaultConfigFileName+")")
	clientCfg := addClientConnectionFlags(cmd, true, baseLogger)

	flags := cmd.Flags()
	flags.StringVar(&bootstrapDir, "bootstrap", "", "initialize certificates + config under this directory before running (idempotent)")
	flags.String("listen", ":9341", "listen address")
	flags.String("listen-proto", "tcp", "listen network (tcp, tcp4, tcp6)")
	flags.String("metrics-listen", lockd.DefaultMetricsListen, "metrics listen address (Prometheus scrape endpoint; empty disables; default off)")
	flags.String("pprof-listen", lockd.DefaultPprofListen, "pprof listen address (debug/pprof endpoints; empty disables)")
	flags.Bool("enable-profiling-metrics", false, "enable Go runtime profiling metrics on the Prometheus endpoint")
	flags.String("store", "", "storage backend URL (mem://, s3://host[:port]/bucket, aws://bucket, disk:///path)")
	flags.String("ha", lockd.DefaultHAMode, "HA mode when multiple servers share a backend (concurrent or failover)")
	flags.Duration("ha-lease-ttl", lockd.DefaultHALeaseTTL, "lease TTL for HA failover mode (ignored in concurrent mode)")
	flags.StringP("default-namespace", "n", lockd.DefaultNamespace, "default namespace applied when callers omit one")
	jsonMaxDefault := humanizeBytes(lockd.DefaultJSONMaxBytes)
	spoolDefault := humanizeBytes(lockd.DefaultPayloadSpoolMemoryThreshold)
	stateCacheDefault := humanizeBytes(lockd.DefaultStateCacheBytes)
	s3PartDefault := humanizeBytes(lockd.DefaultS3MaxPartSize)
	s3EncryptBudgetDefault := humanizeBytes(lockd.DefaultS3SmallEncryptBufferBudget)
	flags.String("json-max", jsonMaxDefault, "maximum JSON payload size")
	flags.String("json-util", lockd.JSONUtilLockd, fmt.Sprintf("JSON compaction engine (%s)", strings.Join(lockd.ValidJSONUtils(), ", ")))
	flags.String("payload-spool-mem", spoolDefault, "bytes to buffer JSON bodies in memory before spooling to disk")
	flags.String("state-cache-bytes", stateCacheDefault, "bytes to cache published state payloads in memory (0 disables)")
	flags.Duration("default-ttl", lockd.DefaultDefaultTTL, "default lease TTL")
	flags.Duration("max-ttl", lockd.DefaultMaxTTL, "maximum lease TTL")
	flags.Duration("acquire-block", lockd.DefaultAcquireBlock, "maximum time to wait on acquire conflicts")
	flags.Duration("sweeper-interval", lockd.DefaultSweeperInterval, "sweeper interval for background tasks")
	flags.Duration("txn-replay-interval", lockd.DefaultTxnReplayInterval, "minimum interval between transaction replay sweeps on active operations")
	flags.Duration("queue-decision-cache-ttl", lockd.DefaultQueueDecisionCacheTTL, "cache duration for empty queue decision checks")
	flags.Int("queue-decision-max-apply", lockd.DefaultQueueDecisionMaxApply, "maximum queue decision items applied per dequeue")
	flags.Duration("queue-decision-apply-timeout", lockd.DefaultQueueDecisionApplyTimeout, "maximum time spent applying queue decisions per dequeue")
	flags.Duration("idle-sweep-grace", lockd.DefaultIdleSweepGrace, "idle time before maintenance sweeper runs")
	flags.Duration("idle-sweep-op-delay", lockd.DefaultIdleSweepOpDelay, "pause between maintenance sweep operations")
	flags.Int("idle-sweep-max-ops", lockd.DefaultIdleSweepMaxOps, "maximum maintenance sweep operations per run")
	flags.Duration("idle-sweep-max-runtime", lockd.DefaultIdleSweepMaxRuntime, "maximum maintenance sweep runtime")
	flags.Duration("drain-grace", lockd.DefaultDrainGrace, "grace period to drain leases before HTTP shutdown (set 0 to disable)")
	flags.Duration("shutdown-timeout", lockd.DefaultShutdownTimeout, "overall shutdown timeout (set 0 to rely on signal deadlines)")
	flags.Duration("disk-retention", 0, "optional retention window for disk backend (0 keeps state indefinitely)")
	flags.Duration("disk-janitor-interval", 0, "override janitor sweep interval for disk backend (default derived from retention)")
	flags.Bool("disk-queue-watch", true, "enable inotify-based queue events for disk backend (Linux only; falls back to polling on unsupported filesystems)")
	flags.Int("disk-lock-file-cache-size", lockd.DefaultDiskLockFileCacheSize, "max cached lockfile descriptors for disk/NFS (0 uses default, negative disables)")
	logstoreSegmentDefault := humanizeBytes(lockd.DefaultLogstoreSegmentSize)
	flags.Int("logstore-commit-max-ops", lockd.DefaultLogstoreCommitMaxOps, "maximum logstore entries per fsync batch (disk/NFS)")
	flags.String("logstore-segment-size", logstoreSegmentDefault, "maximum logstore segment size before roll (disk/NFS)")
	flags.Bool("disable-mem-queue-watch", false, "disable in-memory queue change notifications")
	flags.Bool("disable-storage-encryption", false, "disable kryptograf envelope encryption (plaintext at rest)")
	flags.Bool("storage-encryption-snappy", false, "enable Snappy compression before encrypting objects")
	flags.Bool("disable-krypto-pool", false, "disable kryptograf buffer pool (enabled by default)")
	flags.Int("http2-max-concurrent-streams", lockd.DefaultMaxConcurrentStreams, "maximum concurrent HTTP/2 streams per connection (0 uses http2 default)")
	flags.String("denylist-path", "", "path to certificate denylist (optional)")
	tcTrustDefault := ""
	if dir, err := lockd.DefaultTCTrustDir(); err == nil {
		tcTrustDefault = dir
	}
	flags.String("tc-trust-dir", tcTrustDefault, "directory containing trusted TC CA certificates (for federation)")
	flags.Bool("tc-disable-auth", false, "disable TC client certificate enforcement for transaction coordinator endpoints")
	flags.Bool("tc-allow-default-ca", false, "allow the primary server CA for TC endpoints when TC auth is enabled")
	flags.String("self", "", "this node's endpoint (used for TC leadership and RM registration)")
	flags.StringSlice("join", nil, "seed TC endpoints used to bootstrap leader election")
	flags.Duration("tc-fanout-timeout", lockd.DefaultTCFanoutTimeout, "per-RM timeout for TC fan-out apply calls")
	flags.Int("tc-fanout-attempts", lockd.DefaultTCFanoutMaxAttempts, "maximum attempts per RM apply during TC fan-out")
	flags.Duration("tc-fanout-base-delay", lockd.DefaultTCFanoutBaseDelay, "base backoff delay for TC fan-out retries")
	flags.Duration("tc-fanout-max-delay", lockd.DefaultTCFanoutMaxDelay, "maximum backoff delay for TC fan-out retries")
	flags.Float64("tc-fanout-multiplier", lockd.DefaultTCFanoutMultiplier, "backoff multiplier for TC fan-out retries")
	flags.Duration("tc-decision-retention", lockd.DefaultTCDecisionRetention, "retain decided txn records for this duration")
	flags.String("tc-client-bundle", "", "path to client bundle PEM used by TC fan-out calls when mTLS is enabled")
	flags.String("s3-sse", "", "server-side encryption mode for S3 objects")
	flags.String("s3-kms-key-id", "", "KMS key ID for S3 server-side encryption")
	flags.String("s3-max-part-size", s3PartDefault, "maximum S3 multipart upload part size")
	flags.String("s3-encrypt-buffer-budget", s3EncryptBudgetDefault, "max total bytes for buffered S3 encryption payloads before streaming")
	flags.String("aws-region", "", "AWS region for aws:// backends")
	flags.String("aws-kms-key-id", "", "KMS key ID for aws:// backends")
	flags.String("azure-key", "", "Azure Storage account key (or use LOCKD_AZURE_ACCOUNT_KEY)")
	flags.String("azure-endpoint", lockd.DefaultAzureEndpoint, fmt.Sprintf("Azure Blob service endpoint (defaults to %s)", lockd.DefaultAzureEndpoint))
	flags.String("azure-sas-token", "", "Azure SAS token (optional alternative to account key)")
	flags.Int("storage-retry-attempts", lockd.DefaultStorageRetryMaxAttempts, "maximum storage retry attempts")
	flags.Duration("storage-retry-base-delay", lockd.DefaultStorageRetryBaseDelay, "initial backoff for storage retries")
	flags.Duration("storage-retry-max-delay", lockd.DefaultStorageRetryMaxDelay, "maximum backoff delay for storage retries")
	flags.Float64("storage-retry-multiplier", lockd.DefaultStorageRetryMultiplier, "backoff multiplier for storage retries")
	flags.Int("queue-max-consumers", lockd.DefaultQueueMaxConsumers(), "baseline concurrent queue consumers per server (auto; override to force a fixed cap)")
	flags.Duration("queue-poll-interval", lockd.DefaultQueuePollInterval, "baseline poll interval for queue discovery")
	flags.Duration("queue-poll-jitter", lockd.DefaultQueuePollJitter, "extra random delay added to queue poll interval (set 0 to disable)")
	flags.Duration("queue-resilient-poll-interval", lockd.DefaultQueueResilientPollInterval, "safety poll interval when watchers are active (set 0 to disable)")
	flags.Int("queue-list-page-size", lockd.DefaultQueueListPageSize, "queue list page size per poll")
	flags.Int("indexer-flush-docs", lockd.DefaultIndexerFlushDocs, "flush index segments after this many documents")
	flags.Duration("indexer-flush-interval", lockd.DefaultIndexerFlushInterval, "maximum duration to buffer index postings before flushing")
	flags.Int("query-doc-prefetch", lockd.DefaultQueryDocPrefetch, "prefetch depth for query return=documents (1 disables parallelism)")
	flags.Bool("connguard-enabled", true, "enable listener-level connection guarding")
	flags.Int("connguard-failure-threshold", lockd.DefaultConnguardFailureThreshold, "number of suspicious connection failures before hard-blocking an IP")
	flags.Duration("connguard-failure-window", lockd.DefaultConnguardFailureWindow, "window used to count suspicious connection failures")
	flags.Duration("connguard-block-duration", lockd.DefaultConnguardBlockDuration, "time to block an IP after reaching failure threshold")
	flags.Duration("connguard-probe-timeout", lockd.DefaultConnguardProbeTimeout, "timeout for classification of suspicious plain-TCP attempts")
	flags.Duration("lsf-sample-interval", lockd.DefaultLSFSampleInterval, "sampling interval for the local security force (LSF)")
	flags.Duration("lsf-log-interval", lockd.DefaultLSFLogInterval, "interval between LSF telemetry logs (set 0 to disable)")
	flags.Bool("qrf-disabled", false, "disable perimeter defence quick reaction force (QRF)")
	flags.Int64("qrf-queue-soft-limit", 0, "soft inflight threshold for queue operations (auto when 0)")
	flags.Int64("qrf-queue-hard-limit", 0, "hard inflight threshold for queue operations (auto when 0)")
	flags.Int64("qrf-queue-consumer-soft-limit", 0, "soft limit for concurrent queue consumers (auto when 0)")
	flags.Int64("qrf-queue-consumer-hard-limit", 0, "hard limit for concurrent queue consumers (auto when 0)")
	flags.Int64("qrf-lock-soft-limit", 0, "soft inflight threshold for lock operations (auto when 0)")
	flags.Int64("qrf-lock-hard-limit", 0, "hard inflight threshold for lock operations (auto when 0)")
	flags.Int64("qrf-query-soft-limit", 0, "soft inflight threshold for query operations (auto when 0)")
	flags.Int64("qrf-query-hard-limit", 0, "hard inflight threshold for query operations (auto when 0)")
	flags.String("qrf-memory-soft-limit", "", "approximate RSS soft limit before QRF engages (blank disables)")
	flags.String("qrf-memory-hard-limit", "", "approximate RSS hard limit triggering immediate QRF engagement (blank disables)")
	flags.Float64("qrf-memory-soft-limit-percent", lockd.DefaultQRFMemorySoftLimitPercent, "system memory usage percentage that soft-arms the QRF")
	flags.Float64("qrf-memory-hard-limit-percent", lockd.DefaultQRFMemoryHardLimitPercent, "system memory usage percentage that fully engages the QRF")
	flags.Float64("qrf-memory-strict-headroom-percent", lockd.DefaultQRFMemoryStrictHeadroomPercent, "headroom to subtract from system memory usage when reclaimable cache is unknown")
	flags.String("qrf-swap-soft-limit", "", "swap usage soft limit before QRF engages (e.g. 256MB; blank disables)")
	flags.String("qrf-swap-hard-limit", "", "swap usage hard limit that fully engages the QRF (blank disables)")
	flags.Float64("qrf-swap-soft-limit-percent", 0, "swap utilisation percentage that soft-arms the QRF (0 disables)")
	flags.Float64("qrf-swap-hard-limit-percent", 0, "swap utilisation percentage that fully engages the QRF (0 disables)")
	flags.Float64("qrf-cpu-soft-limit", lockd.DefaultQRFCPUPercentSoftLimit, "system CPU utilisation percentage that soft-arms the QRF (0 disables)")
	flags.Float64("qrf-cpu-hard-limit", lockd.DefaultQRFCPUPercentHardLimit, "system CPU utilisation percentage that fully engages the QRF (0 disables)")
	flags.Float64("qrf-load-soft-limit-multiplier", lockd.DefaultQRFLoadSoftLimitMultiplier, "load average multiplier (relative to LSF baseline) that soft-arms the QRF")
	flags.Float64("qrf-load-hard-limit-multiplier", lockd.DefaultQRFLoadHardLimitMultiplier, "load average multiplier (relative to LSF baseline) that fully engages the QRF")
	flags.Int("qrf-recovery-samples", lockd.DefaultQRFRecoverySamples, "number of consecutive healthy samples before QRF disengages")
	flags.Duration("qrf-soft-delay", lockd.DefaultQRFSoftDelay, "base delay used when QRF is soft-armed")
	flags.Duration("qrf-engaged-delay", lockd.DefaultQRFEngagedDelay, "base delay used when QRF is fully engaged")
	flags.Duration("qrf-recovery-delay", lockd.DefaultQRFRecoveryDelay, "base delay used while QRF is recovering")
	flags.Duration("qrf-max-wait", lockd.DefaultQRFMaxWait, "maximum delay applied by QRF pacing before returning a throttled response")
	flags.String("otlp-endpoint", "", "OTLP collector endpoint (e.g. grpc://localhost:4317)")

	bindFlag := func(name string) {
		flag := flags.Lookup(name)
		if flag == nil {
			flag = persistentFlags.Lookup(name)
		}
		if flag == nil {
			panic(fmt.Sprintf("flag %q not found", name))
		}
		if err := viper.BindPFlag(name, flag); err != nil {
			panic(err)
		}
	}

	viper.SetEnvPrefix("LOCKD")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	names := []string{
		"config",
		"listen", "listen-proto", "metrics-listen", "pprof-listen", "enable-profiling-metrics", "store", "ha", "ha-lease-ttl", "default-namespace", "json-max", "json-util", "payload-spool-mem", "state-cache-bytes", "default-ttl", "max-ttl", "acquire-block",
		"sweeper-interval", "txn-replay-interval", "queue-decision-cache-ttl", "queue-decision-max-apply", "idle-sweep-grace", "idle-sweep-op-delay", "idle-sweep-max-ops", "idle-sweep-max-runtime", "drain-grace", "shutdown-timeout", "disk-retention", "disk-janitor-interval", "disk-lock-file-cache-size",
		"logstore-commit-max-ops", "logstore-segment-size",
		"disable-mtls", "http2-max-concurrent-streams", "bundle", "denylist-path", "tc-trust-dir", "tc-disable-auth", "tc-allow-default-ca",
		"self", "join", "tc-fanout-timeout", "tc-fanout-attempts", "tc-fanout-base-delay", "tc-fanout-max-delay", "tc-fanout-multiplier", "tc-decision-retention", "tc-client-bundle",
		"s3-sse",
		"s3-kms-key-id", "s3-max-part-size", "s3-encrypt-buffer-budget", "aws-region", "aws-kms-key-id", "azure-key", "azure-endpoint", "azure-sas-token",
		"storage-retry-attempts", "storage-retry-base-delay", "storage-retry-max-delay", "storage-retry-multiplier",
		"queue-max-consumers", "queue-poll-interval", "queue-poll-jitter", "queue-resilient-poll-interval",
		"indexer-flush-docs", "indexer-flush-interval",
		"query-doc-prefetch",
		"connguard-enabled", "connguard-failure-threshold", "connguard-failure-window", "connguard-block-duration", "connguard-probe-timeout",
		"disk-queue-watch", "disable-mem-queue-watch", "disable-storage-encryption", "storage-encryption-snappy", "disable-krypto-pool",
		"lsf-sample-interval", "lsf-log-interval",
		"qrf-disabled", "qrf-queue-soft-limit", "qrf-queue-hard-limit", "qrf-queue-consumer-soft-limit", "qrf-queue-consumer-hard-limit", "qrf-lock-soft-limit", "qrf-lock-hard-limit", "qrf-query-soft-limit", "qrf-query-hard-limit",
		"qrf-memory-soft-limit", "qrf-memory-hard-limit", "qrf-memory-soft-limit-percent", "qrf-memory-hard-limit-percent",
		"qrf-memory-strict-headroom-percent",
		"qrf-swap-soft-limit", "qrf-swap-hard-limit", "qrf-swap-soft-limit-percent", "qrf-swap-hard-limit-percent",
		"qrf-cpu-soft-limit", "qrf-cpu-hard-limit", "qrf-load-soft-limit-multiplier", "qrf-load-hard-limit-multiplier",
		"qrf-recovery-samples", "qrf-soft-delay", "qrf-engaged-delay", "qrf-recovery-delay",
		"otlp-endpoint", "log-level",
	}
	for _, name := range names {
		bindFlag(name)
	}
	if err := viper.BindPFlag("tc-join", flags.Lookup("join")); err != nil {
		panic(err)
	}

	cmd.AddCommand(newVerifyCommand(svcfields.WithSubsystem(baseLogger, "cli.verify")))
	cmd.AddCommand(newAuthCommand())
	cmd.AddCommand(newClientCommand(clientCfg))
	cmd.AddCommand(newNamespaceCommand(clientCfg))
	cmd.AddCommand(newIndexCommand(clientCfg))
	cmd.AddCommand(newConfigCommand())
	cmd.AddCommand(newVersionCommand())
	cmd.AddCommand(newTxnRootCommand(clientCfg))
	cmd.AddCommand(newTCCommand())
	cmd.AddCommand(newMCPCommand(
		baseLogger,
		cmd.PersistentFlags().Lookup("server"),
		cmd.PersistentFlags().Lookup("bundle"),
		cmd.PersistentFlags().Lookup("disable-mtls"),
	))

	return cmd
}

func bindConfig(cfg *lockd.Config) error {
	cfg.Listen = viper.GetString("listen")
	cfg.ListenProto = viper.GetString("listen-proto")
	cfg.MetricsListen = viper.GetString("metrics-listen")
	cfg.MetricsListenSet = viper.IsSet("metrics-listen")
	cfg.PprofListen = viper.GetString("pprof-listen")
	cfg.PprofListenSet = viper.IsSet("pprof-listen")
	cfg.EnableProfilingMetrics = viper.GetBool("enable-profiling-metrics")
	cfg.EnableProfilingMetricsSet = viper.IsSet("enable-profiling-metrics")
	cfg.Store = viper.GetString("store")
	cfg.HAMode = viper.GetString("ha")
	cfg.HALeaseTTL = viper.GetDuration("ha-lease-ttl")
	cfg.DefaultNamespace = viper.GetString("default-namespace")
	maxBytes := viper.GetString("json-max")
	if maxBytes != "" {
		size, err := humanize.ParseBytes(maxBytes)
		if err != nil {
			return fmt.Errorf("parse json-max: %w", err)
		}
		cfg.JSONMaxBytes = int64(size)
	}
	cfg.JSONUtil = strings.ToLower(strings.TrimSpace(viper.GetString("json-util")))
	if spool := viper.GetString("payload-spool-mem"); spool != "" {
		size, err := humanize.ParseBytes(spool)
		if err != nil {
			return fmt.Errorf("parse payload-spool-mem: %w", err)
		}
		cfg.SpoolMemoryThreshold = int64(size)
	}
	if cacheBytes := viper.GetString("state-cache-bytes"); cacheBytes != "" {
		size, err := humanize.ParseBytes(cacheBytes)
		if err != nil {
			return fmt.Errorf("parse state-cache-bytes: %w", err)
		}
		cfg.StateCacheBytes = int64(size)
	}
	cfg.StateCacheBytesSet = viper.IsSet("state-cache-bytes")
	cfg.DefaultTTL = viper.GetDuration("default-ttl")
	cfg.MaxTTL = viper.GetDuration("max-ttl")
	cfg.AcquireBlock = viper.GetDuration("acquire-block")
	cfg.SweeperInterval = viper.GetDuration("sweeper-interval")
	cfg.TxnReplayInterval = viper.GetDuration("txn-replay-interval")
	cfg.QueueDecisionCacheTTL = viper.GetDuration("queue-decision-cache-ttl")
	cfg.QueueDecisionMaxApply = viper.GetInt("queue-decision-max-apply")
	cfg.QueueDecisionApplyTimeout = viper.GetDuration("queue-decision-apply-timeout")
	cfg.IdleSweepGrace = viper.GetDuration("idle-sweep-grace")
	cfg.IdleSweepOpDelay = viper.GetDuration("idle-sweep-op-delay")
	cfg.IdleSweepMaxOps = viper.GetInt("idle-sweep-max-ops")
	cfg.IdleSweepMaxRuntime = viper.GetDuration("idle-sweep-max-runtime")
	cfg.DrainGrace = viper.GetDuration("drain-grace")
	cfg.DrainGraceSet = true
	cfg.ShutdownTimeout = viper.GetDuration("shutdown-timeout")
	cfg.ShutdownTimeoutSet = true
	cfg.DiskRetention = viper.GetDuration("disk-retention")
	cfg.DiskJanitorInterval = viper.GetDuration("disk-janitor-interval")
	cfg.DiskLockFileCacheSize = viper.GetInt("disk-lock-file-cache-size")
	cfg.LogstoreCommitMaxOps = viper.GetInt("logstore-commit-max-ops")
	if segment := viper.GetString("logstore-segment-size"); segment != "" {
		size, err := humanize.ParseBytes(segment)
		if err != nil {
			return fmt.Errorf("parse logstore-segment-size: %w", err)
		}
		cfg.LogstoreSegmentSize = int64(size)
	}
	cfg.DisableMTLS = viper.GetBool("disable-mtls")
	if viper.IsSet("mtls") {
		cfg.DisableMTLS = !viper.GetBool("mtls")
	}
	cfg.HTTP2MaxConcurrentStreams = viper.GetInt("http2-max-concurrent-streams")
	cfg.HTTP2MaxConcurrentStreamsSet = true
	cfg.BundlePath = viper.GetString("bundle")
	cfg.DenylistPath = viper.GetString("denylist-path")
	cfg.TCTrustDir = viper.GetString("tc-trust-dir")
	cfg.TCDisableAuth = viper.GetBool("tc-disable-auth")
	cfg.TCAllowDefaultCA = viper.GetBool("tc-allow-default-ca")
	cfg.SelfEndpoint = viper.GetString("self")
	cfg.TCJoinEndpoints = viper.GetStringSlice("tc-join")
	cfg.TCFanoutTimeout = viper.GetDuration("tc-fanout-timeout")
	cfg.TCFanoutMaxAttempts = viper.GetInt("tc-fanout-attempts")
	cfg.TCFanoutBaseDelay = viper.GetDuration("tc-fanout-base-delay")
	cfg.TCFanoutMaxDelay = viper.GetDuration("tc-fanout-max-delay")
	cfg.TCFanoutMultiplier = viper.GetFloat64("tc-fanout-multiplier")
	cfg.TCDecisionRetention = viper.GetDuration("tc-decision-retention")
	cfg.TCClientBundlePath = viper.GetString("tc-client-bundle")
	cfg.S3SSE = viper.GetString("s3-sse")
	cfg.S3KMSKeyID = viper.GetString("s3-kms-key-id")
	cfg.DisableMemQueueWatch = viper.GetBool("disable-mem-queue-watch")
	cfg.DisableStorageEncryption = viper.GetBool("disable-storage-encryption")
	if viper.IsSet("storage-encryption") {
		cfg.DisableStorageEncryption = !viper.GetBool("storage-encryption")
	}
	cfg.DisableKryptoPool = viper.GetBool("disable-krypto-pool")
	cfg.StorageEncryptionSnappy = viper.GetBool("storage-encryption-snappy")
	if cfg.DisableStorageEncryption {
		cfg.StorageEncryptionSnappy = false
	}
	if partSize := viper.GetString("s3-max-part-size"); partSize != "" {
		size, err := humanize.ParseBytes(partSize)
		if err != nil {
			return fmt.Errorf("parse s3-max-part-size: %w", err)
		}
		cfg.S3MaxPartSize = int64(size)
	}
	if budget := viper.GetString("s3-encrypt-buffer-budget"); budget != "" {
		size, err := humanize.ParseBytes(budget)
		if err != nil {
			return fmt.Errorf("parse s3-encrypt-buffer-budget: %w", err)
		}
		cfg.S3SmallEncryptBufferBudget = int64(size)
	}
	cfg.AWSRegion = strings.TrimSpace(viper.GetString("aws-region"))
	cfg.AWSKMSKeyID = strings.TrimSpace(viper.GetString("aws-kms-key-id"))
	if cfg.AWSRegion == "" {
		if v := strings.TrimSpace(os.Getenv("LOCKD_AWS_REGION")); v != "" {
			cfg.AWSRegion = v
		} else if v := strings.TrimSpace(os.Getenv("AWS_REGION")); v != "" {
			cfg.AWSRegion = v
		} else if v := strings.TrimSpace(os.Getenv("AWS_DEFAULT_REGION")); v != "" {
			cfg.AWSRegion = v
		}
	}
	if cfg.AWSKMSKeyID == "" {
		if v := strings.TrimSpace(os.Getenv("LOCKD_AWS_KMS_KEY_ID")); v != "" {
			cfg.AWSKMSKeyID = v
		} else if v := strings.TrimSpace(os.Getenv("AWS_KMS_KEY_ID")); v != "" {
			cfg.AWSKMSKeyID = v
		}
	}
	cfg.AzureAccountKey = viper.GetString("azure-key")
	cfg.AzureEndpoint = viper.GetString("azure-endpoint")
	cfg.AzureSASToken = viper.GetString("azure-sas-token")
	cfg.StorageRetryMaxAttempts = viper.GetInt("storage-retry-attempts")
	cfg.StorageRetryBaseDelay = viper.GetDuration("storage-retry-base-delay")
	cfg.StorageRetryMaxDelay = viper.GetDuration("storage-retry-max-delay")
	cfg.StorageRetryMultiplier = viper.GetFloat64("storage-retry-multiplier")
	cfg.QueueMaxConsumers = viper.GetInt("queue-max-consumers")
	cfg.QueuePollInterval = viper.GetDuration("queue-poll-interval")
	cfg.QueuePollJitter = viper.GetDuration("queue-poll-jitter")
	cfg.QueueResilientPollInterval = viper.GetDuration("queue-resilient-poll-interval")
	cfg.QueueListPageSize = viper.GetInt("queue-list-page-size")
	cfg.IndexerFlushDocs = viper.GetInt("indexer-flush-docs")
	if cfg.IndexerFlushDocs <= 0 {
		cfg.IndexerFlushDocs = lockd.DefaultIndexerFlushDocs
	}
	cfg.IndexerFlushInterval = viper.GetDuration("indexer-flush-interval")
	if cfg.IndexerFlushInterval <= 0 {
		cfg.IndexerFlushInterval = lockd.DefaultIndexerFlushInterval
	}
	cfg.QueryDocPrefetch = viper.GetInt("query-doc-prefetch")
	cfg.ConnguardEnabled = viper.GetBool("connguard-enabled")
	cfg.ConnguardEnabledSet = viper.IsSet("connguard-enabled")
	cfg.ConnguardFailureThreshold = viper.GetInt("connguard-failure-threshold")
	cfg.ConnguardFailureWindow = viper.GetDuration("connguard-failure-window")
	cfg.ConnguardBlockDuration = viper.GetDuration("connguard-block-duration")
	cfg.ConnguardProbeTimeout = viper.GetDuration("connguard-probe-timeout")
	cfg.LSFSampleInterval = viper.GetDuration("lsf-sample-interval")
	cfg.LSFLogInterval = viper.GetDuration("lsf-log-interval")
	cfg.LSFLogIntervalSet = true
	cfg.QRFDisabled = viper.GetBool("qrf-disabled")
	cfg.QRFQueueSoftLimit = viper.GetInt64("qrf-queue-soft-limit")
	cfg.QRFQueueHardLimit = viper.GetInt64("qrf-queue-hard-limit")
	cfg.QRFQueueConsumerSoftLimit = viper.GetInt64("qrf-queue-consumer-soft-limit")
	cfg.QRFQueueConsumerHardLimit = viper.GetInt64("qrf-queue-consumer-hard-limit")
	cfg.QRFLockSoftLimit = viper.GetInt64("qrf-lock-soft-limit")
	cfg.QRFLockHardLimit = viper.GetInt64("qrf-lock-hard-limit")
	cfg.QRFQuerySoftLimit = viper.GetInt64("qrf-query-soft-limit")
	cfg.QRFQueryHardLimit = viper.GetInt64("qrf-query-hard-limit")
	if softMem := viper.GetString("qrf-memory-soft-limit"); softMem != "" {
		bytes, err := humanize.ParseBytes(softMem)
		if err != nil {
			return fmt.Errorf("parse qrf-memory-soft-limit: %w", err)
		}
		cfg.QRFMemorySoftLimitBytes = bytes
	}
	if hardMem := viper.GetString("qrf-memory-hard-limit"); hardMem != "" {
		bytes, err := humanize.ParseBytes(hardMem)
		if err != nil {
			return fmt.Errorf("parse qrf-memory-hard-limit: %w", err)
		}
		cfg.QRFMemoryHardLimitBytes = bytes
	}
	cfg.QRFMemorySoftLimitPercent = viper.GetFloat64("qrf-memory-soft-limit-percent")
	cfg.QRFMemoryHardLimitPercent = viper.GetFloat64("qrf-memory-hard-limit-percent")
	cfg.QRFMemoryStrictHeadroomPercent = viper.GetFloat64("qrf-memory-strict-headroom-percent")
	if softSwap := viper.GetString("qrf-swap-soft-limit"); softSwap != "" {
		bytes, err := humanize.ParseBytes(softSwap)
		if err != nil {
			return fmt.Errorf("parse qrf-swap-soft-limit: %w", err)
		}
		cfg.QRFSwapSoftLimitBytes = bytes
	}
	if hardSwap := viper.GetString("qrf-swap-hard-limit"); hardSwap != "" {
		bytes, err := humanize.ParseBytes(hardSwap)
		if err != nil {
			return fmt.Errorf("parse qrf-swap-hard-limit: %w", err)
		}
		cfg.QRFSwapHardLimitBytes = bytes
	}
	cfg.QRFSwapSoftLimitPercent = viper.GetFloat64("qrf-swap-soft-limit-percent")
	cfg.QRFSwapHardLimitPercent = viper.GetFloat64("qrf-swap-hard-limit-percent")
	cfg.QRFCPUPercentSoftLimit = viper.GetFloat64("qrf-cpu-soft-limit")
	cfg.QRFCPUPercentHardLimit = viper.GetFloat64("qrf-cpu-hard-limit")
	cfg.QRFCPUPercentSoftLimitSet = viper.IsSet("qrf-cpu-soft-limit")
	cfg.QRFCPUPercentHardLimitSet = viper.IsSet("qrf-cpu-hard-limit")
	cfg.QRFLoadSoftLimitMultiplier = viper.GetFloat64("qrf-load-soft-limit-multiplier")
	cfg.QRFLoadHardLimitMultiplier = viper.GetFloat64("qrf-load-hard-limit-multiplier")
	cfg.QRFRecoverySamples = viper.GetInt("qrf-recovery-samples")
	cfg.QRFSoftDelay = viper.GetDuration("qrf-soft-delay")
	cfg.QRFEngagedDelay = viper.GetDuration("qrf-engaged-delay")
	cfg.QRFRecoveryDelay = viper.GetDuration("qrf-recovery-delay")
	cfg.QRFMaxWait = viper.GetDuration("qrf-max-wait")
	cfg.DiskQueueWatch = viper.GetBool("disk-queue-watch")
	cfg.OTLPEndpoint = viper.GetString("otlp-endpoint")
	return nil
}

func withSignalCancel(ctx context.Context) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-signals:
			cancel()
		case <-ctx.Done():
		}
		signal.Stop(signals)
	}()
	return ctx
}
