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
	"github.com/spf13/viper"
	"pkt.systems/logport"
	"pkt.systems/logport/adapters/psl"

	"pkt.systems/lockd"
)

func submain(ctx context.Context) int {
	logger := psl.NewStructured(os.Stderr).WithLogLevel().With("app", "lockd")
	cmd := newRootCommand(logger)
	targetCmd := cmd
	if len(os.Args) > 1 {
		if found, _, err := cmd.Find(os.Args[1:]); err == nil && found != nil {
			targetCmd = found
		}
	}
	ctx = withSignalCancel(ctx)
	if err := cmd.ExecuteContext(ctx); err != nil {
		if err != context.Canceled {
			if targetCmd == cmd {
				logger.Error("command failed", "error", err)
			} else {
				fmt.Fprintf(os.Stderr, "%s\n", err)
			}
		}
		return 1
	}
	return 0
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

func newRootCommand(logger logport.ForLogging) *cobra.Command {
	var cfg lockd.Config
	var logLevel string

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
			ctx := cmd.Context()
			cmd.SilenceUsage = true

			configFile, err := loadConfigFile()
			if err != nil {
				return err
			}
			if configFile != "" {
				logger.Info("loaded config file", "path", configFile)
			}

			if err := bindConfig(&cfg); err != nil {
				return err
			}
			logLevel = viper.GetString("log-level")

			level, ok := logport.ParseLevel(logLevel)
			if ok {
				logger = logger.LogLevel(level)
			}
			logger.Info("starting lockd", "store", cfg.Store, "listen", cfg.Listen, "mtls", cfg.MTLS)

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
					logger.Error("shutdown failed", "error", err)
				}
			}()

			err = server.Start()
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		},
	}

	flags := cmd.Flags()
	flags.StringP("config", "c", "", "path to YAML config file (defaults to $HOME/.lockd/"+lockd.DefaultConfigFileName+")")
	flags.String("listen", ":9341", "listen address")
	flags.String("listen-proto", "tcp", "listen network (tcp, tcp4, tcp6)")
	flags.String("store", "", "storage backend URL (mem://, s3://host[:port]/bucket, aws://bucket, disk:///path)")
	jsonMaxDefault := humanizeBytes(lockd.DefaultJSONMaxBytes)
	spoolDefault := humanizeBytes(lockd.DefaultPayloadSpoolMemoryThreshold)
	s3PartDefault := humanizeBytes(lockd.DefaultS3MaxPartSize)
	flags.String("json-max", jsonMaxDefault, "maximum JSON payload size")
	flags.String("json-util", lockd.JSONUtilLockd, fmt.Sprintf("JSON compaction engine (%s)", strings.Join(lockd.ValidJSONUtils(), ", ")))
	flags.String("payload-spool-mem", spoolDefault, "bytes to buffer JSON bodies in memory before spooling to disk")
	flags.Duration("default-ttl", lockd.DefaultDefaultTTL, "default lease TTL")
	flags.Duration("max-ttl", lockd.DefaultMaxTTL, "maximum lease TTL")
	flags.Duration("acquire-block", lockd.DefaultAcquireBlock, "maximum time to wait on acquire conflicts")
	flags.Duration("sweeper-interval", lockd.DefaultSweeperInterval, "sweeper interval for background tasks")
	flags.Duration("disk-retention", 0, "optional retention window for disk backend (0 keeps state indefinitely)")
	flags.Duration("disk-janitor-interval", 0, "override janitor sweep interval for disk backend (default derived from retention)")
	flags.Bool("disk-queue-watch", true, "enable inotify-based queue events for disk backend (Linux only; falls back to polling on unsupported filesystems)")
	flags.Bool("mem-queue-watch", true, "enable in-memory queue change notifications")
	flags.Bool("storage-encryption", true, "encrypt metadata and state objects using kryptograf (disable for plaintext storage)")
	flags.Bool("storage-encryption-snappy", false, "enable Snappy compression before encrypting objects")
	flags.Bool("mtls", true, "enable mutual TLS")
	flags.String("bundle", "", "path to combined server bundle PEM")
	flags.String("denylist-path", "", "path to certificate denylist (optional)")
	flags.String("s3-sse", "", "server-side encryption mode for S3 objects")
	flags.String("s3-kms-key-id", "", "KMS key ID for S3 server-side encryption")
	flags.String("s3-max-part-size", s3PartDefault, "maximum S3 multipart upload part size")
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
	flags.Duration("lsf-sample-interval", lockd.DefaultLSFSampleInterval, "sampling interval for the local security force (LSF)")
	flags.Duration("lsf-log-interval", lockd.DefaultLSFLogInterval, "interval between LSF telemetry logs (set 0 to disable)")
	flags.Bool("qrf-enabled", true, "enable perimeter defence quick reaction force (QRF)")
	flags.Int64("qrf-queue-soft-limit", 0, "soft inflight threshold for queue operations (auto when 0)")
	flags.Int64("qrf-queue-hard-limit", 0, "hard inflight threshold for queue operations (auto when 0)")
	flags.Int64("qrf-queue-consumer-soft-limit", 0, "soft limit for concurrent queue consumers (auto when 0)")
	flags.Int64("qrf-queue-consumer-hard-limit", 0, "hard limit for concurrent queue consumers (auto when 0)")
	flags.Int64("qrf-lock-soft-limit", 0, "soft inflight threshold for lock operations (auto when 0)")
	flags.Int64("qrf-lock-hard-limit", 0, "hard inflight threshold for lock operations (auto when 0)")
	flags.String("qrf-memory-soft-limit", "", "approximate RSS soft limit before QRF engages (blank disables)")
	flags.String("qrf-memory-hard-limit", "", "approximate RSS hard limit triggering immediate QRF engagement (blank disables)")
	flags.Float64("qrf-memory-soft-limit-percent", lockd.DefaultQRFMemorySoftLimitPercent, "system memory usage percentage that soft-arms the QRF")
	flags.Float64("qrf-memory-hard-limit-percent", lockd.DefaultQRFMemoryHardLimitPercent, "system memory usage percentage that fully engages the QRF")
	flags.Float64("qrf-memory-strict-headroom-percent", lockd.DefaultQRFMemoryStrictHeadroomPercent, "headroom to subtract from system memory usage when reclaimable cache is unknown")
	flags.String("qrf-swap-soft-limit", "", "swap usage soft limit before QRF engages (e.g. 256MB; blank disables)")
	flags.String("qrf-swap-hard-limit", "", "swap usage hard limit that fully engages the QRF (blank disables)")
	flags.Float64("qrf-swap-soft-limit-percent", 0, "swap utilisation percentage that soft-arms the QRF (0 disables)")
	flags.Float64("qrf-swap-hard-limit-percent", 0, "swap utilisation percentage that fully engages the QRF (0 disables)")
	flags.Float64("qrf-cpu-soft-limit", 0, "legacy system CPU utilisation percentage that soft-arms the QRF (0 disables)")
	flags.Float64("qrf-cpu-hard-limit", 0, "legacy system CPU utilisation percentage that fully engages the QRF (0 disables)")
	flags.Float64("qrf-load-soft-limit-multiplier", lockd.DefaultQRFLoadSoftLimitMultiplier, "load average multiplier (relative to LSF baseline) that soft-arms the QRF")
	flags.Float64("qrf-load-hard-limit-multiplier", lockd.DefaultQRFLoadHardLimitMultiplier, "load average multiplier (relative to LSF baseline) that fully engages the QRF")
	flags.Int("qrf-recovery-samples", lockd.DefaultQRFRecoverySamples, "number of consecutive healthy samples before QRF disengages")
	flags.Duration("qrf-soft-retry", lockd.DefaultQRFSoftRetryAfter, "retry-after used when QRF is soft-armed")
	flags.Duration("qrf-engaged-retry", lockd.DefaultQRFEngagedRetryAfter, "retry-after used when QRF is fully engaged")
	flags.Duration("qrf-recovery-retry", lockd.DefaultQRFRecoveryRetryAfter, "retry-after used while QRF is recovering")
	flags.String("otlp-endpoint", "", "OTLP collector endpoint (e.g. grpc://localhost:4317)")
	flags.StringVar(&logLevel, "log-level", "info", "log level (trace,debug,info,...)")

	bindFlag := func(name string) {
		if err := viper.BindPFlag(name, flags.Lookup(name)); err != nil {
			panic(err)
		}
	}

	viper.SetEnvPrefix("LOCKD")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	names := []string{
		"config",
		"listen", "listen-proto", "store", "json-max", "json-util", "payload-spool-mem", "default-ttl", "max-ttl", "acquire-block",
		"sweeper-interval", "disk-retention", "disk-janitor-interval", "mtls", "bundle", "denylist-path", "s3-sse",
		"s3-kms-key-id", "s3-max-part-size", "aws-region", "aws-kms-key-id", "azure-key", "azure-endpoint", "azure-sas-token",
		"storage-retry-attempts", "storage-retry-base-delay", "storage-retry-max-delay", "storage-retry-multiplier",
		"queue-max-consumers", "queue-poll-interval", "queue-poll-jitter", "queue-resilient-poll-interval",
		"disk-queue-watch", "mem-queue-watch", "storage-encryption", "storage-encryption-snappy",
		"lsf-sample-interval", "lsf-log-interval",
		"qrf-enabled", "qrf-queue-soft-limit", "qrf-queue-hard-limit", "qrf-queue-consumer-soft-limit", "qrf-queue-consumer-hard-limit", "qrf-lock-soft-limit", "qrf-lock-hard-limit",
		"qrf-memory-soft-limit", "qrf-memory-hard-limit", "qrf-memory-soft-limit-percent", "qrf-memory-hard-limit-percent",
		"qrf-memory-strict-headroom-percent",
		"qrf-swap-soft-limit", "qrf-swap-hard-limit", "qrf-swap-soft-limit-percent", "qrf-swap-hard-limit-percent",
		"qrf-cpu-soft-limit", "qrf-cpu-hard-limit", "qrf-load-soft-limit-multiplier", "qrf-load-hard-limit-multiplier",
		"qrf-recovery-samples", "qrf-soft-retry", "qrf-engaged-retry", "qrf-recovery-retry",
		"otlp-endpoint", "log-level",
	}
	for _, name := range names {
		bindFlag(name)
	}

	cmd.AddCommand(newVerifyCommand(logger))
	cmd.AddCommand(newAuthCommand())
	cmd.AddCommand(newClientCommand())
	cmd.AddCommand(newConfigCommand())

	return cmd
}

func bindConfig(cfg *lockd.Config) error {
	cfg.Listen = viper.GetString("listen")
	cfg.ListenProto = viper.GetString("listen-proto")
	cfg.Store = viper.GetString("store")
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
	cfg.DefaultTTL = viper.GetDuration("default-ttl")
	cfg.MaxTTL = viper.GetDuration("max-ttl")
	cfg.AcquireBlock = viper.GetDuration("acquire-block")
	cfg.SweeperInterval = viper.GetDuration("sweeper-interval")
	cfg.DiskRetention = viper.GetDuration("disk-retention")
	cfg.DiskJanitorInterval = viper.GetDuration("disk-janitor-interval")
	cfg.MTLS = viper.GetBool("mtls")
	cfg.BundlePath = viper.GetString("bundle")
	cfg.DenylistPath = viper.GetString("denylist-path")
	cfg.S3SSE = viper.GetString("s3-sse")
	cfg.S3KMSKeyID = viper.GetString("s3-kms-key-id")
	cfg.MemQueueWatch = viper.GetBool("mem-queue-watch")
	cfg.MemQueueWatchSet = true
	cfg.StorageEncryptionEnabled = viper.GetBool("storage-encryption")
	cfg.StorageEncryptionSnappy = viper.GetBool("storage-encryption-snappy")
	if partSize := viper.GetString("s3-max-part-size"); partSize != "" {
		size, err := humanize.ParseBytes(partSize)
		if err != nil {
			return fmt.Errorf("parse s3-max-part-size: %w", err)
		}
		cfg.S3MaxPartSize = int64(size)
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
	cfg.LSFSampleInterval = viper.GetDuration("lsf-sample-interval")
	cfg.LSFLogInterval = viper.GetDuration("lsf-log-interval")
	cfg.LSFLogIntervalSet = true
	cfg.QRFEnabled = viper.GetBool("qrf-enabled")
	cfg.QRFQueueSoftLimit = viper.GetInt64("qrf-queue-soft-limit")
	cfg.QRFQueueHardLimit = viper.GetInt64("qrf-queue-hard-limit")
	cfg.QRFQueueConsumerSoftLimit = viper.GetInt64("qrf-queue-consumer-soft-limit")
	cfg.QRFQueueConsumerHardLimit = viper.GetInt64("qrf-queue-consumer-hard-limit")
	cfg.QRFLockSoftLimit = viper.GetInt64("qrf-lock-soft-limit")
	cfg.QRFLockHardLimit = viper.GetInt64("qrf-lock-hard-limit")
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
	cfg.QRFLoadSoftLimitMultiplier = viper.GetFloat64("qrf-load-soft-limit-multiplier")
	cfg.QRFLoadHardLimitMultiplier = viper.GetFloat64("qrf-load-hard-limit-multiplier")
	cfg.QRFRecoverySamples = viper.GetInt("qrf-recovery-samples")
	cfg.QRFSoftRetryAfter = viper.GetDuration("qrf-soft-retry")
	cfg.QRFEngagedRetryAfter = viper.GetDuration("qrf-engaged-retry")
	cfg.QRFRecoveryRetryAfter = viper.GetDuration("qrf-recovery-retry")
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
