package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	port "pkt.systems/logport"
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

func newRootCommand(logger port.ForLogging) *cobra.Command {
	var cfg lockd.Config
	var logLevel string

	cmd := &cobra.Command{
		Use:           "lockd",
		Short:         "lockd is a minimal lock + state coordination service",
		SilenceErrors: true,
		Example: `
  # AWS S3 backend (expects AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
  LOCKD_STORE=s3://my-bucket/prefix LOCKD_S3_REGION=us-west-2 lockd

  # MinIO backend (TLS on by default; use ?insecure=1 or LOCKD_S3_DISABLE_TLS=1 for HTTP)
  LOCKD_STORE=minio://localhost:9000/lockd-data?insecure=1 MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin lockd

  # Embedded Pebble storage
  lockd --store pebble:///var/lib/lockd --listen :9341

  # In-memory storage (tests/dev only)
  lockd --store mem://

  # Prefer stdlib JSON compaction for tiny payloads
  lockd --store mem:// --json-util stdlib
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cmd.SilenceUsage = true

			if err := bindConfig(&cfg); err != nil {
				return err
			}
			logLevel = viper.GetString("log-level")

			level, ok := port.ParseLevel(logLevel)
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
	flags.String("listen", ":9341", "listen address")
	flags.String("listen-proto", "tcp", "listen network (tcp, tcp4, tcp6)")
	flags.String("store", "", "storage backend URL (mem://, s3://bucket/prefix, minio://host:port/bucket, pebble:///path)")
	flags.String("json-max", "100MB", "maximum JSON payload size")
	flags.String("json-util", lockd.JSONUtilLockd, fmt.Sprintf("JSON compaction engine (%s)", strings.Join(lockd.ValidJSONUtils(), ", ")))
	flags.String("payload-spool-mem", "4MB", "bytes to keep JSON updates in memory before spooling to disk")
	flags.Duration("default-ttl", 30*time.Second, "default lease TTL")
	flags.Duration("max-ttl", 5*time.Minute, "maximum lease TTL")
	flags.Duration("acquire-block", 60*time.Second, "maximum time to wait on acquire conflicts")
	flags.Duration("sweeper-interval", 5*time.Second, "sweeper interval for background tasks")
	flags.Bool("mtls", true, "enable mutual TLS")
	flags.String("bundle", "", "path to combined server bundle PEM")
	flags.String("denylist-path", "", "path to certificate denylist (optional)")
	flags.String("s3-region", "", "AWS region for S3 backend")
	flags.String("s3-endpoint", "", "custom S3 endpoint override")
	flags.String("s3-sse", "", "server-side encryption mode for S3 objects")
	flags.String("s3-kms-key-id", "", "KMS key ID for S3 server-side encryption")
	flags.String("s3-max-part-size", "16MB", "maximum S3 multipart upload part size")
	flags.Bool("s3-path-style", false, "force path-style addressing for S3 requests")
	flags.Bool("s3-disable-tls", false, "disable TLS when connecting to the S3 endpoint (testing only)")
	flags.Int("storage-retry-attempts", 4, "maximum storage retry attempts")
	flags.Duration("storage-retry-base-delay", 50*time.Millisecond, "initial backoff for storage retries")
	flags.Duration("storage-retry-max-delay", 2*time.Second, "maximum backoff delay for storage retries")
	flags.Float64("storage-retry-multiplier", 2.0, "backoff multiplier for storage retries")
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
		"listen", "listen-proto", "store", "json-max", "json-util", "payload-spool-mem", "default-ttl", "max-ttl", "acquire-block",
		"sweeper-interval", "mtls", "bundle", "denylist-path", "s3-region",
		"s3-endpoint", "s3-sse", "s3-kms-key-id", "s3-max-part-size", "s3-path-style",
		"s3-disable-tls", "storage-retry-attempts", "storage-retry-base-delay",
		"storage-retry-max-delay", "storage-retry-multiplier", "log-level",
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
	cfg.MTLS = viper.GetBool("mtls")
	cfg.BundlePath = viper.GetString("bundle")
	cfg.DenylistPath = viper.GetString("denylist-path")
	cfg.S3Region = viper.GetString("s3-region")
	cfg.S3Endpoint = viper.GetString("s3-endpoint")
	cfg.S3SSE = viper.GetString("s3-sse")
	cfg.S3KMSKeyID = viper.GetString("s3-kms-key-id")
	if partSize := viper.GetString("s3-max-part-size"); partSize != "" {
		size, err := humanize.ParseBytes(partSize)
		if err != nil {
			return fmt.Errorf("parse s3-max-part-size: %w", err)
		}
		cfg.S3MaxPartSize = int64(size)
	}
	cfg.S3ForcePath = viper.GetBool("s3-path-style")
	cfg.S3DisableTLS = viper.GetBool("s3-disable-tls")
	cfg.StorageRetryMaxAttempts = viper.GetInt("storage-retry-attempts")
	cfg.StorageRetryBaseDelay = viper.GetDuration("storage-retry-base-delay")
	cfg.StorageRetryMaxDelay = viper.GetDuration("storage-retry-max-delay")
	cfg.StorageRetryMultiplier = viper.GetFloat64("storage-retry-multiplier")
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
