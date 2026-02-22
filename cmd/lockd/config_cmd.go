package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"pkt.systems/lockd"
)

func newConfigCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage lockd configuration files",
	}
	cmd.AddCommand(newConfigGenCommand())
	return cmd
}

func newConfigGenCommand() *cobra.Command {
	var outPath string
	var force bool
	var stdout bool
	defaultOutput := "$HOME/.lockd/config.yaml"
	if dir, err := lockd.DefaultConfigDir(); err == nil {
		defaultOutput = filepath.Join(dir, lockd.DefaultConfigFileName)
	}

	cmd := &cobra.Command{
		Use:   "gen",
		Short: "Generate a default lockd configuration file",
		RunE: func(cmd *cobra.Command, args []string) error {
			if stdout && outPath != "" {
				return fmt.Errorf("--stdout and --out are mutually exclusive")
			}
			if outPath == "" {
				dir, err := lockd.DefaultConfigDir()
				if err != nil {
					return fmt.Errorf("resolve config dir: %w", err)
				}
				outPath = filepath.Join(dir, "config.yaml")
			}

			data, err := defaultConfigYAML()
			if err != nil {
				return err
			}

			if stdout {
				fmt.Print(string(data))
				return nil
			}

			if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
				return fmt.Errorf("create config dir: %w", err)
			}
			if !force {
				if _, err := os.Stat(outPath); err == nil {
					return fmt.Errorf("config file %s already exists (use --force to overwrite)", outPath)
				} else if !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("stat config file: %w", err)
				}
			}
			if err := os.WriteFile(outPath, data, 0o600); err != nil {
				return fmt.Errorf("write config file: %w", err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "wrote default config to %s\n", outPath)
			return nil
		},
	}

	cmd.Flags().StringVar(&outPath, "out", "", fmt.Sprintf("output path for generated config (defaults to %s)", defaultOutput))
	cmd.Flags().BoolVar(&force, "force", false, "overwrite the target file if it already exists")
	cmd.Flags().BoolVar(&stdout, "stdout", false, "print the config to stdout instead of writing a file")
	return cmd
}

type configDefaults struct {
	Listen                    string   `yaml:"listen"`
	ListenProto               string   `yaml:"listen-proto"`
	Store                     string   `yaml:"store"`
	HAMode                    string   `yaml:"ha"`
	HALeaseTTL                string   `yaml:"ha-lease-ttl"`
	DefaultNamespace          string   `yaml:"default-namespace"`
	JSONMax                   string   `yaml:"json-max"`
	JSONUtil                  string   `yaml:"json-util"`
	PayloadSpoolMem           string   `yaml:"payload-spool-mem"`
	DefaultTTL                string   `yaml:"default-ttl"`
	MaxTTL                    string   `yaml:"max-ttl"`
	AcquireBlock              string   `yaml:"acquire-block"`
	SweeperInterval           string   `yaml:"sweeper-interval"`
	TxnReplayInterval         string   `yaml:"txn-replay-interval"`
	QueueDecisionCacheTTL     string   `yaml:"queue-decision-cache-ttl"`
	QueueDecisionMaxApply     int      `yaml:"queue-decision-max-apply"`
	QueueDecisionApplyTimeout string   `yaml:"queue-decision-apply-timeout"`
	IdleSweepGrace            string   `yaml:"idle-sweep-grace"`
	IdleSweepOpDelay          string   `yaml:"idle-sweep-op-delay"`
	IdleSweepMaxOps           int      `yaml:"idle-sweep-max-ops"`
	IdleSweepMaxRuntime       string   `yaml:"idle-sweep-max-runtime"`
	DrainGrace                string   `yaml:"drain-grace"`
	ShutdownTimeout           string   `yaml:"shutdown-timeout"`
	DisableMTLS               bool     `yaml:"disable-mtls"`
	HTTP2MaxConcurrentStreams int      `yaml:"http2-max-concurrent-streams"`
	DisableStorageEncryption  bool     `yaml:"disable-storage-encryption"`
	StorageEncryptionSnappy   bool     `yaml:"storage-encryption-snappy"`
	Bundle                    string   `yaml:"bundle"`
	DenylistPath              string   `yaml:"denylist-path"`
	LogstoreCommitMaxOps      int      `yaml:"logstore-commit-max-ops"`
	LogstoreSegmentSize       string   `yaml:"logstore-segment-size"`
	DiskLockFileCacheSize     int      `yaml:"disk-lock-file-cache-size"`
	DisableMemQueueWatch      bool     `yaml:"disable-mem-queue-watch"`
	TCTrustDir                string   `yaml:"tc-trust-dir"`
	TCDisableAuth             bool     `yaml:"tc-disable-auth"`
	TCAllowDefaultCA          bool     `yaml:"tc-allow-default-ca"`
	SelfEndpoint              string   `yaml:"self"`
	TCJoinEndpoints           []string `yaml:"tc-join"`
	TCFanoutTimeout           string   `yaml:"tc-fanout-timeout"`
	TCFanoutAttempts          int      `yaml:"tc-fanout-attempts"`
	TCFanoutBaseDelay         string   `yaml:"tc-fanout-base-delay"`
	TCFanoutMaxDelay          string   `yaml:"tc-fanout-max-delay"`
	TCFanoutMultiplier        float64  `yaml:"tc-fanout-multiplier"`
	TCDecisionRetention       string   `yaml:"tc-decision-retention"`
	TCClientBundle            string   `yaml:"tc-client-bundle"`
	MCPListen                 string   `yaml:"mcp.listen"`
	MCPServer                 string   `yaml:"mcp.server"`
	MCPClientBundle           string   `yaml:"mcp.client-bundle"`
	MCPBundle                 string   `yaml:"mcp.bundle"`
	MCPDisableTLS             bool     `yaml:"mcp.disable-tls"`
	MCPDisableMTLS            bool     `yaml:"mcp.disable-mtls"`
	MCPStateFile              string   `yaml:"mcp.state-file"`
	MCPRefreshStore           string   `yaml:"mcp.refresh-store"`
	MCPIssuer                 string   `yaml:"mcp.issuer"`
	MCPPath                   string   `yaml:"mcp.path"`
	MCPOAuthResourceURL       string   `yaml:"mcp.oauth-resource-url"`
	StoreSSE                  string   `yaml:"s3-sse"`
	StoreKMSKeyID             string   `yaml:"s3-kms-key-id"`
	StoreMaxPartSize          string   `yaml:"s3-max-part-size"`
	StoreEncryptBufferBudget  string   `yaml:"s3-encrypt-buffer-budget"`
	AWSRegion                 string   `yaml:"aws-region"`
	AWSKMSKeyID               string   `yaml:"aws-kms-key-id"`
	StorageRetryMaxAttempts   int      `yaml:"storage-retry-attempts"`
	StorageRetryBaseDelay     string   `yaml:"storage-retry-base-delay"`
	StorageRetryMaxDelay      string   `yaml:"storage-retry-max-delay"`
	StorageRetryMultiplier    float64  `yaml:"storage-retry-multiplier"`
	ConnguardEnabled          bool     `yaml:"connguard-enabled"`
	ConnguardFailureThreshold int      `yaml:"connguard-failure-threshold"`
	ConnguardFailureWindow    string   `yaml:"connguard-failure-window"`
	ConnguardBlockDuration    string   `yaml:"connguard-block-duration"`
	ConnguardProbeTimeout     string   `yaml:"connguard-probe-timeout"`
	LSFSampleInterval         string   `yaml:"lsf-sample-interval"`
	LSFLogInterval            string   `yaml:"lsf-log-interval"`
	LogLevel                  string   `yaml:"log-level"`
}

func configHumanizeBytes(n int64) string {
	return strings.ReplaceAll(humanize.Bytes(uint64(n)), " ", "")
}

func defaultConfigYAML(overrides ...func(*configDefaults)) ([]byte, error) {
	tcTrustDir := ""
	if dir, err := lockd.DefaultTCTrustDir(); err == nil {
		tcTrustDir = dir
	}
	defaults := configDefaults{
		Listen:                    lockd.DefaultListen,
		ListenProto:               lockd.DefaultListenProto,
		Store:                     lockd.DefaultStore,
		HAMode:                    lockd.DefaultHAMode,
		HALeaseTTL:                lockd.DefaultHALeaseTTL.String(),
		DefaultNamespace:          lockd.DefaultNamespace,
		JSONMax:                   configHumanizeBytes(lockd.DefaultJSONMaxBytes),
		JSONUtil:                  lockd.JSONUtilLockd,
		PayloadSpoolMem:           configHumanizeBytes(lockd.DefaultPayloadSpoolMemoryThreshold),
		DefaultTTL:                lockd.DefaultDefaultTTL.String(),
		MaxTTL:                    lockd.DefaultMaxTTL.String(),
		AcquireBlock:              lockd.DefaultAcquireBlock.String(),
		SweeperInterval:           lockd.DefaultSweeperInterval.String(),
		TxnReplayInterval:         lockd.DefaultTxnReplayInterval.String(),
		QueueDecisionCacheTTL:     lockd.DefaultQueueDecisionCacheTTL.String(),
		QueueDecisionMaxApply:     lockd.DefaultQueueDecisionMaxApply,
		QueueDecisionApplyTimeout: lockd.DefaultQueueDecisionApplyTimeout.String(),
		IdleSweepGrace:            lockd.DefaultIdleSweepGrace.String(),
		IdleSweepOpDelay:          lockd.DefaultIdleSweepOpDelay.String(),
		IdleSweepMaxOps:           lockd.DefaultIdleSweepMaxOps,
		IdleSweepMaxRuntime:       lockd.DefaultIdleSweepMaxRuntime.String(),
		DrainGrace:                lockd.DefaultDrainGrace.String(),
		ShutdownTimeout:           lockd.DefaultShutdownTimeout.String(),
		DisableMTLS:               false,
		HTTP2MaxConcurrentStreams: lockd.DefaultMaxConcurrentStreams,
		DisableStorageEncryption:  false,
		StorageEncryptionSnappy:   false,
		Bundle:                    "",
		DenylistPath:              "",
		LogstoreCommitMaxOps:      lockd.DefaultLogstoreCommitMaxOps,
		LogstoreSegmentSize:       configHumanizeBytes(lockd.DefaultLogstoreSegmentSize),
		DiskLockFileCacheSize:     lockd.DefaultDiskLockFileCacheSize,
		DisableMemQueueWatch:      false,
		TCTrustDir:                tcTrustDir,
		TCDisableAuth:             false,
		TCAllowDefaultCA:          false,
		SelfEndpoint:              "",
		TCJoinEndpoints:           nil,
		TCFanoutTimeout:           lockd.DefaultTCFanoutTimeout.String(),
		TCFanoutAttempts:          lockd.DefaultTCFanoutMaxAttempts,
		TCFanoutBaseDelay:         lockd.DefaultTCFanoutBaseDelay.String(),
		TCFanoutMaxDelay:          lockd.DefaultTCFanoutMaxDelay.String(),
		TCFanoutMultiplier:        lockd.DefaultTCFanoutMultiplier,
		TCDecisionRetention:       lockd.DefaultTCDecisionRetention.String(),
		TCClientBundle:            "",
		MCPListen:                 "127.0.0.1:19341",
		MCPServer:                 "https://127.0.0.1:9341",
		MCPClientBundle:           "",
		MCPBundle:                 "",
		MCPDisableTLS:             false,
		MCPDisableMTLS:            false,
		MCPStateFile:              "",
		MCPRefreshStore:           "",
		MCPIssuer:                 "",
		MCPPath:                   "/mcp",
		MCPOAuthResourceURL:       "",
		StoreSSE:                  "",
		StoreKMSKeyID:             "",
		StoreMaxPartSize:          configHumanizeBytes(lockd.DefaultS3MaxPartSize),
		StoreEncryptBufferBudget:  configHumanizeBytes(lockd.DefaultS3SmallEncryptBufferBudget),
		AWSRegion:                 "",
		AWSKMSKeyID:               "",
		StorageRetryMaxAttempts:   lockd.DefaultStorageRetryMaxAttempts,
		StorageRetryBaseDelay:     lockd.DefaultStorageRetryBaseDelay.String(),
		StorageRetryMaxDelay:      lockd.DefaultStorageRetryMaxDelay.String(),
		StorageRetryMultiplier:    lockd.DefaultStorageRetryMultiplier,
		ConnguardEnabled:          true,
		ConnguardFailureThreshold: lockd.DefaultConnguardFailureThreshold,
		ConnguardFailureWindow:    lockd.DefaultConnguardFailureWindow.String(),
		ConnguardBlockDuration:    lockd.DefaultConnguardBlockDuration.String(),
		ConnguardProbeTimeout:     lockd.DefaultConnguardProbeTimeout.String(),
		LSFSampleInterval:         lockd.DefaultLSFSampleInterval.String(),
		LSFLogInterval:            lockd.DefaultLSFLogInterval.String(),
		LogLevel:                  "info",
	}
	for _, fn := range overrides {
		if fn != nil {
			fn(&defaults)
		}
	}

	out, err := yaml.Marshal(&defaults)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	return out, nil
}
