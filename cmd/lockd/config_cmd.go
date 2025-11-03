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
	Listen                   string  `yaml:"listen"`
	ListenProto              string  `yaml:"listen-proto"`
	Store                    string  `yaml:"store"`
	JSONMax                  string  `yaml:"json-max"`
	JSONUtil                 string  `yaml:"json-util"`
	PayloadSpoolMem          string  `yaml:"payload-spool-mem"`
	DefaultTTL               string  `yaml:"default-ttl"`
	MaxTTL                   string  `yaml:"max-ttl"`
	AcquireBlock             string  `yaml:"acquire-block"`
	SweeperInterval          string  `yaml:"sweeper-interval"`
	DrainGrace               string  `yaml:"drain-grace"`
	ShutdownTimeout          string  `yaml:"shutdown-timeout"`
	DisableMTLS              bool    `yaml:"disable-mtls"`
	DisableStorageEncryption bool    `yaml:"disable-storage-encryption"`
	StorageEncryptionSnappy  bool    `yaml:"storage-encryption-snappy"`
	Bundle                   string  `yaml:"bundle"`
	DenylistPath             string  `yaml:"denylist-path"`
	StoreSSE                 string  `yaml:"s3-sse"`
	StoreKMSKeyID            string  `yaml:"s3-kms-key-id"`
	StoreMaxPartSize         string  `yaml:"s3-max-part-size"`
	AWSRegion                string  `yaml:"aws-region"`
	AWSKMSKeyID              string  `yaml:"aws-kms-key-id"`
	StorageRetryMaxAttempts  int     `yaml:"storage-retry-attempts"`
	StorageRetryBaseDelay    string  `yaml:"storage-retry-base-delay"`
	StorageRetryMaxDelay     string  `yaml:"storage-retry-max-delay"`
	StorageRetryMultiplier   float64 `yaml:"storage-retry-multiplier"`
	LogLevel                 string  `yaml:"log-level"`
}

func configHumanizeBytes(n int64) string {
	return strings.ReplaceAll(humanize.Bytes(uint64(n)), " ", "")
}

func defaultConfigYAML() ([]byte, error) {
	defaults := configDefaults{
		Listen:                   lockd.DefaultListen,
		ListenProto:              lockd.DefaultListenProto,
		Store:                    lockd.DefaultStore,
		JSONMax:                  configHumanizeBytes(lockd.DefaultJSONMaxBytes),
		JSONUtil:                 lockd.JSONUtilLockd,
		PayloadSpoolMem:          configHumanizeBytes(lockd.DefaultPayloadSpoolMemoryThreshold),
		DefaultTTL:               lockd.DefaultDefaultTTL.String(),
		MaxTTL:                   lockd.DefaultMaxTTL.String(),
		AcquireBlock:             lockd.DefaultAcquireBlock.String(),
		SweeperInterval:          lockd.DefaultSweeperInterval.String(),
		DrainGrace:               lockd.DefaultDrainGrace.String(),
		ShutdownTimeout:          lockd.DefaultShutdownTimeout.String(),
		DisableMTLS:              false,
		DisableStorageEncryption: false,
		StorageEncryptionSnappy:  false,
		Bundle:                   "",
		DenylistPath:             "",
		StoreSSE:                 "",
		StoreKMSKeyID:            "",
		StoreMaxPartSize:         configHumanizeBytes(lockd.DefaultS3MaxPartSize),
		AWSRegion:                "",
		AWSKMSKeyID:              "",
		StorageRetryMaxAttempts:  lockd.DefaultStorageRetryMaxAttempts,
		StorageRetryBaseDelay:    lockd.DefaultStorageRetryBaseDelay.String(),
		StorageRetryMaxDelay:     lockd.DefaultStorageRetryMaxDelay.String(),
		StorageRetryMultiplier:   lockd.DefaultStorageRetryMultiplier,
		LogLevel:                 "info",
	}

	out, err := yaml.Marshal(&defaults)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	return out, nil
}
