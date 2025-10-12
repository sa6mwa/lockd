package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

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

	cmd.Flags().StringVar(&outPath, "out", "", "output path for generated config (defaults to $HOME/.lockd/config.yaml)")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite the target file if it already exists")
	cmd.Flags().BoolVar(&stdout, "stdout", false, "print the config to stdout instead of writing a file")
	return cmd
}

type configDefaults struct {
	Listen                  string  `yaml:"listen"`
	ListenProto             string  `yaml:"listen-proto"`
	Store                   string  `yaml:"store"`
	JSONMax                 string  `yaml:"json-max"`
	JSONUtil                string  `yaml:"json-util"`
	PayloadSpoolMem         string  `yaml:"payload-spool-mem"`
	DefaultTTL              string  `yaml:"default-ttl"`
	MaxTTL                  string  `yaml:"max-ttl"`
	AcquireBlock            string  `yaml:"acquire-block"`
	SweeperInterval         string  `yaml:"sweeper-interval"`
	MTLS                    bool    `yaml:"mtls"`
	Bundle                  string  `yaml:"bundle"`
	DenylistPath            string  `yaml:"denylist-path"`
	StoreRegion             string  `yaml:"s3-region"`
	StoreEndpoint           string  `yaml:"s3-endpoint"`
	StoreSSE                string  `yaml:"s3-sse"`
	StoreKMSKeyID           string  `yaml:"s3-kms-key-id"`
	StoreMaxPartSize        string  `yaml:"s3-max-part-size"`
	StoreForcePath          bool    `yaml:"s3-path-style"`
	StoreDisableTLS         bool    `yaml:"s3-disable-tls"`
	StorageRetryMaxAttempts int     `yaml:"storage-retry-attempts"`
	StorageRetryBaseDelay   string  `yaml:"storage-retry-base-delay"`
	StorageRetryMaxDelay    string  `yaml:"storage-retry-max-delay"`
	StorageRetryMultiplier  float64 `yaml:"storage-retry-multiplier"`
	LogLevel                string  `yaml:"log-level"`
}

func defaultConfigYAML() ([]byte, error) {
	defaults := configDefaults{
		Listen:                  ":9341",
		ListenProto:             "tcp",
		Store:                   "mem://",
		JSONMax:                 "100MB",
		JSONUtil:                lockd.JSONUtilLockd,
		PayloadSpoolMem:         "4MB",
		DefaultTTL:              (30 * time.Second).String(),
		MaxTTL:                  (5 * time.Minute).String(),
		AcquireBlock:            (60 * time.Second).String(),
		SweeperInterval:         (5 * time.Second).String(),
		MTLS:                    true,
		Bundle:                  "",
		DenylistPath:            "",
		StoreRegion:             "",
		StoreEndpoint:           "",
		StoreSSE:                "",
		StoreKMSKeyID:           "",
		StoreMaxPartSize:        "16MB",
		StoreForcePath:          false,
		StoreDisableTLS:         false,
		StorageRetryMaxAttempts: 6,
		StorageRetryBaseDelay:   (100 * time.Millisecond).String(),
		StorageRetryMaxDelay:    (5 * time.Second).String(),
		StorageRetryMultiplier:  2.0,
		LogLevel:                "info",
	}

	out, err := yaml.Marshal(&defaults)
	if err != nil {
		return nil, fmt.Errorf("marshal config: %w", err)
	}
	return out, nil
}
