package lockd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	JSONUtilLockd               = "lockd"
	JSONUtilJSONV2              = "jsonv2"
	JSONUtilStdlib              = "stdlib"
	defaultSpoolMemoryThreshold = 4 << 20
)

var jsonUtilChoices = []string{
	JSONUtilLockd,
	JSONUtilJSONV2,
	JSONUtilStdlib,
}

// ValidJSONUtils returns the supported jsonutil implementations.
func ValidJSONUtils() []string {
	out := make([]string, len(jsonUtilChoices))
	copy(out, jsonUtilChoices)
	return out
}

func isValidJSONUtil(name string) bool {
	for _, option := range jsonUtilChoices {
		if option == name {
			return true
		}
	}
	return false
}

// Config captures the tunables for a lockd.Server instance.
type Config struct {
	Listen                       string
	ListenProto                  string
	Store                        string
	JSONMaxBytes                 int64
	JSONUtil                     string
	SpoolMemoryThreshold         int64
	DiskRetention                time.Duration
	DiskJanitorInterval          time.Duration
	DefaultTTL                   time.Duration
	MaxTTL                       time.Duration
	AcquireBlock                 time.Duration
	SweeperInterval              time.Duration
	ForUpdateMaxStreams          int
	ForUpdateMaxStreamsPerClient int
	ForUpdateMaxHold             time.Duration

	// mTLS
	MTLS         bool
	BundlePath   string
	DenylistPath string

	// S3-specific options.
	S3Region      string
	S3Endpoint    string
	S3SSE         string
	S3KMSKeyID    string
	S3MaxPartSize int64
	S3ForcePath   bool
	S3DisableTLS  bool

	// Azure-specific options.
	AzureAccount    string
	AzureAccountKey string
	AzureEndpoint   string
	AzureSASToken   string

	// Storage retry tuning.
	StorageRetryMaxAttempts int
	StorageRetryBaseDelay   time.Duration
	StorageRetryMaxDelay    time.Duration
	StorageRetryMultiplier  float64
}

// Validate applies defaults and sanity-checks the configuration.
func (c *Config) Validate() error {
	if c.Listen == "" {
		c.Listen = ":9341"
	}
	if c.ListenProto == "" {
		c.ListenProto = "tcp"
	}
	if c.Store == "" {
		return fmt.Errorf("config: store is required")
	}
	if c.JSONMaxBytes <= 0 {
		c.JSONMaxBytes = 100 * 1024 * 1024 // 100 MB
	}
	if c.JSONUtil == "" {
		c.JSONUtil = JSONUtilLockd
	}
	if !isValidJSONUtil(c.JSONUtil) {
		return fmt.Errorf("config: unknown json util %q (options: %s)", c.JSONUtil, strings.Join(ValidJSONUtils(), ", "))
	}
	if c.SpoolMemoryThreshold <= 0 {
		c.SpoolMemoryThreshold = defaultSpoolMemoryThreshold
	}
	if c.DefaultTTL <= 0 {
		c.DefaultTTL = 30 * time.Second
	}
	if c.MaxTTL <= 0 {
		c.MaxTTL = 5 * time.Minute
	}
	if c.MaxTTL < c.DefaultTTL {
		return fmt.Errorf("config: max ttl must be >= default ttl")
	}
	if c.AcquireBlock <= 0 {
		c.AcquireBlock = 60 * time.Second
	}
	if c.SweeperInterval <= 0 {
		c.SweeperInterval = 5 * time.Second
	}
	if c.ForUpdateMaxStreams <= 0 {
		c.ForUpdateMaxStreams = 100
	}
	if c.ForUpdateMaxStreamsPerClient <= 0 {
		c.ForUpdateMaxStreamsPerClient = 3
	}
	if c.ForUpdateMaxHold <= 0 {
		c.ForUpdateMaxHold = 15 * time.Minute
	}
	if c.DiskRetention < 0 {
		return fmt.Errorf("config: disk retention must be >= 0")
	}
	if c.DiskJanitorInterval < 0 {
		return fmt.Errorf("config: disk janitor interval must be >= 0")
	}
	if c.DiskJanitorInterval == 0 {
		if c.DiskRetention > 0 {
			half := c.DiskRetention / 2
			if half < time.Minute {
				half = time.Minute
			}
			if half > time.Hour {
				half = time.Hour
			}
			c.DiskJanitorInterval = half
		} else {
			c.DiskJanitorInterval = time.Hour
		}
	}
	if c.S3MaxPartSize <= 0 {
		c.S3MaxPartSize = 16 * 1024 * 1024
	}
	if c.MTLS {
		if c.BundlePath == "" {
			path, err := DefaultBundlePath()
			if err != nil {
				return fmt.Errorf("config: resolve bundle path: %w", err)
			}
			c.BundlePath = path
		}
		if _, err := os.Stat(c.BundlePath); err != nil {
			return fmt.Errorf("config: bundle %q not found (run 'lockd auth new server')", c.BundlePath)
		}
	}

	storeLower := strings.ToLower(c.Store)
	if strings.HasPrefix(storeLower, "s3://") {
		if c.S3Region == "" && c.S3Endpoint == "" {
			return fmt.Errorf("config: s3 region or endpoint must be provided for store %q", c.Store)
		}
	}
	if strings.HasPrefix(storeLower, "s3://") || strings.HasPrefix(storeLower, "minio://") {
		if c.StorageRetryMaxAttempts <= 0 {
			c.StorageRetryMaxAttempts = 12
		}
		if c.StorageRetryBaseDelay <= 0 {
			c.StorageRetryBaseDelay = 500 * time.Millisecond
		}
		if c.StorageRetryMultiplier <= 0 {
			c.StorageRetryMultiplier = 2.0
		}
		if c.StorageRetryMaxDelay <= 0 {
			c.StorageRetryMaxDelay = 15 * time.Second
		}
	} else {
		if c.StorageRetryMaxAttempts <= 0 {
			c.StorageRetryMaxAttempts = 6
		}
		if c.StorageRetryBaseDelay <= 0 {
			c.StorageRetryBaseDelay = 100 * time.Millisecond
		}
		if c.StorageRetryMultiplier <= 0 {
			c.StorageRetryMultiplier = 2.0
		}
		if c.StorageRetryMaxDelay <= 0 {
			c.StorageRetryMaxDelay = 5 * time.Second
		}
	}
	return nil
}

// DefaultConfigDir returns the default configuration directory ($HOME/.lockd).
func DefaultConfigDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".lockd"), nil
}

// DefaultBundlePath returns the default server bundle location.
func DefaultBundlePath() (string, error) {
	dir, err := DefaultConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "server.pem"), nil
}
