package lockd

import (
	"fmt"
	"strings"
	"time"
)

// Config captures the tunables for a lockd.Server instance.
type Config struct {
	Listen          string
	Store           string
	JSONMaxBytes    int64
	DefaultTTL      time.Duration
	MaxTTL          time.Duration
	AcquireBlock    time.Duration
	SweeperInterval time.Duration

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

	// Storage retry tuning.
	StorageRetryMaxAttempts int
	StorageRetryBaseDelay   time.Duration
	StorageRetryMaxDelay    time.Duration
	StorageRetryMultiplier  float64
}

// Validate applies defaults and sanity-checks the configuration.
func (c *Config) Validate() error {
	if c.Listen == "" {
		c.Listen = ":8443"
	}
	if c.Store == "" {
		return fmt.Errorf("config: store is required")
	}
	if c.JSONMaxBytes <= 0 {
		c.JSONMaxBytes = 100 * 1024 * 1024 // 100 MB
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
	if c.S3MaxPartSize <= 0 {
		c.S3MaxPartSize = 16 * 1024 * 1024
	}
	if strings.HasPrefix(strings.ToLower(c.Store), "s3://") {
		if c.S3Region == "" && c.S3Endpoint == "" {
			return fmt.Errorf("config: s3 region or endpoint must be provided for store %q", c.Store)
		}
	}
	if strings.HasPrefix(strings.ToLower(c.Store), "s3://") {
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
