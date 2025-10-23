package lockd

import (
	"testing"
	"time"
)

func TestConfigValidateDefaults(t *testing.T) {
	cfg := Config{Store: "mem://"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Listen == "" {
		t.Fatal("expected listen default")
	}
	if cfg.ListenProto != "tcp" {
		t.Fatalf("expected listen proto default tcp, got %s", cfg.ListenProto)
	}
	if cfg.JSONMaxBytes == 0 {
		t.Fatal("expected json max default")
	}
	if cfg.JSONUtil != JSONUtilLockd {
		t.Fatalf("expected json util default %q, got %q", JSONUtilLockd, cfg.JSONUtil)
	}
	if cfg.SpoolMemoryThreshold <= 0 {
		t.Fatal("expected payload spool memory default")
	}
	if cfg.DefaultTTL <= 0 || cfg.MaxTTL <= 0 {
		t.Fatal("expected ttl defaults")
	}
	if cfg.AcquireBlock <= 0 {
		t.Fatal("expected acquire block default")
	}
	if cfg.S3MaxPartSize <= 0 {
		t.Fatal("expected s3 max part size default")
	}
	if cfg.StorageRetryMaxAttempts <= 0 || cfg.StorageRetryBaseDelay <= 0 || cfg.StorageRetryMultiplier <= 0 {
		t.Fatal("expected storage retry defaults")
	}
	if !cfg.MemQueueWatch {
		t.Fatal("expected mem queue watch default enabled")
	}
}

func TestConfigValidateErrors(t *testing.T) {
	var cfg Config
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for missing store")
	}
	cfg = Config{Store: "mem://", DefaultTTL: 10 * time.Second, MaxTTL: 5 * time.Second}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for max ttl < default ttl")
	}
	cfg = Config{Store: "aws://bucket"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for missing aws region")
	}
	cfg = Config{Store: "mem://", JSONUtil: "nope"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid json util")
	}
}
