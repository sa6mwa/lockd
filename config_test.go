package lockd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTestBundleFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "server.pem")
	if err := os.WriteFile(path, []byte("pem"), 0o600); err != nil {
		t.Fatalf("write test bundle: %v", err)
	}
	return path
}

func TestConfigValidateDefaults(t *testing.T) {
	cfg := Config{Store: "mem://", BundlePath: writeTestBundleFile(t)}
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
	if cfg.SweeperInterval != DefaultSweeperInterval {
		t.Fatalf("expected sweeper interval default %s, got %s", DefaultSweeperInterval, cfg.SweeperInterval)
	}
	if cfg.TxnReplayInterval != DefaultTxnReplayInterval {
		t.Fatalf("expected txn replay interval default %s, got %s", DefaultTxnReplayInterval, cfg.TxnReplayInterval)
	}
	if cfg.HTTP2MaxConcurrentStreams != DefaultMaxConcurrentStreams {
		t.Fatalf("expected http2 max concurrent streams default %d, got %d", DefaultMaxConcurrentStreams, cfg.HTTP2MaxConcurrentStreams)
	}
	if cfg.LogstoreCommitMaxOps != DefaultLogstoreCommitMaxOps {
		t.Fatalf("expected logstore commit max ops default %d, got %d", DefaultLogstoreCommitMaxOps, cfg.LogstoreCommitMaxOps)
	}
	if !cfg.LogstoreCompactionEnabled {
		t.Fatal("expected logstore compaction enabled by default")
	}
	if cfg.LogstoreCompactionInterval != DefaultLogstoreCompactionInterval {
		t.Fatalf("expected logstore compaction interval %s, got %s", DefaultLogstoreCompactionInterval, cfg.LogstoreCompactionInterval)
	}
	if cfg.LogstoreCompactionMinSegments != DefaultLogstoreCompactionMinSegments {
		t.Fatalf("expected logstore compaction min segments %d, got %d", DefaultLogstoreCompactionMinSegments, cfg.LogstoreCompactionMinSegments)
	}
	if cfg.LogstoreCompactionMinReclaimBytes != DefaultLogstoreCompactionMinReclaimBytes {
		t.Fatalf("expected logstore compaction min reclaim bytes %d, got %d", DefaultLogstoreCompactionMinReclaimBytes, cfg.LogstoreCompactionMinReclaimBytes)
	}
	if cfg.LogstoreCompactionDeleteGrace != DefaultLogstoreCompactionDeleteGrace {
		t.Fatalf("expected logstore compaction delete grace %s, got %s", DefaultLogstoreCompactionDeleteGrace, cfg.LogstoreCompactionDeleteGrace)
	}
	if cfg.LogstoreCompactionMaxIOBytesPerSec != DefaultLogstoreCompactionMaxIOBytesPerSec {
		t.Fatalf("expected logstore compaction max io bytes/sec %d, got %d", DefaultLogstoreCompactionMaxIOBytesPerSec, cfg.LogstoreCompactionMaxIOBytesPerSec)
	}
	if cfg.S3MaxPartSize <= 0 {
		t.Fatal("expected s3 max part size default")
	}
	if cfg.StorageRetryMaxAttempts <= 0 || cfg.StorageRetryBaseDelay <= 0 || cfg.StorageRetryMultiplier <= 0 {
		t.Fatal("expected storage retry defaults")
	}
	if cfg.DisableMemQueueWatch {
		t.Fatal("expected mem queue watch default enabled (disable flag false)")
	}
	if cfg.DefaultNamespace != DefaultNamespace {
		t.Fatalf("expected default namespace %q, got %q", DefaultNamespace, cfg.DefaultNamespace)
	}
	if cfg.HAMode != DefaultHAMode {
		t.Fatalf("expected ha mode %q, got %q", DefaultHAMode, cfg.HAMode)
	}
	if cfg.HALeaseTTL != DefaultHALeaseTTL {
		t.Fatalf("expected ha lease ttl %s, got %s", DefaultHALeaseTTL, cfg.HALeaseTTL)
	}
	if cfg.HASinglePresenceTTL != DefaultHASinglePresenceTTL {
		t.Fatalf("expected ha single presence ttl %s, got %s", DefaultHASinglePresenceTTL, cfg.HASinglePresenceTTL)
	}
}

func TestConfigValidateAcceptsSingleAndAutoHAModes(t *testing.T) {
	for _, mode := range []string{"single", "auto"} {
		cfg := Config{Store: "mem://", HAMode: mode, BundlePath: writeTestBundleFile(t)}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("validate %s: %v", mode, err)
		}
		if cfg.HAMode != mode {
			t.Fatalf("expected ha mode %q, got %q", mode, cfg.HAMode)
		}
	}
}

func TestConfigValidateHALeaseTTLMinimum(t *testing.T) {
	cfg := Config{
		Listen:     "127.0.0.1:0",
		Store:      "mem://",
		BundlePath: writeTestBundleFile(t),
		HALeaseTTL: 4 * time.Second,
	}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "ha lease ttl must be >= 5s") {
		t.Fatalf("expected ha lease ttl minimum error, got %v", err)
	}

	cfg = Config{
		Listen:     "127.0.0.1:0",
		Store:      "mem://",
		BundlePath: writeTestBundleFile(t),
		HALeaseTTL: 5 * time.Second,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.HALeaseTTL != 5*time.Second {
		t.Fatalf("expected ha lease ttl to remain 5s, got %s", cfg.HALeaseTTL)
	}
}

func TestConfigValidateDisableCompactionThrottlingSetsZeroIOLimit(t *testing.T) {
	cfg := Config{
		Listen:                              "127.0.0.1:0",
		Store:                               "mem://",
		BundlePath:                          writeTestBundleFile(t),
		DisableLogstoreCompactionThrottling: true,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.LogstoreCompactionMaxIOBytesPerSec != 0 {
		t.Fatalf("expected disabled compaction throttling to force 0 io limit, got %d", cfg.LogstoreCompactionMaxIOBytesPerSec)
	}
}

func TestConfigHTTP2MaxConcurrentStreamsZero(t *testing.T) {
	cfg := Config{
		BundlePath:                   writeTestBundleFile(t),
		Store:                        "mem://",
		HTTP2MaxConcurrentStreams:    0,
		HTTP2MaxConcurrentStreamsSet: true,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.HTTP2MaxConcurrentStreams != 0 {
		t.Fatalf("expected http2 max concurrent streams to stay 0, got %d", cfg.HTTP2MaxConcurrentStreams)
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
	cfg = Config{Store: "mem://", BundlePath: writeTestBundleFile(t), HALeaseTTL: 4 * time.Second}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for ha lease ttl below minimum")
	}
	cfg = Config{Store: "mem://", DefaultNamespace: "Invalid Space"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid default namespace")
	}
	cfg = Config{Store: "mem://", TxnReplayInterval: -1}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for negative txn replay interval")
	}
	cfg = Config{Store: "mem://", BundlePath: writeTestBundleFile(t), LogstoreCompactionInterval: -1}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for negative logstore compaction interval")
	}
	cfg = Config{Store: "mem://", BundlePath: writeTestBundleFile(t), LogstoreCompactionMinSegments: -1}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for negative logstore compaction min segments")
	}
	cfg = Config{Store: "mem://", BundlePath: writeTestBundleFile(t), LogstoreCompactionMinReclaimBytes: -1}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for negative logstore compaction min reclaim bytes")
	}
	cfg = Config{Store: "mem://", BundlePath: writeTestBundleFile(t), LogstoreCompactionDeleteGrace: -1}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for negative logstore compaction delete grace")
	}
	cfg = Config{Store: "mem://", BundlePath: writeTestBundleFile(t), LogstoreCompactionMaxIOBytesPerSec: -1}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for negative logstore compaction max io bytes/sec")
	}
	cfg = Config{
		Store:               "mem://",
		ListenProto:         "unix",
		ConnguardEnabled:    true,
		ConnguardEnabledSet: true,
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for connguard on unix listener")
	}
}

func TestConfigValidateConnguardDisabledForUnix(t *testing.T) {
	cfg := Config{
		BundlePath:  writeTestBundleFile(t),
		Store:       "mem://",
		ListenProto: "unix",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.ConnguardEnabled {
		t.Fatal("expected connguard to be disabled for unix listener")
	}
	if cfg.ConnguardProbeTimeout != 0 {
		t.Fatalf("expected connguard probe timeout 0 for unix listener, got %s", cfg.ConnguardProbeTimeout)
	}
}

func TestConfigValidateConnguardExplicitlyDisabledForUnix(t *testing.T) {
	cfg := Config{
		BundlePath:          writeTestBundleFile(t),
		Store:               "mem://",
		ListenProto:         "unix",
		ConnguardEnabled:    false,
		ConnguardEnabledSet: true,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.ConnguardEnabled {
		t.Fatal("expected connguard to remain disabled for unix listener")
	}
}

func TestConfigTxnReplayIntervalDefaultsToSweeper(t *testing.T) {
	cfg := Config{
		BundlePath:      writeTestBundleFile(t),
		Store:           "mem://",
		SweeperInterval: 2 * time.Second,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.TxnReplayInterval != cfg.SweeperInterval {
		t.Fatalf("expected txn replay interval %s, got %s", cfg.SweeperInterval, cfg.TxnReplayInterval)
	}
}

func TestConfigValidateJoinRequiresMTLS(t *testing.T) {
	cfg := Config{
		Store:           "mem://",
		DisableMTLS:     true,
		SelfEndpoint:    "http://self",
		TCJoinEndpoints: []string{"http://self", "http://peer"},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for tc-join without mTLS")
	}
}

func TestConfigValidateJoinSelfOnlyWithoutMTLS(t *testing.T) {
	cfg := Config{
		Store:                    "mem://",
		DisableMTLS:              true,
		DisableStorageEncryption: true,
		SelfEndpoint:             "http://self",
		TCJoinEndpoints:          []string{"http://self"},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected tc-join self-only to pass without mTLS, got %v", err)
	}
}

func TestConfigValidateBundlePathExpandsEnv(t *testing.T) {
	dir := t.TempDir()
	bundle := filepath.Join(dir, "server.pem")
	if err := os.WriteFile(bundle, []byte("pem"), 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}
	t.Setenv("LOCKD_TEST_BUNDLE_DIR", dir)
	cfg := Config{
		Store:      "mem://",
		BundlePath: "$LOCKD_TEST_BUNDLE_DIR/server.pem",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.BundlePath != bundle {
		t.Fatalf("expected expanded bundle path %q, got %q", bundle, cfg.BundlePath)
	}
}

func TestConfigValidateBundlePathExpandsHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	bundle := filepath.Join(home, ".lockd", "server.pem")
	if err := os.MkdirAll(filepath.Dir(bundle), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(bundle, []byte("pem"), 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}
	cfg := Config{
		Store:      "mem://",
		BundlePath: "~/.lockd/server.pem",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.BundlePath != bundle {
		t.Fatalf("expected expanded bundle path %q, got %q", bundle, cfg.BundlePath)
	}
}

func TestConfigValidateBundlePathDisableExpansion(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("LOCKD_TEST_BUNDLE_DIR", dir)
	cfg := Config{
		Store:                      "mem://",
		BundlePath:                 "$LOCKD_TEST_BUNDLE_DIR/server.pem",
		BundlePathDisableExpansion: true,
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error for unexpanded bundle path")
	}
	if !strings.Contains(err.Error(), "$LOCKD_TEST_BUNDLE_DIR/server.pem") {
		t.Fatalf("expected literal bundle path in error, got %v", err)
	}
}
