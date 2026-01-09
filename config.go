package lockd

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"

	"pkt.systems/kryptograf/keymgmt"
)

const (
	// JSONUtilLockd selects the native lockd streaming JSON compactor.
	JSONUtilLockd = "lockd"
	// JSONUtilJSONV2 enables the Go 1.25 json/v2 tokenizer pipeline.
	JSONUtilJSONV2 = "jsonv2"
	// JSONUtilStdlib opts into the encoding/json standard library implementation.
	JSONUtilStdlib = "stdlib"
)

const (
	defaultSpoolMemoryThreshold = 4 << 20
)

const (
	defaultQueueConsumersPerCPU = 64
	defaultQueueConsumersFloor  = 128
	defaultQueueConsumersCeil   = 4096
	// DefaultQueuePollInterval controls how often the dispatcher polls storage when no event hint exists.
	DefaultQueuePollInterval = 3 * time.Second
	// DefaultQueuePollJitter adds randomised delay to poll intervals to stagger load.
	DefaultQueuePollJitter = 500 * time.Millisecond
	// DefaultQueueResilientPollInterval bounds how often watchers fall back to polling to recover missed events.
	DefaultQueueResilientPollInterval = 5 * time.Minute
)

const (
	// DefaultPayloadSpoolMemoryThreshold defines how much JSON payload is buffered in memory before spilling to disk.
	DefaultPayloadSpoolMemoryThreshold = defaultSpoolMemoryThreshold
	// DefaultListen is the default TCP endpoint the server binds to.
	DefaultListen = ":9341"
	// DefaultListenProto controls the scheme used when no protocol is configured.
	DefaultListenProto = "tcp"
	// DefaultStore points the server at the in-memory backend when no store is provided.
	DefaultStore = "mem://"
	// DefaultJSONMaxBytes bounds incoming JSON payloads.
	DefaultJSONMaxBytes = 100 * 1024 * 1024
	// DefaultAttachmentMaxBytes bounds attachment payloads when not specified by the caller.
	DefaultAttachmentMaxBytes = int64(1 << 40)
	// DefaultDefaultTTL is the baseline lease duration handed to new acquirers.
	DefaultDefaultTTL = 30 * time.Second
	// DefaultMaxTTL is the hard ceiling enforced on user-supplied TTLs.
	DefaultMaxTTL = 30 * time.Minute
	// DefaultAcquireBlock controls how long acquire requests block before timing out.
	DefaultAcquireBlock = 60 * time.Second
	// DefaultSweeperInterval sets the tick frequency for idle maintenance sweeps.
	DefaultSweeperInterval = 5 * time.Minute
	// DefaultTxnReplayInterval throttles transaction replay sweeps on active operations.
	DefaultTxnReplayInterval = 5 * time.Second
	// DefaultIdleSweepGrace controls how long the server must be idle before running maintenance sweeps.
	DefaultIdleSweepGrace = 5 * time.Minute
	// DefaultIdleSweepOpDelay pauses between maintenance sweep operations to reduce backend pressure.
	DefaultIdleSweepOpDelay = 100 * time.Millisecond
	// DefaultIdleSweepMaxOps caps how many sweep operations execute per run.
	DefaultIdleSweepMaxOps = 1000
	// DefaultIdleSweepMaxRuntime caps how long a maintenance sweep run may execute.
	DefaultIdleSweepMaxRuntime = 30 * time.Second
	// DefaultDrainGrace is the grace period granted before HTTP shutdown begins.
	DefaultDrainGrace = 10 * time.Second
	// DefaultShutdownTimeout caps the total shutdown time (drain + HTTP server).
	DefaultShutdownTimeout = 10 * time.Second
	// DefaultMaxConcurrentStreams sets the default HTTP/2 MaxConcurrentStreams when not explicitly configured.
	DefaultMaxConcurrentStreams = 1024
	// DefaultS3MaxPartSize tunes multipart uploads when writing state to S3-compatible stores.
	DefaultS3MaxPartSize = 16 * 1024 * 1024
	// DefaultStorageRetryMaxAttempts describes how many transient storage errors are retried.
	DefaultStorageRetryMaxAttempts = 6
	// DefaultStorageRetryBaseDelay configures the base delay between storage retries.
	DefaultStorageRetryBaseDelay = 100 * time.Millisecond
	// DefaultStorageRetryMaxDelay caps the exponential backoff between storage retries.
	DefaultStorageRetryMaxDelay = 5 * time.Second
	// DefaultStorageRetryMultiplier defines the exponential backoff ratio.
	DefaultStorageRetryMultiplier = 2.0
	// DefaultClientBlock drives the CLI client's default acquire block duration.
	DefaultClientBlock = 10 * time.Second
	// DefaultAzureEndpoint is empty so we can derive endpoints automatically for public regions.
	DefaultAzureEndpoint = ""
	// DefaultAzureEndpointPattern expands Azure account names into their HTTPS endpoint.
	DefaultAzureEndpointPattern = "https://%s.blob.core.windows.net"
	// DefaultAzureEndpointHelp documents the Azure endpoint format in CLI help output.
	DefaultAzureEndpointHelp = "https://<account>.blob.core.windows.net"
	// DefaultConfigFileName is the config file searched for when --config is omitted.
	DefaultConfigFileName = "config.yaml"
	// DefaultServerBundleName is the PEM bundle name emitted by lockd auth helpers.
	DefaultServerBundleName = "server.pem"
)

const (
	// DefaultQRFSoftRetryAfter throttles lightly while the QRF is soft-armed.
	DefaultQRFSoftRetryAfter = 100 * time.Millisecond
	// DefaultQRFEngagedRetryAfter throttles aggressively when the QRF is fully engaged.
	DefaultQRFEngagedRetryAfter = 500 * time.Millisecond
	// DefaultQRFRecoveryRetryAfter moderates throttling while recovering.
	DefaultQRFRecoveryRetryAfter = 200 * time.Millisecond
	// DefaultTCFanoutTimeout bounds how long the TC waits per RM apply request.
	DefaultTCFanoutTimeout = 5 * time.Second
	// DefaultTCFanoutMaxAttempts describes how many times to retry RM apply calls.
	DefaultTCFanoutMaxAttempts = 4
	// DefaultTCFanoutBaseDelay configures the base backoff between RM apply retries.
	DefaultTCFanoutBaseDelay = 100 * time.Millisecond
	// DefaultTCFanoutMaxDelay caps RM apply retry backoff.
	DefaultTCFanoutMaxDelay = 2 * time.Second
	// DefaultTCFanoutMultiplier defines the exponential retry multiplier.
	DefaultTCFanoutMultiplier = 2.0
	// DefaultTCDecisionRetention retains decided txn records for recovery/fan-out.
	DefaultTCDecisionRetention = 24 * time.Hour
	// DefaultQRFRecoverySamples controls how many consecutive healthy samples are required before disengaging.
	DefaultQRFRecoverySamples = 5
	// DefaultQRFMemorySoftLimitPercent applies a soft guardrail when overall memory usage crosses this percentage.
	DefaultQRFMemorySoftLimitPercent = 80.0
	// DefaultQRFMemoryHardLimitPercent applies a hard guardrail when overall memory usage crosses this percentage.
	DefaultQRFMemoryHardLimitPercent = 90.0
	// DefaultQRFMemoryStrictHeadroomPercent discounts this much usage when reclaimable cache is unknown.
	DefaultQRFMemoryStrictHeadroomPercent = 15.0
	// DefaultQRFMemorySoftLimitBytes is disabled by default; set explicitly to enforce a process RSS cap.
	DefaultQRFMemorySoftLimitBytes = 0
	// DefaultQRFMemoryHardLimitBytes is disabled by default; set explicitly to enforce a hard process RSS cap.
	DefaultQRFMemoryHardLimitBytes = 0
	// DefaultQRFLoadSoftLimitMultiplier is the baseline load-average multiplier that soft-arms the QRF.
	DefaultQRFLoadSoftLimitMultiplier = 4.0
	// DefaultQRFLoadHardLimitMultiplier is the load-average multiplier that fully engages the QRF.
	DefaultQRFLoadHardLimitMultiplier = 8.0
	// DefaultQRFQueueConsumerSoftLimitRatio controls the default soft ceiling for concurrent queue consumers.
	DefaultQRFQueueConsumerSoftLimitRatio = 0.75
	// DefaultLSFSampleInterval configures how frequently the LSF observer samples system metrics.
	DefaultLSFSampleInterval = 200 * time.Millisecond
	// DefaultLSFLogInterval controls how often the LSF emits lockd.lsf.sample telemetry logs.
	DefaultLSFLogInterval = 15 * time.Second

	// DefaultIndexerFlushDocs determines how many documents trigger a flush.
	DefaultIndexerFlushDocs = 1000
	// DefaultIndexerFlushInterval bounds how long a memtable buffers before flushing.
	DefaultIndexerFlushInterval = 30 * time.Second
)

// DefaultQueueMaxConsumers returns an adaptive per-server consumer ceiling derived from CPU count.
func DefaultQueueMaxConsumers() int {
	cores := max(runtime.NumCPU(), 1)
	value := max(cores*defaultQueueConsumersPerCPU, defaultQueueConsumersFloor)
	if value > defaultQueueConsumersCeil {
		value = defaultQueueConsumersCeil
	}
	return value
}

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
	return slices.Contains(jsonUtilChoices, name)
}

// Config captures the tunables for a lockd.Server instance.
type Config struct {
	Listen                string
	ListenProto           string
	Store                 string
	DefaultNamespace      string
	JSONMaxBytes          int64
	AttachmentMaxBytes    int64
	JSONUtil              string
	SpoolMemoryThreshold  int64
	DiskRetention         time.Duration
	DiskJanitorInterval   time.Duration
	DiskQueueWatch        bool
	DisableMemQueueWatch  bool
	DefaultTTL            time.Duration
	MaxTTL                time.Duration
	AcquireBlock          time.Duration
	SweeperInterval       time.Duration
	TxnReplayInterval     time.Duration
	IdleSweepGrace        time.Duration
	IdleSweepOpDelay       time.Duration
	IdleSweepMaxOps        int
	IdleSweepMaxRuntime    time.Duration
	DrainGrace            time.Duration
	DrainGraceSet         bool
	ShutdownTimeout       time.Duration
	ShutdownTimeoutSet    bool
	OTLPEndpoint          string
	DisableHTTPTracing    bool
	DisableStorageTracing bool

	// mTLS
	DisableMTLS  bool
	BundlePath   string
	BundlePEM    []byte
	DenylistPath string
	// HTTP/2 tuning.
	HTTP2MaxConcurrentStreams    int
	HTTP2MaxConcurrentStreamsSet bool
	// TC federation / auth
	TCTrustDir       string
	TCDisableAuth    bool
	TCAllowDefaultCA bool
	// TC leader election
	SelfEndpoint    string
	TCJoinEndpoints []string // optional seed endpoints for initial TC peer discovery
	// TC coordinator / fan-out
	TCFanoutTimeout     time.Duration
	TCFanoutMaxAttempts int
	TCFanoutBaseDelay   time.Duration
	TCFanoutMaxDelay    time.Duration
	TCFanoutMultiplier  float64
	TCDecisionRetention time.Duration
	TCClientBundlePath  string
	// Storage encryption
	DisableStorageEncryption bool
	StorageEncryptionSnappy  bool
	MetadataRootKey          keymgmt.RootKey
	MetadataDescriptor       keymgmt.Descriptor
	MetadataContext          string
	DisableKryptoPool        bool

	// Object-store options.
	S3SSE             string
	S3KMSKeyID        string
	AWSKMSKeyID       string
	S3MaxPartSize     int64
	AWSRegion         string
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3SessionToken    string

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

	// Queue dispatcher tuning.
	QueueMaxConsumers          int
	QueuePollInterval          time.Duration
	QueuePollJitter            time.Duration
	QueueResilientPollInterval time.Duration

	// Indexer tuning
	IndexerFlushDocs        int
	IndexerFlushInterval    time.Duration
	IndexerFlushDocsSet     bool
	IndexerFlushIntervalSet bool

	// Quick Reaction Force (perimeter defense) configuration.
	QRFEnabled                     bool
	QRFQueueSoftLimit              int64
	QRFQueueHardLimit              int64
	QRFQueueConsumerSoftLimit      int64
	QRFQueueConsumerHardLimit      int64
	QRFLockSoftLimit               int64
	QRFLockHardLimit               int64
	QRFMemorySoftLimitBytes        uint64
	QRFMemoryHardLimitBytes        uint64
	QRFMemorySoftLimitPercent      float64
	QRFMemoryHardLimitPercent      float64
	QRFMemoryStrictHeadroomPercent float64
	QRFSwapSoftLimitBytes          uint64
	QRFSwapHardLimitBytes          uint64
	QRFSwapSoftLimitPercent        float64
	QRFSwapHardLimitPercent        float64
	QRFCPUPercentSoftLimit         float64
	QRFCPUPercentHardLimit         float64
	QRFLoadSoftLimitMultiplier     float64
	QRFLoadHardLimitMultiplier     float64
	QRFRecoverySamples             int
	QRFSoftRetryAfter              time.Duration
	QRFEngagedRetryAfter           time.Duration
	QRFRecoveryRetryAfter          time.Duration

	// Local Security Force configuration.
	LSFSampleInterval time.Duration
	LSFLogInterval    time.Duration
	LSFLogIntervalSet bool
}

// MTLSEnabled reports whether mutual TLS is active.
func (c Config) MTLSEnabled() bool {
	return !c.DisableMTLS
}

// StorageEncryptionEnabled reports whether kryptograf envelope encryption is active.
func (c Config) StorageEncryptionEnabled() bool {
	return !c.DisableStorageEncryption
}

// Validate applies defaults and sanity-checks the configuration.
func (c *Config) Validate() error {
	if c.DisableStorageEncryption {
		c.StorageEncryptionSnappy = false
	}
	if c.Listen == "" {
		c.Listen = DefaultListen
	}
	if c.ListenProto == "" {
		c.ListenProto = DefaultListenProto
	}
	if c.Store == "" {
		return fmt.Errorf("config: store is required")
	}
	ns, err := NormalizeNamespace(c.DefaultNamespace, DefaultNamespace)
	if err != nil {
		return fmt.Errorf("config: default namespace: %w", err)
	}
	c.DefaultNamespace = ns
	if c.JSONMaxBytes <= 0 {
		c.JSONMaxBytes = DefaultJSONMaxBytes
	}
	if c.AttachmentMaxBytes <= 0 {
		c.AttachmentMaxBytes = DefaultAttachmentMaxBytes
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
		c.DefaultTTL = DefaultDefaultTTL
	}
	if c.MaxTTL <= 0 {
		c.MaxTTL = DefaultMaxTTL
	}
	if c.MaxTTL < c.DefaultTTL {
		return fmt.Errorf("config: max ttl must be >= default ttl")
	}
	if c.AcquireBlock <= 0 {
		c.AcquireBlock = DefaultAcquireBlock
	}
	if c.SweeperInterval <= 0 {
		c.SweeperInterval = DefaultSweeperInterval
	}
	if c.TxnReplayInterval < 0 {
		return fmt.Errorf("config: txn replay interval must be >= 0")
	}
	if c.TxnReplayInterval <= 0 {
		c.TxnReplayInterval = DefaultTxnReplayInterval
		if c.SweeperInterval > 0 && c.SweeperInterval < c.TxnReplayInterval {
			c.TxnReplayInterval = c.SweeperInterval
		}
	}
	if c.IdleSweepGrace <= 0 {
		c.IdleSweepGrace = DefaultIdleSweepGrace
	}
	if c.IdleSweepOpDelay <= 0 {
		c.IdleSweepOpDelay = DefaultIdleSweepOpDelay
	}
	if c.IdleSweepMaxOps <= 0 {
		c.IdleSweepMaxOps = DefaultIdleSweepMaxOps
	}
	if c.IdleSweepMaxRuntime <= 0 {
		c.IdleSweepMaxRuntime = DefaultIdleSweepMaxRuntime
	}
	if c.DrainGrace < 0 {
		return fmt.Errorf("config: drain grace must be >= 0")
	}
	if c.ShutdownTimeout < 0 {
		return fmt.Errorf("config: shutdown timeout must be >= 0")
	}
	if !c.DrainGraceSet && c.DrainGrace > 0 {
		c.DrainGraceSet = true
	}
	if !c.ShutdownTimeoutSet && c.ShutdownTimeout > 0 {
		c.ShutdownTimeoutSet = true
	}
	if c.HTTP2MaxConcurrentStreams < 0 {
		return fmt.Errorf("config: http2 max concurrent streams must be >= 0")
	}
	if c.HTTP2MaxConcurrentStreams != 0 {
		c.HTTP2MaxConcurrentStreamsSet = true
	}
	if !c.HTTP2MaxConcurrentStreamsSet {
		c.HTTP2MaxConcurrentStreams = DefaultMaxConcurrentStreams
	}
	if c.DiskRetention < 0 {
		return fmt.Errorf("config: disk retention must be >= 0")
	}
	if c.DiskJanitorInterval < 0 {
		return fmt.Errorf("config: disk janitor interval must be >= 0")
	}
	if c.DiskJanitorInterval == 0 {
		if c.DiskRetention > 0 {
			half := max(c.DiskRetention/2, time.Minute)
			if half > time.Hour {
				half = time.Hour
			}
			c.DiskJanitorInterval = half
		} else {
			c.DiskJanitorInterval = time.Hour
		}
	}
	if c.S3MaxPartSize <= 0 {
		c.S3MaxPartSize = DefaultS3MaxPartSize
	}
	if c.QueueMaxConsumers <= 0 {
		c.QueueMaxConsumers = DefaultQueueMaxConsumers()
	}
	if c.QueuePollInterval <= 0 {
		c.QueuePollInterval = DefaultQueuePollInterval
	}
	if c.QueuePollJitter < 0 {
		return fmt.Errorf("config: queue poll jitter must be >= 0")
	}
	if c.QueueResilientPollInterval <= 0 {
		c.QueueResilientPollInterval = DefaultQueueResilientPollInterval
	}
	if c.LSFSampleInterval <= 0 {
		c.LSFSampleInterval = DefaultLSFSampleInterval
	}
	if !c.LSFLogIntervalSet && c.LSFLogInterval > 0 {
		c.LSFLogIntervalSet = true
	}
	if c.LSFLogInterval < 0 {
		return fmt.Errorf("config: lsf log interval must be >= 0")
	}
	if !c.LSFLogIntervalSet {
		c.LSFLogInterval = DefaultLSFLogInterval
	}
	if c.TCTrustDir == "" {
		dir, err := DefaultTCTrustDir()
		if err != nil {
			return fmt.Errorf("config: resolve tc trust dir: %w", err)
		}
		c.TCTrustDir = dir
	}
	normalizeEndpoint := func(raw string) string {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			return ""
		}
		return strings.TrimSuffix(trimmed, "/")
	}
	if strings.TrimSpace(c.SelfEndpoint) != "" {
		c.SelfEndpoint = normalizeEndpoint(c.SelfEndpoint)
	}
	if len(c.TCJoinEndpoints) > 0 {
		normalized := make([]string, 0, len(c.TCJoinEndpoints))
		seen := make(map[string]struct{}, len(c.TCJoinEndpoints))
		for _, raw := range c.TCJoinEndpoints {
			endpoint := normalizeEndpoint(raw)
			if endpoint == "" {
				continue
			}
			if _, ok := seen[endpoint]; ok {
				return fmt.Errorf("config: duplicate tc-join endpoint %q", endpoint)
			}
			seen[endpoint] = struct{}{}
			normalized = append(normalized, endpoint)
		}
		sort.Strings(normalized)
		c.TCJoinEndpoints = normalized
		if len(normalized) == 0 {
			return fmt.Errorf("config: tc-join endpoints required when join is configured")
		}
		self := strings.TrimSpace(c.SelfEndpoint)
		if self == "" {
			return fmt.Errorf("config: self endpoint required when tc-join is set")
		}
		if !c.MTLSEnabled() {
			nonSelf := make([]string, 0, len(c.TCJoinEndpoints))
			for _, endpoint := range c.TCJoinEndpoints {
				if endpoint != self {
					nonSelf = append(nonSelf, endpoint)
				}
			}
			if len(nonSelf) > 0 {
				return fmt.Errorf("config: mTLS required when tc-join includes non-self endpoints")
			}
		}
	}
	if c.TCFanoutTimeout <= 0 {
		c.TCFanoutTimeout = DefaultTCFanoutTimeout
	}
	if c.TCFanoutMaxAttempts <= 0 {
		c.TCFanoutMaxAttempts = DefaultTCFanoutMaxAttempts
	}
	if c.TCFanoutBaseDelay <= 0 {
		c.TCFanoutBaseDelay = DefaultTCFanoutBaseDelay
	}
	if c.TCFanoutMaxDelay <= 0 {
		c.TCFanoutMaxDelay = DefaultTCFanoutMaxDelay
	}
	if c.TCFanoutMultiplier <= 1.0 {
		c.TCFanoutMultiplier = DefaultTCFanoutMultiplier
	}
	if c.TCDecisionRetention <= 0 {
		c.TCDecisionRetention = DefaultTCDecisionRetention
	}
	tcClientRequired := c.MTLSEnabled() && !c.TCDisableAuth && (len(c.TCJoinEndpoints) > 0 || strings.TrimSpace(c.SelfEndpoint) != "")
	if c.MTLSEnabled() && strings.TrimSpace(c.TCClientBundlePath) != "" {
		path, err := ResolveClientBundlePath(ClientBundleRoleTC, c.TCClientBundlePath)
		if err != nil {
			return fmt.Errorf("config: invalid tc client bundle: %w", err)
		}
		c.TCClientBundlePath = path
	} else if tcClientRequired {
		path, err := ResolveClientBundlePath(ClientBundleRoleTC, "")
		if err != nil {
			return fmt.Errorf("config: resolve tc client bundle: %w", err)
		}
		c.TCClientBundlePath = path
	}
	if c.QRFQueueSoftLimit < 0 {
		return fmt.Errorf("config: qrf queue soft limit must be >= 0")
	}
	if c.QRFQueueHardLimit < 0 {
		return fmt.Errorf("config: qrf queue hard limit must be >= 0")
	}
	if c.QRFQueueHardLimit > 0 && (c.QRFQueueSoftLimit == 0 || c.QRFQueueSoftLimit > c.QRFQueueHardLimit) {
		c.QRFQueueSoftLimit = c.QRFQueueHardLimit
	}
	if c.QRFQueueConsumerSoftLimit < 0 {
		return fmt.Errorf("config: qrf queue consumer soft limit must be >= 0")
	}
	if c.QRFQueueConsumerHardLimit < 0 {
		return fmt.Errorf("config: qrf queue consumer hard limit must be >= 0")
	}
	if c.QRFQueueConsumerHardLimit <= 0 {
		c.QRFQueueConsumerHardLimit = int64(c.QueueMaxConsumers)
	}
	if c.QRFQueueConsumerSoftLimit <= 0 {
		soft := int64(float64(c.QRFQueueConsumerHardLimit) * DefaultQRFQueueConsumerSoftLimitRatio)
		if soft <= 0 && c.QRFQueueConsumerHardLimit > 0 {
			soft = c.QRFQueueConsumerHardLimit
		}
		c.QRFQueueConsumerSoftLimit = soft
	}
	if c.QRFQueueConsumerSoftLimit > c.QRFQueueConsumerHardLimit {
		c.QRFQueueConsumerSoftLimit = c.QRFQueueConsumerHardLimit
	}
	if c.QRFLockSoftLimit < 0 {
		return fmt.Errorf("config: qrf lock soft limit must be >= 0")
	}
	if c.QRFLockHardLimit < 0 {
		return fmt.Errorf("config: qrf lock hard limit must be >= 0")
	}
	if c.QRFLockHardLimit > 0 && (c.QRFLockSoftLimit == 0 || c.QRFLockSoftLimit > c.QRFLockHardLimit) {
		c.QRFLockSoftLimit = c.QRFLockHardLimit
	}
	if c.QRFMemoryHardLimitPercent <= 0 {
		c.QRFMemoryHardLimitPercent = DefaultQRFMemoryHardLimitPercent
	}
	if c.QRFMemorySoftLimitPercent <= 0 {
		c.QRFMemorySoftLimitPercent = DefaultQRFMemorySoftLimitPercent
	}
	if c.QRFMemorySoftLimitPercent > c.QRFMemoryHardLimitPercent {
		c.QRFMemorySoftLimitPercent = c.QRFMemoryHardLimitPercent
	}
	if c.QRFMemoryStrictHeadroomPercent < 0 {
		return fmt.Errorf("config: qrf memory strict headroom percent must be >= 0")
	}
	if c.QRFMemoryStrictHeadroomPercent == 0 {
		c.QRFMemoryStrictHeadroomPercent = DefaultQRFMemoryStrictHeadroomPercent
	}
	if c.QRFCPUPercentSoftLimit < 0 {
		c.QRFCPUPercentSoftLimit = 0
	}
	if c.QRFCPUPercentHardLimit < 0 {
		c.QRFCPUPercentHardLimit = 0
	}
	if c.QRFCPUPercentHardLimit > 0 && (c.QRFCPUPercentSoftLimit == 0 || c.QRFCPUPercentSoftLimit > c.QRFCPUPercentHardLimit) {
		c.QRFCPUPercentSoftLimit = c.QRFCPUPercentHardLimit
	}
	if c.QRFLoadSoftLimitMultiplier <= 0 {
		c.QRFLoadSoftLimitMultiplier = DefaultQRFLoadSoftLimitMultiplier
	}
	if c.QRFLoadHardLimitMultiplier <= 0 {
		c.QRFLoadHardLimitMultiplier = DefaultQRFLoadHardLimitMultiplier
	}
	if c.QRFLoadHardLimitMultiplier > 0 && c.QRFLoadSoftLimitMultiplier > c.QRFLoadHardLimitMultiplier {
		c.QRFLoadSoftLimitMultiplier = c.QRFLoadHardLimitMultiplier
	}
	if c.QRFSwapSoftLimitPercent < 0 {
		c.QRFSwapSoftLimitPercent = 0
	}
	if c.QRFSwapHardLimitPercent < 0 {
		c.QRFSwapHardLimitPercent = 0
	}
	if c.QRFSwapHardLimitPercent > 0 && (c.QRFSwapSoftLimitPercent == 0 || c.QRFSwapSoftLimitPercent > c.QRFSwapHardLimitPercent) {
		c.QRFSwapSoftLimitPercent = c.QRFSwapHardLimitPercent
	}
	if c.QRFSwapHardLimitBytes > 0 && (c.QRFSwapSoftLimitBytes == 0 || c.QRFSwapSoftLimitBytes > c.QRFSwapHardLimitBytes) {
		c.QRFSwapSoftLimitBytes = c.QRFSwapHardLimitBytes
	}
	if c.QRFRecoverySamples <= 0 {
		c.QRFRecoverySamples = DefaultQRFRecoverySamples
	}
	if c.QRFSoftRetryAfter <= 0 {
		c.QRFSoftRetryAfter = DefaultQRFSoftRetryAfter
	}
	if c.QRFEngagedRetryAfter <= 0 {
		c.QRFEngagedRetryAfter = DefaultQRFEngagedRetryAfter
	}
	if c.QRFRecoveryRetryAfter <= 0 {
		c.QRFRecoveryRetryAfter = DefaultQRFRecoveryRetryAfter
	}
	requireBundle := c.MTLSEnabled() || c.StorageEncryptionEnabled()
	if requireBundle {
		if len(c.BundlePEM) == 0 {
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
	}

	storeLower := strings.ToLower(c.Store)
	if strings.HasPrefix(storeLower, "aws://") && c.AWSRegion == "" {
		return fmt.Errorf("config: aws region must be provided for store %q", c.Store)
	}
	if strings.HasPrefix(storeLower, "s3://") || strings.HasPrefix(storeLower, "aws://") {
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
			c.StorageRetryMaxAttempts = DefaultStorageRetryMaxAttempts
		}
		if c.StorageRetryBaseDelay <= 0 {
			c.StorageRetryBaseDelay = DefaultStorageRetryBaseDelay
		}
		if c.StorageRetryMultiplier <= 0 {
			c.StorageRetryMultiplier = DefaultStorageRetryMultiplier
		}
		if c.StorageRetryMaxDelay <= 0 {
			c.StorageRetryMaxDelay = DefaultStorageRetryMaxDelay
		}
	}
	return nil
}

// DefaultConfigDir returns the default configuration directory ($HOME/.lockd).
func DefaultConfigDir() (string, error) {
	if override := strings.TrimSpace(os.Getenv("LOCKD_CONFIG_DIR")); override != "" {
		if filepath.IsAbs(override) {
			return override, nil
		}
		abs, err := filepath.Abs(override)
		if err != nil {
			return "", err
		}
		return abs, nil
	}
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

// DefaultCAPath returns the default CA bundle location.
func DefaultCAPath() (string, error) {
	dir, err := DefaultConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "ca.pem"), nil
}

// DefaultTCTrustDir returns the default directory holding trusted TC CA certificates.
func DefaultTCTrustDir() (string, error) {
	dir, err := DefaultConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "tc-trust.d"), nil
}
