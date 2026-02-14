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
	"pkt.systems/lockd/internal/pathutil"
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
	// DefaultQueueListPageSize caps how many queue metadata entries are fetched per poll.
	DefaultQueueListPageSize = 128
)

const (
	// DefaultPayloadSpoolMemoryThreshold defines how much JSON payload is buffered in memory before spilling to disk.
	DefaultPayloadSpoolMemoryThreshold = defaultSpoolMemoryThreshold
	// DefaultListen is the default TCP endpoint the server binds to.
	DefaultListen = ":9341"
	// DefaultListenProto controls the scheme used when no protocol is configured.
	DefaultListenProto = "tcp"
	// DefaultMetricsListen is the default metrics endpoint (Prometheus scrape).
	// Empty disables metrics unless explicitly configured.
	DefaultMetricsListen = ""
	// DefaultPprofListen is the default pprof debug listener (empty disables).
	DefaultPprofListen = ""
	// DefaultStore points the server at the in-memory backend when no store is provided.
	DefaultStore = "mem://"
	// DefaultHAMode controls multi-node behaviour when multiple servers share a backend.
	DefaultHAMode = "failover"
	// DefaultHALeaseTTL controls how long HA failover leases are held in failover mode.
	DefaultHALeaseTTL = 5 * time.Second
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
	// DefaultQueueDecisionCacheTTL bounds how long empty queue decision checks are cached.
	DefaultQueueDecisionCacheTTL = 60 * time.Second
	// DefaultQueueDecisionMaxApply caps how many queue decision items are applied per dequeue attempt.
	DefaultQueueDecisionMaxApply = 50
	// DefaultQueueDecisionApplyTimeout bounds how long a dequeue spends applying queued decisions.
	DefaultQueueDecisionApplyTimeout = 2 * time.Second
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
	// DefaultLogstoreCommitMaxOps caps how many logstore entries are committed per fsync batch.
	DefaultLogstoreCommitMaxOps = 1024
	// DefaultLogstoreSegmentSize caps the size of a single logstore segment before rolling.
	DefaultLogstoreSegmentSize = int64(64 << 20)
	// DefaultQueryDocPrefetch caps the number of in-flight document fetches for query return=documents.
	DefaultQueryDocPrefetch = 8
	// DefaultDiskLockFileCacheSize caps cached lockfile descriptors (disk/NFS).
	DefaultDiskLockFileCacheSize = 2048
	// DefaultS3MaxPartSize tunes multipart uploads when writing state to S3-compatible stores.
	DefaultS3MaxPartSize = 16 * 1024 * 1024
	// DefaultS3SmallEncryptBufferBudget caps concurrent small-object encryption buffers for S3 backends.
	DefaultS3SmallEncryptBufferBudget = 64 * 1024 * 1024
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
	// DefaultQRFSoftDelay sets the base delay while the QRF is soft-armed.
	DefaultQRFSoftDelay = 50 * time.Millisecond
	// DefaultQRFEngagedDelay sets the base delay when the QRF is fully engaged.
	DefaultQRFEngagedDelay = 250 * time.Millisecond
	// DefaultQRFRecoveryDelay sets the base delay while recovering.
	DefaultQRFRecoveryDelay = 200 * time.Millisecond
	// DefaultQRFMaxWait caps how long a request will wait under QRF pacing before failing.
	DefaultQRFMaxWait = 5 * time.Second
	// DefaultStateCacheBytes caps in-memory cached state payloads for hot reads.
	DefaultStateCacheBytes = 64 << 20
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
	DefaultQRFMemorySoftLimitPercent = 75.0
	// DefaultQRFMemoryHardLimitPercent applies a hard guardrail when overall memory usage crosses this percentage.
	DefaultQRFMemoryHardLimitPercent = 85.0
	// DefaultQRFMemoryStrictHeadroomPercent discounts this much usage when reclaimable cache is unknown.
	DefaultQRFMemoryStrictHeadroomPercent = 15.0
	// DefaultQRFMemorySoftLimitBytes is disabled by default; set explicitly to enforce a process RSS cap.
	DefaultQRFMemorySoftLimitBytes = 0
	// DefaultQRFMemoryHardLimitBytes is disabled by default; set explicitly to enforce a hard process RSS cap.
	DefaultQRFMemoryHardLimitBytes = 0
	// DefaultQRFSwapSoftLimitPercent disables swap-based QRF by default.
	DefaultQRFSwapSoftLimitPercent = 0.0
	// DefaultQRFSwapHardLimitPercent disables swap-based QRF by default.
	DefaultQRFSwapHardLimitPercent = 0.0
	// DefaultQRFCPUPercentSoftLimit applies a soft guardrail when CPU utilisation crosses this percentage.
	DefaultQRFCPUPercentSoftLimit = 70.0
	// DefaultQRFCPUPercentHardLimit applies a hard guardrail when CPU utilisation crosses this percentage.
	DefaultQRFCPUPercentHardLimit = 85.0
	// DefaultQRFLoadSoftLimitMultiplier is the baseline load-average multiplier that soft-arms the QRF.
	DefaultQRFLoadSoftLimitMultiplier = 4.0
	// DefaultQRFLoadHardLimitMultiplier is the load-average multiplier that fully engages the QRF.
	DefaultQRFLoadHardLimitMultiplier = 8.0
	// DefaultQRFQueueConsumerSoftLimitRatio controls the default soft ceiling for concurrent queue consumers.
	DefaultQRFQueueConsumerSoftLimitRatio = 0.75
	// DefaultConnguardFailureThreshold is the number of suspicious connection events required before hard-blocking an IP.
	DefaultConnguardFailureThreshold = 5
	// DefaultConnguardFailureWindow is the rolling window for suspicious connect attempts.
	DefaultConnguardFailureWindow = 30 * time.Second
	// DefaultConnguardBlockDuration controls how long an IP remains blocked.
	DefaultConnguardBlockDuration = 5 * time.Minute
	// DefaultConnguardProbeTimeout bounds the wait for early classification on plain TCP connections.
	DefaultConnguardProbeTimeout = 250 * time.Millisecond
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
	// Listen is the server bind address (for example ":9341").
	Listen string
	// ListenProto selects listener type (for example "tcp").
	ListenProto string
	// MetricsListen is the metrics endpoint bind address; empty disables metrics.
	MetricsListen string
	// MetricsListenSet reports whether MetricsListen was explicitly set by caller/flags/env.
	MetricsListenSet bool
	// PprofListen is the pprof endpoint bind address; empty disables pprof.
	PprofListen string
	// PprofListenSet reports whether PprofListen was explicitly set by caller/flags/env.
	PprofListenSet bool
	// EnableProfilingMetrics enables runtime profiling metrics on the metrics endpoint.
	EnableProfilingMetrics bool
	// EnableProfilingMetricsSet reports whether profiling metrics toggle was explicitly set.
	EnableProfilingMetricsSet bool
	// Store is the backend DSN (for example mem://, disk://..., s3://..., azure://...).
	Store string
	// HAMode controls cluster coordination strategy ("concurrent" or "failover").
	HAMode string
	// HALeaseTTL controls leader-lease lifetime in failover mode.
	HALeaseTTL time.Duration
	// DefaultNamespace is used when requests omit namespace.
	DefaultNamespace string
	// JSONMaxBytes caps incoming JSON payload size.
	JSONMaxBytes int64
	// AttachmentMaxBytes caps attachment payload size.
	AttachmentMaxBytes int64
	// JSONUtil selects JSON implementation (lockd/jsonv2/stdlib).
	JSONUtil string
	// SpoolMemoryThreshold controls memory buffering before payload spill-to-disk.
	SpoolMemoryThreshold int64
	// DiskRetention controls retention for disk-backed transient files/log fragments.
	DiskRetention time.Duration
	// DiskJanitorInterval controls how often disk retention janitor runs.
	DiskJanitorInterval time.Duration
	// DiskQueueWatch enables native filesystem queue-watch acceleration on disk backends.
	DiskQueueWatch bool
	// DiskLockFileCacheSize caps cached lock-file descriptors for disk/NFS locking.
	DiskLockFileCacheSize int
	// LogstoreCommitMaxOps caps logstore entries committed per fsync batch.
	LogstoreCommitMaxOps int
	// LogstoreSegmentSize caps logstore segment size before rolling.
	LogstoreSegmentSize int64
	// DisableMemQueueWatch disables in-memory queue watch hints (poll-only fallback).
	DisableMemQueueWatch bool
	// DefaultTTL is the default lease TTL for acquire/dequeue operations.
	DefaultTTL time.Duration
	// MaxTTL is the maximum allowed lease TTL.
	MaxTTL time.Duration
	// AcquireBlock is the default acquire/dequeue blocking window.
	AcquireBlock time.Duration
	// SweeperInterval controls maintenance sweep cadence.
	SweeperInterval time.Duration
	// TxnReplayInterval controls how often transaction replay scans run.
	TxnReplayInterval time.Duration
	// QueueDecisionCacheTTL controls empty decision-cache TTL for queue decision checks.
	QueueDecisionCacheTTL time.Duration
	// QueueDecisionMaxApply caps decision records applied per dequeue attempt.
	QueueDecisionMaxApply int
	// QueueDecisionApplyTimeout caps dequeue time spent applying queued decisions.
	QueueDecisionApplyTimeout time.Duration
	// StateCacheBytes caps in-memory state cache size (0 uses default, negative disables).
	StateCacheBytes int64
	// StateCacheBytesSet reports whether StateCacheBytes was explicitly set.
	StateCacheBytesSet bool
	// IdleSweepGrace is required idle time before maintenance sweeps begin.
	IdleSweepGrace time.Duration
	// IdleSweepOpDelay inserts pacing delay between maintenance operations.
	IdleSweepOpDelay time.Duration
	// IdleSweepMaxOps caps maintenance operations per sweep pass.
	IdleSweepMaxOps int
	// IdleSweepMaxRuntime caps wall-clock duration of a sweep pass.
	IdleSweepMaxRuntime time.Duration
	// DrainGrace is pre-shutdown lease-drain grace period.
	DrainGrace time.Duration
	// DrainGraceSet reports whether DrainGrace was explicitly set.
	DrainGraceSet bool
	// ShutdownTimeout caps total graceful shutdown duration (drain + HTTP shutdown).
	ShutdownTimeout time.Duration
	// ShutdownTimeoutSet reports whether ShutdownTimeout was explicitly set.
	ShutdownTimeoutSet bool
	// OTLPEndpoint enables OTLP export to the given collector endpoint.
	OTLPEndpoint string
	// DisableHTTPTracing disables OpenTelemetry spans for HTTP handlers.
	DisableHTTPTracing bool
	// DisableStorageTracing disables OpenTelemetry spans in storage backends.
	DisableStorageTracing bool

	// DisableMTLS disables mutual TLS on the public HTTP server.
	DisableMTLS bool
	// BundlePath points to server PEM bundle (CA + server cert + key + metadata material).
	BundlePath string
	// BundlePathDisableExpansion disables env/tilde expansion for BundlePath.
	BundlePathDisableExpansion bool
	// BundlePEM provides server bundle bytes directly (takes precedence when non-empty).
	BundlePEM []byte
	// DenylistPath points to serial denylist file merged with bundle denylist entries.
	DenylistPath string

	// HTTP2MaxConcurrentStreams sets HTTP/2 MaxConcurrentStreams; 0 uses default.
	HTTP2MaxConcurrentStreams int
	// HTTP2MaxConcurrentStreamsSet reports whether HTTP2MaxConcurrentStreams was explicitly set.
	HTTP2MaxConcurrentStreamsSet bool

	// TCTrustDir is directory of trusted CA certs for TC federation calls.
	TCTrustDir string
	// TCDisableAuth disables TC peer/client auth checks (testing/isolated setups only).
	TCDisableAuth bool
	// TCAllowDefaultCA allows trust fallback to local default CA material when explicit trust is absent.
	TCAllowDefaultCA bool

	// SelfEndpoint is this node's externally reachable endpoint for TC federation.
	SelfEndpoint string
	// TCJoinEndpoints is optional seed endpoint list used for initial TC peer discovery.
	TCJoinEndpoints []string

	// TCFanoutTimeout caps each remote apply attempt during TC fan-out.
	TCFanoutTimeout time.Duration
	// TCFanoutMaxAttempts caps retry attempts for TC fan-out calls.
	TCFanoutMaxAttempts int
	// TCFanoutBaseDelay is exponential backoff base for TC fan-out retries.
	TCFanoutBaseDelay time.Duration
	// TCFanoutMaxDelay caps TC fan-out backoff.
	TCFanoutMaxDelay time.Duration
	// TCFanoutMultiplier is exponential growth factor for TC fan-out retries.
	TCFanoutMultiplier float64
	// TCDecisionRetention keeps decided transaction records for replay/fan-out recovery.
	TCDecisionRetention time.Duration
	// TCClientBundlePath points to TC client bundle used for mTLS fan-out calls.
	TCClientBundlePath string

	// DisableStorageEncryption disables kryptograf envelope encryption at rest.
	DisableStorageEncryption bool
	// StorageEncryptionSnappy enables Snappy compression before encryption.
	StorageEncryptionSnappy bool
	// MetadataRootKey is kryptograf root key used to derive metadata/object keys.
	MetadataRootKey keymgmt.RootKey
	// MetadataDescriptor is kryptograf descriptor used for metadata encryption context.
	MetadataDescriptor keymgmt.Descriptor
	// MetadataContext is CA-derived context identifier used for encryption material lookup.
	MetadataContext string
	// DisableKryptoPool disables pooled crypto buffers (diagnostic mode).
	DisableKryptoPool bool

	// S3SSE controls server-side encryption mode for S3 writes (for example AES256/KMS).
	S3SSE string
	// S3KMSKeyID is KMS key identifier for S3 SSE-KMS mode.
	S3KMSKeyID string
	// AWSKMSKeyID is AWS KMS key identifier used by lockd's envelope crypto integrations.
	AWSKMSKeyID string
	// S3MaxPartSize controls multipart upload part size.
	S3MaxPartSize int64
	// S3SmallEncryptBufferBudget caps concurrent small-object encryption buffers.
	S3SmallEncryptBufferBudget int64
	// AWSRegion sets AWS region for aws:// and related integrations.
	AWSRegion string
	// S3AccessKeyID sets static S3 access key credential.
	S3AccessKeyID string
	// S3SecretAccessKey sets static S3 secret credential.
	S3SecretAccessKey string
	// S3SessionToken sets optional session token for temporary S3 credentials.
	S3SessionToken string

	// AzureAccount is the Azure storage account name.
	AzureAccount string
	// AzureAccountKey is the shared-key credential for Azure Blob.
	AzureAccountKey string
	// AzureEndpoint overrides Azure Blob endpoint URL.
	AzureEndpoint string
	// AzureSASToken configures SAS-token auth for Azure Blob.
	AzureSASToken string

	// StorageRetryMaxAttempts caps transient backend retry attempts.
	StorageRetryMaxAttempts int
	// StorageRetryBaseDelay is exponential retry base delay for backend operations.
	StorageRetryBaseDelay time.Duration
	// StorageRetryMaxDelay caps backend retry backoff.
	StorageRetryMaxDelay time.Duration
	// StorageRetryMultiplier is exponential growth factor for backend retries.
	StorageRetryMultiplier float64

	// QueueMaxConsumers caps concurrent queue consumer workers per server.
	QueueMaxConsumers int
	// QueuePollInterval controls queue poll cadence when no watch hint exists.
	QueuePollInterval time.Duration
	// QueuePollJitter adds random delay to queue polling intervals.
	QueuePollJitter time.Duration
	// QueueResilientPollInterval is fallback full-poll cadence to recover missed events.
	QueueResilientPollInterval time.Duration
	// QueueListPageSize caps queue list page size per poll request.
	QueueListPageSize int

	// IndexerFlushDocs flushes in-memory index docs after this many buffered docs.
	IndexerFlushDocs int
	// IndexerFlushInterval flushes in-memory index docs after this wall-clock interval.
	IndexerFlushInterval time.Duration
	// IndexerFlushDocsSet reports whether IndexerFlushDocs was explicitly set.
	IndexerFlushDocsSet bool
	// IndexerFlushIntervalSet reports whether IndexerFlushInterval was explicitly set.
	IndexerFlushIntervalSet bool

	// QueryDocPrefetch caps concurrent state fetches for query return=documents.
	QueryDocPrefetch int

	// QRFDisabled disables Quick Reaction Force request shaping.
	QRFDisabled bool
	// QRFQueueSoftLimit soft-arms QRF when in-flight queue leases exceed this count.
	QRFQueueSoftLimit int64
	// QRFQueueHardLimit fully engages QRF when in-flight queue leases exceed this count.
	QRFQueueHardLimit int64
	// QRFQueueConsumerSoftLimit soft-arms QRF when active queue consumers exceed this count.
	QRFQueueConsumerSoftLimit int64
	// QRFQueueConsumerHardLimit fully engages QRF when active queue consumers exceed this count.
	QRFQueueConsumerHardLimit int64
	// QRFLockSoftLimit soft-arms QRF when in-flight lock leases exceed this count.
	QRFLockSoftLimit int64
	// QRFLockHardLimit fully engages QRF when in-flight lock leases exceed this count.
	QRFLockHardLimit int64
	// QRFQuerySoftLimit soft-arms QRF when concurrent query load exceeds this count.
	QRFQuerySoftLimit int64
	// QRFQueryHardLimit fully engages QRF when concurrent query load exceeds this count.
	QRFQueryHardLimit int64
	// QRFMemorySoftLimitBytes soft-arms QRF when process memory exceeds this absolute byte threshold.
	QRFMemorySoftLimitBytes uint64
	// QRFMemoryHardLimitBytes fully engages QRF when process memory exceeds this absolute byte threshold.
	QRFMemoryHardLimitBytes uint64
	// QRFMemorySoftLimitPercent soft-arms QRF when process memory exceeds this percentage.
	QRFMemorySoftLimitPercent float64
	// QRFMemoryHardLimitPercent fully engages QRF when process memory exceeds this percentage.
	QRFMemoryHardLimitPercent float64
	// QRFMemoryStrictHeadroomPercent reserves this headroom when reclaimable cache is uncertain.
	QRFMemoryStrictHeadroomPercent float64
	// QRFSwapSoftLimitBytes soft-arms QRF when swap usage exceeds this absolute byte threshold.
	QRFSwapSoftLimitBytes uint64
	// QRFSwapHardLimitBytes fully engages QRF when swap usage exceeds this absolute byte threshold.
	QRFSwapHardLimitBytes uint64
	// QRFSwapSoftLimitPercent soft-arms QRF when swap usage exceeds this percentage.
	QRFSwapSoftLimitPercent float64
	// QRFSwapHardLimitPercent fully engages QRF when swap usage exceeds this percentage.
	QRFSwapHardLimitPercent float64
	// QRFCPUPercentSoftLimit soft-arms QRF when CPU utilization exceeds this percentage.
	QRFCPUPercentSoftLimit float64
	// QRFCPUPercentHardLimit fully engages QRF when CPU utilization exceeds this percentage.
	QRFCPUPercentHardLimit float64
	// QRFCPUPercentSoftLimitSet reports whether QRFCPUPercentSoftLimit was explicitly set.
	QRFCPUPercentSoftLimitSet bool
	// QRFCPUPercentHardLimitSet reports whether QRFCPUPercentHardLimit was explicitly set.
	QRFCPUPercentHardLimitSet bool
	// QRFLoadSoftLimitMultiplier soft-arms QRF when load average exceeds this CPU-multiplier.
	QRFLoadSoftLimitMultiplier float64
	// QRFLoadHardLimitMultiplier fully engages QRF when load average exceeds this CPU-multiplier.
	QRFLoadHardLimitMultiplier float64
	// QRFRecoverySamples is number of healthy samples required to disengage/recover.
	QRFRecoverySamples int
	// QRFSoftDelay is per-request pacing delay while soft-armed.
	QRFSoftDelay time.Duration
	// QRFEngagedDelay is per-request pacing delay while engaged.
	QRFEngagedDelay time.Duration
	// QRFRecoveryDelay is per-request pacing delay while recovering.
	QRFRecoveryDelay time.Duration
	// QRFMaxWait caps total time a request may wait under QRF pacing.
	QRFMaxWait time.Duration

	// LSFSampleInterval controls Local Security Force sample cadence.
	LSFSampleInterval time.Duration
	// LSFLogInterval controls cadence of lockd.lsf.sample telemetry logs.
	LSFLogInterval time.Duration
	// LSFLogIntervalSet reports whether LSFLogInterval was explicitly set.
	LSFLogIntervalSet bool

	// ConnguardEnabled enables suspicious-connection protection in the listener path.
	ConnguardEnabled bool
	// ConnguardEnabledSet reports whether ConnguardEnabled was explicitly set.
	ConnguardEnabledSet bool
	// ConnguardFailureThreshold controls how many suspicious connection events trigger a hard block.
	ConnguardFailureThreshold int
	// ConnguardFailureWindow is the rolling window used to count suspicious connection events.
	ConnguardFailureWindow time.Duration
	// ConnguardBlockDuration controls how long a suspicious source IP is blocked.
	ConnguardBlockDuration time.Duration
	// ConnguardProbeTimeout controls how long non-TLS connections are probed before classification.
	ConnguardProbeTimeout time.Duration
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
	if !c.MetricsListenSet && c.MetricsListen == "" {
		c.MetricsListen = DefaultMetricsListen
	}
	if !c.PprofListenSet && c.PprofListen == "" {
		c.PprofListen = DefaultPprofListen
	}
	if c.EnableProfilingMetrics && strings.TrimSpace(c.MetricsListen) == "" {
		return fmt.Errorf("config: profiling metrics require metrics-listen")
	}
	if c.Store == "" {
		return fmt.Errorf("config: store is required")
	}
	c.HAMode = strings.ToLower(strings.TrimSpace(c.HAMode))
	if c.HAMode == "" {
		c.HAMode = DefaultHAMode
	}
	switch c.HAMode {
	case "concurrent", "failover":
	default:
		return fmt.Errorf("config: ha mode must be %q or %q", "concurrent", "failover")
	}
	if c.HALeaseTTL == 0 {
		c.HALeaseTTL = DefaultHALeaseTTL
	} else if c.HALeaseTTL < 0 {
		return fmt.Errorf("config: ha lease ttl must be >= 0")
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
	if c.QueueDecisionCacheTTL < 0 {
		return fmt.Errorf("config: queue decision cache ttl must be >= 0")
	}
	if c.QueueDecisionCacheTTL == 0 {
		c.QueueDecisionCacheTTL = DefaultQueueDecisionCacheTTL
	}
	if c.QueueDecisionMaxApply <= 0 {
		c.QueueDecisionMaxApply = DefaultQueueDecisionMaxApply
	}
	if c.QueueDecisionApplyTimeout <= 0 {
		c.QueueDecisionApplyTimeout = DefaultQueueDecisionApplyTimeout
	}
	if !c.StateCacheBytesSet {
		if c.StateCacheBytes < 0 {
			c.StateCacheBytes = 0
		} else if c.StateCacheBytes == 0 {
			c.StateCacheBytes = DefaultStateCacheBytes
		}
	}
	if c.QueryDocPrefetch <= 0 {
		c.QueryDocPrefetch = DefaultQueryDocPrefetch
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
	if c.DiskLockFileCacheSize == 0 {
		c.DiskLockFileCacheSize = DefaultDiskLockFileCacheSize
	}
	if c.LogstoreCommitMaxOps <= 0 {
		c.LogstoreCommitMaxOps = DefaultLogstoreCommitMaxOps
	}
	if c.LogstoreSegmentSize <= 0 {
		c.LogstoreSegmentSize = DefaultLogstoreSegmentSize
	}
	if c.S3MaxPartSize <= 0 {
		c.S3MaxPartSize = DefaultS3MaxPartSize
	}
	if c.S3SmallEncryptBufferBudget <= 0 {
		c.S3SmallEncryptBufferBudget = DefaultS3SmallEncryptBufferBudget
	}
	if c.QueueMaxConsumers <= 0 {
		c.QueueMaxConsumers = DefaultQueueMaxConsumers()
	}
	if c.QueuePollInterval <= 0 {
		c.QueuePollInterval = DefaultQueuePollInterval
	}
	if c.QueueListPageSize <= 0 {
		c.QueueListPageSize = DefaultQueueListPageSize
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
	if !c.ConnguardEnabledSet {
		c.ConnguardEnabled = true
	}
	if c.ConnguardFailureThreshold < 0 {
		return fmt.Errorf("config: connguard failure threshold must be >= 0")
	}
	if c.ConnguardFailureThreshold == 0 {
		c.ConnguardFailureThreshold = DefaultConnguardFailureThreshold
	}
	if c.ConnguardFailureWindow < 0 {
		return fmt.Errorf("config: connguard failure window must be >= 0")
	}
	if c.ConnguardFailureWindow == 0 {
		c.ConnguardFailureWindow = DefaultConnguardFailureWindow
	}
	if c.ConnguardBlockDuration < 0 {
		return fmt.Errorf("config: connguard block duration must be >= 0")
	}
	if c.ConnguardBlockDuration == 0 {
		c.ConnguardBlockDuration = DefaultConnguardBlockDuration
	}
	if c.ConnguardProbeTimeout < 0 {
		return fmt.Errorf("config: connguard probe timeout must be >= 0")
	}
	if c.ConnguardProbeTimeout == 0 {
		c.ConnguardProbeTimeout = DefaultConnguardProbeTimeout
	}
	if c.ConnguardFailureThreshold < 2 {
		return fmt.Errorf("config: connguard failure threshold must be >= 2")
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
	if c.QRFQuerySoftLimit < 0 {
		return fmt.Errorf("config: qrf query soft limit must be >= 0")
	}
	if c.QRFQueryHardLimit < 0 {
		return fmt.Errorf("config: qrf query hard limit must be >= 0")
	}
	if c.QRFQueryHardLimit > 0 && (c.QRFQuerySoftLimit == 0 || c.QRFQuerySoftLimit > c.QRFQueryHardLimit) {
		c.QRFQuerySoftLimit = c.QRFQueryHardLimit
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
	if !c.QRFCPUPercentSoftLimitSet {
		c.QRFCPUPercentSoftLimit = DefaultQRFCPUPercentSoftLimit
	}
	if !c.QRFCPUPercentHardLimitSet {
		c.QRFCPUPercentHardLimit = DefaultQRFCPUPercentHardLimit
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
	if c.QRFSoftDelay <= 0 {
		c.QRFSoftDelay = DefaultQRFSoftDelay
	}
	if c.QRFEngagedDelay <= 0 {
		c.QRFEngagedDelay = DefaultQRFEngagedDelay
	}
	if c.QRFRecoveryDelay <= 0 {
		c.QRFRecoveryDelay = DefaultQRFRecoveryDelay
	}
	if c.QRFMaxWait <= 0 {
		c.QRFMaxWait = DefaultQRFMaxWait
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
			if !c.BundlePathDisableExpansion {
				path, err := pathutil.ExpandUserAndEnv(c.BundlePath)
				if err != nil {
					return fmt.Errorf("config: expand bundle path: %w", err)
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
