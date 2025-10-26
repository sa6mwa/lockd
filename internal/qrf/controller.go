package qrf

import (
	"math"
	"sync"
	"time"

	"pkt.systems/logport"
)

// Kind identifies the type of operation under evaluation.
type Kind int

const (
	// KindQueueProducer marks queue enqueue operations.
	KindQueueProducer Kind = iota
	// KindQueueConsumer marks queue dequeue operations.
	KindQueueConsumer
	// KindQueueAck marks queue ack/nack/extend operations.
	KindQueueAck
	// KindLock marks lock-related operations (acquire/keepalive/release, state ops).
	KindLock
)

// State represents the current posture of the quick reaction force.
type State int

const (
	// StateDisengaged indicates the QRF is idle.
	StateDisengaged State = iota
	// StateSoftArm denotes that the LSF has raised an alert and light throttling may apply.
	StateSoftArm
	// StateEngaged signals that aggressive throttling is in effect.
	StateEngaged
	// StateRecovery denotes the system is recovering and throttling is easing.
	StateRecovery
)

func (s State) String() string {
	switch s {
	case StateDisengaged:
		return "disengaged"
	case StateSoftArm:
		return "soft_arm"
	case StateEngaged:
		return "engaged"
	case StateRecovery:
		return "recovery"
	default:
		return "unknown"
	}
}

// Config configures controller thresholds and throttle timings.
type Config struct {
	Enabled bool

	QueueSoftLimit         int64
	QueueHardLimit         int64
	QueueConsumerSoftLimit int64
	QueueConsumerHardLimit int64
	LockSoftLimit          int64
	LockHardLimit          int64

	MemorySoftLimitBytes        uint64
	MemoryHardLimitBytes        uint64
	MemorySoftLimitPercent      float64
	MemoryHardLimitPercent      float64
	MemoryStrictHeadroomPercent float64
	SwapSoftLimitBytes          uint64
	SwapHardLimitBytes          uint64
	SwapSoftLimitPercent        float64
	SwapHardLimitPercent        float64

	CPUPercentSoftLimit float64
	CPUPercentHardLimit float64

	LoadSoftLimitMultiplier float64
	LoadHardLimitMultiplier float64

	RecoverySamples int

	SoftRetryAfter     time.Duration
	EngagedRetryAfter  time.Duration
	RecoveryRetryAfter time.Duration

	Logger logport.ForLogging
}

// Snapshot captures the instantaneous metrics observed by the LSF.
type Snapshot struct {
	QueueProducerInflight           int64
	QueueConsumerInflight           int64
	QueueAckInflight                int64
	LockInflight                    int64
	RSSBytes                        uint64
	SwapBytes                       uint64
	SystemMemoryUsedPercent         float64
	SystemMemoryIncludesReclaimable bool
	SystemSwapUsedPercent           float64
	SystemCPUPercent                float64
	SystemLoad1                     float64
	SystemLoad5                     float64
	SystemLoad15                    float64
	Load1Baseline                   float64
	Load5Baseline                   float64
	Load15Baseline                  float64
	Load1Multiplier                 float64
	Load5Multiplier                 float64
	Load15Multiplier                float64
	Goroutines                      int
	CollectedAt                     time.Time
}

// Decision reports whether an operation should be throttled.
type Decision struct {
	Throttle   bool
	RetryAfter time.Duration
	State      State
	Reason     string
}

// Controller manages the QRF state machine.
type Controller struct {
	cfg    Config
	logger logport.ForLogging

	mu                 sync.RWMutex
	state              State
	lastReason         string
	lastSnapshot       Snapshot
	consecutiveHealthy int
}

// NewController constructs a QRF controller using the supplied configuration.
func NewController(cfg Config) *Controller {
	logger := cfg.Logger
	if logger == nil {
		logger = logport.NoopLogger()
	}
	return &Controller{
		cfg:    cfg,
		logger: logger.With("sys", "control.qrf.controller"),
		state:  StateDisengaged,
	}
}

// Observe ingests a new snapshot from the LSF and updates the QRF posture.
func (c *Controller) Observe(snapshot Snapshot) {
	if !c.cfg.Enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastSnapshot = snapshot

	prev := c.state
	next := prev

	totalQueue := snapshot.QueueProducerInflight + snapshot.QueueConsumerInflight + snapshot.QueueAckInflight
	hard, hardReason := c.hardBreach(totalQueue, snapshot)
	soft, softReason := c.softBreach(totalQueue, snapshot)
	healthy := c.isHealthy(totalQueue, snapshot)

	switch {
	case hard:
		next = StateEngaged
		c.consecutiveHealthy = 0
		c.lastReason = hardReason
	case soft:
		if prev == StateEngaged {
			next = StateEngaged
			// keep current reason
		} else {
			next = StateSoftArm
			c.lastReason = softReason
		}
		c.consecutiveHealthy = 0
	default:
		if healthy {
			c.consecutiveHealthy++
		} else {
			c.consecutiveHealthy = 0
		}

		switch prev {
		case StateEngaged:
			if healthy && c.consecutiveHealthy >= c.cfg.RecoverySamples {
				next = StateRecovery
				c.consecutiveHealthy = 0
				c.lastReason = "metrics recovering"
			}
		case StateRecovery:
			if healthy && c.consecutiveHealthy >= c.cfg.RecoverySamples {
				next = StateDisengaged
				c.consecutiveHealthy = 0
				c.lastReason = "metrics stabilised"
			} else if soft {
				next = StateSoftArm
				c.consecutiveHealthy = 0
				c.lastReason = softReason
			} else if hard {
				next = StateEngaged
				c.consecutiveHealthy = 0
				c.lastReason = hardReason
			}
		case StateSoftArm:
			if healthy && c.consecutiveHealthy >= c.cfg.RecoverySamples {
				next = StateDisengaged
				c.consecutiveHealthy = 0
				c.lastReason = "metrics stabilised"
			}
		default: // Disengaged
			if healthy {
				// nothing to do
			}
		}
	}

	if next != prev {
		c.state = next
		c.logTransition(prev, next, c.lastReason, snapshot)
		if next == StateEngaged || next == StateSoftArm {
			c.consecutiveHealthy = 0
		}
	}
}

// Decide reports whether an operation of the given kind should be throttled.
func (c *Controller) Decide(kind Kind) Decision {
	if !c.cfg.Enabled {
		return Decision{Throttle: false, State: StateDisengaged}
	}

	c.mu.RLock()
	state := c.state
	reason := c.lastReason
	snapshot := c.lastSnapshot
	c.mu.RUnlock()

	consumerHardExceeded := c.consumerHardExceeded(snapshot)
	consumerLimitExceeded := consumerHardExceeded || c.consumerSoftExceeded(snapshot)
	needsMoreConsumers := snapshot.QueueProducerInflight > snapshot.QueueConsumerInflight

	totalQueue := snapshot.QueueProducerInflight + snapshot.QueueConsumerInflight + snapshot.QueueAckInflight
	healthy := c.isHealthy(totalQueue, snapshot)

	switch state {
	case StateDisengaged:
		return Decision{Throttle: false, State: StateDisengaged}
	case StateSoftArm:
		switch kind {
		case KindQueueProducer:
			if healthy {
				return Decision{Throttle: false, State: StateSoftArm}
			}
			return Decision{
				Throttle:   true,
				State:      StateSoftArm,
				RetryAfter: nonZero(c.cfg.SoftRetryAfter, 50*time.Millisecond),
				Reason:     reason,
			}
		case KindQueueConsumer, KindQueueAck:
			if consumerHardExceeded || (consumerLimitExceeded && !needsMoreConsumers) {
				limitReason := reason
				if consumerHardExceeded {
					limitReason = "queue_consumer_hard"
				} else {
					limitReason = "queue_consumer_soft"
				}
				return Decision{
					Throttle:   true,
					State:      StateSoftArm,
					RetryAfter: nonZero(c.cfg.SoftRetryAfter, 50*time.Millisecond),
					Reason:     limitReason,
				}
			}
			return Decision{Throttle: false, State: state}
		default:
			return Decision{
				Throttle:   true,
				State:      StateSoftArm,
				RetryAfter: nonZero(c.cfg.SoftRetryAfter, 50*time.Millisecond),
				Reason:     reason,
			}
		}
	case StateEngaged:
		switch kind {
		case KindQueueProducer:
			return Decision{
				Throttle:   true,
				State:      StateEngaged,
				RetryAfter: nonZero(c.cfg.EngagedRetryAfter, 500*time.Millisecond),
				Reason:     reason,
			}
		case KindQueueConsumer, KindQueueAck:
			if consumerHardExceeded || (consumerLimitExceeded && !needsMoreConsumers) {
				limitReason := reason
				if consumerHardExceeded {
					limitReason = "queue_consumer_hard"
				} else {
					limitReason = "queue_consumer_soft"
				}
				return Decision{
					Throttle:   true,
					State:      StateEngaged,
					RetryAfter: nonZero(c.cfg.RecoveryRetryAfter, 200*time.Millisecond),
					Reason:     limitReason,
				}
			}
			if c.cfg.QueueSoftLimit == 0 || totalQueue <= c.cfg.QueueSoftLimit {
				return Decision{Throttle: false, State: state}
			}
			if snapshot.QueueProducerInflight == 0 {
				return Decision{Throttle: false, State: state}
			}
			if snapshot.QueueConsumerInflight <= snapshot.QueueProducerInflight {
				return Decision{Throttle: false, State: state}
			}
			return Decision{
				Throttle:   true,
				State:      StateEngaged,
				RetryAfter: nonZero(c.cfg.RecoveryRetryAfter, 200*time.Millisecond),
				Reason:     reason,
			}
		default:
			return Decision{
				Throttle:   true,
				State:      StateEngaged,
				RetryAfter: nonZero(c.cfg.EngagedRetryAfter, 500*time.Millisecond),
				Reason:     reason,
			}
		}
	case StateRecovery:
		switch kind {
		case KindQueueProducer:
			return Decision{
				Throttle:   true,
				State:      StateRecovery,
				RetryAfter: nonZero(c.cfg.RecoveryRetryAfter, 200*time.Millisecond),
				Reason:     reason,
			}
		case KindQueueConsumer, KindQueueAck:
			if consumerHardExceeded || (consumerLimitExceeded && !needsMoreConsumers) {
				limitReason := reason
				if consumerHardExceeded {
					limitReason = "queue_consumer_hard"
				} else {
					limitReason = "queue_consumer_soft"
				}
				return Decision{
					Throttle:   true,
					State:      StateRecovery,
					RetryAfter: nonZero(c.cfg.RecoveryRetryAfter, 200*time.Millisecond),
					Reason:     limitReason,
				}
			}
			if c.cfg.QueueSoftLimit == 0 || totalQueue <= c.cfg.QueueSoftLimit {
				return Decision{Throttle: false, State: state}
			}
			if snapshot.QueueProducerInflight == 0 {
				return Decision{Throttle: false, State: state}
			}
			if snapshot.QueueConsumerInflight <= snapshot.QueueProducerInflight {
				return Decision{Throttle: false, State: state}
			}
			return Decision{
				Throttle:   true,
				State:      StateRecovery,
				RetryAfter: nonZero(c.cfg.RecoveryRetryAfter, 200*time.Millisecond),
				Reason:     reason,
			}
		default:
			return Decision{
				Throttle:   true,
				State:      StateRecovery,
				RetryAfter: nonZero(c.cfg.RecoveryRetryAfter, 200*time.Millisecond),
				Reason:     reason,
			}
		}
	default:
		return Decision{Throttle: false, State: state}
	}
}

// State returns the current QRF posture.
func (c *Controller) State() State {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// Snapshot returns the last metrics snapshot observed by the controller.
func (c *Controller) Snapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastSnapshot
}

// Status returns the current state, reason, and snapshot without mutating controller state.
func (c *Controller) Status() (State, string, Snapshot) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state, c.lastReason, c.lastSnapshot
}

func (c *Controller) hardBreach(totalQueue int64, s Snapshot) (bool, string) {
	memPercent := c.effectiveMemoryPercent(s)
	if c.cfg.QueueHardLimit > 0 && totalQueue >= c.cfg.QueueHardLimit {
		return true, "queue_inflight_hard"
	}
	if c.consumerHardExceeded(s) {
		return true, "queue_consumer_hard"
	}
	if c.cfg.LockHardLimit > 0 && s.LockInflight >= c.cfg.LockHardLimit {
		return true, "lock_inflight_hard"
	}
	if c.cfg.MemoryHardLimitPercent > 0 && memPercent >= c.cfg.MemoryHardLimitPercent {
		return true, "memory_hard"
	}
	if c.cfg.MemoryHardLimitBytes > 0 && s.RSSBytes >= c.cfg.MemoryHardLimitBytes {
		return true, "memory_hard"
	}
	if c.cfg.SwapHardLimitPercent > 0 && s.SystemSwapUsedPercent >= c.cfg.SwapHardLimitPercent {
		return true, "swap_hard"
	}
	if c.cfg.SwapHardLimitBytes > 0 && s.SwapBytes >= c.cfg.SwapHardLimitBytes {
		return true, "swap_hard"
	}
	if c.cfg.CPUPercentHardLimit > 0 && s.SystemCPUPercent >= c.cfg.CPUPercentHardLimit {
		return true, "cpu_hard"
	}
	if c.cfg.LoadHardLimitMultiplier > 0 && s.Load1Multiplier >= c.cfg.LoadHardLimitMultiplier {
		return true, "load_hard"
	}
	return false, ""
}

func (c *Controller) softBreach(totalQueue int64, s Snapshot) (bool, string) {
	memPercent := c.effectiveMemoryPercent(s)
	if c.cfg.QueueSoftLimit > 0 && totalQueue >= c.cfg.QueueSoftLimit {
		return true, "queue_inflight_soft"
	}
	if c.consumerSoftExceeded(s) {
		return true, "queue_consumer_soft"
	}
	if c.cfg.LockSoftLimit > 0 && s.LockInflight >= c.cfg.LockSoftLimit {
		return true, "lock_inflight_soft"
	}
	if c.cfg.MemorySoftLimitPercent > 0 && memPercent >= c.cfg.MemorySoftLimitPercent {
		return true, "memory_soft"
	}
	if c.cfg.MemorySoftLimitBytes > 0 && s.RSSBytes >= c.cfg.MemorySoftLimitBytes {
		return true, "memory_soft"
	}
	if c.cfg.SwapSoftLimitPercent > 0 && s.SystemSwapUsedPercent >= c.cfg.SwapSoftLimitPercent {
		return true, "swap_soft"
	}
	if c.cfg.SwapSoftLimitBytes > 0 && s.SwapBytes >= c.cfg.SwapSoftLimitBytes {
		return true, "swap_soft"
	}
	if c.cfg.CPUPercentSoftLimit > 0 && s.SystemCPUPercent >= c.cfg.CPUPercentSoftLimit {
		return true, "cpu_soft"
	}
	if c.cfg.LoadSoftLimitMultiplier > 0 && s.Load1Multiplier >= c.cfg.LoadSoftLimitMultiplier {
		return true, "load_soft"
	}
	return false, ""
}

func (c *Controller) isHealthy(totalQueue int64, s Snapshot) bool {
	memPercent := c.effectiveMemoryPercent(s)
	queueHealthy := c.cfg.QueueSoftLimit == 0 || totalQueue <= maxInt64(1, c.cfg.QueueSoftLimit/2)
	if !queueHealthy && c.cfg.QueueSoftLimit > 0 && s.QueueProducerInflight == 0 && totalQueue <= c.cfg.QueueSoftLimit {
		queueHealthy = true
	}
	consumerHealthy := c.consumerHealthy(s)
	lockHealthy := c.cfg.LockSoftLimit == 0 || s.LockInflight <= maxInt64(1, c.cfg.LockSoftLimit/2)
	memHealthy := (c.cfg.MemorySoftLimitPercent == 0 || memPercent <= percentRecoveryTarget(c.cfg.MemorySoftLimitPercent)) && (c.cfg.MemorySoftLimitBytes == 0 || s.RSSBytes <= c.cfg.MemorySoftLimitBytes/2)
	swapHealthy := (c.cfg.SwapSoftLimitPercent == 0 || s.SystemSwapUsedPercent <= percentRecoveryTarget(c.cfg.SwapSoftLimitPercent)) && (c.cfg.SwapSoftLimitBytes == 0 || s.SwapBytes <= c.cfg.SwapSoftLimitBytes/2)
	cpuHealthy := c.cfg.CPUPercentSoftLimit == 0 || s.SystemCPUPercent <= percentRecoveryTarget(c.cfg.CPUPercentSoftLimit)
	loadHealthy := c.cfg.LoadSoftLimitMultiplier == 0 || s.Load1Multiplier <= multiplierRecoveryTarget(c.cfg.LoadSoftLimitMultiplier)
	return queueHealthy && consumerHealthy && lockHealthy && memHealthy && swapHealthy && cpuHealthy && loadHealthy
}

func (c *Controller) logTransition(prev, next State, reason string, snapshot Snapshot) {
	effectiveMemory := c.effectiveMemoryPercent(snapshot)
	switch next {
	case StateEngaged:
		c.logger.Warn("lockd.qrf.engaged",
			"previous_state", prev.String(),
			"reason", reason,
			"queue_producer_inflight", snapshot.QueueProducerInflight,
			"queue_consumer_inflight", snapshot.QueueConsumerInflight,
			"queue_ack_inflight", snapshot.QueueAckInflight,
			"lock_inflight", snapshot.LockInflight,
			"rss_bytes", snapshot.RSSBytes,
			"swap_bytes", snapshot.SwapBytes,
			"system_memory_percent", snapshot.SystemMemoryUsedPercent,
			"system_memory_includes_reclaimable", snapshot.SystemMemoryIncludesReclaimable,
			"system_memory_percent_effective", effectiveMemory,
			"system_swap_percent", snapshot.SystemSwapUsedPercent,
			"system_cpu_percent", snapshot.SystemCPUPercent,
			"system_load1", snapshot.SystemLoad1,
			"system_load5", snapshot.SystemLoad5,
			"system_load15", snapshot.SystemLoad15,
			"load1_multiplier", snapshot.Load1Multiplier,
			"load5_multiplier", snapshot.Load5Multiplier,
			"load15_multiplier", snapshot.Load15Multiplier,
			"goroutines", snapshot.Goroutines,
		)
	case StateSoftArm:
		c.logger.Info("lockd.qrf.soft_arm",
			"previous_state", prev.String(),
			"reason", reason,
			"queue_producer_inflight", snapshot.QueueProducerInflight,
			"queue_consumer_inflight", snapshot.QueueConsumerInflight,
			"queue_ack_inflight", snapshot.QueueAckInflight,
			"lock_inflight", snapshot.LockInflight,
			"rss_bytes", snapshot.RSSBytes,
			"swap_bytes", snapshot.SwapBytes,
			"system_memory_percent", snapshot.SystemMemoryUsedPercent,
			"system_memory_includes_reclaimable", snapshot.SystemMemoryIncludesReclaimable,
			"system_memory_percent_effective", effectiveMemory,
			"system_swap_percent", snapshot.SystemSwapUsedPercent,
			"system_cpu_percent", snapshot.SystemCPUPercent,
			"system_load1", snapshot.SystemLoad1,
			"system_load5", snapshot.SystemLoad5,
			"system_load15", snapshot.SystemLoad15,
			"load1_multiplier", snapshot.Load1Multiplier,
			"load5_multiplier", snapshot.Load5Multiplier,
			"load15_multiplier", snapshot.Load15Multiplier,
			"goroutines", snapshot.Goroutines,
		)
	case StateRecovery:
		c.logger.Info("lockd.qrf.recovery",
			"previous_state", prev.String(),
			"reason", reason,
			"queue_producer_inflight", snapshot.QueueProducerInflight,
			"queue_consumer_inflight", snapshot.QueueConsumerInflight,
			"queue_ack_inflight", snapshot.QueueAckInflight,
			"lock_inflight", snapshot.LockInflight,
			"rss_bytes", snapshot.RSSBytes,
			"swap_bytes", snapshot.SwapBytes,
			"system_memory_percent", snapshot.SystemMemoryUsedPercent,
			"system_memory_includes_reclaimable", snapshot.SystemMemoryIncludesReclaimable,
			"system_memory_percent_effective", effectiveMemory,
			"system_swap_percent", snapshot.SystemSwapUsedPercent,
			"system_cpu_percent", snapshot.SystemCPUPercent,
			"system_load1", snapshot.SystemLoad1,
			"system_load5", snapshot.SystemLoad5,
			"system_load15", snapshot.SystemLoad15,
			"load1_multiplier", snapshot.Load1Multiplier,
			"load5_multiplier", snapshot.Load5Multiplier,
			"load15_multiplier", snapshot.Load15Multiplier,
			"goroutines", snapshot.Goroutines,
		)
	case StateDisengaged:
		c.logger.Info("lockd.qrf.disengaged",
			"previous_state", prev.String(),
			"reason", reason,
			"queue_producer_inflight", snapshot.QueueProducerInflight,
			"queue_consumer_inflight", snapshot.QueueConsumerInflight,
			"queue_ack_inflight", snapshot.QueueAckInflight,
			"lock_inflight", snapshot.LockInflight,
			"rss_bytes", snapshot.RSSBytes,
			"swap_bytes", snapshot.SwapBytes,
			"system_memory_percent", snapshot.SystemMemoryUsedPercent,
			"system_memory_includes_reclaimable", snapshot.SystemMemoryIncludesReclaimable,
			"system_memory_percent_effective", effectiveMemory,
			"system_swap_percent", snapshot.SystemSwapUsedPercent,
			"system_cpu_percent", snapshot.SystemCPUPercent,
			"system_load1", snapshot.SystemLoad1,
			"system_load5", snapshot.SystemLoad5,
			"system_load15", snapshot.SystemLoad15,
			"load1_multiplier", snapshot.Load1Multiplier,
			"load5_multiplier", snapshot.Load5Multiplier,
			"load15_multiplier", snapshot.Load15Multiplier,
			"goroutines", snapshot.Goroutines,
		)
	}
}

func nonZero(d time.Duration, fallback time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return fallback
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (c *Controller) effectiveMemoryPercent(s Snapshot) float64 {
	percent := s.SystemMemoryUsedPercent
	if s.SystemMemoryIncludesReclaimable {
		return percent
	}
	headroom := c.cfg.MemoryStrictHeadroomPercent
	if headroom <= 0 {
		return percent
	}
	adjusted := percent - headroom
	if adjusted < 0 {
		return 0
	}
	return adjusted
}

func percentRecoveryTarget(limit float64) float64 {
	if limit <= 0 {
		return 0
	}
	target := limit - 10
	if target < 0 {
		target = 0
	}
	return target
}

func multiplierRecoveryTarget(limit float64) float64 {
	if limit <= 0 {
		return 0
	}
	if limit <= 1 {
		return 1
	}
	return math.Max(1, limit*0.5)
}

func (c *Controller) consumerSoftExceeded(s Snapshot) bool {
	if c.cfg.QueueConsumerSoftLimit <= 0 {
		return false
	}
	return s.QueueConsumerInflight >= c.cfg.QueueConsumerSoftLimit
}

func (c *Controller) consumerHardExceeded(s Snapshot) bool {
	if c.cfg.QueueConsumerHardLimit <= 0 {
		return false
	}
	return s.QueueConsumerInflight >= c.cfg.QueueConsumerHardLimit
}

func (c *Controller) consumerHealthy(s Snapshot) bool {
	limit := c.cfg.QueueConsumerSoftLimit
	if limit <= 0 {
		limit = c.cfg.QueueConsumerHardLimit
	}
	if limit <= 0 {
		return true
	}
	return s.QueueConsumerInflight <= maxInt64(1, limit/2)
}
