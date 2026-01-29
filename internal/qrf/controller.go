package qrf

import (
	"context"
	"math"
	"sync"
	"time"

	"pkt.systems/lockd/internal/svcfields"
	"pkt.systems/pslog"
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
	// KindQuery marks query operations.
	KindQuery
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
	QuerySoftLimit         int64
	QueryHardLimit         int64

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

	SoftDelay     time.Duration
	EngagedDelay  time.Duration
	RecoveryDelay time.Duration
	MaxWait       time.Duration

	Logger pslog.Logger
}

// Snapshot captures the instantaneous metrics observed by the LSF.
type Snapshot struct {
	QueueProducerInflight           int64
	QueueConsumerInflight           int64
	QueueAckInflight                int64
	LockInflight                    int64
	QueryInflight                   int64
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

// Status reports the current controller state and snapshot.
type Status struct {
	State    State
	Reason   string
	Snapshot Snapshot
}

// Decision reports whether an operation should be throttled.
type Decision struct {
	Throttle bool
	Delay    time.Duration
	State    State
	Reason   string
}

// WaitError is returned when the throttle delay exceeds the configured max wait.
type WaitError struct {
	Delay  time.Duration
	Reason string
}

func (e *WaitError) Error() string {
	return "throttled: perimeter defence engaged"
}

// Controller manages the QRF state machine.
type Controller struct {
	cfg     Config
	logger  pslog.Logger
	metrics *qrfMetrics

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
		logger = pslog.NoopLogger()
	}
	controller := &Controller{
		cfg:    cfg,
		logger: svcfields.WithSubsystem(logger, "control.qrf.controller"),
		state:  StateDisengaged,
	}
	controller.metrics = newQRFMetrics(logger, controller)
	return controller
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
		default:
			// Disengaged stays idle until soft/hard trigger arrives.
		}
	}

	if next != prev {
		c.state = next
		c.logTransition(prev, next, c.lastReason, snapshot)
		if c.metrics != nil {
			c.metrics.recordTransition(context.TODO(), prev, next, c.lastReason)
		}
		if next == StateEngaged || next == StateSoftArm {
			c.consecutiveHealthy = 0
		}
	}
}

// Decide reports whether an operation of the given kind should be throttled.
func (c *Controller) Decide(kind Kind) Decision {
	if !c.cfg.Enabled {
		return c.recordDecision(kind, Decision{Throttle: false, State: StateDisengaged})
	}

	c.mu.RLock()
	state := c.state
	reason := c.lastReason
	snapshot := c.lastSnapshot
	c.mu.RUnlock()

	totalQueue := snapshot.QueueProducerInflight + snapshot.QueueConsumerInflight + snapshot.QueueAckInflight
	consumerHardExceeded := c.consumerHardExceeded(snapshot)
	consumerLimitExceeded := consumerHardExceeded || c.consumerSoftExceeded(snapshot)
	needsMoreConsumers := snapshot.QueueProducerInflight > snapshot.QueueConsumerInflight

	queueSoftExceeded := c.queueSoftExceeded(totalQueue)
	queueHardExceeded := c.queueHardExceeded(totalQueue)
	lockSoftExceeded := c.lockSoftExceeded(snapshot)
	lockHardExceeded := c.lockHardExceeded(snapshot)
	querySoftExceeded := c.querySoftExceeded(snapshot)
	queryHardExceeded := c.queryHardExceeded(snapshot)
	globalSoftExceeded := c.globalSoftExceeded(snapshot)
	globalHardExceeded := c.globalHardExceeded(snapshot)
	globalPressure := globalSoftExceeded || globalHardExceeded
	queueDominant, lockDominant, queryDominant := dominantKinds(snapshot)
	anyDominant := queueDominant || lockDominant || queryDominant

	switch state {
	case StateDisengaged:
		return c.recordDecision(kind, Decision{Throttle: false, State: StateDisengaged})
	case StateSoftArm:
		switch kind {
		case KindQueueProducer:
			if queueSoftExceeded || queueHardExceeded {
				return c.recordDecision(kind, Decision{
					Throttle: true,
					State:    StateSoftArm,
					Delay:    nonZero(c.cfg.SoftDelay, 50*time.Millisecond),
					Reason:   reason,
				})
			}
			if globalPressure && (queueDominant || !anyDominant) {
				return c.recordDecision(kind, Decision{
					Throttle: true,
					State:    StateSoftArm,
					Delay:    nonZero(c.cfg.SoftDelay, 50*time.Millisecond),
					Reason:   reason,
				})
			}
			return c.recordDecision(kind, Decision{Throttle: false, State: StateSoftArm})
		case KindQueueConsumer, KindQueueAck:
			if consumerHardExceeded || (consumerLimitExceeded && !needsMoreConsumers) {
				limitReason := "queue_consumer_soft"
				if consumerHardExceeded {
					limitReason = "queue_consumer_hard"
				}
				return c.recordDecision(kind, Decision{
					Throttle: true,
					State:    StateSoftArm,
					Delay:    nonZero(c.cfg.SoftDelay, 50*time.Millisecond),
					Reason:   limitReason,
				})
			}
			return c.recordDecision(kind, Decision{Throttle: false, State: state})
		case KindLock:
			return c.decideNonQueue(kind, state, reason, lockSoftExceeded, lockHardExceeded, globalPressure, lockDominant, anyDominant, "lock_inflight_soft", "lock_inflight_hard")
		case KindQuery:
			return c.decideNonQueue(kind, state, reason, querySoftExceeded, queryHardExceeded, globalPressure, queryDominant, anyDominant, "query_inflight_soft", "query_inflight_hard")
		default:
			return c.decideNonQueue(kind, state, reason, false, false, globalPressure, false, anyDominant, "", "")
		}
	case StateEngaged:
		switch kind {
		case KindQueueProducer:
			if queueHardExceeded || queueSoftExceeded {
				return c.recordDecision(kind, Decision{
					Throttle: true,
					State:    StateEngaged,
					Delay:    nonZero(c.cfg.EngagedDelay, 500*time.Millisecond),
					Reason:   reason,
				})
			}
			if globalPressure && (queueDominant || !anyDominant) {
				return c.recordDecision(kind, Decision{
					Throttle: true,
					State:    StateEngaged,
					Delay:    nonZero(c.cfg.EngagedDelay, 500*time.Millisecond),
					Reason:   reason,
				})
			}
			return c.recordDecision(kind, Decision{Throttle: false, State: state})
		case KindQueueConsumer, KindQueueAck:
			if consumerHardExceeded || (consumerLimitExceeded && !needsMoreConsumers) {
				limitReason := "queue_consumer_soft"
				if consumerHardExceeded {
					limitReason = "queue_consumer_hard"
				}
				return c.recordDecision(kind, Decision{
					Throttle: true,
					State:    StateEngaged,
					Delay:    nonZero(c.cfg.RecoveryDelay, 200*time.Millisecond),
					Reason:   limitReason,
				})
			}
			if c.cfg.QueueSoftLimit == 0 || totalQueue <= c.cfg.QueueSoftLimit {
				return c.recordDecision(kind, Decision{Throttle: false, State: state})
			}
			if snapshot.QueueProducerInflight == 0 {
				return c.recordDecision(kind, Decision{Throttle: false, State: state})
			}
			if snapshot.QueueConsumerInflight <= snapshot.QueueProducerInflight {
				return c.recordDecision(kind, Decision{Throttle: false, State: state})
			}
			return c.recordDecision(kind, Decision{
				Throttle: true,
				State:    StateEngaged,
				Delay:    nonZero(c.cfg.RecoveryDelay, 200*time.Millisecond),
				Reason:   reason,
			})
		case KindLock:
			return c.decideNonQueue(kind, state, reason, lockSoftExceeded, lockHardExceeded, globalPressure, lockDominant, anyDominant, "lock_inflight_soft", "lock_inflight_hard")
		case KindQuery:
			return c.decideNonQueue(kind, state, reason, querySoftExceeded, queryHardExceeded, globalPressure, queryDominant, anyDominant, "query_inflight_soft", "query_inflight_hard")
		default:
			return c.decideNonQueue(kind, state, reason, false, false, globalPressure, false, anyDominant, "", "")
		}
	case StateRecovery:
		switch kind {
		case KindQueueProducer:
			if queueHardExceeded || queueSoftExceeded {
				return c.recordDecision(kind, Decision{
					Throttle: true,
					State:    StateRecovery,
					Delay:    nonZero(c.cfg.RecoveryDelay, 200*time.Millisecond),
					Reason:   reason,
				})
			}
			if globalPressure && (queueDominant || !anyDominant) {
				return c.recordDecision(kind, Decision{
					Throttle: true,
					State:    StateRecovery,
					Delay:    nonZero(c.cfg.RecoveryDelay, 200*time.Millisecond),
					Reason:   reason,
				})
			}
			return c.recordDecision(kind, Decision{Throttle: false, State: state})
		case KindQueueConsumer, KindQueueAck:
			if consumerHardExceeded || (consumerLimitExceeded && !needsMoreConsumers) {
				limitReason := "queue_consumer_soft"
				if consumerHardExceeded {
					limitReason = "queue_consumer_hard"
				}
				return c.recordDecision(kind, Decision{
					Throttle: true,
					State:    StateRecovery,
					Delay:    nonZero(c.cfg.RecoveryDelay, 200*time.Millisecond),
					Reason:   limitReason,
				})
			}
			if c.cfg.QueueSoftLimit == 0 || totalQueue <= c.cfg.QueueSoftLimit {
				return c.recordDecision(kind, Decision{Throttle: false, State: state})
			}
			if snapshot.QueueProducerInflight == 0 {
				return c.recordDecision(kind, Decision{Throttle: false, State: state})
			}
			if snapshot.QueueConsumerInflight <= snapshot.QueueProducerInflight {
				return c.recordDecision(kind, Decision{Throttle: false, State: state})
			}
			return c.recordDecision(kind, Decision{
				Throttle: true,
				State:    StateRecovery,
				Delay:    nonZero(c.cfg.RecoveryDelay, 200*time.Millisecond),
				Reason:   reason,
			})
		case KindLock:
			return c.decideNonQueue(kind, state, reason, lockSoftExceeded, lockHardExceeded, globalPressure, lockDominant, anyDominant, "lock_inflight_soft", "lock_inflight_hard")
		case KindQuery:
			return c.decideNonQueue(kind, state, reason, querySoftExceeded, queryHardExceeded, globalPressure, queryDominant, anyDominant, "query_inflight_soft", "query_inflight_hard")
		default:
			return c.decideNonQueue(kind, state, reason, false, false, globalPressure, false, anyDominant, "", "")
		}
	default:
		return c.recordDecision(kind, Decision{Throttle: false, State: state})
	}
}

// Wait applies throttling by sleeping for a computed duration when pressure is detected.
// It returns a WaitError only if the computed delay exceeds the configured max wait.
func (c *Controller) Wait(ctx context.Context, kind Kind) error {
	if c == nil || !c.cfg.Enabled {
		return nil
	}
	decision := c.Decide(kind)
	if !decision.Throttle {
		return nil
	}
	delay := c.delayForDecision(decision)
	if delay <= 0 {
		return nil
	}
	maxWait := c.cfg.MaxWait
	if maxWait <= 0 {
		return &WaitError{Delay: delay, Reason: decision.Reason}
	}
	waitFor := delay
	if maxWait > 0 && waitFor > maxWait {
		waitFor = maxWait
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctxDeadlineSoon := false
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return ctx.Err()
		}
		if remaining < delay {
			ctxDeadlineSoon = true
		}
		if remaining < waitFor {
			waitFor = remaining
		}
	}
	if err := sleepWithContext(ctx, waitFor); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if ctxDeadlineSoon {
		return context.DeadlineExceeded
	}
	if maxWait > 0 && delay > maxWait {
		return &WaitError{Delay: delay, Reason: decision.Reason}
	}
	return nil
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
func (c *Controller) Status() Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return Status{State: c.state, Reason: c.lastReason, Snapshot: c.lastSnapshot}
}

func (c *Controller) recordDecision(kind Kind, decision Decision) Decision {
	if c == nil || c.metrics == nil {
		return decision
	}
	c.metrics.recordDecision(context.TODO(), kind, decision)
	return decision
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
	if c.cfg.QueryHardLimit > 0 && s.QueryInflight >= c.cfg.QueryHardLimit {
		return true, "query_inflight_hard"
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
	if c.cfg.QuerySoftLimit > 0 && s.QueryInflight >= c.cfg.QuerySoftLimit {
		return true, "query_inflight_soft"
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
	queryHealthy := c.cfg.QuerySoftLimit == 0 || s.QueryInflight <= maxInt64(1, c.cfg.QuerySoftLimit/2)
	memHealthy := (c.cfg.MemorySoftLimitPercent == 0 || memPercent <= percentRecoveryTarget(c.cfg.MemorySoftLimitPercent)) && (c.cfg.MemorySoftLimitBytes == 0 || s.RSSBytes <= c.cfg.MemorySoftLimitBytes/2)
	swapHealthy := (c.cfg.SwapSoftLimitPercent == 0 || s.SystemSwapUsedPercent <= percentRecoveryTarget(c.cfg.SwapSoftLimitPercent)) && (c.cfg.SwapSoftLimitBytes == 0 || s.SwapBytes <= c.cfg.SwapSoftLimitBytes/2)
	cpuHealthy := c.cfg.CPUPercentSoftLimit == 0 || s.SystemCPUPercent <= percentRecoveryTarget(c.cfg.CPUPercentSoftLimit)
	loadHealthy := c.cfg.LoadSoftLimitMultiplier == 0 || s.Load1Multiplier <= multiplierRecoveryTarget(c.cfg.LoadSoftLimitMultiplier)
	return queueHealthy && consumerHealthy && lockHealthy && queryHealthy && memHealthy && swapHealthy && cpuHealthy && loadHealthy
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
			"query_inflight", snapshot.QueryInflight,
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
			"query_inflight", snapshot.QueryInflight,
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
			"query_inflight", snapshot.QueryInflight,
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
			"query_inflight", snapshot.QueryInflight,
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

func (c *Controller) queueSoftExceeded(totalQueue int64) bool {
	return c.cfg.QueueSoftLimit > 0 && totalQueue >= c.cfg.QueueSoftLimit
}

func (c *Controller) queueHardExceeded(totalQueue int64) bool {
	return c.cfg.QueueHardLimit > 0 && totalQueue >= c.cfg.QueueHardLimit
}

func (c *Controller) lockSoftExceeded(s Snapshot) bool {
	return c.cfg.LockSoftLimit > 0 && s.LockInflight >= c.cfg.LockSoftLimit
}

func (c *Controller) lockHardExceeded(s Snapshot) bool {
	return c.cfg.LockHardLimit > 0 && s.LockInflight >= c.cfg.LockHardLimit
}

func (c *Controller) querySoftExceeded(s Snapshot) bool {
	return c.cfg.QuerySoftLimit > 0 && s.QueryInflight >= c.cfg.QuerySoftLimit
}

func (c *Controller) queryHardExceeded(s Snapshot) bool {
	return c.cfg.QueryHardLimit > 0 && s.QueryInflight >= c.cfg.QueryHardLimit
}

func (c *Controller) globalSoftExceeded(s Snapshot) bool {
	memPercent := c.effectiveMemoryPercent(s)
	if c.cfg.MemorySoftLimitPercent > 0 && memPercent >= c.cfg.MemorySoftLimitPercent {
		return true
	}
	if c.cfg.MemorySoftLimitBytes > 0 && s.RSSBytes >= c.cfg.MemorySoftLimitBytes {
		return true
	}
	if c.cfg.SwapSoftLimitPercent > 0 && s.SystemSwapUsedPercent >= c.cfg.SwapSoftLimitPercent {
		return true
	}
	if c.cfg.SwapSoftLimitBytes > 0 && s.SwapBytes >= c.cfg.SwapSoftLimitBytes {
		return true
	}
	if c.cfg.CPUPercentSoftLimit > 0 && s.SystemCPUPercent >= c.cfg.CPUPercentSoftLimit {
		return true
	}
	if c.cfg.LoadSoftLimitMultiplier > 0 && s.Load1Multiplier >= c.cfg.LoadSoftLimitMultiplier {
		return true
	}
	return false
}

func (c *Controller) globalHardExceeded(s Snapshot) bool {
	memPercent := c.effectiveMemoryPercent(s)
	if c.cfg.MemoryHardLimitPercent > 0 && memPercent >= c.cfg.MemoryHardLimitPercent {
		return true
	}
	if c.cfg.MemoryHardLimitBytes > 0 && s.RSSBytes >= c.cfg.MemoryHardLimitBytes {
		return true
	}
	if c.cfg.SwapHardLimitPercent > 0 && s.SystemSwapUsedPercent >= c.cfg.SwapHardLimitPercent {
		return true
	}
	if c.cfg.SwapHardLimitBytes > 0 && s.SwapBytes >= c.cfg.SwapHardLimitBytes {
		return true
	}
	if c.cfg.CPUPercentHardLimit > 0 && s.SystemCPUPercent >= c.cfg.CPUPercentHardLimit {
		return true
	}
	if c.cfg.LoadHardLimitMultiplier > 0 && s.Load1Multiplier >= c.cfg.LoadHardLimitMultiplier {
		return true
	}
	return false
}

func dominantKinds(s Snapshot) (queue bool, lock bool, query bool) {
	queueTotal := s.QueueProducerInflight + s.QueueConsumerInflight + s.QueueAckInflight
	max := queueTotal
	if s.LockInflight > max {
		max = s.LockInflight
	}
	if s.QueryInflight > max {
		max = s.QueryInflight
	}
	if max == 0 {
		return false, false, false
	}
	return queueTotal == max, s.LockInflight == max, s.QueryInflight == max
}

func (c *Controller) decideNonQueue(kind Kind, state State, reason string, softExceeded, hardExceeded bool, globalPressure bool, dominant bool, anyDominant bool, softReason, hardReason string) Decision {
	if hardExceeded {
		return c.recordDecision(kind, Decision{
			Throttle: true,
			State:    state,
			Delay:    baseDelayForState(c.cfg, state),
			Reason:   hardReason,
		})
	}
	if softExceeded {
		return c.recordDecision(kind, Decision{
			Throttle: true,
			State:    state,
			Delay:    baseDelayForState(c.cfg, state),
			Reason:   softReason,
		})
	}
	if globalPressure && (dominant || !anyDominant) {
		return c.recordDecision(kind, Decision{
			Throttle: true,
			State:    state,
			Delay:    baseDelayForState(c.cfg, state),
			Reason:   reason,
		})
	}
	return c.recordDecision(kind, Decision{Throttle: false, State: state})
}

func baseDelayForState(cfg Config, state State) time.Duration {
	switch state {
	case StateSoftArm:
		return nonZero(cfg.SoftDelay, 50*time.Millisecond)
	case StateEngaged:
		return nonZero(cfg.EngagedDelay, 500*time.Millisecond)
	case StateRecovery:
		return nonZero(cfg.RecoveryDelay, 200*time.Millisecond)
	default:
		return 0
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Controller) delayForDecision(decision Decision) time.Duration {
	base := decision.Delay
	if base <= 0 {
		base = baseDelayForState(c.cfg, decision.State)
	}
	if base <= 0 {
		return 0
	}
	pressure := c.pressureForReason(decision.Reason)
	if pressure <= 0 {
		pressure = 0.1
	}
	if pressure > 1 {
		pressure = 1
	}
	minDelay := minDelayForState(decision.State, base)
	scaled := time.Duration(float64(base) * pressure)
	if scaled < minDelay {
		scaled = minDelay
	}
	if scaled > base {
		scaled = base
	}
	return scaled
}

func minDelayForState(state State, base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	min := base / 10
	switch state {
	case StateEngaged:
		if min < 10*time.Millisecond {
			min = 10 * time.Millisecond
		}
	case StateRecovery:
		if min < 5*time.Millisecond {
			min = 5 * time.Millisecond
		}
	default:
		if min < 2*time.Millisecond {
			min = 2 * time.Millisecond
		}
	}
	if min > base {
		return base
	}
	return min
}

func (c *Controller) pressureForReason(reason string) float64 {
	s := c.Snapshot()
	switch reason {
	case "queue_inflight_soft", "queue_inflight_hard":
		total := s.QueueProducerInflight + s.QueueConsumerInflight + s.QueueAckInflight
		return ratio(float64(total), float64(c.cfg.QueueSoftLimit), float64(c.cfg.QueueHardLimit))
	case "queue_consumer_soft", "queue_consumer_hard":
		return ratio(float64(s.QueueConsumerInflight), float64(c.cfg.QueueConsumerSoftLimit), float64(c.cfg.QueueConsumerHardLimit))
	case "lock_inflight_soft", "lock_inflight_hard":
		return ratio(float64(s.LockInflight), float64(c.cfg.LockSoftLimit), float64(c.cfg.LockHardLimit))
	case "query_inflight_soft", "query_inflight_hard":
		return ratio(float64(s.QueryInflight), float64(c.cfg.QuerySoftLimit), float64(c.cfg.QueryHardLimit))
	case "memory_soft", "memory_hard":
		memPercent := c.effectiveMemoryPercent(s)
		if c.cfg.MemorySoftLimitPercent > 0 || c.cfg.MemoryHardLimitPercent > 0 {
			return ratio(memPercent, c.cfg.MemorySoftLimitPercent, c.cfg.MemoryHardLimitPercent)
		}
		return ratio(float64(s.RSSBytes), float64(c.cfg.MemorySoftLimitBytes), float64(c.cfg.MemoryHardLimitBytes))
	case "swap_soft", "swap_hard":
		if c.cfg.SwapSoftLimitPercent > 0 || c.cfg.SwapHardLimitPercent > 0 {
			return ratio(s.SystemSwapUsedPercent, c.cfg.SwapSoftLimitPercent, c.cfg.SwapHardLimitPercent)
		}
		return ratio(float64(s.SwapBytes), float64(c.cfg.SwapSoftLimitBytes), float64(c.cfg.SwapHardLimitBytes))
	case "cpu_soft", "cpu_hard":
		return ratio(s.SystemCPUPercent, c.cfg.CPUPercentSoftLimit, c.cfg.CPUPercentHardLimit)
	case "load_soft", "load_hard":
		return ratio(s.Load1Multiplier, c.cfg.LoadSoftLimitMultiplier, c.cfg.LoadHardLimitMultiplier)
	default:
		return 1
	}
}

func ratio(value, soft, hard float64) float64 {
	if soft <= 0 && hard <= 0 {
		return 1
	}
	if soft <= 0 && hard > 0 {
		soft = hard / 2
	}
	if hard <= soft {
		if soft == 0 {
			return 1
		}
		hard = soft * 2
	}
	if value <= soft {
		return 0
	}
	return math.Max(0, math.Min(1, (value-soft)/(hard-soft)))
}
