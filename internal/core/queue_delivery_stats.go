package core

import (
	"os"
	"sync/atomic"
	"time"
)

var (
	queueDeliveryStatsEnable atomic.Bool

	queueDeliveryAcquireCount   atomic.Int64
	queueDeliveryAcquireNanos   atomic.Int64
	queueDeliveryIncrementCount atomic.Int64
	queueDeliveryIncrementNanos atomic.Int64
	queueDeliveryPayloadCount   atomic.Int64
	queueDeliveryPayloadNanos   atomic.Int64
	queueAckCount               atomic.Int64
	queueAckNanos               atomic.Int64
)

func init() {
	if os.Getenv("LOCKD_QUEUE_DELIVERY_STATS") == "1" {
		queueDeliveryStatsEnable.Store(true)
	}
}

// EnableQueueDeliveryStats turns on queue delivery timing stats at runtime.
func EnableQueueDeliveryStats() {
	queueDeliveryStatsEnable.Store(true)
}

// QueueDeliveryStats tracks queue delivery timing counters.
type QueueDeliveryStats struct {
	AcquireCount   int64
	AcquireNanos   int64
	IncrementCount int64
	IncrementNanos int64
	PayloadCount   int64
	PayloadNanos   int64
	AckCount       int64
	AckNanos       int64
}

// QueueDeliveryStatsSnapshot returns the current queue delivery counters.
func QueueDeliveryStatsSnapshot() QueueDeliveryStats {
	return QueueDeliveryStats{
		AcquireCount:   queueDeliveryAcquireCount.Load(),
		AcquireNanos:   queueDeliveryAcquireNanos.Load(),
		IncrementCount: queueDeliveryIncrementCount.Load(),
		IncrementNanos: queueDeliveryIncrementNanos.Load(),
		PayloadCount:   queueDeliveryPayloadCount.Load(),
		PayloadNanos:   queueDeliveryPayloadNanos.Load(),
		AckCount:       queueAckCount.Load(),
		AckNanos:       queueAckNanos.Load(),
	}
}

// ResetQueueDeliveryStats clears all queue delivery counters.
func ResetQueueDeliveryStats() {
	queueDeliveryAcquireCount.Store(0)
	queueDeliveryAcquireNanos.Store(0)
	queueDeliveryIncrementCount.Store(0)
	queueDeliveryIncrementNanos.Store(0)
	queueDeliveryPayloadCount.Store(0)
	queueDeliveryPayloadNanos.Store(0)
	queueAckCount.Store(0)
	queueAckNanos.Store(0)
}

func queueDeliveryStatsEnabled() bool {
	return queueDeliveryStatsEnable.Load()
}

func recordQueueDeliveryAcquire(d time.Duration) {
	if !queueDeliveryStatsEnabled() {
		return
	}
	queueDeliveryAcquireCount.Add(1)
	queueDeliveryAcquireNanos.Add(d.Nanoseconds())
}

func recordQueueDeliveryIncrement(d time.Duration) {
	if !queueDeliveryStatsEnabled() {
		return
	}
	queueDeliveryIncrementCount.Add(1)
	queueDeliveryIncrementNanos.Add(d.Nanoseconds())
}

func recordQueueDeliveryPayload(d time.Duration) {
	if !queueDeliveryStatsEnabled() {
		return
	}
	queueDeliveryPayloadCount.Add(1)
	queueDeliveryPayloadNanos.Add(d.Nanoseconds())
}

func recordQueueAck(d time.Duration) {
	if !queueDeliveryStatsEnabled() {
		return
	}
	queueAckCount.Add(1)
	queueAckNanos.Add(d.Nanoseconds())
}
