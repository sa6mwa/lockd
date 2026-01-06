package lsf

import (
	"context"
	"testing"
	"time"

	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/pslog"
)

func newTestObserver() (*Observer, *qrf.Controller) {
	ctrl := qrf.NewController(qrf.Config{
		Enabled:            true,
		QueueSoftLimit:     10,
		QueueHardLimit:     20,
		LockSoftLimit:      10,
		LockHardLimit:      20,
		SoftRetryAfter:     50 * time.Millisecond,
		EngagedRetryAfter:  200 * time.Millisecond,
		RecoveryRetryAfter: 100 * time.Millisecond,
		RecoverySamples:    1,
		Logger:             pslog.NoopLogger(),
	})
	obs := NewObserver(Config{Enabled: true, SampleInterval: 10 * time.Millisecond}, ctrl, pslog.NoopLogger())
	return obs, ctrl
}

func TestObserverCounters(t *testing.T) {
	obs, ctrl := newTestObserver()

	// Baseline sample
	obs.sample(time.Now())

	finishProd := obs.BeginQueueProducer()
	finishCons := obs.BeginQueueConsumer()
	finishAck := obs.BeginQueueAck()
	finishLock := obs.BeginLockOp()

	obs.sample(time.Now())
	snap := ctrl.Snapshot()
	if snap.QueueProducerInflight != 1 {
		t.Fatalf("expected producer inflight 1, got %d", snap.QueueProducerInflight)
	}
	if snap.QueueConsumerInflight != 1 {
		t.Fatalf("expected consumer inflight 1, got %d", snap.QueueConsumerInflight)
	}
	if snap.QueueAckInflight != 1 {
		t.Fatalf("expected ack inflight 1, got %d", snap.QueueAckInflight)
	}
	if snap.LockInflight != 1 {
		t.Fatalf("expected lock inflight 1, got %d", snap.LockInflight)
	}

	finishProd()
	finishCons()
	finishAck()
	finishLock()

	obs.sample(time.Now())
	snap = ctrl.Snapshot()
	if snap.QueueProducerInflight != 0 || snap.QueueConsumerInflight != 0 || snap.QueueAckInflight != 0 || snap.LockInflight != 0 {
		t.Fatalf("expected zero inflight counts after closures, got %+v", snap)
	}
}

func TestObserverStartStop(t *testing.T) {
	obs, ctrl := newTestObserver()

	ctx, cancel := context.WithCancel(context.Background())
	obs.Start(ctx)

	time.Sleep(30 * time.Millisecond)
	cancel()
	obs.Wait()

	deadline := time.Now().Add(50 * time.Millisecond)
	for ctrl.Snapshot().CollectedAt.IsZero() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if ctrl.Snapshot().CollectedAt.IsZero() {
		t.Fatalf("expected controller to receive at least one snapshot")
	}
}

func TestObserverDisabledCounters(t *testing.T) {
	obs := NewObserver(Config{Enabled: false}, nil, pslog.NoopLogger())

	prod := obs.BeginQueueProducer()
	cons := obs.BeginQueueConsumer()
	ack := obs.BeginQueueAck()
	lockFinish := obs.BeginLockOp()

	prod()
	cons()
	ack()
	lockFinish()

	// With QRF nil and disabled flag, sample should be a no-op but must not panic.
	obs.sample(time.Now())
}
