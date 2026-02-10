package client_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/client"
)

func TestQueueStateHandleFirstSaveMem(t *testing.T) {
	ts := lockd.StartTestServer(t)
	cli := ts.Client
	if cli == nil {
		t.Fatalf("test server did not provide client")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	queue := fmt.Sprintf("state-first-save-%d", time.Now().UnixNano())
	if _, err := cli.EnqueueBytes(ctx, queue, []byte(`{"kind":"first-save"}`), client.EnqueueOptions{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	msg, err := cli.DequeueWithState(ctx, queue, client.DequeueOptions{
		Owner:        "worker-first-save",
		BlockSeconds: 5,
	})
	if err != nil {
		t.Fatalf("dequeue with state: %v", err)
	}
	if msg == nil {
		t.Fatalf("expected message")
	}
	defer msg.Close()
	if msg.StateHandle() == nil {
		t.Fatalf("expected state handle")
	}

	var current struct {
		Counter int `json:"counter"`
	}
	if err := msg.StateHandle().Load(ctx, &current); err != nil {
		t.Fatalf("load initial state: %v", err)
	}
	if current.Counter != 0 {
		t.Fatalf("expected initial counter=0, got %d", current.Counter)
	}

	current.Counter = 1
	if err := msg.StateHandle().Save(ctx, &current); err != nil {
		t.Fatalf("save initial state: %v", err)
	}

	var verify struct {
		Counter int `json:"counter"`
	}
	if err := msg.StateHandle().Load(ctx, &verify); err != nil {
		t.Fatalf("reload state: %v", err)
	}
	if verify.Counter != 1 {
		t.Fatalf("expected reloaded counter=1, got %d", verify.Counter)
	}

	if err := msg.Ack(ctx); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

func TestStartConsumerStateFirstSaveMem(t *testing.T) {
	ts := lockd.StartTestServer(t)
	cli := ts.Client
	if cli == nil {
		t.Fatalf("test server did not provide client")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	queue := fmt.Sprintf("start-consumer-state-save-%d", time.Now().UnixNano())
	if _, err := cli.EnqueueBytes(ctx, queue, []byte(`{"kind":"start-consumer-state"}`), client.EnqueueOptions{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var (
		loadedEmpty  atomic.Bool
		savedState   atomic.Bool
		reloadedSave atomic.Bool
		acked        atomic.Bool
	)

	err := cli.StartConsumer(ctx, client.ConsumerConfig{
		Name:      "state-first-save",
		Queue:     queue,
		WithState: true,
		Options: client.SubscribeOptions{
			Owner:        "worker-state-first-save",
			Prefetch:     1,
			BlockSeconds: 5,
		},
		MessageHandler: func(handlerCtx context.Context, cm client.ConsumerMessage) error {
			if cm.Message == nil || cm.State == nil {
				return fmt.Errorf("expected message and state handle")
			}
			defer cm.Message.Close()

			var current struct {
				Counter int `json:"counter"`
			}
			if err := cm.State.Load(handlerCtx, &current); err != nil {
				return fmt.Errorf("load initial state: %w", err)
			}
			if current.Counter != 0 {
				return fmt.Errorf("expected initial counter=0, got %d", current.Counter)
			}
			loadedEmpty.Store(true)

			current.Counter = 1
			if err := cm.State.Save(handlerCtx, &current); err != nil {
				return fmt.Errorf("save initial state: %w", err)
			}
			savedState.Store(true)

			var verify struct {
				Counter int `json:"counter"`
			}
			if err := cm.State.Load(handlerCtx, &verify); err != nil {
				return fmt.Errorf("reload state: %w", err)
			}
			if verify.Counter != 1 {
				return fmt.Errorf("expected reloaded counter=1, got %d", verify.Counter)
			}
			reloadedSave.Store(true)

			ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer ackCancel()
			if err := cm.Message.Ack(ackCtx); err != nil {
				return fmt.Errorf("ack: %w", err)
			}
			acked.Store(true)
			cancel()
			return nil
		},
		ErrorHandler: func(_ context.Context, event client.ConsumerError) error {
			return event.Err
		},
	})
	if err != nil {
		t.Fatalf("start consumer: %v", err)
	}
	if !loadedEmpty.Load() {
		t.Fatalf("expected empty state load to run")
	}
	if !savedState.Load() {
		t.Fatalf("expected initial state save to run")
	}
	if !reloadedSave.Load() {
		t.Fatalf("expected saved state reload to run")
	}
	if !acked.Load() {
		t.Fatalf("expected ack to run")
	}
}

func TestStartConsumerStatePersistsAcrossNackMem(t *testing.T) {
	ts := lockd.StartTestServer(t)
	cli := ts.Client
	if cli == nil {
		t.Fatalf("test server did not provide client")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	queue := fmt.Sprintf("start-consumer-state-retry-%d", time.Now().UnixNano())
	if _, err := cli.EnqueueBytes(ctx, queue, []byte(`{"kind":"retry-state"}`), client.EnqueueOptions{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var (
		deliveries   atomic.Int32
		finalCounter atomic.Int32
		acked        atomic.Bool
	)

	err := cli.StartConsumer(ctx, client.ConsumerConfig{
		Name:      "state-persist-retry",
		Queue:     queue,
		WithState: true,
		Options: client.SubscribeOptions{
			Owner:        "worker-state-persist-retry",
			Prefetch:     1,
			BlockSeconds: 5,
		},
		MessageHandler: func(handlerCtx context.Context, cm client.ConsumerMessage) error {
			if cm.Message == nil || cm.State == nil {
				return fmt.Errorf("expected message and state handle")
			}
			defer cm.Message.Close()

			deliveries.Add(1)

			var state struct {
				Counter int `json:"counter"`
			}
			if err := cm.State.Load(handlerCtx, &state); err != nil {
				return fmt.Errorf("load state: %w", err)
			}
			if state.Counter >= 2 {
				finalCounter.Store(int32(state.Counter))
				ackCtx, ackCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer ackCancel()
				if err := cm.Message.Ack(ackCtx); err != nil {
					return fmt.Errorf("ack: %w", err)
				}
				acked.Store(true)
				cancel()
				return nil
			}

			state.Counter++
			if err := cm.State.Save(handlerCtx, &state); err != nil {
				return fmt.Errorf("save state: %w", err)
			}
			nackCtx, nackCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer nackCancel()
			return cm.Message.Nack(nackCtx, 100*time.Millisecond, nil)
		},
		ErrorHandler: func(_ context.Context, event client.ConsumerError) error {
			if errors.Is(event.Err, context.Canceled) || errors.Is(event.Err, context.DeadlineExceeded) {
				return nil
			}
			return event.Err
		},
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("start consumer: %v", err)
	}
	if !acked.Load() {
		t.Fatalf("expected message to be acked after state persisted across retries")
	}
	if deliveries.Load() < 3 {
		t.Fatalf("expected at least 3 deliveries, got %d", deliveries.Load())
	}
	if finalCounter.Load() < 2 {
		t.Fatalf("expected persisted counter >= 2, got %d", finalCounter.Load())
	}
}
