package client_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/client"
)

func TestWatchQueueEmitsAvailabilityEvents(t *testing.T) {
	t.Parallel()
	ts := lockd.StartTestServer(t)
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}

	queueName := "watch-queue"
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	type event struct {
		available bool
		queue     string
	}
	events := make(chan event, 8)
	errCh := make(chan error, 1)

	var once sync.Once
	go func() {
		err := cli.WatchQueue(ctx, queueName, client.WatchQueueOptions{}, func(_ context.Context, ev client.QueueWatchEvent) error {
			events <- event{available: ev.Available, queue: ev.Queue}
			if ev.Available {
				once.Do(cancel)
			}
			return nil
		})
		errCh <- err
	}()

	select {
	case ev := <-events:
		if ev.queue != queueName {
			t.Fatalf("unexpected queue name %q", ev.queue)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for initial watch event")
	}

	if _, err := cli.EnqueueBytes(context.Background(), queueName, []byte(`{"kind":"watch"}`), client.EnqueueOptions{}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	foundAvailable := false
	deadline := time.After(4 * time.Second)
	for !foundAvailable {
		select {
		case ev := <-events:
			if ev.available {
				foundAvailable = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for available watch event")
		}
	}

	if err := <-errCh; err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		t.Fatalf("watch queue returned error: %v", err)
	}
}
