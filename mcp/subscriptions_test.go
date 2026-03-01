package mcp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/pslog"
)

type fakeQueueWatcher struct{}

func (f *fakeQueueWatcher) WatchQueue(ctx context.Context, queue string, opts lockdclient.WatchQueueOptions, handler lockdclient.QueueWatchHandler) error {
	if handler == nil {
		return nil
	}
	if err := handler(ctx, lockdclient.QueueWatchEvent{
		Namespace: opts.Namespace,
		Queue:     queue,
		Available: true,
		ChangedAt: time.Now().UTC(),
	}); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

type errorQueueWatcher struct {
	err    error
	called chan struct{}
}

func (f *errorQueueWatcher) WatchQueue(_ context.Context, _ string, _ lockdclient.WatchQueueOptions, _ lockdclient.QueueWatchHandler) error {
	if f.called != nil {
		close(f.called)
	}
	return f.err
}

func TestSubscriptionManagerForwardsQueueWatchToProgressNotifications(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	progressCh := make(chan *mcpsdk.ProgressNotificationClientRequest, 2)
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "0.0.1"}, &mcpsdk.ClientOptions{
		ProgressNotificationHandler: func(_ context.Context, req *mcpsdk.ProgressNotificationClientRequest) {
			progressCh <- req
		},
	})
	server := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "test-server", Version: "0.0.1"}, nil)

	t1, t2 := mcpsdk.NewInMemoryTransports()
	ss, err := server.Connect(ctx, t1, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cs.Close()
	defer ss.Close()

	manager := newSubscriptionManager(&fakeQueueWatcher{}, pslog.NewStructured(context.Background(), io.Discard))
	defer manager.Close()

	added, err := manager.Subscribe(ctx, ss, "cid-1", "mcp", "lockd.agent.bus")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if !added {
		t.Fatalf("expected first subscribe to add a subscription")
	}

	added, err = manager.Subscribe(ctx, ss, "cid-1", "mcp", "lockd.agent.bus")
	if err != nil {
		t.Fatalf("duplicate subscribe: %v", err)
	}
	if added {
		t.Fatalf("expected duplicate subscribe to be idempotent")
	}

	select {
	case req := <-progressCh:
		if req == nil || req.Params == nil {
			t.Fatalf("expected progress notification payload")
		}
		if req.Params.ProgressToken == nil {
			t.Fatalf("expected progress token to be set")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for forwarded queue watch event")
	}

	if removed := manager.Unsubscribe(ss, "mcp", "lockd.agent.bus"); !removed {
		t.Fatalf("expected unsubscribe to remove subscription")
	}
	if removed := manager.Unsubscribe(ss, "mcp", "lockd.agent.bus"); removed {
		t.Fatalf("expected second unsubscribe to be idempotent false")
	}
}

func TestSubscriptionManagerCloseCancelsWatchers(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	server := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "test-server", Version: "0.0.1"}, nil)
	t1, t2 := mcpsdk.NewInMemoryTransports()
	ss, err := server.Connect(ctx, t1, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cs.Close()
	defer ss.Close()

	manager := newSubscriptionManager(&fakeQueueWatcher{}, pslog.NewStructured(context.Background(), io.Discard))
	added, err := manager.Subscribe(ctx, ss, "cid-1", "mcp", "lockd.agent.bus")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if !added {
		t.Fatalf("expected first subscribe to add a subscription")
	}

	manager.Close()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	for {
		manager.mu.Lock()
		remaining := len(manager.subs)
		manager.mu.Unlock()
		if remaining == 0 {
			break
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("subscriptions not cleared after Close")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestSubscriptionManagerNilSession(t *testing.T) {
	t.Parallel()
	manager := newSubscriptionManager(&fakeQueueWatcher{}, pslog.NewStructured(context.Background(), io.Discard))
	defer manager.Close()
	_, err := manager.Subscribe(context.Background(), nil, "cid", "mcp", "lockd.agent.bus")
	if err == nil {
		t.Fatalf("expected error for nil session")
	}
}

func TestSubscriptionManagerClosedStreamErrorDoesNotWarn(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	server := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "test-server", Version: "0.0.1"}, nil)
	t1, t2 := mcpsdk.NewInMemoryTransports()
	ss, err := server.Connect(ctx, t1, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cs.Close()
	defer ss.Close()

	var logBuf bytes.Buffer
	watcherCalled := make(chan struct{})
	manager := newSubscriptionManager(&errorQueueWatcher{
		err:    errors.New("rejected by transport: undelivered message: stream not connected or already closed"),
		called: watcherCalled,
	}, pslog.NewStructured(context.Background(), &logBuf))
	defer manager.Close()

	added, err := manager.Subscribe(ctx, ss, "cid-1", "mcp", "lockd.agent.bus")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if !added {
		t.Fatalf("expected first subscribe to add a subscription")
	}

	select {
	case <-watcherCalled:
	case <-time.After(2 * time.Second):
		t.Fatalf("watcher was not called")
	}
	waitForSubscriptionsToClear(t, manager, 2*time.Second)
	if strings.Contains(logBuf.String(), "mcp.queue.subscription.error") {
		t.Fatalf("expected closed-stream watch error to be treated as benign; logs=%s", logBuf.String())
	}
}

func TestSubscriptionManagerUnexpectedWatchErrorWarns(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	server := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "test-server", Version: "0.0.1"}, nil)
	t1, t2 := mcpsdk.NewInMemoryTransports()
	ss, err := server.Connect(ctx, t1, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	cs, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	defer cs.Close()
	defer ss.Close()

	var logBuf bytes.Buffer
	watcherCalled := make(chan struct{})
	manager := newSubscriptionManager(&errorQueueWatcher{
		err:    errors.New("boom"),
		called: watcherCalled,
	}, pslog.NewStructured(context.Background(), &logBuf))
	defer manager.Close()

	added, err := manager.Subscribe(ctx, ss, "cid-1", "mcp", "lockd.agent.bus")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if !added {
		t.Fatalf("expected first subscribe to add a subscription")
	}

	select {
	case <-watcherCalled:
	case <-time.After(2 * time.Second):
		t.Fatalf("watcher was not called")
	}
	waitForSubscriptionsToClear(t, manager, 2*time.Second)
	if !strings.Contains(logBuf.String(), "mcp.queue.subscription.error") {
		t.Fatalf("expected watch error warning for non-benign error; logs=%s", logBuf.String())
	}
}

func waitForSubscriptionsToClear(t *testing.T, manager *subscriptionManager, timeout time.Duration) {
	t.Helper()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), timeout)
	defer waitCancel()
	for {
		manager.mu.Lock()
		remaining := len(manager.subs)
		manager.mu.Unlock()
		if remaining == 0 {
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("subscriptions not cleared")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
