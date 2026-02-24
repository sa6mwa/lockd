package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/pslog"
)

type subscriptionManager struct {
	logger   pslog.Logger
	upstream queueWatcher

	rootCtx context.Context
	cancel  context.CancelFunc

	mu             sync.Mutex
	subs           map[string]*queueSubscription
	sessionSubs    map[string]map[string]struct{}
	sessionWaiters map[string]struct{}
}

type queueWatcher interface {
	WatchQueue(ctx context.Context, queue string, opts lockdclient.WatchQueueOptions, handler lockdclient.QueueWatchHandler) error
}

type queueSubscription struct {
	key       string
	sessionID string
	session   *mcpsdk.ServerSession
	clientID  string
	namespace string
	queue     string
	cancel    context.CancelFunc
	seq       atomic.Uint64
}

func newSubscriptionManager(upstream queueWatcher, logger pslog.Logger) *subscriptionManager {
	ctx, cancel := context.WithCancel(context.Background())
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	return &subscriptionManager{
		logger:         logger,
		upstream:       upstream,
		rootCtx:        ctx,
		cancel:         cancel,
		subs:           make(map[string]*queueSubscription),
		sessionSubs:    make(map[string]map[string]struct{}),
		sessionWaiters: make(map[string]struct{}),
	}
}

func (m *subscriptionManager) Close() {
	m.cancel()
	m.mu.Lock()
	subs := make([]*queueSubscription, 0, len(m.subs))
	for _, sub := range m.subs {
		subs = append(subs, sub)
	}
	m.subs = make(map[string]*queueSubscription)
	m.sessionSubs = make(map[string]map[string]struct{})
	m.sessionWaiters = make(map[string]struct{})
	m.mu.Unlock()
	for _, sub := range subs {
		sub.cancel()
	}
}

func (m *subscriptionManager) subscriptionKey(sessionID, namespace, queue string) string {
	return fmt.Sprintf("%s|%s|%s", sessionID, namespace, queue)
}

func (m *subscriptionManager) Subscribe(ctx context.Context, session *mcpsdk.ServerSession, clientID, namespace, queue string) (bool, error) {
	if session == nil {
		return false, fmt.Errorf("session required")
	}
	namespace = strings.TrimSpace(namespace)
	queue = strings.TrimSpace(queue)
	if namespace == "" {
		return false, fmt.Errorf("namespace required")
	}
	if queue == "" {
		return false, fmt.Errorf("queue required")
	}
	sessionID := strings.TrimSpace(session.ID())
	if sessionID == "" {
		sessionID = fmt.Sprintf("session-%p", session)
	}
	key := m.subscriptionKey(sessionID, namespace, queue)

	subCtx, cancel := context.WithCancel(m.rootCtx)
	sub := &queueSubscription{
		key:       key,
		sessionID: sessionID,
		session:   session,
		clientID:  strings.TrimSpace(clientID),
		namespace: namespace,
		queue:     queue,
		cancel:    cancel,
	}

	startWaiter := false
	m.mu.Lock()
	if _, exists := m.subs[key]; exists {
		m.mu.Unlock()
		cancel()
		return false, nil
	}
	m.subs[key] = sub
	if m.sessionSubs[sessionID] == nil {
		m.sessionSubs[sessionID] = make(map[string]struct{})
	}
	m.sessionSubs[sessionID][key] = struct{}{}
	if _, exists := m.sessionWaiters[sessionID]; !exists {
		m.sessionWaiters[sessionID] = struct{}{}
		startWaiter = true
	}
	m.mu.Unlock()

	if startWaiter {
		go m.waitSession(session, sessionID)
	}
	go m.run(subCtx, sub)
	m.logger.Info("mcp.queue.subscription.add",
		"session_id", sessionID,
		"client_id", sub.clientID,
		"namespace", namespace,
		"queue", queue,
	)
	return true, nil
}

func (m *subscriptionManager) Unsubscribe(session *mcpsdk.ServerSession, namespace, queue string) bool {
	if session == nil {
		return false
	}
	sessionID := strings.TrimSpace(session.ID())
	if sessionID == "" {
		sessionID = fmt.Sprintf("session-%p", session)
	}
	key := m.subscriptionKey(sessionID, strings.TrimSpace(namespace), strings.TrimSpace(queue))
	sub := m.remove(key)
	if sub == nil {
		return false
	}
	sub.cancel()
	m.logger.Info("mcp.queue.subscription.remove",
		"session_id", sessionID,
		"client_id", sub.clientID,
		"namespace", sub.namespace,
		"queue", sub.queue,
	)
	return true
}

func (m *subscriptionManager) waitSession(session *mcpsdk.ServerSession, sessionID string) {
	if session == nil {
		return
	}
	_ = session.Wait()
	m.unsubscribeSession(sessionID)
}

func (m *subscriptionManager) unsubscribeSession(sessionID string) {
	m.mu.Lock()
	keys := make([]string, 0, len(m.sessionSubs[sessionID]))
	for key := range m.sessionSubs[sessionID] {
		keys = append(keys, key)
	}
	delete(m.sessionSubs, sessionID)
	delete(m.sessionWaiters, sessionID)
	m.mu.Unlock()
	for _, key := range keys {
		sub := m.remove(key)
		if sub == nil {
			continue
		}
		sub.cancel()
		m.logger.Info("mcp.queue.subscription.session_closed",
			"session_id", sessionID,
			"client_id", sub.clientID,
			"namespace", sub.namespace,
			"queue", sub.queue,
		)
	}
}

func (m *subscriptionManager) remove(key string) *queueSubscription {
	m.mu.Lock()
	defer m.mu.Unlock()
	sub, ok := m.subs[key]
	if !ok {
		return nil
	}
	delete(m.subs, key)
	if bySession := m.sessionSubs[sub.sessionID]; bySession != nil {
		delete(bySession, key)
		if len(bySession) == 0 {
			delete(m.sessionSubs, sub.sessionID)
			delete(m.sessionWaiters, sub.sessionID)
		}
	}
	return sub
}

func (m *subscriptionManager) run(ctx context.Context, sub *queueSubscription) {
	defer m.remove(sub.key)
	err := m.upstream.WatchQueue(ctx, sub.queue, lockdclient.WatchQueueOptions{
		Namespace: sub.namespace,
	}, func(handlerCtx context.Context, ev lockdclient.QueueWatchEvent) error {
		if handlerCtx == nil {
			handlerCtx = context.Background()
		}
		seq := sub.seq.Add(1)
		payload := map[string]any{
			"type":            "lockd.queue.message_available",
			"namespace":       ev.Namespace,
			"queue":           ev.Queue,
			"available":       ev.Available,
			"head_message_id": ev.HeadMessageID,
			"changed_at_unix": ev.ChangedAt.Unix(),
			"correlation_id":  ev.CorrelationID,
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		notifyCtx, cancel := context.WithTimeout(handlerCtx, 5*time.Second)
		defer cancel()
		return sub.session.NotifyProgress(notifyCtx, &mcpsdk.ProgressNotificationParams{
			ProgressToken: fmt.Sprintf("lockd.queue/%s/%s", sub.namespace, sub.queue),
			Progress:      float64(seq),
			Message:       string(raw),
		})
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		m.logger.Warn("mcp.queue.subscription.error",
			"session_id", sub.sessionID,
			"client_id", sub.clientID,
			"namespace", sub.namespace,
			"queue", sub.queue,
			"error", err,
		)
	}
}
