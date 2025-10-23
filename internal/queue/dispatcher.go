package queue

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/logport"
)

type candidateProvider interface {
	NextCandidate(ctx context.Context, queue string, startAfter string, pageSize int) (*MessageDescriptor, string, error)
}

type messageReadiness interface {
	EnsureMessageReady(ctx context.Context, queue, id string) error
}

type readyCacheRefresher interface {
	RefreshReadyCache(ctx context.Context, queue string) error
}

const defaultQueuePageSize = 32

// ErrTooManyConsumers indicates the per-server consumer cap was reached.
var ErrTooManyConsumers = errors.New("queue: too many consumers")

// Candidate describes a message snapshot that can be leased by a consumer.
type Candidate struct {
	Descriptor MessageDescriptor
	NextCursor string
}

// Stats captures dispatcher-level metrics for a specific queue.
type Stats struct {
	Queue             string
	WaitingConsumers  int
	PendingCandidates int
	TotalConsumers    int
}

// WatchSubscription reports queue change events.
type WatchSubscription interface {
	Events() <-chan struct{}
	Close() error
}

// WatchFactory constructs WatchSubscriptions for queue names.
type WatchFactory interface {
	Subscribe(queue string) (WatchSubscription, error)
}

type watchFactoryFunc func(string) (WatchSubscription, error)

func (f watchFactoryFunc) Subscribe(queue string) (WatchSubscription, error) {
	return f(queue)
}

// Dispatcher centralises queue polling so storage is only listed once per queue
// regardless of how many consumers are connected.
type Dispatcher struct {
	svc          candidateProvider
	pollInterval time.Duration
	pollJitter   time.Duration
	maxConsumers int

	mu             sync.Mutex
	totalConsumers int
	queues         map[string]*queueState

	wake                  chan struct{}
	schedulerOnce         sync.Once
	watchFactory          WatchFactory
	logger                logport.ForLogging
	resilientPollInterval time.Duration
}

// DispatcherOption customises Dispatcher behaviour.
type DispatcherOption func(*Dispatcher)

// WithPollInterval sets the base polling interval (default 3s).
func WithPollInterval(d time.Duration) DispatcherOption {
	return func(disp *Dispatcher) {
		if d > 0 {
			disp.pollInterval = d
		}
	}
}

// WithPollJitter sets the additional random jitter added to the polling interval.
func WithPollJitter(d time.Duration) DispatcherOption {
	return func(disp *Dispatcher) {
		if d >= 0 {
			disp.pollJitter = d
		}
	}
}

// WithMaxConsumers caps the number of simultaneous Wait calls (default 1000).
func WithMaxConsumers(n int) DispatcherOption {
	return func(disp *Dispatcher) {
		if n > 0 {
			disp.maxConsumers = n
		}
	}
}

// WithWatchFactory installs a queue change watcher factory used to reduce polling when available.
func WithWatchFactory(factory WatchFactory) DispatcherOption {
	return func(disp *Dispatcher) {
		disp.watchFactory = factory
	}
}

// WithLogger assigns a base logger used for dispatcher diagnostics.
func WithLogger(logger logport.ForLogging) DispatcherOption {
	return func(disp *Dispatcher) {
		disp.logger = logger
	}
}

// WithResilientPollInterval configures the safety poll interval used when watchers are active.
func WithResilientPollInterval(interval time.Duration) DispatcherOption {
	return func(disp *Dispatcher) {
		if interval > 0 {
			disp.resilientPollInterval = interval
		}
	}
}

// WatchFactoryFromStorage adapts a storage.QueueChangeFeed into a Dispatcher watch factory.
func WatchFactoryFromStorage(feed storage.QueueChangeFeed) WatchFactory {
	if feed == nil {
		return nil
	}
	return watchFactoryFunc(func(queue string) (WatchSubscription, error) {
		sub, err := feed.SubscribeQueueChanges(queue)
		if err != nil {
			return nil, err
		}
		return storageWatchAdapter{sub: sub}, nil
	})
}

type storageWatchAdapter struct {
	sub storage.QueueChangeSubscription
}

func (a storageWatchAdapter) Events() <-chan struct{} {
	return a.sub.Events()
}

func (a storageWatchAdapter) Close() error {
	return a.sub.Close()
}

// NewDispatcher builds a dispatcher.
func NewDispatcher(svc candidateProvider, opts ...DispatcherOption) *Dispatcher {
	d := &Dispatcher{
		svc:                   svc,
		pollInterval:          3 * time.Second,
		pollJitter:            500 * time.Millisecond,
		maxConsumers:          1000,
		queues:                make(map[string]*queueState),
		wake:                  make(chan struct{}, 1),
		resilientPollInterval: 5 * time.Minute,
	}
	for _, opt := range opts {
		opt(d)
	}
	if d.logger == nil {
		d.logger = logport.NoopLogger()
	}
	if d.resilientPollInterval <= 0 {
		d.resilientPollInterval = 5 * time.Minute
	}
	d.logger = d.logger.With("svc", "queue_dispatcher")
	return d
}

// Wait blocks until a candidate is available for the queue or the context is cancelled.
func (d *Dispatcher) Wait(ctx context.Context, queue string) (*Candidate, error) {
	ctx = contextWithDefault(ctx)
	qs, err := d.queueState(queue)
	if err != nil {
		return nil, err
	}
	if err := d.tryRegisterConsumer(); err != nil {
		if errors.Is(err, ErrTooManyConsumers) {
			qs.logger.Warn("queue.dispatcher.consumer_limit", "max_consumers", d.maxConsumers)
		} else {
			qs.logger.Warn("queue.dispatcher.consumer_register_failed", "error", err)
		}
		return nil, err
	}
	defer d.releaseConsumer()

	if cand := qs.popPending(); cand != nil {
		qs.logger.Debug("queue.dispatcher.deliver.pending", "cursor", cand.NextCursor)
		return cand, nil
	}

	w := newWaiter(ctx)
	if delivered := qs.addWaiter(w); delivered {
		select {
		case cand := <-w.result:
			if cand != nil {
				qs.logger.Debug("queue.dispatcher.deliver.immediate", "cursor", cand.NextCursor)
			}
			return cand, nil
		case err := <-w.err:
			return nil, err
		default:
		}
	} else {
		qs.ensureWatcher()
		qs.markNeedsPoll("waiter")
		qs.logger.Trace("queue.dispatcher.waiter_added", "waiters", qs.waiterCount())
	}

	d.ensureScheduler()
	d.wakeScheduler()

	select {
	case cand := <-w.result:
		if cand != nil {
			qs.logger.Debug("queue.dispatcher.deliver.waiter", "cursor", cand.NextCursor)
		}
		return cand, nil
	case err := <-w.err:
		return nil, err
	case <-ctx.Done():
		qs.logger.Debug("queue.dispatcher.wait.context_done", "error", ctx.Err())
		qs.removeWaiter(w)
		return nil, ctx.Err()
	}
}

// Try performs a non-blocking fetch for an immediately available candidate.
// It returns nil when no message is ready.
func (d *Dispatcher) Try(ctx context.Context, queue string) (*Candidate, error) {
	ctx = contextWithDefault(ctx)
	qs, err := d.queueState(queue)
	if err != nil {
		return nil, err
	}
	if err := d.tryRegisterConsumer(); err != nil {
		if errors.Is(err, ErrTooManyConsumers) {
			qs.logger.Warn("queue.dispatcher.consumer_limit", "max_consumers", d.maxConsumers)
		} else {
			qs.logger.Warn("queue.dispatcher.consumer_register_failed", "error", err)
		}
		return nil, err
	}
	defer d.releaseConsumer()

	if cand := qs.popPending(); cand != nil {
		qs.logger.Debug("queue.dispatcher.deliver.pending", "cursor", cand.NextCursor)
		return cand, nil
	}

	reasons := qs.preparePoll()
	pollTime := time.Now()
	d.ensureScheduler()
	d.wakeScheduler()

	cand, err := qs.fetchWithContext(ctx)
	qs.recordPoll(pollTime, reasons)
	if err != nil {
		if isMissingMetadataError(err) || errors.Is(err, storage.ErrNotFound) {
			qs.logDiscrepancy(reasons, err)
			qs.markNeedsPoll("missing_metadata")
			return nil, nil
		}
		qs.logger.Warn("queue.dispatcher.fetch.error", "error", err)
		qs.markNeedsPoll("retry_after_error")
		return nil, err
	}
	if cand == nil {
		if containsReason(reasons, "watch_event") {
			qs.logDiscrepancy(reasons, nil)
		}
		return nil, nil
	}
	qs.logger.Debug("queue.dispatcher.deliver.fetch", "cursor", cand.NextCursor, "reasons", reasons)
	return cand, nil
}

// Notify nudges the dispatcher to poll the queue immediately (typically after enqueue).
func (d *Dispatcher) Notify(queue string) {
	wake := false
	if qs, err := d.queueState(queue); err == nil {
		wake = qs.markNeedsPollIfDemand("notify")
	} else if d.logger != nil {
		d.logger.Warn("queue.dispatcher.notify.error", "queue", queue, "error", err)
	}
	if !wake {
		return
	}
	d.ensureScheduler()
	d.wakeScheduler()
}

// QueueStats returns a snapshot of dispatcher metrics for the given queue.
func (d *Dispatcher) QueueStats(queue string) Stats {
	stats := Stats{}
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return stats
	}
	stats.Queue = name
	d.mu.Lock()
	stats.TotalConsumers = d.totalConsumers
	qs, ok := d.queues[name]
	d.mu.Unlock()
	if !ok {
		return stats
	}
	snap := qs.snapshot()
	stats.WaitingConsumers = snap.waiters
	stats.PendingCandidates = snap.pending
	return stats
}

// HasActiveWatcher reports whether the dispatcher currently maintains an active watch
// subscription for the provided queue.
func (d *Dispatcher) HasActiveWatcher(queue string) bool {
	qs, err := d.queueState(queue)
	if err != nil {
		return false
	}
	return qs.hasActiveWatcher()
}

func (d *Dispatcher) queueState(queue string) (*queueState, error) {
	name, err := sanitizeQueueName(queue)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if qs, ok := d.queues[name]; ok {
		return qs, nil
	}
	qs := &queueState{
		dispatcher:       d,
		name:             name,
		pageSize:         defaultQueuePageSize,
		logger:           d.logger.With("queue", name),
		needsPoll:        true,
		fallbackInterval: d.resilientPollInterval,
		pollReasons:      []string{"initial"},
	}
	d.queues[name] = qs
	d.logger.Trace("queue.dispatcher.register", "queue", name)
	return qs, nil
}

func (d *Dispatcher) tryRegisterConsumer() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.maxConsumers > 0 && d.totalConsumers >= d.maxConsumers {
		return ErrTooManyConsumers
	}
	d.totalConsumers++
	return nil
}

func (d *Dispatcher) releaseConsumer() {
	d.mu.Lock()
	if d.totalConsumers > 0 {
		d.totalConsumers--
	}
	d.mu.Unlock()
}

func (d *Dispatcher) ensureScheduler() {
	d.schedulerOnce.Do(func() {
		go d.runScheduler()
	})
}

func (d *Dispatcher) runScheduler() {
	for {
		if !d.hasActiveQueues() {
			d.logger.Trace("queue.dispatcher.scheduler.state", "state", "idle")
			d.blockUntilWake()
			continue
		}
		d.logger.Trace("queue.dispatcher.scheduler.state", "state", "polling")
		interval := d.nextInterval()
		timer := time.NewTimer(interval)
		d.pollQueues()
		select {
		case <-d.wake:
		case <-timer.C:
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

func (d *Dispatcher) blockUntilWake() {
	for {
		if !d.hasConsumers() {
			<-d.wake
			return
		}
		interval := d.nextInterval()
		if interval < 0 {
			interval = 0
		}
		timer := time.NewTimer(interval)
		select {
		case <-d.wake:
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
			return
		}
	}
}

func (d *Dispatcher) hasConsumers() bool {
	d.mu.Lock()
	total := d.totalConsumers
	d.mu.Unlock()
	return total > 0
}

func (d *Dispatcher) hasActiveQueues() bool {
	d.mu.Lock()
	queues := make([]*queueState, 0, len(d.queues))
	for _, qs := range d.queues {
		queues = append(queues, qs)
	}
	d.mu.Unlock()
	for _, qs := range queues {
		if qs.hasDemand() {
			qs.logger.Trace("queue.dispatcher.scheduler.active", "status", "has_demand")
			return true
		}
		qs.logger.Trace("queue.dispatcher.scheduler.active", "status", "no_demand")
	}
	return false
}

func (d *Dispatcher) pollQueues() {
	d.mu.Lock()
	queues := make([]*queueState, 0, len(d.queues))
	for _, qs := range d.queues {
		queues = append(queues, qs)
	}
	d.mu.Unlock()

	for _, qs := range queues {
		if !qs.hasDemand() {
			qs.logger.Trace("queue.dispatcher.poll.skip", "reason", "no_demand")
			continue
		}
		qs.poll()
	}
}

func (d *Dispatcher) wakeScheduler() {
	select {
	case d.wake <- struct{}{}:
		if d.logger != nil {
			d.logger.Trace("queue.dispatcher.wake", "reason", "signal")
		}
	default:
		select {
		case <-d.wake:
		default:
		}
		select {
		case d.wake <- struct{}{}:
			if d.logger != nil {
				d.logger.Trace("queue.dispatcher.wake", "reason", "resignal")
			}
		default:
			if d.logger != nil {
				d.logger.Trace("queue.dispatcher.wake.skip", "reason", "buffer_full")
			}
		}
	}
}

func (d *Dispatcher) nextInterval() time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()

	interval := d.pollInterval
	if interval <= 0 {
		interval = 3 * time.Second
	}
	useJitter := true
	now := time.Now()
	found := false

	for _, qs := range d.queues {
		qs.mu.Lock()
		watching := qs.watchSub != nil && !qs.watchDisabled
		need := qs.needsPoll
		fallback := qs.fallbackInterval
		last := qs.lastPoll
		waiters := len(qs.waiters)
		qs.mu.Unlock()

		if watching {
			if need {
				return 0
			}
			if fallback > 0 && !last.IsZero() {
				until := last.Add(fallback).Sub(now)
				if until <= 0 {
					return 0
				}
				if !found || until < interval {
					interval = until
					useJitter = false
					found = true
				}
			}
		} else if waiters > 0 {
			if !found || d.pollInterval < interval {
				interval = d.pollInterval
				useJitter = true
				found = true
			}
		}
	}

	if !found {
		interval = d.pollInterval
		if interval <= 0 {
			interval = 3 * time.Second
		}
	}
	if interval < 0 {
		interval = 0
	}
	if useJitter && d.pollJitter > 0 && interval > 0 {
		interval += time.Duration(rand.Int63n(int64(d.pollJitter)))
	}
	return interval
}

type queueState struct {
	dispatcher *Dispatcher
	name       string

	mu                  sync.Mutex
	waiters             []*waiter
	pending             []*Candidate
	cursor              string
	pageSize            int
	watchSub            WatchSubscription
	watchStop           chan struct{}
	watchDisabled       bool
	logger              logport.ForLogging
	watchDisabledReason string
	needsPoll           bool
	fallbackInterval    time.Duration
	lastPoll            time.Time
	pollReasons         []string
	lastPollReasons     []string
	discrepancyCount    int
	lastDiscrepancy     time.Time
	lastCacheRefresh    time.Time
}

type queueStateSnapshot struct {
	waiters int
	pending int
}

func containsReason(reasons []string, reason string) bool {
	for _, r := range reasons {
		if r == reason {
			return true
		}
	}
	return false
}

func isMissingMetadataError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "missing object metadata")
}

func (qs *queueState) snapshot() queueStateSnapshot {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	return queueStateSnapshot{
		waiters: len(qs.waiters),
		pending: len(qs.pending),
	}
}

func (qs *queueState) popPending() *Candidate {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	if len(qs.pending) == 0 {
		return nil
	}
	cand := qs.pending[0]
	qs.pending = qs.pending[1:]
	return cand
}

func (qs *queueState) markNeedsPoll(reason string) {
	if reason == "" {
		reason = "unspecified"
	}
	qs.mu.Lock()
	qs.needsPoll = true
	if !containsReason(qs.pollReasons, reason) {
		qs.pollReasons = append(qs.pollReasons, reason)
	}
	qs.mu.Unlock()
}

func (qs *queueState) markNeedsPollIfDemand(reason string) bool {
	if reason == "" {
		reason = "unspecified"
	}
	qs.mu.Lock()
	waiters := len(qs.waiters)
	if waiters == 0 && (qs.watchSub == nil || qs.watchDisabled) {
		qs.mu.Unlock()
		return false
	}
	qs.needsPoll = true
	if !containsReason(qs.pollReasons, reason) {
		qs.pollReasons = append(qs.pollReasons, reason)
	}
	qs.mu.Unlock()
	return true
}

func (qs *queueState) waiterCount() int {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	return len(qs.waiters)
}

func (qs *queueState) hasActiveWatcher() bool {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	return qs.watchSub != nil && !qs.watchDisabled
}

func (qs *queueState) preparePoll() []string {
	qs.mu.Lock()
	reasons := append([]string(nil), qs.pollReasons...)
	if len(reasons) == 0 {
		reasons = append(reasons, "demand")
	}
	qs.pollReasons = nil
	qs.needsPoll = false
	qs.mu.Unlock()
	return reasons
}

func (qs *queueState) recordPoll(ts time.Time, reasons []string) {
	qs.mu.Lock()
	qs.lastPoll = ts
	qs.lastPollReasons = append([]string(nil), reasons...)
	qs.mu.Unlock()
	qs.logger.Trace("queue.dispatcher.poll.complete", "reasons", reasons, "elapsed", time.Since(ts))
}

func (qs *queueState) logDiscrepancy(reasons []string, err error) {
	qs.mu.Lock()
	qs.discrepancyCount++
	qs.lastDiscrepancy = time.Now()
	count := qs.discrepancyCount
	qs.mu.Unlock()
	fields := []any{"reasons", reasons}
	if err != nil {
		fields = append(fields, "error", err)
	}
	if count >= 3 {
		qs.logger.Warn("queue.dispatcher.watch.discrepancy", fields...)
	} else {
		qs.logger.Debug("queue.dispatcher.watch.discrepancy", fields...)
	}
}

func (qs *queueState) resetDiscrepancyLocked() {
	qs.discrepancyCount = 0
	qs.lastDiscrepancy = time.Time{}
}

func (qs *queueState) resetDiscrepancy() {
	qs.mu.Lock()
	qs.resetDiscrepancyLocked()
	qs.mu.Unlock()
}

func (qs *queueState) addWaiter(w *waiter) bool {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	qs.pruneWaitersLocked()
	if len(qs.pending) > 0 {
		cand := qs.pending[0]
		qs.pending = qs.pending[1:]
		if w.deliver(cand) {
			return true
		}
		// if delivery failed, requeue candidate for the next waiter
		qs.pending = append([]*Candidate{cand}, qs.pending...)
	}
	qs.waiters = append(qs.waiters, w)
	return false
}

func (qs *queueState) removeWaiter(target *waiter) {
	qs.mu.Lock()
	removed := false
	for i, w := range qs.waiters {
		if w == target {
			qs.waiters = append(qs.waiters[:i], qs.waiters[i+1:]...)
			removed = true
			break
		}
	}
	qs.mu.Unlock()
	if removed {
		qs.maybeStopWatcher()
	}
}

func (qs *queueState) ensureWatcher() {
	factory := qs.dispatcher.watchFactory
	if factory == nil {
		return
	}
	qs.mu.Lock()
	disabled := qs.watchDisabled
	need := qs.watchSub == nil && len(qs.waiters) > 0 && !qs.watchDisabled
	qs.mu.Unlock()
	if disabled || !need {
		return
	}
	sub, err := factory.Subscribe(qs.name)
	if err != nil {
		if errors.Is(err, storage.ErrNotImplemented) {
			qs.disableWatcher("not_implemented")
		} else {
			qs.logger.Warn("queue.dispatcher.watch.subscribe_failed", "error", err)
			qs.disableWatcher("subscribe_failed")
		}
		return
	}
	stop := make(chan struct{})
	qs.mu.Lock()
	if qs.watchSub != nil || len(qs.waiters) == 0 {
		qs.mu.Unlock()
		close(stop)
		_ = sub.Close()
		return
	}
	qs.watchSub = sub
	qs.watchStop = stop
	qs.mu.Unlock()
	qs.logger.Debug("queue.dispatcher.watch.start")
	go qs.consumeWatcher(stop, sub)
}

func (qs *queueState) maybeStopWatcher() {
	qs.mu.Lock()
	stop := qs.watchStop
	if qs.watchSub == nil || len(qs.waiters) > 0 {
		qs.mu.Unlock()
		return
	}
	qs.watchSub = nil
	qs.watchStop = nil
	qs.mu.Unlock()
	if stop != nil {
		close(stop)
	}
	qs.logger.Debug("queue.dispatcher.watch.stop")
}

func (qs *queueState) consumeWatcher(stop <-chan struct{}, sub WatchSubscription) {
	defer sub.Close()
	defer qs.clearWatcher(sub)
	for {
		select {
		case <-stop:
			qs.logger.Debug("queue.dispatcher.watch.stop_signal")
			return
		case _, ok := <-sub.Events():
			if !ok {
				qs.logger.Debug("queue.dispatcher.watch.channel_closed")
				return
			}
			qs.logger.Trace("queue.dispatcher.watch.event")
			qs.markNeedsPoll("watch_event")
			qs.triggerCacheRefresh()
			qs.dispatcher.wakeScheduler()
		}
	}
}

func (qs *queueState) clearWatcher(sub WatchSubscription) {
	qs.mu.Lock()
	if qs.watchSub == sub {
		qs.watchSub = nil
		qs.watchStop = nil
	}
	qs.mu.Unlock()
}

func (qs *queueState) disableWatcher(reason string) {
	qs.mu.Lock()
	alreadyDisabled := qs.watchDisabled
	if !qs.watchDisabled {
		qs.watchDisabled = true
		qs.watchDisabledReason = reason
	}
	qs.mu.Unlock()
	qs.markNeedsPoll("watch_disabled")
	if !alreadyDisabled {
		qs.logger.Info("queue.dispatcher.watch.disabled", "reason", reason)
	}
}

func (qs *queueState) triggerCacheRefresh() {
	refresher, ok := qs.dispatcher.svc.(readyCacheRefresher)
	if !ok {
		return
	}
	const minInterval = 25 * time.Millisecond
	qs.mu.Lock()
	if time.Since(qs.lastCacheRefresh) < minInterval {
		qs.mu.Unlock()
		return
	}
	qs.lastCacheRefresh = time.Now()
	qs.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := refresher.RefreshReadyCache(ctx, qs.name); err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			qs.logger.Trace("queue.dispatcher.cache.refresh_error", "error", err)
		}
	}()
}

func (qs *queueState) hasDemand() bool {
	qs.mu.Lock()
	qs.pruneWaitersLocked()
	count := len(qs.waiters)
	watching := qs.watchSub != nil && !qs.watchDisabled
	needsPoll := qs.needsPoll
	lastPoll := qs.lastPoll
	fallback := qs.fallbackInterval
	qs.mu.Unlock()
	if count == 0 {
		qs.maybeStopWatcher()
		qs.logger.Trace("queue.dispatcher.has_demand", "waiters", count, "watching", watching, "needs_poll", needsPoll, "result", false)
		return false
	}
	if watching {
		if needsPoll {
			qs.logger.Trace("queue.dispatcher.has_demand", "waiters", count, "watching", watching, "needs_poll", needsPoll, "result", true)
			return true
		}
		if fallback > 0 && !lastPoll.IsZero() && time.Since(lastPoll) >= fallback {
			qs.markNeedsPoll("fallback_timeout")
			qs.logger.Trace("queue.dispatcher.has_demand", "waiters", count, "watching", watching, "needs_poll", true, "result", true, "reason", "fallback")
			return true
		}
		qs.logger.Trace("queue.dispatcher.has_demand", "waiters", count, "watching", watching, "needs_poll", needsPoll, "result", false)
		return false
	}
	qs.ensureWatcher()
	if !needsPoll {
		qs.markNeedsPoll("demand")
	}
	qs.logger.Trace("queue.dispatcher.has_demand", "waiters", count, "watching", watching, "needs_poll", true, "result", true, "reason", "no_watcher")
	return true
}

func (qs *queueState) pruneWaitersLocked() {
	dst := qs.waiters[:0]
	for _, w := range qs.waiters {
		if w.ctx.Err() != nil {
			w.deliverError(w.ctx.Err())
			continue
		}
		dst = append(dst, w)
	}
	qs.waiters = dst
}

func (qs *queueState) poll() {
	qs.logger.Trace("queue.dispatcher.poll.begin")
	fetched := 0
	for {
		if !qs.hasDemand() {
			return
		}
		reasons := qs.preparePoll()
		pollTime := time.Now()
		cand, err := qs.fetch()
		qs.recordPoll(pollTime, reasons)
		if err != nil {
			if isMissingMetadataError(err) || errors.Is(err, storage.ErrNotFound) {
				qs.logDiscrepancy(reasons, err)
				qs.markNeedsPoll("missing_metadata")
				continue
			}
			if errors.Is(err, storage.ErrCASMismatch) {
				qs.logger.Debug("queue.dispatcher.fetch.cas_mismatch", "reasons", reasons)
				qs.markNeedsPoll("cas_mismatch")
				continue
			}
			if containsReason(reasons, "watch_event") {
				qs.logDiscrepancy(reasons, err)
			}
			if errors.Is(err, storage.ErrNotFound) {
				qs.resetCursor()
				return
			}
			qs.markNeedsPoll("retry_after_error")
			qs.failOne(err)
			return
		}
		if cand == nil {
			if containsReason(reasons, "watch_event") {
				qs.logDiscrepancy(reasons, nil)
			} else {
				qs.logger.Trace("queue.dispatcher.poll.no_candidate", "reasons", reasons)
			}
			return
		}
		fetched++
		qs.deliverCandidate(cand)
		if fetched >= defaultCachePageSize {
			return
		}
	}
}

func (qs *queueState) fetch() (*Candidate, error) {
	qs.mu.Lock()
	cursor := qs.cursor
	pageSize := qs.pageSize
	if pageSize <= 0 {
		pageSize = defaultQueuePageSize
	}
	qs.mu.Unlock()
	qs.logger.Trace("queue.dispatcher.fetch.start", "cursor", cursor, "page_size", pageSize)

	desc, next, err := qs.dispatcher.svc.NextCandidate(context.Background(), qs.name, cursor, pageSize)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			qs.logger.Trace("queue.dispatcher.fetch.empty", "cursor", cursor)
			return nil, nil
		}
		if !isMissingMetadataError(err) {
			qs.logger.Warn("queue.dispatcher.fetch.error", "cursor", cursor, "error", err)
		}
		return nil, err
	}
	if desc == nil {
		qs.logger.Trace("queue.dispatcher.fetch.none", "cursor", cursor, "next", next)
	} else {
		qs.logger.Trace("queue.dispatcher.fetch.success", "cursor", cursor, "next", next, "mid", desc.Document.ID)
	}
	if checker, ok := qs.dispatcher.svc.(messageReadiness); ok {
		if err := checker.EnsureMessageReady(context.Background(), desc.Document.Queue, desc.Document.ID); err != nil {
			return nil, err
		}
	}
	qs.logger.Trace("queue.dispatcher.fetch.success", "mid", desc.Document.ID, "next_cursor", next)
	return &Candidate{Descriptor: *desc, NextCursor: next}, nil
}

func (qs *queueState) fetchWithContext(ctx context.Context) (*Candidate, error) {
	qs.mu.Lock()
	cursor := qs.cursor
	pageSize := qs.pageSize
	if pageSize <= 0 {
		pageSize = defaultQueuePageSize
	}
	qs.mu.Unlock()

	start := time.Now()
	desc, next, err := qs.dispatcher.svc.NextCandidate(contextWithDefault(ctx), qs.name, cursor, pageSize)
	elapsed := time.Since(start)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			qs.logger.Trace("queue.dispatcher.fetch.empty", "cursor", cursor)
			return nil, nil
		}
		if !isMissingMetadataError(err) {
			qs.logger.Warn("queue.dispatcher.fetch.error", "cursor", cursor, "error", err, "elapsed", elapsed)
		}
		return nil, err
	}
	cand := &Candidate{Descriptor: *desc, NextCursor: next}
	qs.mu.Lock()
	qs.cursor = next
	qs.mu.Unlock()
	if checker, ok := qs.dispatcher.svc.(messageReadiness); ok {
		if err := checker.EnsureMessageReady(ctx, desc.Document.Queue, desc.Document.ID); err != nil {
			return nil, err
		}
	}
	qs.logger.Trace("queue.dispatcher.fetch.success", "mid", desc.Document.ID, "next_cursor", next, "elapsed", elapsed)
	return cand, nil
}

func (qs *queueState) deliverCandidate(cand *Candidate) bool {
	qs.mu.Lock()
	stopWatcher := false
	triggerPoll := false
	qs.cursor = cand.NextCursor
	for len(qs.waiters) > 0 {
		w := qs.waiters[0]
		qs.waiters = qs.waiters[1:]
		if w.ctx.Err() != nil {
			w.deliverError(w.ctx.Err())
			continue
		}
		if w.deliver(cand) {
			if len(qs.waiters) == 0 {
				stopWatcher = true
			}
			triggerPoll = true
			qs.logger.Debug("queue.dispatcher.deliver.to_waiter", "cursor", cand.NextCursor, "mid", cand.Descriptor.Document.ID)
			qs.resetDiscrepancyLocked()
			qs.mu.Unlock()
			if stopWatcher {
				qs.maybeStopWatcher()
			}
			if triggerPoll {
				qs.markNeedsPoll("delivered")
				qs.dispatcher.ensureScheduler()
				qs.dispatcher.wakeScheduler()
			}
			return true
		}
	}
	qs.pending = append(qs.pending, cand)
	qs.logger.Debug("queue.dispatcher.deliver.queued", "cursor", cand.NextCursor, "mid", cand.Descriptor.Document.ID)
	if len(qs.waiters) == 0 {
		stopWatcher = true
	}
	triggerPoll = true
	qs.mu.Unlock()
	if stopWatcher {
		qs.maybeStopWatcher()
	}
	if triggerPoll {
		qs.markNeedsPoll("queued")
		qs.dispatcher.ensureScheduler()
		qs.dispatcher.wakeScheduler()
	}
	return false
}

func (qs *queueState) failOne(err error) {
	qs.logger.Warn("queue.dispatcher.deliver.error", "error", err)
	qs.mu.Lock()
	defer qs.mu.Unlock()
	for len(qs.waiters) > 0 {
		w := qs.waiters[0]
		qs.waiters = qs.waiters[1:]
		if w.deliverError(err) {
			return
		}
	}
}

func (qs *queueState) resetCursor() {
	qs.mu.Lock()
	qs.cursor = ""
	qs.mu.Unlock()
}

type waiter struct {
	ctx    context.Context
	result chan *Candidate
	err    chan error
}

func newWaiter(ctx context.Context) *waiter {
	return &waiter{
		ctx:    ctx,
		result: make(chan *Candidate, 1),
		err:    make(chan error, 1),
	}
}

func (w *waiter) deliver(c *Candidate) bool {
	select {
	case <-w.ctx.Done():
		return false
	default:
	}
	select {
	case w.result <- c:
		return true
	default:
		return false
	}
}

func (w *waiter) deliverError(err error) bool {
	select {
	case w.err <- err:
		return true
	default:
		return false
	}
}

func contextWithDefault(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}
