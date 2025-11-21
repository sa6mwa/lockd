package core

import (
	"context"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/lsf"
	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/search"
	indexer "pkt.systems/lockd/internal/search/index"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/pslog"
)

// Config captures the dependencies and behavioural knobs required by the core
// domain services. It mirrors the HTTP handler wiring but is transport agnostic.
type Config struct {
	Store                  storage.Backend
	Crypto                 *storage.Crypto
	QueueService           QueueProvider
	QueueDispatcher        QueueDispatcher
	SearchAdapter          search.Adapter
	NamespaceConfigs       *namespaces.ConfigStore
	DefaultNamespaceConfig namespaces.Config
	IndexManager           *indexer.Manager
	DefaultNamespace       string
	Logger                 pslog.Logger
	Clock                  clock.Clock

	DefaultTTL       time.Duration
	MaxTTL           time.Duration
	AcquireBlock     time.Duration
	JSONMaxBytes     int64
	SpoolThreshold   int64
	EnforceIdentity  bool
	MetaWarmup       WarmupConfig
	StateWarmup      WarmupConfig
	LSFObserver      *lsf.Observer
	QRFController    *qrf.Controller
	ShutdownState    func() ShutdownState
	NamespaceTracker *NamespaceTracker
}

// WarmupConfig governs backend warmup retries for meta/state.
type WarmupConfig struct {
	Attempts int
	Initial  time.Duration
	Max      time.Duration
}

// ShutdownState exposes the server's current shutdown posture.
type ShutdownState struct {
	Draining  bool
	Remaining time.Duration
	Notify    bool
}

// QueueProvider represents the subset of queue.Dispatcher/queue.Service used by core.
type QueueProvider interface{}

// QueueDispatcher represents the optional dispatcher used for watcher notifications.
type QueueDispatcher interface {
	Notify(namespace, queue string)
	Try(ctx context.Context, namespace, queue string) (*queue.Candidate, error)
	Wait(ctx context.Context, namespace, queue string) (*queue.Candidate, error)
	HasActiveWatcher(namespace, queue string) bool
}
