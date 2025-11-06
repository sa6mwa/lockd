//go:build linux

package disk

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/fsnotify/fsnotify"

	"pkt.systems/lockd/internal/storage"
)

const nfsSuperMagic = 0x6969

func queueWatchSupported(root string) bool {
	var st syscall.Statfs_t
	if err := syscall.Statfs(root, &st); err != nil {
		return false
	}
	if st.Type == nfsSuperMagic {
		return false
	}
	return true
}

// SubscribeQueueChanges registers a filesystem watcher for queue message changes.
func (s *Store) SubscribeQueueChanges(namespace, queue string) (storage.QueueChangeSubscription, error) {
	if !s.queueWatchEnabled {
		return nil, storage.ErrNotImplemented
	}
	namespace = strings.TrimSpace(namespace)
	queue = strings.TrimSpace(queue)
	if namespace == "" {
		return nil, fmt.Errorf("disk: queue watcher namespace required")
	}
	if queue == "" {
		return nil, fmt.Errorf("disk: queue watcher requires queue name")
	}
	encoded := encodePathSegments([]string{"q", queue, "msg"})
	dir := filepath.Join(append([]string{s.namespaceObjectsDir(namespace)}, encoded...)...)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("disk: prepare queue directory %q: %w", dir, err)
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("disk: create queue watcher: %w", err)
	}
	if err := watcher.Add(dir); err != nil {
		watcher.Close()
		return nil, fmt.Errorf("disk: watch queue directory %q: %w", dir, err)
	}
	sub := &queueChangeSubscription{
		watcher: watcher,
		events:  make(chan struct{}, 1),
		stop:    make(chan struct{}),
	}
	go sub.run()
	return sub, nil
}

type queueChangeSubscription struct {
	watcher *fsnotify.Watcher
	events  chan struct{}
	stop    chan struct{}
	once    sync.Once
}

func (q *queueChangeSubscription) Events() <-chan struct{} {
	return q.events
}

func (q *queueChangeSubscription) Close() error {
	q.once.Do(func() {
		close(q.stop)
		q.watcher.Close()
	})
	return nil
}

func (q *queueChangeSubscription) run() {
	defer close(q.events)
	for {
		select {
		case <-q.stop:
			return
		case _, ok := <-q.watcher.Events:
			if !ok {
				return
			}
			q.signal()
		case <-q.watcher.Errors:
			q.signal()
		}
	}
}

func (q *queueChangeSubscription) signal() {
	select {
	case q.events <- struct{}{}:
	default:
	}
}
