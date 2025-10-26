//go:build linux

package disk

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
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
func (s *Store) SubscribeQueueChanges(queue string) (storage.QueueChangeSubscription, error) {
	if !s.queueWatchEnabled {
		return nil, storage.ErrNotImplemented
	}
	normalized, err := s.normalizeObjectKey(path.Join("q", queue, "msg"))
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(s.objectDir, filepath.FromSlash(normalized))
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
