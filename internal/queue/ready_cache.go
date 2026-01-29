package queue

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/lockd/internal/storage"
)

const (
	defaultCachePageSize      = 128
	maxIncrementalScans       = 64
	readyCacheLoadConcurrency = 8
)

var (
	readyCacheTrace       = os.Getenv("LOCKD_READY_CACHE_TRACE") == "1"
	readyCacheStatsEnable atomic.Bool

	readyCacheInflightHit      atomic.Int64
	readyCacheCursorResets     atomic.Int64
	readyCacheInflightClears   atomic.Int64
	readyCacheListScans        atomic.Int64
	readyCacheListObjects      atomic.Int64
	readyCacheListNanos        atomic.Int64
	readyCacheLoadCount        atomic.Int64
	readyCacheLoadNanos        atomic.Int64
	readyCacheNextCalls        atomic.Int64
	readyCacheRefreshWaits     atomic.Int64
	readyCacheRefreshWaitNanos atomic.Int64
)

func init() {
	if os.Getenv("LOCKD_READY_CACHE_STATS") == "1" {
		readyCacheStatsEnable.Store(true)
	}
}

// EnableReadyCacheStats turns on ready cache stats collection at runtime.
func EnableReadyCacheStats() {
	readyCacheStatsEnable.Store(true)
}

// ReadyCacheStats captures optional counters for queue cache behavior.
type ReadyCacheStats struct {
	InflightSkips    int64
	CursorResets     int64
	InflightClears   int64
	ListScans        int64
	ListObjects      int64
	ListNanos        int64
	LoadCount        int64
	LoadNanos        int64
	NextCalls        int64
	RefreshWaits     int64
	RefreshWaitNanos int64
}

// ReadyCacheStatsSnapshot returns the current ready cache counters.
func ReadyCacheStatsSnapshot() ReadyCacheStats {
	return ReadyCacheStats{
		InflightSkips:    readyCacheInflightHit.Load(),
		CursorResets:     readyCacheCursorResets.Load(),
		InflightClears:   readyCacheInflightClears.Load(),
		ListScans:        readyCacheListScans.Load(),
		ListObjects:      readyCacheListObjects.Load(),
		ListNanos:        readyCacheListNanos.Load(),
		LoadCount:        readyCacheLoadCount.Load(),
		LoadNanos:        readyCacheLoadNanos.Load(),
		NextCalls:        readyCacheNextCalls.Load(),
		RefreshWaits:     readyCacheRefreshWaits.Load(),
		RefreshWaitNanos: readyCacheRefreshWaitNanos.Load(),
	}
}

// ResetReadyCacheStats clears ready cache counters.
func ResetReadyCacheStats() {
	readyCacheInflightHit.Store(0)
	readyCacheCursorResets.Store(0)
	readyCacheInflightClears.Store(0)
	readyCacheListScans.Store(0)
	readyCacheListObjects.Store(0)
	readyCacheListNanos.Store(0)
	readyCacheLoadCount.Store(0)
	readyCacheLoadNanos.Store(0)
	readyCacheNextCalls.Store(0)
	readyCacheRefreshWaits.Store(0)
	readyCacheRefreshWaitNanos.Store(0)
}

func readyCacheTracef(format string, args ...any) {
	if !readyCacheTrace {
		return
	}
	fmt.Fprintf(os.Stderr, format, args...)
}

type readyCache struct {
	svc   *Service
	ns    string
	queue string

	mu       sync.Mutex
	entries  map[string]*readyEntry
	items    readyHeap
	cursor   string
	inflight map[string]inflightEntry

	refreshMu   sync.Mutex
	refreshWait chan struct{}
}

type readyEntry struct {
	descriptor MessageDescriptor
	due        time.Time
	index      int
}

type inflightEntry struct {
	etag    string
	expires time.Time
}

type readyHeap []*readyEntry

func newReadyCache(svc *Service, namespace, queue string) *readyCache {
	return &readyCache{
		svc:      svc,
		ns:       namespace,
		queue:    queue,
		entries:  make(map[string]*readyEntry),
		items:    readyHeap{},
		inflight: make(map[string]inflightEntry),
	}
}

func (h readyHeap) Len() int { return len(h) }

func (h readyHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].descriptor.MetadataKey < h[j].descriptor.MetadataKey
	}
	return h[i].due.Before(h[j].due)
}

func (h readyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *readyHeap) Push(x any) {
	entry := x.(*readyEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}

func (h *readyHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	entry.index = -1
	*h = old[:n-1]
	return entry
}

func (c *readyCache) popReady(now time.Time) *MessageDescriptor {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.items.Len() == 0 {
		return nil
	}
	entry := c.items[0]
	if entry.due.After(now) {
		return nil
	}
	heap.Pop(&c.items)
	delete(c.entries, entry.descriptor.Document.ID)
	desc := entry.descriptor
	if c.inflight != nil {
		c.inflight[desc.Document.ID] = inflightEntry{
			etag:    desc.MetadataETag,
			expires: c.inflightExpiry(desc, now),
		}
	}
	return &desc
}

func (c *readyCache) inflightExpiry(desc MessageDescriptor, now time.Time) time.Time {
	ttl := time.Duration(desc.Document.VisibilityTimeout) * time.Second
	if ttl <= 0 {
		if c != nil && c.svc != nil && c.svc.cfg.DefaultVisibilityTimeout > 0 {
			ttl = c.svc.cfg.DefaultVisibilityTimeout
		} else {
			ttl = 30 * time.Second
		}
	}
	if ttl < 2*time.Second {
		ttl = 2 * time.Second
	}
	return now.Add(ttl)
}

func (c *readyCache) upsert(desc MessageDescriptor) {
	id := desc.Document.ID
	due := desc.Document.NotVisibleUntil

	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[id]; ok {
		entry.descriptor = desc
		entry.due = due
		heap.Fix(&c.items, entry.index)
		return
	}
	entry := &readyEntry{
		descriptor: desc,
		due:        due,
	}
	heap.Push(&c.items, entry)
	c.entries[id] = entry
}

func (c *readyCache) setCursor(cursor string) {
	c.mu.Lock()
	c.cursor = cursor
	c.mu.Unlock()
}

func (c *readyCache) getCursor() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cursor
}

func (c *readyCache) resetWithCursor(cursor string) {
	c.mu.Lock()
	c.cursor = cursor
	if cursor != "" {
		c.entries = make(map[string]*readyEntry)
		c.items = readyHeap{}
		if c.inflight != nil {
			if readyCacheStatsEnable.Load() {
				readyCacheCursorResets.Add(1)
			}
			now := time.Now()
			for id, inflight := range c.inflight {
				if now.After(inflight.expires) {
					delete(c.inflight, id)
					if readyCacheStatsEnable.Load() {
						readyCacheInflightClears.Add(1)
					}
				}
			}
		}
	}
	c.mu.Unlock()
}

func (c *readyCache) invalidate() {
	c.mu.Lock()
	c.entries = make(map[string]*readyEntry)
	c.items = readyHeap{}
	c.cursor = ""
	if c.inflight != nil {
		c.inflight = make(map[string]inflightEntry)
	}
	c.mu.Unlock()
}

func (c *readyCache) clearInflight(id string) {
	if c == nil || c.inflight == nil {
		return
	}
	c.mu.Lock()
	if _, ok := c.inflight[id]; ok {
		delete(c.inflight, id)
		if readyCacheStatsEnable.Load() {
			readyCacheInflightClears.Add(1)
		}
	}
	c.mu.Unlock()
}

func (c *readyCache) remove(id string) {
	c.mu.Lock()
	entry, ok := c.entries[id]
	if !ok {
		if c.inflight != nil {
			delete(c.inflight, id)
		}
		c.mu.Unlock()
		return
	}
	heap.Remove(&c.items, entry.index)
	delete(c.entries, id)
	if c.inflight != nil {
		delete(c.inflight, id)
	}
	c.mu.Unlock()
}

func (c *readyCache) entry(id string) (*readyEntry, bool) {
	c.mu.Lock()
	entry, ok := c.entries[id]
	c.mu.Unlock()
	return entry, ok
}

func (c *readyCache) next(ctx context.Context, pageSize int, now time.Time) (*MessageDescriptor, error) {
	if pageSize <= 0 {
		pageSize = defaultQueuePageSize
	}
	if readyCacheStatsEnable.Load() {
		readyCacheNextCalls.Add(1)
	}
	if desc := c.popReady(now); desc != nil {
		return desc, nil
	}
	if _, err := c.refresh(ctx, pageSize, now, false); err != nil {
		return nil, err
	}
	if desc := c.popReady(now); desc != nil {
		return desc, nil
	}
	// Force a full scan if still nothing visible.
	if _, err := c.refresh(ctx, pageSize, now, true); err != nil {
		return nil, err
	}
	if desc := c.popReady(now); desc != nil {
		return desc, nil
	}
	if readyCacheTrace {
		c.mu.Lock()
		size := len(c.entries)
		earliest := time.Time{}
		if c.items.Len() > 0 {
			earliest = c.items[0].due
		}
		cursor := c.cursor
		c.mu.Unlock()
		readyCacheTracef("ready_cache empty namespace=%s queue=%s size=%d cursor=%q earliest_due=%s now=%s\n",
			c.ns,
			c.queue,
			size,
			cursor,
			earliest.Format(time.RFC3339Nano),
			now.Format(time.RFC3339Nano),
		)
	}
	return nil, nil
}

func (c *readyCache) beginRefresh() (chan struct{}, bool) {
	c.refreshMu.Lock()
	if c.refreshWait == nil {
		wait := make(chan struct{})
		c.refreshWait = wait
		c.refreshMu.Unlock()
		return wait, true
	}
	wait := c.refreshWait
	c.refreshMu.Unlock()
	return wait, false
}

func (c *readyCache) endRefresh(wait chan struct{}) {
	c.refreshMu.Lock()
	if c.refreshWait == wait {
		close(wait)
		c.refreshWait = nil
	}
	c.refreshMu.Unlock()
}

func (c *readyCache) refresh(ctx context.Context, pageSize int, now time.Time, forceFull bool) (bool, error) {
	wait, leader := c.beginRefresh()
	if !leader {
		var waitStart time.Time
		if readyCacheStatsEnable.Load() {
			waitStart = time.Now()
		}
		var err error
		if ctx == nil {
			<-wait
		} else {
			select {
			case <-wait:
			case <-ctx.Done():
				err = ctx.Err()
			}
		}
		if readyCacheStatsEnable.Load() && !waitStart.IsZero() {
			readyCacheRefreshWaits.Add(1)
			readyCacheRefreshWaitNanos.Add(time.Since(waitStart).Nanoseconds())
		}
		return false, err
	}
	defer c.endRefresh(wait)

	start := time.Now()
	if pageSize <= 0 {
		pageSize = defaultCachePageSize
	}
	targetReady := pageSize
	if targetReady < defaultCachePageSize {
		targetReady = defaultCachePageSize
	}
	if targetReady > pageSize*4 {
		targetReady = pageSize * 4
	}
	if targetReady <= 0 {
		targetReady = pageSize
	}
	maxReadyScans := 4
	if forceFull {
		c.resetWithCursor("")
	}
	cursor := c.getCursor()

	readyAdded := false
	readyCount := 0
	objectsSeen := 0
	scans := 0
	listDuration := time.Duration(0)
	for {
		scans++
		listStart := time.Now()
		objects, next, truncated, err := c.svc.listMessageObjects(ctx, c.ns, c.queue, cursor, pageSize)
		listDuration += time.Since(listStart)
		if err != nil {
			if readyCacheTrace {
				if elapsed := time.Since(start); elapsed > 0 {
					fmt.Fprintf(os.Stderr, "ready_cache refresh error namespace=%s queue=%s scans=%d elapsed=%s\n", c.ns, c.queue, scans, elapsed)
				}
			}
			return readyAdded, err
		}
		toLoad := make([]storage.ObjectInfo, 0, len(objects))
		for _, obj := range objects {
			objectsSeen++
			if !strings.HasSuffix(obj.Key, metaFileExtension) {
				continue
			}
			id := strings.TrimSuffix(path.Base(obj.Key), metaFileExtension)
			existing, exists := c.entry(id)
			if exists && obj.ETag != "" && existing.descriptor.MetadataETag == obj.ETag {
				isReady, err := c.handleDescriptor(ctx, existing.descriptor, now)
				if err != nil {
					return readyAdded, err
				}
				if isReady {
					readyAdded = true
				}
				continue
			}
			toLoad = append(toLoad, obj)
		}
		if len(toLoad) > 0 {
			loadCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			var wg sync.WaitGroup
			sem := make(chan struct{}, readyCacheLoadConcurrency)
			errCh := make(chan error, 1)
			var readyCountLoad atomic.Int64

			for _, obj := range toLoad {
				sem <- struct{}{}
				wg.Add(1)
				go func(obj storage.ObjectInfo) {
					defer wg.Done()
					defer func() { <-sem }()
					if loadCtx.Err() != nil {
						return
					}
					var loadStart time.Time
					if readyCacheStatsEnable.Load() {
						loadStart = time.Now()
					}
					descPtr, err := c.svc.loadDescriptor(loadCtx, c.ns, c.queue, obj.Key)
					if readyCacheStatsEnable.Load() {
						readyCacheLoadCount.Add(1)
						readyCacheLoadNanos.Add(time.Since(loadStart).Nanoseconds())
					}
					if err != nil {
						if errors.Is(err, storage.ErrNotFound) {
							return
						}
						select {
						case errCh <- err:
							cancel()
						default:
						}
						return
					}
					desc := *descPtr
					if desc.MetadataETag == "" {
						desc.MetadataETag = obj.ETag
					}
					isReady, err := c.handleDescriptor(loadCtx, desc, now)
					if err != nil {
						select {
						case errCh <- err:
							cancel()
						default:
						}
						return
					}
					if isReady {
						readyCountLoad.Add(1)
					}
				}(obj)
			}

			wg.Wait()
			select {
			case err := <-errCh:
				return readyAdded, err
			default:
			}
			if added := int(readyCountLoad.Load()); added > 0 {
				readyAdded = true
				readyCount += added
			}
		}

		cursor = next
		if readyAdded && !forceFull {
			if readyCount >= targetReady || scans >= maxReadyScans || !truncated || scans >= maxIncrementalScans {
				if !truncated {
					cursor = ""
				}
				c.setCursor(cursor)
				break
			}
		}
		if !truncated || scans >= maxIncrementalScans {
			if readyCacheTrace {
				readyCacheTracef(
					"ready_cache refresh namespace=%s queue=%s force=%t scans=%d objects=%d ready=%t cursor=%q next=%q truncated=%t elapsed=%s\n",
					c.ns,
					c.queue,
					forceFull,
					scans,
					objectsSeen,
					readyAdded,
					cursor,
					next,
					truncated,
					time.Since(start),
				)
			}
			if !truncated {
				cursor = ""
			}
			c.setCursor(cursor)
			break
		}
		// continue scanning next page
	}
	if readyCacheTrace {
		listAvg := time.Duration(0)
		if scans > 0 {
			listAvg = listDuration / time.Duration(scans)
		}
		if elapsed := time.Since(start); elapsed > 0 {
			fmt.Fprintf(
				os.Stderr,
				"ready_cache refresh namespace=%s queue=%s readyAdded=%v objects=%d scans=%d elapsed=%s listAvg=%s pageSize=%d\n",
				c.ns,
				c.queue,
				readyAdded,
				objectsSeen,
				scans,
				elapsed,
				listAvg,
				pageSize,
			)
		}
	}
	if readyCacheStatsEnable.Load() && scans > 0 {
		readyCacheListScans.Add(int64(scans))
		readyCacheListObjects.Add(int64(objectsSeen))
		readyCacheListNanos.Add(listDuration.Nanoseconds())
	}
	return readyAdded, nil
}

func (c *readyCache) handleDescriptor(ctx context.Context, desc MessageDescriptor, now time.Time) (bool, error) {
	doc := desc.Document
	if c.inflight != nil {
		skip := false
		c.mu.Lock()
		if inflight, ok := c.inflight[doc.ID]; ok {
			if now.Before(inflight.expires) {
				skip = true
			} else {
				delete(c.inflight, doc.ID)
			}
		}
		c.mu.Unlock()
		if skip {
			if readyCacheStatsEnable.Load() {
				readyCacheInflightHit.Add(1)
			}
			return false, nil
		}
	}

	if doc.ExpiresAt != nil && doc.ExpiresAt.Before(now) {
		_ = c.svc.deleteMessageInternal(ctx, c.ns, c.queue, doc.ID, desc.MetadataETag, false)
		c.remove(doc.ID)
		return false, nil
	}

	if doc.MaxAttempts > 0 && doc.Attempts >= doc.MaxAttempts {
		docCopy := doc
		if err := c.svc.moveToDLQInternal(ctx, c.ns, c.queue, doc.ID, &docCopy, desc.MetadataETag, false); err != nil {
			return false, err
		}
		c.remove(doc.ID)
		return false, nil
	}

	c.upsert(desc)
	return !doc.NotVisibleUntil.After(now), nil
}
