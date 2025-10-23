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
	"time"

	"pkt.systems/lockd/internal/storage"
)

const (
	defaultCachePageSize = 128
	maxIncrementalScans  = 64
)

var readyCacheTrace = os.Getenv("LOCKD_READY_CACHE_TRACE") == "1"

type readyCache struct {
	svc   *Service
	queue string

	mu      sync.Mutex
	entries map[string]*readyEntry
	items   readyHeap
	cursor  string
}

type readyEntry struct {
	descriptor MessageDescriptor
	due        time.Time
	index      int
}

type readyHeap []*readyEntry

func newReadyCache(svc *Service, queue string) *readyCache {
	return &readyCache{
		svc:     svc,
		queue:   queue,
		entries: make(map[string]*readyEntry),
		items:   readyHeap{},
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
	return &desc
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
	}
	c.mu.Unlock()
}

func (c *readyCache) invalidate() {
	c.mu.Lock()
	c.entries = make(map[string]*readyEntry)
	c.items = readyHeap{}
	c.cursor = ""
	c.mu.Unlock()
}

func (c *readyCache) remove(id string) {
	c.mu.Lock()
	entry, ok := c.entries[id]
	if !ok {
		c.mu.Unlock()
		return
	}
	heap.Remove(&c.items, entry.index)
	delete(c.entries, id)
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
		pageSize = defaultCachePageSize
	}
	if pageSize < defaultCachePageSize {
		pageSize = defaultCachePageSize
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
	return c.popReady(now), nil
}

func (c *readyCache) refresh(ctx context.Context, pageSize int, now time.Time, forceFull bool) (bool, error) {
	start := time.Now()
	if pageSize <= 0 {
		pageSize = defaultCachePageSize
	}
	if pageSize < 32 {
		pageSize = 32
	}
	if pageSize > 256 {
		pageSize = 256
	}
	if forceFull {
		c.resetWithCursor("")
	}
	cursor := c.getCursor()

	readyAdded := false
	scans := 0
	for {
		scans++
		objects, next, truncated, err := c.svc.listMessageObjects(ctx, c.queue, cursor, pageSize)
		if err != nil {
			if readyCacheTrace {
				if elapsed := time.Since(start); elapsed > 0 {
					fmt.Fprintf(os.Stderr, "ready_cache refresh error queue=%s scans=%d elapsed=%s\n", c.queue, scans, elapsed)
				}
			}
			return readyAdded, err
		}
		for _, obj := range objects {
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
			descPtr, err := c.svc.loadDescriptor(ctx, obj.Key)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					continue
				}
				return readyAdded, err
			}
			desc := *descPtr
			if desc.MetadataETag == "" {
				desc.MetadataETag = obj.ETag
			}
			isReady, err := c.handleDescriptor(ctx, desc, now)
			if err != nil {
				return readyAdded, err
			}
			if isReady {
				readyAdded = true
			}
		}

		cursor = next
		if !truncated || scans >= maxIncrementalScans {
			if !truncated {
				cursor = ""
			}
			c.setCursor(cursor)
			break
		}
		// continue scanning next page
	}
	if readyCacheTrace {
		if elapsed := time.Since(start); elapsed > 0 {
			fmt.Fprintf(os.Stderr, "ready_cache refresh queue=%s readyAdded=%v scans=%d elapsed=%s pageSize=%d\n", c.queue, readyAdded, scans, elapsed, pageSize)
		}
	}
	return readyAdded, nil
}

func (c *readyCache) handleDescriptor(ctx context.Context, desc MessageDescriptor, now time.Time) (bool, error) {
	doc := desc.Document
	if doc.ExpiresAt != nil && doc.ExpiresAt.Before(now) {
		_ = c.svc.deleteMessageInternal(ctx, c.queue, doc.ID, desc.MetadataETag, false)
		c.remove(doc.ID)
		return false, nil
	}

	if doc.MaxAttempts > 0 && doc.Attempts >= doc.MaxAttempts {
		docCopy := doc
		if err := c.svc.moveToDLQInternal(ctx, c.queue, doc.ID, &docCopy, desc.MetadataETag, false); err != nil {
			return false, err
		}
		c.remove(doc.ID)
		return false, nil
	}

	c.upsert(desc)
	return !doc.NotVisibleUntil.After(now), nil
}
