package index

import (
	"container/list"
	"sync"
)

const defaultSegmentCacheLimit = 128

type segmentCache struct {
	mu    sync.Mutex
	max   int
	items map[string]*list.Element
	order *list.List
}

type cacheEntry struct {
	key     string
	segment *Segment
}

func newSegmentCache(max int) *segmentCache {
	if max <= 0 {
		return nil
	}
	return &segmentCache{
		max:   max,
		items: make(map[string]*list.Element, max),
		order: list.New(),
	}
}

func (c *segmentCache) get(key string) (*Segment, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(elem)
	entry := elem.Value.(*cacheEntry)
	return entry.segment, true
}

func (c *segmentCache) put(key string, seg *Segment) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		elem.Value.(*cacheEntry).segment = seg
		c.order.MoveToFront(elem)
		return
	}
	elem := c.order.PushFront(&cacheEntry{key: key, segment: seg})
	c.items[key] = elem
	if c.order.Len() <= c.max {
		return
	}
	back := c.order.Back()
	if back == nil {
		return
	}
	entry := back.Value.(*cacheEntry)
	delete(c.items, entry.key)
	c.order.Remove(back)
}

func (c *segmentCache) drop(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return
	}
	delete(c.items, key)
	c.order.Remove(elem)
}

func cacheKey(namespace, segmentID string) string {
	return namespace + "::" + segmentID
}
