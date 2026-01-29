package core

import (
	"container/list"
	"sync"
)

type stateCache struct {
	mu       sync.Mutex
	maxBytes int64
	used     int64
	lru      *list.List
	entries  map[string]*list.Element
}

type stateCacheEntry struct {
	key  string
	data []byte
}

func newStateCache(maxBytes int64) *stateCache {
	if maxBytes <= 0 {
		return nil
	}
	return &stateCache{
		maxBytes: maxBytes,
		lru:      list.New(),
		entries:  make(map[string]*list.Element),
	}
}

func (c *stateCache) get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.entries[key]; ok {
		c.lru.MoveToFront(elem)
		entry := elem.Value.(*stateCacheEntry)
		return entry.data, true
	}
	return nil, false
}

func (c *stateCache) put(key string, data []byte) bool {
	if c == nil || len(data) == 0 {
		return false
	}
	size := int64(len(data))
	if size > c.maxBytes {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.entries[key]; ok {
		entry := elem.Value.(*stateCacheEntry)
		c.used -= int64(len(entry.data))
		entry.data = data
		c.used += size
		c.lru.MoveToFront(elem)
		c.evictLocked()
		return true
	}
	entry := &stateCacheEntry{key: key, data: data}
	elem := c.lru.PushFront(entry)
	c.entries[key] = elem
	c.used += size
	c.evictLocked()
	return true
}

func (c *stateCache) evictLocked() {
	for c.used > c.maxBytes && c.lru.Len() > 0 {
		elem := c.lru.Back()
		if elem == nil {
			return
		}
		entry := elem.Value.(*stateCacheEntry)
		delete(c.entries, entry.key)
		c.used -= int64(len(entry.data))
		c.lru.Remove(elem)
	}
}
