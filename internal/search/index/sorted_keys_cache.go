package index

import (
	"container/list"
	"slices"
	"sync"
)

const defaultSortedKeysCacheLimit = 256

type sortedKeysCache struct {
	mu    sync.Mutex
	max   int
	items map[string]*list.Element
	order *list.List
}

type sortedKeysCacheEntry struct {
	key  string
	keys []string
}

func newSortedKeysCache(max int) *sortedKeysCache {
	if max <= 0 {
		return nil
	}
	return &sortedKeysCache{
		max:   max,
		items: make(map[string]*list.Element, max),
		order: list.New(),
	}
}

func (c *sortedKeysCache) get(key string) ([]string, bool) {
	if c == nil || key == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(elem)
	return elem.Value.(*sortedKeysCacheEntry).keys, true
}

func (c *sortedKeysCache) put(key string, keys []string) {
	if c == nil || key == "" {
		return
	}
	stored := slices.Clone(keys)
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*sortedKeysCacheEntry)
		entry.keys = stored
		c.order.MoveToFront(elem)
		return
	}
	elem := c.order.PushFront(&sortedKeysCacheEntry{key: key, keys: stored})
	c.items[key] = elem
	if c.order.Len() <= c.max {
		return
	}
	back := c.order.Back()
	if back == nil {
		return
	}
	entry := back.Value.(*sortedKeysCacheEntry)
	delete(c.items, entry.key)
	c.order.Remove(back)
}
