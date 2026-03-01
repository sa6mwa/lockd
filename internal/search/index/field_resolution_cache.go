package index

import "sync"

type fieldResolutionCache struct {
	mu      sync.RWMutex
	entries map[string][]string
}

func newFieldResolutionCache() *fieldResolutionCache {
	return &fieldResolutionCache{
		entries: make(map[string][]string, 32),
	}
}

func (c *fieldResolutionCache) get(field string) ([]string, bool) {
	if c == nil || field == "" {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	fields, ok := c.entries[field]
	if !ok {
		return nil, false
	}
	return fields, true
}

func (c *fieldResolutionCache) put(field string, fields []string) {
	if c == nil || field == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[field] = fields
}

func (c *fieldResolutionCache) len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
