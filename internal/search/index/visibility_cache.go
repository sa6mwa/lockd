package index

import (
	"sync"
	"time"
)

type visibilityState struct {
	seq       uint64
	updatedAt time.Time
	entries   map[string]bool
	checkedAt time.Time
}

type visibilityCache struct {
	mu     sync.Mutex
	states map[string]*visibilityState
}

func newVisibilityCache() *visibilityCache {
	return &visibilityCache{states: make(map[string]*visibilityState)}
}

func (c *visibilityCache) get(namespace string) *visibilityState {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.states[namespace]
}

func (c *visibilityCache) set(namespace string, state *visibilityState) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if state == nil {
		delete(c.states, namespace)
		return
	}
	c.states[namespace] = state
}
