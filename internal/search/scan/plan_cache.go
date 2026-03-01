package scan

import (
	"container/list"
	"sync"

	"pkt.systems/lql"
)

const defaultQueryPlanCacheLimit = 256

type queryPlanCache struct {
	mu    sync.Mutex
	max   int
	order *list.List
	index map[string]*list.Element
}

type queryPlanCacheEntry struct {
	key  string
	plan lql.QueryStreamPlan
}

func newQueryPlanCache(max int) *queryPlanCache {
	if max <= 0 {
		max = defaultQueryPlanCacheLimit
	}
	return &queryPlanCache{
		max:   max,
		order: list.New(),
		index: make(map[string]*list.Element, max),
	}
}

func (c *queryPlanCache) get(key string) (lql.QueryStreamPlan, bool) {
	if c == nil || key == "" {
		return lql.QueryStreamPlan{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.index[key]
	if !ok {
		return lql.QueryStreamPlan{}, false
	}
	c.order.MoveToFront(elem)
	entry := elem.Value.(*queryPlanCacheEntry)
	return entry.plan, true
}

func (c *queryPlanCache) put(key string, plan lql.QueryStreamPlan) {
	if c == nil || key == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.index[key]; ok {
		entry := elem.Value.(*queryPlanCacheEntry)
		entry.plan = plan
		c.order.MoveToFront(elem)
		return
	}
	elem := c.order.PushFront(&queryPlanCacheEntry{key: key, plan: plan})
	c.index[key] = elem
	for c.order.Len() > c.max {
		c.evictOldest()
	}
}

func (c *queryPlanCache) evictOldest() {
	back := c.order.Back()
	if back == nil {
		return
	}
	entry := back.Value.(*queryPlanCacheEntry)
	delete(c.index, entry.key)
	c.order.Remove(back)
}

func (c *queryPlanCache) len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
