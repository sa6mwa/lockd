package index

import (
	"container/list"
	"sync"

	"pkt.systems/lockd/api"
	"pkt.systems/lql"
)

const defaultSelectorPlanCacheLimit = 512

type selectorExecutionPlan struct {
	cacheKey        string
	selector        api.Selector
	useLegacyFilter bool
	requirePostEval bool
	postFilterPlan  lql.QueryStreamPlan
}

type selectorPlanCache struct {
	mu    sync.Mutex
	max   int
	items map[string]*list.Element
	order *list.List
}

type selectorPlanCacheEntry struct {
	key  string
	plan selectorExecutionPlan
}

func newSelectorPlanCache(max int) *selectorPlanCache {
	if max <= 0 {
		return nil
	}
	return &selectorPlanCache{
		max:   max,
		items: make(map[string]*list.Element, max),
		order: list.New(),
	}
}

func (c *selectorPlanCache) get(key string) (selectorExecutionPlan, bool) {
	if c == nil || key == "" {
		return selectorExecutionPlan{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return selectorExecutionPlan{}, false
	}
	c.order.MoveToFront(elem)
	return elem.Value.(*selectorPlanCacheEntry).plan, true
}

func (c *selectorPlanCache) put(key string, plan selectorExecutionPlan) {
	if c == nil || key == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*selectorPlanCacheEntry)
		entry.plan = plan
		c.order.MoveToFront(elem)
		return
	}
	elem := c.order.PushFront(&selectorPlanCacheEntry{key: key, plan: plan})
	c.items[key] = elem
	if c.order.Len() <= c.max {
		return
	}
	back := c.order.Back()
	if back == nil {
		return
	}
	entry := back.Value.(*selectorPlanCacheEntry)
	delete(c.items, entry.key)
	c.order.Remove(back)
}

func (c *selectorPlanCache) len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}
