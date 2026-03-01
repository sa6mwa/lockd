package client

import (
	"container/list"
	"fmt"
	"sync"

	"pkt.systems/lockd/api"
)

const defaultQuerySelectorCacheLimit = 512

var querySelectorExprCache = newQuerySelectorCache(defaultQuerySelectorCacheLimit)

type querySelectorCache struct {
	mu    sync.Mutex
	max   int
	items map[string]*list.Element
	order *list.List
}

type querySelectorCacheEntry struct {
	key      string
	selector api.Selector
	errMsg   string
}

func newQuerySelectorCache(max int) *querySelectorCache {
	if max <= 0 {
		return nil
	}
	return &querySelectorCache{
		max:   max,
		items: make(map[string]*list.Element, max),
		order: list.New(),
	}
}

func (c *querySelectorCache) get(expr string) (api.Selector, bool, error) {
	if c == nil || expr == "" {
		return api.Selector{}, false, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[expr]
	if !ok {
		return api.Selector{}, false, nil
	}
	c.order.MoveToFront(elem)
	entry := elem.Value.(*querySelectorCacheEntry)
	if entry.errMsg != "" {
		return api.Selector{}, true, fmt.Errorf("%s", entry.errMsg)
	}
	return entry.selector, true, nil
}

func (c *querySelectorCache) put(expr string, selector api.Selector, err error) {
	if c == nil || expr == "" {
		return
	}
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[expr]; ok {
		entry := elem.Value.(*querySelectorCacheEntry)
		entry.selector = selector
		entry.errMsg = errMsg
		c.order.MoveToFront(elem)
		return
	}
	elem := c.order.PushFront(&querySelectorCacheEntry{key: expr, selector: selector, errMsg: errMsg})
	c.items[expr] = elem
	if c.order.Len() <= c.max {
		return
	}
	back := c.order.Back()
	if back == nil {
		return
	}
	entry := back.Value.(*querySelectorCacheEntry)
	delete(c.items, entry.key)
	c.order.Remove(back)
}
