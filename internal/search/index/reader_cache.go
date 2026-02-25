package index

import (
	"container/list"
	"strconv"
	"strings"
	"sync"
)

const defaultPreparedReaderCacheLimit = 32

type preparedReaderCache struct {
	mu    sync.Mutex
	max   int
	items map[string]*list.Element
	order *list.List
}

type preparedReaderCacheEntry struct {
	key    string
	reader *segmentReader
}

func newPreparedReaderCache(max int) *preparedReaderCache {
	if max <= 0 {
		return nil
	}
	return &preparedReaderCache{
		max:   max,
		items: make(map[string]*list.Element, max),
		order: list.New(),
	}
}

func (c *preparedReaderCache) get(key string) (*segmentReader, bool) {
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
	return elem.Value.(*preparedReaderCacheEntry).reader, true
}

func (c *preparedReaderCache) put(key string, reader *segmentReader) {
	if c == nil || key == "" || reader == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*preparedReaderCacheEntry)
		entry.reader = reader
		c.order.MoveToFront(elem)
		return
	}
	elem := c.order.PushFront(&preparedReaderCacheEntry{key: key, reader: reader})
	c.items[key] = elem
	if c.order.Len() <= c.max {
		return
	}
	back := c.order.Back()
	if back == nil {
		return
	}
	entry := back.Value.(*preparedReaderCacheEntry)
	delete(c.items, entry.key)
	c.order.Remove(back)
}

func (c *preparedReaderCache) len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func preparedReaderCacheKey(namespace string, manifest *Manifest, manifestETag string) string {
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		ns = "default"
	}
	if etag := strings.TrimSpace(manifestETag); etag != "" {
		return ns + "::etag=" + etag
	}
	if manifest == nil {
		return ns + "::manifest=nil"
	}
	var b strings.Builder
	b.Grow(96)
	b.WriteString(ns)
	b.WriteString("::seq=")
	b.WriteString(strconv.FormatUint(manifest.Seq, 10))
	b.WriteString("::fmt=")
	b.WriteString(strconv.FormatUint(uint64(manifest.Format), 10))
	b.WriteString("::updated=")
	b.WriteString(strconv.FormatInt(manifest.UpdatedAt.UnixNano(), 10))
	for _, shard := range manifest.Shards {
		if shard == nil {
			continue
		}
		b.WriteString("::shard=")
		b.WriteString(strconv.FormatUint(uint64(shard.ID), 10))
		for _, ref := range shard.Segments {
			b.WriteString(",")
			b.WriteString(ref.ID)
		}
	}
	return b.String()
}
