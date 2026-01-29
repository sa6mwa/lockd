package disk

import (
	"container/list"
	"os"
	"sync"
)

type lockFileEntry struct {
	path string
	file *os.File
	refs int
	elem *list.Element
}

type lockFileCache struct {
	max     int
	mu      sync.Mutex
	entries map[string]*lockFileEntry
	lru     *list.List
}

func newLockFileCache(max int) *lockFileCache {
	if max <= 0 {
		return nil
	}
	return &lockFileCache{
		max:     max,
		entries: make(map[string]*lockFileEntry),
		lru:     list.New(),
	}
}

func (c *lockFileCache) acquire(path string) (*lockFileEntry, error) {
	if c == nil {
		return nil, nil
	}
	c.mu.Lock()
	if entry := c.entries[path]; entry != nil {
		entry.refs++
		if entry.elem != nil {
			c.lru.Remove(entry.elem)
			entry.elem = nil
		}
		c.mu.Unlock()
		return entry, nil
	}
	c.mu.Unlock()

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if entry := c.entries[path]; entry != nil {
		entry.refs++
		if entry.elem != nil {
			c.lru.Remove(entry.elem)
			entry.elem = nil
		}
		c.mu.Unlock()
		_ = f.Close()
		return entry, nil
	}
	entry := &lockFileEntry{path: path, file: f, refs: 1}
	c.entries[path] = entry
	c.mu.Unlock()
	return entry, nil
}

func (c *lockFileCache) release(entry *lockFileEntry) {
	if c == nil || entry == nil {
		return
	}
	var toClose []*os.File
	c.mu.Lock()
	if entry.refs > 0 {
		entry.refs--
	}
	if entry.refs == 0 && entry.elem == nil {
		entry.elem = c.lru.PushFront(entry)
	}
	toClose = c.evictLocked()
	c.mu.Unlock()
	for _, f := range toClose {
		_ = f.Close()
	}
}

func (c *lockFileCache) discard(entry *lockFileEntry) {
	if c == nil || entry == nil {
		return
	}
	var file *os.File
	c.mu.Lock()
	if entry.elem != nil {
		c.lru.Remove(entry.elem)
		entry.elem = nil
	}
	delete(c.entries, entry.path)
	file = entry.file
	entry.file = nil
	entry.refs = 0
	c.mu.Unlock()
	if file != nil {
		_ = file.Close()
	}
}

func (c *lockFileCache) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	files := make([]*os.File, 0, len(c.entries))
	for _, entry := range c.entries {
		if entry.file != nil {
			files = append(files, entry.file)
		}
	}
	c.entries = make(map[string]*lockFileEntry)
	c.lru.Init()
	c.mu.Unlock()
	for _, f := range files {
		_ = f.Close()
	}
}

func (c *lockFileCache) evictLocked() []*os.File {
	if c == nil || c.max <= 0 {
		return nil
	}
	var toClose []*os.File
	for c.lru.Len() > c.max {
		back := c.lru.Back()
		if back == nil {
			break
		}
		entry := back.Value.(*lockFileEntry)
		c.lru.Remove(back)
		entry.elem = nil
		delete(c.entries, entry.path)
		if entry.file != nil {
			toClose = append(toClose, entry.file)
			entry.file = nil
		}
	}
	return toClose
}
