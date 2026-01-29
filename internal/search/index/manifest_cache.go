package index

import (
	"sync"
	"time"
)

const manifestCacheTTL = 500 * time.Millisecond

type manifestCacheEntry struct {
	manifest  *Manifest
	etag      string
	checkedAt time.Time
}

type manifestCache struct {
	mu     sync.Mutex
	values map[string]manifestCacheEntry
}

func newManifestCache() *manifestCache {
	return &manifestCache{values: make(map[string]manifestCacheEntry)}
}

func (c *manifestCache) get(namespace string) (manifestCacheEntry, bool) {
	if c == nil {
		return manifestCacheEntry{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.values[namespace]
	if !ok {
		return manifestCacheEntry{}, false
	}
	if time.Since(entry.checkedAt) > manifestCacheTTL {
		return manifestCacheEntry{}, false
	}
	entry.checkedAt = time.Now()
	c.values[namespace] = entry
	return entry, true
}

func (c *manifestCache) set(namespace string, manifest *Manifest, etag string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[namespace] = manifestCacheEntry{
		manifest:  cloneManifest(manifest),
		etag:      etag,
		checkedAt: time.Now(),
	}
}

func cloneManifest(manifest *Manifest) *Manifest {
	if manifest == nil {
		return NewManifest()
	}
	clone := NewManifest()
	clone.Seq = manifest.Seq
	clone.UpdatedAt = manifest.UpdatedAt
	if len(manifest.Shards) == 0 {
		return clone
	}
	for shardID, shard := range manifest.Shards {
		if shard == nil {
			continue
		}
		segRefs := append([]SegmentRef(nil), shard.Segments...)
		clone.Shards[shardID] = &Shard{
			ID:       shard.ID,
			Segments: segRefs,
		}
	}
	return clone
}
