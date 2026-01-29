package index

import (
	"sort"
	"time"

	"pkt.systems/lockd/internal/uuidv7"
)

type visibilityMemTable struct {
	entries map[string]bool
}

func newVisibilityMemTable() *visibilityMemTable {
	return &visibilityMemTable{entries: make(map[string]bool)}
}

func (m *visibilityMemTable) Reset() {
	m.entries = make(map[string]bool)
}

func (m *visibilityMemTable) Set(key string, visible bool) {
	if key == "" {
		return
	}
	m.entries[key] = visible
}

func (m *visibilityMemTable) Len() int {
	return len(m.entries)
}

func (m *visibilityMemTable) Flush(now time.Time) *VisibilitySegment {
	segment := &VisibilitySegment{
		ID:        uuidv7.NewString(),
		CreatedAt: now.UTC(),
	}
	if len(m.entries) == 0 {
		return segment
	}
	keys := make([]string, 0, len(m.entries))
	for key := range m.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entries := make([]VisibilityEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, VisibilityEntry{Key: key, Visible: m.entries[key]})
	}
	segment.Entries = entries
	m.Reset()
	return segment
}
