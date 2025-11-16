package index

import (
	"sort"
	"time"

	"pkt.systems/lockd/internal/uuidv7"
)

// MemTable buffers postings before they are flushed into immutable segments.
type MemTable struct {
	fields  map[string]map[string][]string
	docKeys map[string]struct{}
}

// NewMemTable returns an empty memtable.
func NewMemTable() *MemTable {
	return &MemTable{fields: make(map[string]map[string][]string), docKeys: make(map[string]struct{})}
}

// Add records a posting for field/term → key.
func (m *MemTable) Add(field, term, key string) {
	if field == "" || term == "" || key == "" {
		return
	}
	m.docKeys[key] = struct{}{}
	terms := m.fields[field]
	if terms == nil {
		terms = make(map[string][]string)
		m.fields[field] = terms
	}
	terms[term] = append(terms[term], key)
}

// Reset clears the memtable contents.
func (m *MemTable) Reset() {
	m.fields = make(map[string]map[string][]string)
	m.docKeys = make(map[string]struct{})
}

// Flush converts the current contents into a Segment and resets the memtable.
func (m *MemTable) Flush(now time.Time) *Segment {
	segmentID := uuidv7.NewString()
	segment := NewSegment(segmentID, now.UTC())
	for field, terms := range m.fields {
		block := FieldBlock{Postings: make(map[string][]string, len(terms))}
		for term, keys := range terms {
			block.Postings[term] = dedupeAndSort(keys)
		}
		segment.Fields[field] = block
	}
	m.Reset()
	return segment
}

// DocCount returns the unique document count currently buffered.
func (m *MemTable) DocCount() uint64 {
	return uint64(len(m.docKeys))
}

func dedupeAndSort(keys []string) []string {
	if len(keys) <= 1 {
		return append([]string(nil), keys...)
	}
	copyKeys := append([]string(nil), keys...)
	sort.Strings(copyKeys)
	uniq := copyKeys[:0]
	for i, key := range copyKeys {
		if i == 0 || key != copyKeys[i-1] {
			uniq = append(uniq, key)
		}
	}
	return uniq
}
