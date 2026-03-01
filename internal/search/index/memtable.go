package index

import (
	"sort"
	"strings"
	"time"

	"pkt.systems/lockd/internal/uuidv7"
)

// MemTable buffers postings before they are flushed into immutable segments.
type MemTable struct {
	fields  map[string]map[string][]string
	docKeys map[string]struct{}
	docMeta map[string]DocumentMetadata
}

// NewMemTable returns an empty memtable.
func NewMemTable() *MemTable {
	return &MemTable{
		fields:  make(map[string]map[string][]string),
		docKeys: make(map[string]struct{}),
		docMeta: make(map[string]DocumentMetadata),
	}
}

// Add records a posting for field/term → key.
func (m *MemTable) Add(field, term, key string) {
	if field == "" || term == "" || key == "" {
		return
	}
	terms := m.fields[field]
	if terms == nil {
		terms = make(map[string][]string, 8)
		m.fields[field] = terms
	}
	keys := terms[term]
	if keys == nil {
		term = strings.Clone(term)
	}
	terms[term] = append(keys, key)
}

// AddTerms records postings for field/terms -> key and reports whether at least
// one non-empty term was accepted.
func (m *MemTable) AddTerms(field string, terms []string, key string) bool {
	if field == "" || key == "" || len(terms) == 0 {
		return false
	}
	postings := m.fields[field]
	if postings == nil {
		postings = make(map[string][]string, len(terms))
		m.fields[field] = postings
	}
	added := false
	for _, term := range terms {
		if term == "" {
			continue
		}
		keys := postings[term]
		if keys == nil {
			term = strings.Clone(term)
		}
		postings[term] = append(keys, key)
		added = true
	}
	return added
}

// TrackDoc records a document key once for doc-count accounting.
func (m *MemTable) TrackDoc(key string) {
	if key == "" {
		return
	}
	m.docKeys[key] = struct{}{}
}

// Reset clears the memtable contents.
func (m *MemTable) Reset() {
	m.fields = make(map[string]map[string][]string)
	m.docKeys = make(map[string]struct{})
	m.docMeta = make(map[string]DocumentMetadata)
}

// Flush converts the current contents into a Segment and resets the memtable.
func (m *MemTable) Flush(now time.Time) *Segment {
	segmentID := uuidv7.NewString()
	segment := NewSegment(segmentID, now.UTC())
	segment.docCount = uint64(len(m.docKeys))
	for field, terms := range m.fields {
		for term, keys := range terms {
			terms[term] = dedupeAndSort(keys)
		}
		segment.Fields[field] = FieldBlock{Postings: terms}
	}
	if len(m.docMeta) > 0 {
		segment.DocMeta = make(map[string]DocumentMetadata, len(m.docMeta))
		for key, meta := range m.docMeta {
			segment.DocMeta[key] = meta
		}
	}
	m.Reset()
	return segment
}

// DocCount returns the unique document count currently buffered.
func (m *MemTable) DocCount() uint64 {
	return uint64(len(m.docKeys))
}

// SetMeta stores document metadata for the given key.
func (m *MemTable) SetMeta(key string, meta DocumentMetadata) {
	if key == "" {
		return
	}
	if len(meta.StateDescriptor) > 0 {
		meta.StateDescriptor = append([]byte(nil), meta.StateDescriptor...)
	}
	m.docMeta[key] = meta
}

func dedupeAndSort(keys []string) []string {
	if len(keys) <= 1 {
		return keys
	}
	nonDecreasing := true
	hasDup := false
	prev := keys[0]
	for i := 1; i < len(keys); i++ {
		cur := keys[i]
		if cur < prev {
			nonDecreasing = false
			break
		}
		if cur == prev {
			hasDup = true
		}
		prev = cur
	}
	if nonDecreasing {
		if !hasDup {
			return keys
		}
		writeIdx := 1
		last := keys[0]
		for i := 1; i < len(keys); i++ {
			if keys[i] == last {
				continue
			}
			last = keys[i]
			keys[writeIdx] = keys[i]
			writeIdx++
		}
		return keys[:writeIdx]
	}
	sort.Strings(keys)
	writeIdx := 1
	for i := 1; i < len(keys); i++ {
		if keys[i] != keys[writeIdx-1] {
			keys[writeIdx] = keys[i]
			writeIdx++
		}
	}
	return keys[:writeIdx]
}
