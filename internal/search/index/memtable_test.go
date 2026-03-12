package index

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func TestDedupeAndSortSortedUniqueInPlace(t *testing.T) {
	keys := []string{"a", "b", "c"}
	ptr := &keys[0]

	out := dedupeAndSort(keys)

	if &out[0] != ptr {
		t.Fatalf("expected dedupeAndSort to reuse backing array for sorted unique input")
	}
	if !reflect.DeepEqual(out, []string{"a", "b", "c"}) {
		t.Fatalf("unexpected output: %v", out)
	}
}

func TestDedupeAndSortSortedDuplicatesInPlace(t *testing.T) {
	keys := []string{"a", "a", "b", "b", "c"}
	ptr := &keys[0]

	out := dedupeAndSort(keys)

	if &out[0] != ptr {
		t.Fatalf("expected dedupeAndSort to reuse backing array for sorted input")
	}
	if !reflect.DeepEqual(out, []string{"a", "b", "c"}) {
		t.Fatalf("unexpected output: %v", out)
	}
}

func TestDedupeAndSortUnsortedDuplicatesInPlace(t *testing.T) {
	keys := []string{"c", "a", "b", "a", "c", "b"}
	ptr := &keys[0]

	out := dedupeAndSort(keys)

	if &out[0] != ptr {
		t.Fatalf("expected dedupeAndSort to sort and dedupe in-place")
	}
	if !reflect.DeepEqual(out, []string{"a", "b", "c"}) {
		t.Fatalf("unexpected output: %v", out)
	}
}

func TestMemTableAddTermsReportsAcceptedTerms(t *testing.T) {
	m := NewMemTable()
	if m.AddTerms("", []string{"a"}, "k1") {
		t.Fatalf("expected empty field to be ignored")
	}
	if m.AddTerms("field", []string{""}, "k1") {
		t.Fatalf("expected empty terms to be ignored")
	}
	if !m.AddTerms("field", []string{"a", "", "b"}, "k1") {
		t.Fatalf("expected non-empty terms to be accepted")
	}
	if got := m.fields["field"]["a"]; !reflect.DeepEqual(got, []string{"k1"}) {
		t.Fatalf("unexpected postings for term a: %v", got)
	}
	if got := m.fields["field"]["b"]; !reflect.DeepEqual(got, []string{"k1"}) {
		t.Fatalf("unexpected postings for term b: %v", got)
	}
}

func TestMemTableAddTermsClonesFirstTermKey(t *testing.T) {
	base := strings.Repeat("abcdef", 8)
	term := base[5:8]
	m := NewMemTable()
	if !m.AddTerms("field", []string{term}, "k1") {
		t.Fatalf("expected term to be accepted")
	}
	var stored string
	for k := range m.fields["field"] {
		stored = k
		break
	}
	if stored == "" {
		t.Fatalf("expected stored term key")
	}
	if unsafe.StringData(stored) == unsafe.StringData(term) {
		t.Fatalf("expected stored term to be detached from source backing buffer")
	}
	if got := m.fields["field"][term]; !reflect.DeepEqual(got, []string{"k1"}) {
		t.Fatalf("unexpected postings: %v", got)
	}
}

func TestMemTableSetMetaClonesDescriptor(t *testing.T) {
	m := NewMemTable()
	desc := []byte{1, 2, 3}
	m.SetMeta("k1", DocumentMetadata{StateDescriptor: desc})
	desc[0] = 9
	if got := m.docMeta["k1"].StateDescriptor; !reflect.DeepEqual(got, []byte{1, 2, 3}) {
		t.Fatalf("expected descriptor clone in memtable, got %v", got)
	}
}

func TestMemTableFlushTransfersPostingsAndResets(t *testing.T) {
	m := NewMemTable()
	if !m.AddTerms("field", []string{"b", "a", "b"}, "k2") {
		t.Fatalf("expected terms for k2 to be accepted")
	}
	if !m.AddTerms("field", []string{"a"}, "k1") {
		t.Fatalf("expected terms for k1 to be accepted")
	}
	m.TrackDoc("k2")
	m.TrackDoc("k1")
	desc := []byte{1, 2, 3}
	m.SetMeta("k1", DocumentMetadata{StateDescriptor: desc})

	seg := m.Flush(time.Unix(1_700_000_000, 0))
	if seg == nil {
		t.Fatalf("expected segment")
	}

	if got := seg.Fields["field"].Postings["a"]; !reflect.DeepEqual(got, []string{"k1", "k2"}) {
		t.Fatalf("unexpected postings for a: %v", got)
	}
	if got := seg.Fields["field"].Postings["b"]; !reflect.DeepEqual(got, []string{"k2"}) {
		t.Fatalf("unexpected postings for b: %v", got)
	}
	if got := seg.DocMeta["k1"].StateDescriptor; !reflect.DeepEqual(got, []byte{1, 2, 3}) {
		t.Fatalf("unexpected descriptor clone: %v", got)
	}
	if len(m.fields) != 0 || len(m.docKeys) != 0 || len(m.docMeta) != 0 {
		t.Fatalf("expected memtable to reset after flush")
	}

	desc[0] = 9
	if got := seg.DocMeta["k1"].StateDescriptor; !reflect.DeepEqual(got, []byte{1, 2, 3}) {
		t.Fatalf("segment descriptor changed unexpectedly: %v", got)
	}

	if !m.AddTerms("field", []string{"a"}, "k3") {
		t.Fatalf("expected post-flush add to succeed")
	}
	if got := seg.Fields["field"].Postings["a"]; !reflect.DeepEqual(got, []string{"k1", "k2"}) {
		t.Fatalf("segment postings mutated after reset: %v", got)
	}
}

func TestMemTableRepeatedTrigramsRecordedOncePerDocument(t *testing.T) {
	m := NewMemTable()
	const (
		docCount    = 4
		payloadSize = 1 << 20
	)
	payload := strings.Repeat("x", payloadSize)
	gramField := containsGramField("/message")

	for i := 0; i < docCount; i++ {
		doc := Document{Key: fmt.Sprintf("doc-%d", i)}
		doc.AddString("/message", payload)
		for field, terms := range doc.Fields {
			if !m.AddTerms(field, terms, doc.Key) {
				t.Fatalf("expected indexed terms for %s", field)
			}
		}
		m.TrackDoc(doc.Key)
	}

	got := m.fields[gramField]["xxx"]
	want := []string{"doc-0", "doc-1", "doc-2", "doc-3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected trigram postings: got len=%d want=%d", len(got), len(want))
	}
}

func TestMemTableLargeRepeatedStringTotalAllocBounded(t *testing.T) {
	const (
		docCount    = 4
		payloadSize = 1 << 20
		allocCap    = uint64(24 << 20)
	)
	payload := strings.Repeat("x", payloadSize)

	run := func() int {
		m := NewMemTable()
		for i := 0; i < docCount; i++ {
			doc := Document{Key: fmt.Sprintf("doc-%d", i)}
			doc.AddString("/message", payload)
			for field, terms := range doc.Fields {
				m.AddTerms(field, terms, doc.Key)
			}
			m.TrackDoc(doc.Key)
		}
		return len(m.fields[containsGramField("/message")]["xxx"])
	}

	if got := run(); got != docCount {
		t.Fatalf("warm trigram postings mismatch: got=%d want=%d", got, docCount)
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	if got := run(); got != docCount {
		t.Fatalf("trigram postings mismatch: got=%d want=%d", got, docCount)
	}

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	delta := after.TotalAlloc - before.TotalAlloc
	if delta > allocCap {
		t.Fatalf("repeated-string indexing alloc too large: got=%d cap=%d payload=%d docs=%d", delta, allocCap, payloadSize, docCount)
	}
}
