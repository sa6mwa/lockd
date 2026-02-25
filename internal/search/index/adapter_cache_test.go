package index

import (
	"context"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/namespaces"
)

func TestSegmentReaderCompiledCacheLifecycle(t *testing.T) {
	ctx := context.Background()
	const (
		namespace = namespaces.Default
		segmentID = "seg-cache-lifecycle"
	)
	backend := &countingGetObjectBackend{
		Backend: memory.New(),
		gets:    make(map[string]int),
	}
	store := NewStore(backend, nil)

	seed := NewSegment(segmentID, time.Unix(1_700_000_010, 0))
	seed.Fields["/status"] = FieldBlock{Postings: map[string][]string{
		"open": {"doc-1"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespace, seed); err != nil {
		t.Fatalf("write segment: %v", err)
	}

	manifest := NewManifest()
	manifest.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{{
			ID:        segmentID,
			CreatedAt: seed.CreatedAt,
			DocCount:  seed.DocCount(),
		}},
	}
	reader := newSegmentReader(namespace, manifest, store, nil)

	// Force first read through storage backend to prove load behavior.
	if store.cache != nil {
		store.cache.drop(cacheKey(namespace, segmentID))
	}
	first, err := reader.loadCompiledSegment(ctx, segmentID)
	if err != nil {
		t.Fatalf("load compiled segment (first): %v", err)
	}
	if got := backend.count(segmentObject(segmentID)); got != 1 {
		t.Fatalf("expected one backend segment read, got %d", got)
	}
	firstDoc := compiledTermDocKey(t, reader, first, "/status", "open")
	if firstDoc != "doc-1" {
		t.Fatalf("unexpected first compiled term key %q", firstDoc)
	}

	// Compiled cache hit should return the exact same compiled pointer.
	second, err := reader.loadCompiledSegment(ctx, segmentID)
	if err != nil {
		t.Fatalf("load compiled segment (cache hit): %v", err)
	}
	if second != first {
		t.Fatal("expected compiled cache hit to return same pointer")
	}
	if got := backend.count(segmentObject(segmentID)); got != 1 {
		t.Fatalf("expected backend reads unchanged after compiled cache hit, got %d", got)
	}

	// Evict compiled form only: should rebuild from raw reader cache, no backend IO.
	delete(reader.compiled, segmentID)
	rebuilt, err := reader.loadCompiledSegment(ctx, segmentID)
	if err != nil {
		t.Fatalf("load compiled segment (rebuild): %v", err)
	}
	if rebuilt == first {
		t.Fatal("expected compiled rebuild pointer to differ after eviction")
	}
	if got := backend.count(segmentObject(segmentID)); got != 1 {
		t.Fatalf("expected no extra backend reads when rebuilding from raw cache, got %d", got)
	}
	rebuiltDoc := compiledTermDocKey(t, reader, rebuilt, "/status", "open")
	if rebuiltDoc != "doc-1" {
		t.Fatalf("unexpected rebuilt compiled term key %q", rebuiltDoc)
	}

	// Rewrite the segment object, evict caches, and ensure reload sees new state.
	updated := NewSegment(segmentID, time.Unix(1_700_000_020, 0))
	updated.Fields["/status"] = FieldBlock{Postings: map[string][]string{
		"open": {"doc-2"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespace, updated); err != nil {
		t.Fatalf("rewrite segment: %v", err)
	}
	delete(reader.compiled, segmentID)
	delete(reader.cache, segmentID)
	if store.cache != nil {
		store.cache.drop(cacheKey(namespace, segmentID))
	}

	reloaded, err := reader.loadCompiledSegment(ctx, segmentID)
	if err != nil {
		t.Fatalf("load compiled segment (reload): %v", err)
	}
	if got := backend.count(segmentObject(segmentID)); got != 2 {
		t.Fatalf("expected two backend segment reads after reload, got %d", got)
	}
	reloadedDoc := compiledTermDocKey(t, reader, reloaded, "/status", "open")
	if reloadedDoc != "doc-2" {
		t.Fatalf("unexpected reloaded compiled term key %q", reloadedDoc)
	}
}

func TestStoreSegmentCacheEvictionReload(t *testing.T) {
	ctx := context.Background()
	const namespace = namespaces.Default
	backend := &countingGetObjectBackend{
		Backend: memory.New(),
		gets:    make(map[string]int),
	}
	store := NewStore(backend, nil)
	store.cache = newSegmentCache(1)

	segA := NewSegment("seg-cache-a", time.Unix(1_700_000_030, 0))
	segA.Fields["/status"] = FieldBlock{Postings: map[string][]string{
		"open": {"a-doc"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespace, segA); err != nil {
		t.Fatalf("write segment A: %v", err)
	}

	segB := NewSegment("seg-cache-b", time.Unix(1_700_000_040, 0))
	segB.Fields["/status"] = FieldBlock{Postings: map[string][]string{
		"open": {"b-doc"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespace, segB); err != nil {
		t.Fatalf("write segment B: %v", err)
	}

	if _, err := store.LoadSegment(ctx, namespace, segA.ID); err != nil {
		t.Fatalf("load segment A (first): %v", err)
	}
	if got := backend.count(segmentObject(segA.ID)); got != 1 {
		t.Fatalf("expected one backend read for segment A, got %d", got)
	}

	if _, err := store.LoadSegment(ctx, namespace, segB.ID); err != nil {
		t.Fatalf("load segment B (first): %v", err)
	}
	if got := backend.count(segmentObject(segB.ID)); got != 1 {
		t.Fatalf("expected one backend read for segment B, got %d", got)
	}

	// Cache max=1 means A was evicted when B was loaded.
	if _, err := store.LoadSegment(ctx, namespace, segA.ID); err != nil {
		t.Fatalf("load segment A (second): %v", err)
	}
	if got := backend.count(segmentObject(segA.ID)); got != 2 {
		t.Fatalf("expected second backend read for evicted segment A, got %d", got)
	}
}

func TestCompiledSegmentDictionaries(t *testing.T) {
	ctx := context.Background()
	const (
		namespace = namespaces.Default
		segmentID = "seg-dicts"
	)
	store := NewStore(memory.New(), nil)
	seed := NewSegment(segmentID, time.Unix(1_700_000_050, 0))
	seed.Fields["/status"] = FieldBlock{Postings: map[string][]string{
		"open":   {"doc-a"},
		"closed": {"doc-b"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespace, seed); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	manifest := NewManifest()
	manifest.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{{
			ID:        segmentID,
			CreatedAt: seed.CreatedAt,
			DocCount:  seed.DocCount(),
		}},
	}
	reader := newSegmentReader(namespace, manifest, store, nil)
	compiled, err := reader.loadCompiledSegment(ctx, segmentID)
	if err != nil {
		t.Fatalf("load compiled segment: %v", err)
	}

	fieldID, ok := reader.fieldIDs["/status"]
	if !ok {
		t.Fatal("missing field id for /status")
	}
	block, ok := compiled.fieldsByID[fieldID]
	if !ok {
		t.Fatal("missing compiled field block for /status")
	}
	if len(block.termIDs) != 2 || len(block.terms) != 2 || len(block.postingsByID) != 2 {
		t.Fatalf("unexpected dictionary sizes: termIDs=%d terms=%d postings=%d",
			len(block.termIDs), len(block.terms), len(block.postingsByID))
	}
	openKey := compiledTermDocKey(t, reader, compiled, "/status", "open")
	closedKey := compiledTermDocKey(t, reader, compiled, "/status", "closed")
	if openKey != "doc-a" || closedKey != "doc-b" {
		t.Fatalf("unexpected dictionary doc mapping open=%q closed=%q", openKey, closedKey)
	}
}

func TestAdapterPreparedReaderCacheLifecycle(t *testing.T) {
	ctx := context.Background()
	store := NewStore(memory.New(), nil)
	const namespace = namespaces.Default

	seg1 := NewSegment("seg-reader-cache-1", time.Unix(1_700_000_111, 0))
	seg1.Fields["/status"] = FieldBlock{Postings: map[string][]string{
		"open": {"doc-open-1"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespace, seg1); err != nil {
		t.Fatalf("write segment1: %v", err)
	}
	manifest1 := NewManifest()
	manifest1.Seq = 1
	manifest1.UpdatedAt = seg1.CreatedAt
	manifest1.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{{
			ID:        seg1.ID,
			CreatedAt: seg1.CreatedAt,
			DocCount:  seg1.DocCount(),
		}},
	}
	if _, err := store.SaveManifest(ctx, namespace, manifest1, ""); err != nil {
		t.Fatalf("save manifest1: %v", err)
	}
	writeIndexedState(t, store.backend, namespace, "doc-open-1", map[string]any{"status": "open"})

	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	openReq := search.Request{
		Namespace: namespace,
		Selector: api.Selector{
			Eq: &api.Term{Field: "/status", Value: "open"},
		},
		Limit:  10,
		Engine: search.EngineIndex,
	}
	resp1, err := adapter.Query(ctx, openReq)
	if err != nil {
		t.Fatalf("query open first: %v", err)
	}
	if len(resp1.Keys) != 1 || resp1.Keys[0] != "doc-open-1" {
		t.Fatalf("unexpected first query keys %v", resp1.Keys)
	}
	if got := adapter.readers.len(); got != 1 {
		t.Fatalf("expected one prepared reader cache entry after first query, got %d", got)
	}
	resp2, err := adapter.Query(ctx, openReq)
	if err != nil {
		t.Fatalf("query open second: %v", err)
	}
	if len(resp2.Keys) != 1 || resp2.Keys[0] != "doc-open-1" {
		t.Fatalf("unexpected second query keys %v", resp2.Keys)
	}
	if got := adapter.readers.len(); got != 1 {
		t.Fatalf("expected prepared reader cache reuse, got %d entries", got)
	}

	seg2 := NewSegment("seg-reader-cache-2", time.Unix(1_700_000_222, 0))
	seg2.Fields["/status"] = FieldBlock{Postings: map[string][]string{
		"closed": {"doc-closed-1"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespace, seg2); err != nil {
		t.Fatalf("write segment2: %v", err)
	}
	manifest2 := NewManifest()
	manifest2.Seq = 2
	manifest2.UpdatedAt = seg2.CreatedAt
	manifest2.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{{
			ID:        seg2.ID,
			CreatedAt: seg2.CreatedAt,
			DocCount:  seg2.DocCount(),
		}},
	}
	if _, err := store.SaveManifest(ctx, namespace, manifest2, ""); err != nil {
		t.Fatalf("save manifest2: %v", err)
	}
	writeIndexedState(t, store.backend, namespace, "doc-closed-1", map[string]any{"status": "closed"})

	closedReq := search.Request{
		Namespace: namespace,
		Selector: api.Selector{
			Eq: &api.Term{Field: "/status", Value: "closed"},
		},
		Limit:  10,
		Engine: search.EngineIndex,
	}
	resp3, err := adapter.Query(ctx, closedReq)
	if err != nil {
		t.Fatalf("query closed after manifest update: %v", err)
	}
	if len(resp3.Keys) != 1 || resp3.Keys[0] != "doc-closed-1" {
		t.Fatalf("unexpected third query keys %v", resp3.Keys)
	}
	if got := adapter.readers.len(); got != 2 {
		t.Fatalf("expected second prepared reader cache entry after manifest change, got %d", got)
	}
}

func compiledTermDocKey(t *testing.T, reader *segmentReader, seg *compiledSegment, field string, term string) string {
	t.Helper()
	if seg == nil {
		t.Fatal("compiled segment is nil")
	}
	fieldID, ok := reader.fieldIDs[field]
	if !ok {
		t.Fatalf("missing field id for %q", field)
	}
	block, ok := seg.fieldsByID[fieldID]
	if !ok {
		t.Fatalf("missing compiled field %q", field)
	}
	docIDs := block.docIDsForTermInto(term, nil)
	if len(docIDs) == 0 {
		t.Fatalf("missing compiled term %q for field %q", term, field)
	}
	if len(docIDs) != 1 {
		t.Fatalf("expected exactly one doc id, got %d", len(docIDs))
	}
	key := reader.keyByDocID(docIDs[0])
	if key == "" {
		t.Fatalf("missing key for doc id %d", docIDs[0])
	}
	return key
}

type countingGetObjectBackend struct {
	storage.Backend
	mu   sync.Mutex
	gets map[string]int
}

func (b *countingGetObjectBackend) GetObject(ctx context.Context, namespace, key string) (storage.GetObjectResult, error) {
	b.mu.Lock()
	b.gets[key]++
	b.mu.Unlock()
	return b.Backend.GetObject(ctx, namespace, key)
}

func (b *countingGetObjectBackend) count(key string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.gets[key]
}
