package index

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"slices"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/namespaces"
)

func TestManifestRoundTrip(t *testing.T) {
	m := NewManifest()
	m.Seq = 7
	m.UpdatedAt = time.Unix(1_700_000_000, 0)
	m.Shards[0] = &Shard{ID: 0, Segments: []SegmentRef{{ID: "seg-1", CreatedAt: m.UpdatedAt, DocCount: 5}}}
	msg := m.ToProto()
	clone := ManifestFromProto(msg)
	if clone.Seq != m.Seq || len(clone.Shards) != len(m.Shards) || clone.Format != m.Format {
		t.Fatalf("manifest mismatch: %#v != %#v", clone, m)
	}
}

func TestSegmentRoundTrip(t *testing.T) {
	seg := NewSegment("seg-1", time.Unix(1_700_000_010, 0))
	seg.Fields["status"] = FieldBlock{Postings: map[string][]string{"open": {"a", "b"}}}
	seg.DocMeta["a"] = DocumentMetadata{
		StateETag:           "etag-a",
		StatePlaintextBytes: 42,
		PublishedVersion:    2,
		StateDescriptor:     []byte("desc-a"),
	}
	if err := seg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	msg := seg.ToProto()
	loaded := SegmentFromProto(msg)
	if loaded.ID != seg.ID || loaded.DocCount() != seg.DocCount() || len(loaded.DocMeta) != 1 {
		t.Fatalf("segment mismatch: %+v vs %+v", loaded, seg)
	}
}

func TestStoreManifestLifecycle(t *testing.T) {
	store := memory.New()
	idxStore := NewStore(store, nil)
	ctx := context.Background()
	manifest := NewManifest()
	manifest.Seq = 1
	manifest.UpdatedAt = time.Now().UTC()
	manifest.Shards[0] = &Shard{ID: 0}
	if _, err := idxStore.SaveManifest(ctx, "default", manifest, ""); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	manifestRes, err := idxStore.LoadManifest(ctx, "default")
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	loaded := manifestRes.Manifest
	if manifestRes.ETag == "" || loaded.Seq != manifest.Seq {
		t.Fatalf("unexpected manifest: %+v etag=%q", loaded, manifestRes.ETag)
	}
}

func TestStoreSegmentLifecycle(t *testing.T) {
	store := memory.New()
	idxStore := NewStore(store, nil)
	ctx := context.Background()
	seg := NewSegment("seg-seed", time.Now())
	seg.Fields["status"] = FieldBlock{Postings: map[string][]string{"open": {"doc-1"}}}
	if _, _, err := idxStore.WriteSegment(ctx, "default", seg); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	loaded, err := idxStore.LoadSegment(ctx, "default", seg.ID)
	if err != nil {
		t.Fatalf("load segment: %v", err)
	}
	if loaded.DocCount() != seg.DocCount() {
		t.Fatalf("doc count mismatch")
	}
	if err := idxStore.DeleteSegment(ctx, "default", seg.ID); err != nil {
		t.Fatalf("delete segment: %v", err)
	}
}

func TestVisibilityManifestLifecycle(t *testing.T) {
	store := memory.New()
	idxStore := NewStore(store, nil)
	ctx := context.Background()
	manifest := NewVisibilityManifest()
	manifest.Seq = 1
	manifest.UpdatedAt = time.Now().UTC()
	manifest.Segments = []VisibilitySegmentRef{{ID: "vis-1", CreatedAt: manifest.UpdatedAt, Entries: 2}}
	if _, err := idxStore.SaveVisibilityManifest(ctx, "default", manifest, ""); err != nil {
		t.Fatalf("save visibility manifest: %v", err)
	}
	manifestRes, err := idxStore.LoadVisibilityManifest(ctx, "default")
	if err != nil {
		t.Fatalf("load visibility manifest: %v", err)
	}
	loaded := manifestRes.Manifest
	if manifestRes.ETag == "" || loaded.Seq != manifest.Seq || len(loaded.Segments) != 1 {
		t.Fatalf("unexpected visibility manifest: %+v etag=%q", loaded, manifestRes.ETag)
	}
}

func TestVisibilitySegmentLifecycle(t *testing.T) {
	store := memory.New()
	idxStore := NewStore(store, nil)
	ctx := context.Background()
	segment := &VisibilitySegment{
		ID:        "vis-seed",
		CreatedAt: time.Now(),
		Entries: []VisibilityEntry{
			{Key: "alpha", Visible: true},
			{Key: "beta", Visible: false},
		},
	}
	if _, _, err := idxStore.WriteVisibilitySegment(ctx, "default", segment); err != nil {
		t.Fatalf("write visibility segment: %v", err)
	}
	loaded, err := idxStore.LoadVisibilitySegment(ctx, "default", segment.ID)
	if err != nil {
		t.Fatalf("load visibility segment: %v", err)
	}
	if loaded == nil || len(loaded.Entries) != 2 {
		t.Fatalf("unexpected visibility entries: %+v", loaded)
	}
	if err := idxStore.DeleteVisibilitySegment(ctx, "default", segment.ID); err != nil {
		t.Fatalf("delete visibility segment: %v", err)
	}
}

func TestIndexAdapterQuery(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := NewStore(mem, nil)
	segment := NewSegment("seg-query", time.Unix(1_700_000_020, 0))
	segment.Fields["status"] = FieldBlock{Postings: map[string][]string{
		"open":   {"orders-open-1", "orders-open-2", "orders-open-3"},
		"closed": {"orders-closed"},
	}}
	segment.Fields["device.telemetry.battery_mv"] = FieldBlock{Postings: map[string][]string{
		"4200": {"device-gw-42"},
		"3300": {"device-gw-7"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	manifest := NewManifest()
	manifest.Seq = 2
	manifest.UpdatedAt = segment.CreatedAt
	manifest.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{{
			ID:        segment.ID,
			CreatedAt: segment.CreatedAt,
			DocCount:  segment.DocCount(),
		}},
	}
	if _, err := store.SaveManifest(ctx, namespaces.Default, manifest, ""); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	seedMeta := func(key string, published bool, hidden bool) {
		meta := &storage.Meta{
			Version:          1,
			PublishedVersion: 0,
			StateETag:        "etag-" + key,
		}
		if published {
			meta.PublishedVersion = 1
		}
		if hidden {
			meta.SetQueryHidden(true)
		}
		if _, err := mem.StoreMeta(ctx, namespaces.Default, key, meta, ""); err != nil {
			t.Fatalf("store meta %s: %v", key, err)
		}
	}
	seedMeta("orders-open-1", true, false)
	seedMeta("orders-open-2", true, true) // hidden
	seedMeta("orders-open-3", true, false)
	seedMeta("orders-closed", true, false)
	seedMeta("device-gw-42", true, false)
	seedMeta("device-gw-7", false, false) // unpublished

	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}

	// Eq selector should skip hidden keys.
	resp, err := adapter.Query(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			Eq: &api.Term{Field: "status", Value: "open"},
		},
		Limit:  10,
		Engine: search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query eq: %v", err)
	}
	want := []string{"orders-open-1", "orders-open-3"}
	if !slices.Equal(resp.Keys, want) {
		t.Fatalf("unexpected eq keys %v", resp.Keys)
	}

	// Range selector should only include published matches.
	rangeResp, err := adapter.Query(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			Range: &api.RangeTerm{Field: "device.telemetry.battery_mv", GTE: floatPtr(4000)},
		},
		Limit:  5,
		Engine: search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query range: %v", err)
	}
	if len(rangeResp.Keys) != 1 || rangeResp.Keys[0] != "device-gw-42" {
		t.Fatalf("unexpected range keys %v", rangeResp.Keys)
	}

	// Pagination should advance past the last returned key even with hidden entries.
	page1, err := adapter.Query(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			Eq: &api.Term{Field: "status", Value: "open"},
		},
		Limit:  1,
		Engine: search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query page1: %v", err)
	}
	if len(page1.Keys) != 1 || page1.Keys[0] != "orders-open-1" || page1.Cursor == "" {
		t.Fatalf("unexpected page1 %+v", page1)
	}
	page2, err := adapter.Query(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			Eq: &api.Term{Field: "status", Value: "open"},
		},
		Limit:  2,
		Cursor: page1.Cursor,
		Engine: search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query page2: %v", err)
	}
	if len(page2.Keys) != 1 || page2.Keys[0] != "orders-open-3" || page2.Cursor != "" {
		t.Fatalf("unexpected page2 %+v", page2)
	}
}

func TestIndexAdapterQueryContainsSelectors(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := NewStore(mem, nil)
	segment := NewSegment("seg-contains", time.Unix(1_700_000_030, 0))
	segment.Fields["/message"] = FieldBlock{Postings: map[string][]string{
		"timeout while syncing": {"log-1"},
		"operation complete":    {"log-2"},
	}}
	segment.Fields["/service"] = FieldBlock{Postings: map[string][]string{
		"auth-api":    {"log-1"},
		"billing-api": {"log-2"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	manifest := NewManifest()
	manifest.Seq = 3
	manifest.UpdatedAt = segment.CreatedAt
	manifest.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{{
			ID:        segment.ID,
			CreatedAt: segment.CreatedAt,
			DocCount:  segment.DocCount(),
		}},
	}
	if _, err := store.SaveManifest(ctx, namespaces.Default, manifest, ""); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	for _, key := range []string{"log-1", "log-2"} {
		meta := &storage.Meta{
			Version:          1,
			PublishedVersion: 1,
			StateETag:        "etag-" + key,
		}
		if _, err := mem.StoreMeta(ctx, namespaces.Default, key, meta, ""); err != nil {
			t.Fatalf("store meta %s: %v", key, err)
		}
	}

	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}

	containsResp, err := adapter.Query(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			Contains: &api.Term{Field: "/message", Value: "timeout"},
		},
		Limit:  10,
		Engine: search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query contains: %v", err)
	}
	if !slices.Equal(containsResp.Keys, []string{"log-1"}) {
		t.Fatalf("unexpected contains keys %v", containsResp.Keys)
	}

	icontainsResp, err := adapter.Query(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			IContains: &api.Term{Field: "/message", Value: "TIMEOUT"},
		},
		Limit:  10,
		Engine: search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query icontains: %v", err)
	}
	if !slices.Equal(icontainsResp.Keys, []string{"log-1"}) {
		t.Fatalf("unexpected icontains keys %v", icontainsResp.Keys)
	}

	iprefixResp, err := adapter.Query(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			IPrefix: &api.Term{Field: "/service", Value: "AUTH"},
		},
		Limit:  10,
		Engine: search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query iprefix: %v", err)
	}
	if !slices.Equal(iprefixResp.Keys, []string{"log-1"}) {
		t.Fatalf("unexpected iprefix keys %v", iprefixResp.Keys)
	}
}

func TestIndexAdapterQueryDocumentsStreamsMatches(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := NewStore(mem, nil)
	segment := NewSegment("seg-docs", time.Unix(1_700_000_050, 0))
	segment.Fields["/status"] = FieldBlock{Postings: map[string][]string{
		"open":   {"orders-open-1"},
		"closed": {"orders-closed-1"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	manifest := NewManifest()
	manifest.Seq = 4
	manifest.UpdatedAt = segment.CreatedAt
	manifest.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{{
			ID:        segment.ID,
			CreatedAt: segment.CreatedAt,
			DocCount:  segment.DocCount(),
		}},
	}
	if _, err := store.SaveManifest(ctx, namespaces.Default, manifest, ""); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	writeIndexedState(t, mem, namespaces.Default, "orders-open-1", map[string]any{"status": "open"})
	writeIndexedState(t, mem, namespaces.Default, "orders-closed-1", map[string]any{"status": "closed"})

	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	sink := &capturingIndexDocumentSink{}
	result, err := adapter.QueryDocuments(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			Eq: &api.Term{Field: "/status", Value: "open"},
		},
		Limit:  10,
		Engine: search.EngineIndex,
	}, sink)
	if err != nil {
		t.Fatalf("query documents: %v", err)
	}
	if !slices.Equal(result.Keys, []string{"orders-open-1"}) {
		t.Fatalf("unexpected streamed keys %v", result.Keys)
	}
	if len(sink.rows) != 1 || sink.rows[0].key != "orders-open-1" {
		t.Fatalf("unexpected streamed rows %+v", sink.rows)
	}
	var doc map[string]any
	if err := json.Unmarshal(sink.rows[0].doc, &doc); err != nil {
		t.Fatalf("decode streamed doc: %v", err)
	}
	if doc["status"] != "open" {
		t.Fatalf("unexpected document payload %+v", doc)
	}
}

func TestIndexAdapterDocMeta(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := NewStore(mem, nil)
	segment := NewSegment("seg-meta", time.Unix(1_700_000_060, 0))
	segment.Fields["status"] = FieldBlock{Postings: map[string][]string{
		"open": {"doc-1"},
	}}
	segment.DocMeta["doc-1"] = DocumentMetadata{
		StateETag:           "etag-doc-1",
		StatePlaintextBytes: 128,
		PublishedVersion:    3,
		StateDescriptor:     []byte("desc-1"),
	}
	if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	manifest := NewManifest()
	manifest.Seq = 1
	manifest.UpdatedAt = segment.CreatedAt
	manifest.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{{
			ID:        segment.ID,
			CreatedAt: segment.CreatedAt,
			DocCount:  segment.DocCount(),
		}},
	}
	if _, err := store.SaveManifest(ctx, namespaces.Default, manifest, ""); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	meta := &storage.Meta{
		Version:          3,
		PublishedVersion: 3,
		StateETag:        "etag-doc-1",
	}
	if _, err := mem.StoreMeta(ctx, namespaces.Default, "doc-1", meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}
	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	resp, err := adapter.Query(ctx, search.Request{
		Namespace:      namespaces.Default,
		Selector:       api.Selector{Eq: &api.Term{Field: "status", Value: "open"}},
		Limit:          1,
		Engine:         search.EngineIndex,
		IncludeDocMeta: true,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(resp.DocMeta) != 1 {
		t.Fatalf("expected doc meta, got %+v", resp.DocMeta)
	}
	if meta := resp.DocMeta["doc-1"]; meta.StateETag != "etag-doc-1" || meta.PublishedVersion != 3 {
		t.Fatalf("unexpected doc meta: %+v", meta)
	}
}

func TestIndexAdapterDocMetaPrefersNewestSegment(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := NewStore(mem, nil)

	oldSeg := NewSegment("seg-old", time.Unix(1_700_000_000, 0))
	oldSeg.Fields["status"] = FieldBlock{Postings: map[string][]string{
		"open": {"doc-1"},
	}}
	oldSeg.DocMeta["doc-1"] = DocumentMetadata{
		StateETag:           "etag-old",
		StatePlaintextBytes: 64,
		PublishedVersion:    1,
	}
	if _, _, err := store.WriteSegment(ctx, namespaces.Default, oldSeg); err != nil {
		t.Fatalf("write old segment: %v", err)
	}

	newSeg := NewSegment("seg-new", time.Unix(1_700_000_120, 0))
	newSeg.Fields["status"] = FieldBlock{Postings: map[string][]string{
		"open": {"doc-1"},
	}}
	newSeg.DocMeta["doc-1"] = DocumentMetadata{
		StateETag:           "etag-new",
		StatePlaintextBytes: 128,
		PublishedVersion:    2,
	}
	if _, _, err := store.WriteSegment(ctx, namespaces.Default, newSeg); err != nil {
		t.Fatalf("write new segment: %v", err)
	}

	manifest := NewManifest()
	manifest.Seq = 2
	manifest.UpdatedAt = newSeg.CreatedAt
	manifest.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{
			{
				ID:        newSeg.ID,
				CreatedAt: newSeg.CreatedAt,
				DocCount:  newSeg.DocCount(),
			},
			{
				ID:        oldSeg.ID,
				CreatedAt: oldSeg.CreatedAt,
				DocCount:  oldSeg.DocCount(),
			},
		},
	}
	if _, err := store.SaveManifest(ctx, namespaces.Default, manifest, ""); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	meta := &storage.Meta{
		Version:          2,
		PublishedVersion: 2,
		StateETag:        "etag-new",
	}
	if _, err := mem.StoreMeta(ctx, namespaces.Default, "doc-1", meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}

	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	resp, err := adapter.Query(ctx, search.Request{
		Namespace:      namespaces.Default,
		Selector:       api.Selector{Eq: &api.Term{Field: "status", Value: "open"}},
		Limit:          1,
		Engine:         search.EngineIndex,
		IncludeDocMeta: true,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if meta := resp.DocMeta["doc-1"]; meta.StateETag != "etag-new" || meta.PublishedVersion != 2 {
		t.Fatalf("expected newest doc meta, got %+v", meta)
	}
}

func TestIndexAdapterRespectsVisibilityLedger(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	store := NewStore(mem, nil)
	segment := NewSegment("seg-vis", time.Unix(1_700_000_040, 0))
	segment.Fields["status"] = FieldBlock{Postings: map[string][]string{
		"open": {"orders-open-1", "orders-open-2", "orders-open-3"},
	}}
	if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	manifest := NewManifest()
	manifest.Seq = 1
	manifest.UpdatedAt = segment.CreatedAt
	manifest.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{{
			ID:        segment.ID,
			CreatedAt: segment.CreatedAt,
			DocCount:  segment.DocCount(),
		}},
	}
	if _, err := store.SaveManifest(ctx, namespaces.Default, manifest, ""); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	// All meta are published and not hidden.
	for _, key := range []string{"orders-open-1", "orders-open-2", "orders-open-3"} {
		meta := &storage.Meta{
			Version:          1,
			PublishedVersion: 1,
			StateETag:        "etag-" + key,
		}
		if _, err := mem.StoreMeta(ctx, namespaces.Default, key, meta, ""); err != nil {
			t.Fatalf("store meta %s: %v", key, err)
		}
	}

	// Visibility ledger hides orders-open-2.
	visSegment := &VisibilitySegment{
		ID:        "vis-1",
		CreatedAt: time.Now(),
		Entries: []VisibilityEntry{
			{Key: "orders-open-1", Visible: true},
			{Key: "orders-open-2", Visible: false},
			{Key: "orders-open-3", Visible: true},
		},
	}
	if _, _, err := store.WriteVisibilitySegment(ctx, namespaces.Default, visSegment); err != nil {
		t.Fatalf("write visibility segment: %v", err)
	}
	visManifest := NewVisibilityManifest()
	visManifest.Seq = 1
	visManifest.UpdatedAt = visSegment.CreatedAt
	visManifest.Segments = []VisibilitySegmentRef{{
		ID:        visSegment.ID,
		CreatedAt: visSegment.CreatedAt,
		Entries:   uint64(len(visSegment.Entries)),
	}}
	if _, err := store.SaveVisibilityManifest(ctx, namespaces.Default, visManifest, ""); err != nil {
		t.Fatalf("save visibility manifest: %v", err)
	}

	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	resp, err := adapter.Query(ctx, search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			Eq: &api.Term{Field: "status", Value: "open"},
		},
		Limit:  10,
		Engine: search.EngineIndex,
	})
	if err != nil {
		t.Fatalf("query eq: %v", err)
	}
	want := []string{"orders-open-1", "orders-open-3"}
	if !slices.Equal(resp.Keys, want) {
		t.Fatalf("unexpected eq keys %v", resp.Keys)
	}
}

func floatPtr(val float64) *float64 {
	return &val
}

type indexDocumentRow struct {
	namespace string
	key       string
	version   int64
	doc       []byte
}

type capturingIndexDocumentSink struct {
	rows []indexDocumentRow
}

func (s *capturingIndexDocumentSink) OnDocument(_ context.Context, namespace, key string, version int64, reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	s.rows = append(s.rows, indexDocumentRow{
		namespace: namespace,
		key:       key,
		version:   version,
		doc:       append([]byte(nil), data...),
	})
	return nil
}

func writeIndexedState(t *testing.T, store storage.Backend, namespace, key string, doc map[string]any) {
	t.Helper()
	payload, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	stateRes, err := store.WriteState(context.Background(), namespace, key, bytes.NewReader(payload), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write state: %v", err)
	}
	meta := &storage.Meta{
		Version:             1,
		PublishedVersion:    1,
		StateETag:           stateRes.NewETag,
		StatePlaintextBytes: stateRes.BytesWritten,
		StateDescriptor:     append([]byte(nil), stateRes.Descriptor...),
	}
	if _, err := store.StoreMeta(context.Background(), namespace, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}
}
