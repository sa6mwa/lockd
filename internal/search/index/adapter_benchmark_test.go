package index

import (
	"context"
	"fmt"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/namespaces"
)

var (
	benchResolvedFields []string
	benchDocIDSetSize   int
	benchResultSize     int
)

func BenchmarkSegmentReaderResolveWildcardFields(b *testing.B) {
	ctx := context.Background()
	reader, _ := buildSyntheticWildcardBenchIndex(ctx, b, 256, 32)

	// Warm path cache before measuring.
	warm, err := reader.resolveSelectorFields(ctx, "/logs/*/message")
	if err != nil {
		b.Fatalf("resolve selector fields: %v", err)
	}
	if len(warm) == 0 {
		b.Fatalf("expected resolved fields")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fields, err := reader.resolveSelectorFields(ctx, "/logs/*/message")
		if err != nil {
			b.Fatalf("resolve selector fields: %v", err)
		}
		benchResolvedFields = fields
	}
}

func BenchmarkSegmentReaderResolveRecursiveFields(b *testing.B) {
	ctx := context.Background()
	reader, _ := buildSyntheticWildcardBenchIndex(ctx, b, 256, 32)

	warm, err := reader.resolveSelectorFields(ctx, "/logs/.../message")
	if err != nil {
		b.Fatalf("resolve selector fields: %v", err)
	}
	if len(warm) == 0 {
		b.Fatalf("expected resolved fields")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fields, err := reader.resolveSelectorFields(ctx, "/logs/.../message")
		if err != nil {
			b.Fatalf("resolve selector fields: %v", err)
		}
		benchResolvedFields = fields
	}
}

func BenchmarkSegmentReaderWildcardContains(b *testing.B) {
	ctx := context.Background()
	reader, _ := buildSyntheticWildcardBenchIndex(ctx, b, 256, 32)
	term := &api.Term{Field: "/logs/*/message", Value: "timeout"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set, err := reader.docIDsForContains(ctx, term, true)
		if err != nil {
			b.Fatalf("docIDsForContains: %v", err)
		}
		benchDocIDSetSize = len(set)
	}
}

func BenchmarkAdapterQueryWildcardContains(b *testing.B) {
	ctx := context.Background()
	_, adapter := buildSyntheticWildcardBenchIndex(ctx, b, 128, 32)
	req := search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			IContains: &api.Term{Field: "/logs/*/message", Value: "TIMEOUT"},
		},
		Limit:  10_000,
		Engine: search.EngineIndex,
	}
	warmResp, warmErr := adapter.Query(ctx, req)
	if warmErr != nil {
		b.Fatalf("adapter warm query: %v", warmErr)
	}
	benchResultSize = len(warmResp.Keys)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := adapter.Query(ctx, req)
		if err != nil {
			b.Fatalf("adapter query: %v", err)
		}
		benchResultSize = len(resp.Keys)
	}
}

func BenchmarkAdapterQueryRecursiveDeepLowSelectivity(b *testing.B) {
	ctx := context.Background()
	_, adapter := buildSyntheticHierarchyBenchIndex(ctx, b, 4, 7, 2, 20)
	req := search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			IContains: &api.Term{Field: "/tree/.../message", Value: "TIMEOUT"},
		},
		Limit:  20_000,
		Engine: search.EngineIndex,
	}
	warmResp, warmErr := adapter.Query(ctx, req)
	if warmErr != nil {
		b.Fatalf("adapter warm recursive low query: %v", warmErr)
	}
	benchResultSize = len(warmResp.Keys)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := adapter.Query(ctx, req)
		if err != nil {
			b.Fatalf("adapter recursive low query: %v", err)
		}
		benchResultSize = len(resp.Keys)
	}
}

func BenchmarkAdapterQueryRecursiveDeepHighSelectivity(b *testing.B) {
	ctx := context.Background()
	_, adapter := buildSyntheticHierarchyBenchIndex(ctx, b, 4, 7, 2, 2)
	req := search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			IContains: &api.Term{Field: "/tree/.../message", Value: "TIMEOUT"},
		},
		Limit:  20_000,
		Engine: search.EngineIndex,
	}
	warmResp, warmErr := adapter.Query(ctx, req)
	if warmErr != nil {
		b.Fatalf("adapter warm recursive high query: %v", warmErr)
	}
	benchResultSize = len(warmResp.Keys)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := adapter.Query(ctx, req)
		if err != nil {
			b.Fatalf("adapter recursive high query: %v", err)
		}
		benchResultSize = len(resp.Keys)
	}
}

func BenchmarkAdapterQueryWildcardWideLowSelectivity(b *testing.B) {
	ctx := context.Background()
	_, adapter := buildSyntheticHierarchyBenchIndex(ctx, b, 3, 12, 2, 24)
	req := search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			IContains: &api.Term{Field: "/tree/*/*/*/message", Value: "TIMEOUT"},
		},
		Limit:  20_000,
		Engine: search.EngineIndex,
	}
	warmResp, warmErr := adapter.Query(ctx, req)
	if warmErr != nil {
		b.Fatalf("adapter warm wildcard low query: %v", warmErr)
	}
	benchResultSize = len(warmResp.Keys)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := adapter.Query(ctx, req)
		if err != nil {
			b.Fatalf("adapter wildcard low query: %v", err)
		}
		benchResultSize = len(resp.Keys)
	}
}

func BenchmarkAdapterQueryWildcardWideHighSelectivity(b *testing.B) {
	ctx := context.Background()
	_, adapter := buildSyntheticHierarchyBenchIndex(ctx, b, 3, 12, 2, 2)
	req := search.Request{
		Namespace: namespaces.Default,
		Selector: api.Selector{
			IContains: &api.Term{Field: "/tree/*/*/*/message", Value: "TIMEOUT"},
		},
		Limit:  20_000,
		Engine: search.EngineIndex,
	}
	warmResp, warmErr := adapter.Query(ctx, req)
	if warmErr != nil {
		b.Fatalf("adapter warm wildcard high query: %v", warmErr)
	}
	benchResultSize = len(warmResp.Keys)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := adapter.Query(ctx, req)
		if err != nil {
			b.Fatalf("adapter wildcard high query: %v", err)
		}
		benchResultSize = len(resp.Keys)
	}
}

func buildSyntheticWildcardBenchIndex(
	ctx context.Context,
	b testing.TB,
	fieldCount int,
	docsPerField int,
) (*segmentReader, *Adapter) {
	b.Helper()

	mem := memory.New()
	store := NewStore(mem, nil)
	segment := NewSegment("bench-seg", time.Unix(1_700_000_200, 0))

	for i := 0; i < fieldCount; i++ {
		field := fmt.Sprintf("/logs/%03d/message", i)
		timeoutTerm := "timeout while syncing"
		okTerm := "operation complete"
		block := FieldBlock{Postings: map[string][]string{
			timeoutTerm: make([]string, 0, docsPerField/2+1),
			okTerm:      make([]string, 0, docsPerField/2+1),
		}}
		for j := 0; j < docsPerField; j++ {
			key := fmt.Sprintf("doc-%03d-%03d", i, j)
			if j%2 == 0 {
				block.Postings[timeoutTerm] = append(block.Postings[timeoutTerm], key)
			} else {
				block.Postings[okTerm] = append(block.Postings[okTerm], key)
			}
			meta := &storage.Meta{
				Version:          1,
				PublishedVersion: 1,
				StateETag:        "etag-" + key,
			}
			if _, err := mem.StoreMeta(ctx, namespaces.Default, key, meta, ""); err != nil {
				b.Fatalf("store meta %s: %v", key, err)
			}
		}
		segment.Fields[field] = block

		gramField := containsGramField(field)
		gramPostings := make(map[string][]string)
		timeoutKeys := block.Postings[timeoutTerm]
		for _, gram := range normalizedTrigrams("timeout") {
			gramPostings[gram] = append([]string(nil), timeoutKeys...)
		}
		segment.Fields[gramField] = FieldBlock{Postings: gramPostings}
	}

	if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
		b.Fatalf("write segment: %v", err)
	}
	manifest := NewManifest()
	manifest.Seq = 1
	manifest.UpdatedAt = segment.CreatedAt
	manifest.Format = IndexFormatVersionV4
	manifest.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{{
			ID:        segment.ID,
			CreatedAt: segment.CreatedAt,
			DocCount:  segment.DocCount(),
		}},
	}
	if _, err := store.SaveManifest(ctx, namespaces.Default, manifest, ""); err != nil {
		b.Fatalf("save manifest: %v", err)
	}

	reader := newSegmentReader(namespaces.Default, manifest, store, nil)
	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		b.Fatalf("new adapter: %v", err)
	}
	return reader, adapter
}

func buildSyntheticHierarchyBenchIndex(
	ctx context.Context,
	b testing.TB,
	depth int,
	branch int,
	docsPerLeaf int,
	timeoutModulo int,
) (*segmentReader, *Adapter) {
	b.Helper()
	if depth <= 0 {
		depth = 3
	}
	if branch <= 0 {
		branch = 8
	}
	if docsPerLeaf <= 0 {
		docsPerLeaf = 2
	}
	if timeoutModulo <= 0 {
		timeoutModulo = 2
	}

	mem := memory.New()
	store := NewStore(mem, nil)
	segment := NewSegment("bench-hierarchy-seg", time.Unix(1_700_000_230, 0))

	var path []int
	var addLeaf func(level int)
	docSeq := 0
	addLeaf = func(level int) {
		if level == depth {
			field := "/tree"
			for _, part := range path {
				field += fmt.Sprintf("/n%02d", part)
			}
			field += "/message"

			timeoutTerm := "timeout while syncing"
			okTerm := "operation complete"
			timeoutKeys := make([]string, 0, docsPerLeaf)
			okKeys := make([]string, 0, docsPerLeaf)
			for j := 0; j < docsPerLeaf; j++ {
				key := fmt.Sprintf("hdoc-%06d", docSeq)
				docSeq++
				if docSeq%timeoutModulo == 0 {
					timeoutKeys = append(timeoutKeys, key)
				} else {
					okKeys = append(okKeys, key)
				}
				meta := &storage.Meta{
					Version:          1,
					PublishedVersion: 1,
					StateETag:        "etag-" + key,
				}
				if _, err := mem.StoreMeta(ctx, namespaces.Default, key, meta, ""); err != nil {
					b.Fatalf("store meta %s: %v", key, err)
				}
			}
			postings := make(map[string][]string, 2)
			if len(timeoutKeys) > 0 {
				postings[timeoutTerm] = timeoutKeys
			}
			if len(okKeys) > 0 {
				postings[okTerm] = okKeys
			}
			block := FieldBlock{Postings: postings}
			segment.Fields[field] = block

			gramField := containsGramField(field)
			gramPostings := make(map[string][]string)
			for _, gram := range normalizedTrigrams("timeout") {
				gramPostings[gram] = append([]string(nil), timeoutKeys...)
			}
			if len(timeoutKeys) == 0 {
				return
			}
			segment.Fields[gramField] = FieldBlock{Postings: gramPostings}
			return
		}
		for i := 0; i < branch; i++ {
			path = append(path, i)
			addLeaf(level + 1)
			path = path[:len(path)-1]
		}
	}
	addLeaf(0)

	if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
		b.Fatalf("write hierarchy segment: %v", err)
	}
	manifest := NewManifest()
	manifest.Seq = 1
	manifest.UpdatedAt = segment.CreatedAt
	manifest.Format = IndexFormatVersionV4
	manifest.Shards[0] = &Shard{
		ID: 0,
		Segments: []SegmentRef{{
			ID:        segment.ID,
			CreatedAt: segment.CreatedAt,
			DocCount:  segment.DocCount(),
		}},
	}
	if _, err := store.SaveManifest(ctx, namespaces.Default, manifest, ""); err != nil {
		b.Fatalf("save hierarchy manifest: %v", err)
	}

	reader := newSegmentReader(namespaces.Default, manifest, store, nil)
	adapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		b.Fatalf("new adapter: %v", err)
	}
	return reader, adapter
}
