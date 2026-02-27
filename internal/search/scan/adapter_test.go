package scan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/disk"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lql"
)

func TestScanAdapterMatchAll(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeMetaOnly(t, store, "default", "empty")
	writeState(t, store, "default", "orders/1", map[string]any{"status": "open"})
	writeState(t, store, "default", "orders/2", map[string]any{"status": "closed"})

	adapter, err := New(Config{Backend: store, MaxDocumentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	result, err := adapter.Query(ctx, search.Request{Namespace: "default", Limit: 10})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(result.Keys))
	}
	if result.Cursor != "" {
		t.Fatalf("expected empty cursor, got %q", result.Cursor)
	}
}

func TestScanAdapterSelector(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "orders/1", map[string]any{
		"status": "open",
		"amount": 150.5,
	})
	writeState(t, store, "default", "orders/2", map[string]any{
		"status": "closed",
		"amount": 99,
	})
	writeState(t, store, "default", "orders/3", map[string]any{
		"status": "open",
		"amount": 400,
	})

	sel, err := lql.ParseSelectorString(`
and.eq{field=/status,value=open},
and.range{field=/amount,gte=120,lt=200}`)
	if err != nil || sel.IsEmpty() {
		t.Fatalf("selector parse: %v", err)
	}

	adapter, err := New(Config{Backend: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	result, err := adapter.Query(ctx, search.Request{
		Namespace: "default",
		Selector:  sel,
		Limit:     5,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Keys) != 1 || result.Keys[0] != "orders/1" {
		t.Fatalf("unexpected keys %v", result.Keys)
	}
	if result.Cursor != "" {
		t.Fatalf("expected empty cursor, got %q", result.Cursor)
	}
}

func TestScanAdapterCachesCompiledSelectorPlans(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "orders/1", map[string]any{
		"status": "open",
		"amount": 150.5,
	})

	sel, err := lql.ParseSelectorString(`and.eq{field=/status,value=open}`)
	if err != nil || sel.IsEmpty() {
		t.Fatalf("selector parse: %v", err)
	}

	adapter, err := New(Config{Backend: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	for i := 0; i < 3; i++ {
		_, err := adapter.Query(ctx, search.Request{
			Namespace: "default",
			Selector:  sel,
			Limit:     5,
		})
		if err != nil {
			t.Fatalf("query run %d: %v", i, err)
		}
	}
	if got := adapter.plans.len(); got != 1 {
		t.Fatalf("expected one cached selector plan, got %d", got)
	}

	sink := &capturingDocumentSink{}
	_, err = adapter.QueryDocuments(ctx, search.Request{
		Namespace: "default",
		Selector:  sel,
		Limit:     5,
	}, sink)
	if err != nil {
		t.Fatalf("query documents: %v", err)
	}
	if got := adapter.plans.len(); got != 1 {
		t.Fatalf("expected cached selector plan reuse for query documents, got %d", got)
	}
}

func TestScanAdapterSelectorNumericMapKeyCompatibility(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "voucher/1", map[string]any{
		"voucher": map[string]any{
			"book": "GENERAL",
			"lines": map[string]any{
				"10": map[string]any{"amount": 3500},
			},
		},
	})

	sel, err := lql.ParseSelectorString(`
and.eq{field=/voucher/book,value=GENERAL},
and.range{field=/voucher/lines/10/amount,gte=3000}`)
	if err != nil || sel.IsEmpty() {
		t.Fatalf("selector parse: %v", err)
	}

	adapter, err := New(Config{Backend: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	result, err := adapter.Query(ctx, search.Request{
		Namespace: "default",
		Selector:  sel,
		Limit:     5,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Keys) != 1 || result.Keys[0] != "voucher/1" {
		t.Fatalf("unexpected keys %v", result.Keys)
	}
}

func TestScanAdapterCursor(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		writeState(t, store, "default", fmt.Sprintf("key-%02d", i), map[string]any{"status": "open"})
	}
	adapter, err := New(Config{Backend: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	first, err := adapter.Query(ctx, search.Request{Namespace: "default", Limit: 2})
	if err != nil {
		t.Fatalf("first query: %v", err)
	}
	if len(first.Keys) != 2 || first.Cursor == "" {
		t.Fatalf("unexpected first result %+v", first)
	}
	second, err := adapter.Query(ctx, search.Request{
		Namespace: "default",
		Limit:     2,
		Cursor:    first.Cursor,
	})
	if err != nil {
		t.Fatalf("second query: %v", err)
	}
	if len(second.Keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(second.Keys))
	}
}

func TestScanAdapterQueryDocumentsMatchAll(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "orders/1", map[string]any{"status": "open"})
	writeState(t, store, "default", "orders/2", map[string]any{"status": "closed"})

	adapter, err := New(Config{Backend: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	sink := &capturingDocumentSink{}
	result, err := adapter.QueryDocuments(ctx, search.Request{
		Namespace: "default",
		Limit:     10,
	}, sink)
	if err != nil {
		t.Fatalf("query documents: %v", err)
	}
	if len(result.Keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(result.Keys))
	}
	assertQueryMetadataInt(t, result.Metadata, search.MetadataQueryCandidatesSeen, 2)
	assertQueryMetadataInt(t, result.Metadata, search.MetadataQueryCandidatesMatched, 2)
	if len(sink.rows) != 2 {
		t.Fatalf("expected 2 streamed docs, got %d", len(sink.rows))
	}
	if sink.rows[0].key != "orders/1" || sink.rows[1].key != "orders/2" {
		t.Fatalf("unexpected streamed key order: %+v", sink.rows)
	}
	var doc map[string]any
	if err := json.Unmarshal(sink.rows[0].doc, &doc); err != nil {
		t.Fatalf("unmarshal doc: %v", err)
	}
	if doc["status"] != "open" {
		t.Fatalf("unexpected document payload: %+v", doc)
	}
}

func TestScanAdapterQueryDocumentsSelectorReadsStateOncePerCandidate(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "orders/1", map[string]any{"status": "open"})
	writeState(t, store, "default", "orders/2", map[string]any{"status": "closed"})

	backend := &countingReadBackend{Backend: store, reads: map[string]int{}}
	adapter, err := New(Config{Backend: backend})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	sel, err := lql.ParseSelectorString(`and.eq{field=/status,value=open}`)
	if err != nil || sel.IsEmpty() {
		t.Fatalf("selector parse: %v", err)
	}
	sink := &capturingDocumentSink{}
	result, err := adapter.QueryDocuments(ctx, search.Request{
		Namespace: "default",
		Selector:  sel,
		Limit:     10,
	}, sink)
	if err != nil {
		t.Fatalf("query documents: %v", err)
	}
	if len(result.Keys) != 1 || result.Keys[0] != "orders/1" {
		t.Fatalf("unexpected result keys: %+v", result.Keys)
	}
	assertQueryMetadataInt(t, result.Metadata, search.MetadataQueryCandidatesSeen, 2)
	assertQueryMetadataInt(t, result.Metadata, search.MetadataQueryCandidatesMatched, 1)
	if len(sink.rows) != 1 || sink.rows[0].key != "orders/1" {
		t.Fatalf("unexpected streamed rows: %+v", sink.rows)
	}
	if got := backend.reads["orders/1"]; got != 1 {
		t.Fatalf("expected one read for matched key, got %d", got)
	}
	if got := backend.reads["orders/2"]; got != 1 {
		t.Fatalf("expected one read for non-matched candidate, got %d", got)
	}
}

func TestScanAdapterQueryDocumentsNumericMapKeyCompatibility(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "voucher/1", map[string]any{
		"voucher": map[string]any{
			"book": "GENERAL",
			"lines": map[string]any{
				"10": map[string]any{"amount": 3500},
			},
		},
	})
	writeState(t, store, "default", "voucher/2", map[string]any{
		"voucher": map[string]any{
			"book": "GENERAL",
			"lines": map[string]any{
				"10": map[string]any{"amount": 1200},
			},
		},
	})

	sel, err := lql.ParseSelectorString(`
and.eq{field=/voucher/book,value=GENERAL},
and.range{field=/voucher/lines/10/amount,gte=3000}`)
	if err != nil || sel.IsEmpty() {
		t.Fatalf("selector parse: %v", err)
	}

	backend := &countingReadBackend{Backend: store, reads: map[string]int{}}
	adapter, err := New(Config{Backend: backend})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	sink := &capturingDocumentSink{}
	result, err := adapter.QueryDocuments(ctx, search.Request{
		Namespace: "default",
		Selector:  sel,
		Limit:     5,
	}, sink)
	if err != nil {
		t.Fatalf("query documents: %v", err)
	}
	if len(result.Keys) != 1 || result.Keys[0] != "voucher/1" {
		t.Fatalf("unexpected keys %v", result.Keys)
	}
	if len(sink.rows) != 1 || sink.rows[0].key != "voucher/1" {
		t.Fatalf("unexpected streamed rows: %+v", sink.rows)
	}
	assertQueryMetadataInt(t, result.Metadata, search.MetadataQueryCandidatesSeen, 2)
	assertQueryMetadataInt(t, result.Metadata, search.MetadataQueryCandidatesMatched, 1)
	if got := backend.reads["voucher/1"]; got != 1 {
		t.Fatalf("expected one read for matched key, got %d", got)
	}
	if got := backend.reads["voucher/2"]; got != 1 {
		t.Fatalf("expected one read for non-matched key, got %d", got)
	}
}

func TestScanAdapterQueryDocumentsLargeMatchReportsSpill(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "orders/large", map[string]any{
		"message": strings.Repeat("timeout payload ", 20_000),
	})

	adapter, err := New(Config{Backend: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	sel, err := lql.ParseSelectorString(`contains{field=/message,value=timeout}`)
	if err != nil || sel.IsEmpty() {
		t.Fatalf("selector parse: %v", err)
	}
	sink := &capturingDocumentSink{}
	result, err := adapter.QueryDocuments(ctx, search.Request{
		Namespace: "default",
		Selector:  sel,
		Limit:     10,
	}, sink)
	if err != nil {
		t.Fatalf("query documents: %v", err)
	}
	if len(result.Keys) != 1 || result.Keys[0] != "orders/large" {
		t.Fatalf("unexpected result keys: %+v", result.Keys)
	}
	if len(sink.rows) != 1 {
		t.Fatalf("expected one streamed row, got %d", len(sink.rows))
	}
	if spills := queryMetadataInt(t, result.Metadata, search.MetadataQuerySpillCount); spills < 1 {
		t.Fatalf("expected spill count >= 1, got %d", spills)
	}
	if spilledBytes := queryMetadataInt(t, result.Metadata, search.MetadataQuerySpillBytes); spilledBytes <= 0 {
		t.Fatalf("expected spill bytes > 0, got %d", spilledBytes)
	}
}

func TestScanAdapterQueryDocumentsLargeMatchesHeapBounded(t *testing.T) {
	ctx := context.Background()
	store, err := disk.New(disk.Config{Root: t.TempDir(), QueueWatch: false})
	if err != nil {
		t.Fatalf("new disk store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	const (
		docCount = 24
		docBytes = 2 << 20
	)
	message := strings.Repeat("timeout payload ", docBytes/len("timeout payload "))
	for i := 0; i < docCount; i++ {
		writeState(t, store, "default", fmt.Sprintf("orders/%03d", i), map[string]any{
			"message": message,
			"id":      i,
		})
	}

	adapter, err := New(Config{Backend: store, MaxDocumentBytes: 8 << 20})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	sel, err := lql.ParseSelectorString(`contains{field=/message,value=timeout}`)
	if err != nil || sel.IsEmpty() {
		t.Fatalf("selector parse: %v", err)
	}

	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	var peakDelta atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				var stats runtime.MemStats
				runtime.ReadMemStats(&stats)
				delta := int64(stats.HeapAlloc) - int64(baseline.HeapAlloc)
				if delta < 0 {
					delta = 0
				}
				for {
					current := peakDelta.Load()
					if delta <= current || peakDelta.CompareAndSwap(current, delta) {
						break
					}
				}
			}
		}
	}()

	sink := &drainingDocumentSink{
		chunkBytes: 32 << 10,
		perChunk:   100 * time.Microsecond,
	}
	result, err := adapter.QueryDocuments(ctx, search.Request{
		Namespace: "default",
		Selector:  sel,
		Limit:     docCount,
	}, sink)
	close(stop)
	<-done
	if err != nil {
		t.Fatalf("query documents: %v", err)
	}
	if len(result.Keys) != docCount {
		t.Fatalf("expected %d streamed keys, got %d", docCount, len(result.Keys))
	}
	if sink.docs != docCount {
		t.Fatalf("expected %d streamed docs, got %d", docCount, sink.docs)
	}
	assertQueryMetadataInt(t, result.Metadata, search.MetadataQueryCandidatesSeen, docCount)
	assertQueryMetadataInt(t, result.Metadata, search.MetadataQueryCandidatesMatched, docCount)
	captured := queryMetadataInt(t, result.Metadata, search.MetadataQueryBytesCaptured)
	if captured < int64(docCount*docBytes) {
		t.Fatalf("expected captured bytes >= %d, got %d", docCount*docBytes, captured)
	}
	if spills := queryMetadataInt(t, result.Metadata, search.MetadataQuerySpillCount); spills < docCount {
		t.Fatalf("expected spill count >= %d, got %d", docCount, spills)
	}

	const heapDeltaCap = int64(24 << 20) // 24 MiB additional heap while streaming ~48 MiB payload.
	if got := peakDelta.Load(); got > heapDeltaCap {
		t.Fatalf("query-doc heap delta too large: got=%d cap=%d", got, heapDeltaCap)
	}
}

func TestScanAdapterQueryDocumentsSingle50MiBMatchHeapBounded(t *testing.T) {
	ctx := context.Background()
	store, err := disk.New(disk.Config{Root: t.TempDir(), QueueWatch: false})
	if err != nil {
		t.Fatalf("new disk store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	const docBytes = 50 << 20
	writeState(t, store, "default", "orders/huge", map[string]any{
		"kind":    "alloc-gate",
		"message": strings.Repeat("timeout payload ", docBytes/len("timeout payload ")),
		"id":      1,
	})

	adapter, err := New(Config{Backend: store, MaxDocumentBytes: 80 << 20})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	sel, err := lql.ParseSelectorString(`eq{field=/kind,value=alloc-gate}`)
	if err != nil || sel.IsEmpty() {
		t.Fatalf("selector parse: %v", err)
	}
	req := search.Request{
		Namespace: "default",
		Selector:  sel,
		Limit:     1,
	}

	// Warm once so one-time setup cost does not pollute the gate.
	if _, err := adapter.QueryDocuments(ctx, req, &drainingDocumentSink{}); err != nil {
		t.Fatalf("warm query documents: %v", err)
	}

	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	var peakDelta atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				var stats runtime.MemStats
				runtime.ReadMemStats(&stats)
				delta := int64(stats.HeapAlloc) - int64(baseline.HeapAlloc)
				if delta < 0 {
					delta = 0
				}
				for {
					current := peakDelta.Load()
					if delta <= current || peakDelta.CompareAndSwap(current, delta) {
						break
					}
				}
			}
		}
	}()

	sink := &drainingDocumentSink{}
	result, err := adapter.QueryDocuments(ctx, req, sink)
	close(stop)
	<-done
	if err != nil {
		t.Fatalf("query documents: %v", err)
	}
	if len(result.Keys) != 1 || result.Keys[0] != "orders/huge" {
		t.Fatalf("unexpected result keys: %+v", result.Keys)
	}
	if sink.docs != 1 {
		t.Fatalf("expected one streamed doc, got %d", sink.docs)
	}
	assertQueryMetadataInt(t, result.Metadata, search.MetadataQueryCandidatesSeen, 1)
	assertQueryMetadataInt(t, result.Metadata, search.MetadataQueryCandidatesMatched, 1)
	if spills := queryMetadataInt(t, result.Metadata, search.MetadataQuerySpillCount); spills < 1 {
		t.Fatalf("expected spill count >= 1, got %d", spills)
	}

	const heapDeltaCap = int64(24 << 20) // 24 MiB cap while matching/streaming 50 MiB.
	if got := peakDelta.Load(); got > heapDeltaCap {
		t.Fatalf("query-doc heap delta too large: got=%d cap=%d payload=%d", got, heapDeltaCap, docBytes)
	}
}

func TestScanAdapterInvalidCursor(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "orders/1", map[string]any{"status": "open"})
	adapter, err := New(Config{Backend: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	_, err = adapter.Query(ctx, search.Request{
		Namespace: "default",
		Cursor:    "invalid",
	})
	if !errors.Is(err, search.ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor, got %v", err)
	}
}

func TestScanAdapterSkipsReservedAndHidden(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "lockd-diagnostics/123", map[string]any{"internal": true})
	writeState(t, store, "default", "orders/visible", map[string]any{"status": "open"})
	writeState(t, store, "default", "orders/hidden", map[string]any{"status": "open"})
	metaRes, err := store.LoadMeta(ctx, "default", "orders/hidden")
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	meta := metaRes.Meta
	meta.MarkQueryExcluded()
	if _, err := store.StoreMeta(ctx, "default", "orders/hidden", meta, metaRes.ETag); err != nil {
		t.Fatalf("store meta: %v", err)
	}
	adapter, err := New(Config{Backend: store})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	result, err := adapter.Query(ctx, search.Request{Namespace: "default", Limit: 10})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Keys) != 1 || result.Keys[0] != "orders/visible" {
		t.Fatalf("expected only visible order, got %+v", result.Keys)
	}
}

func TestScanAdapterCapabilities(t *testing.T) {
	adapter, err := New(Config{Backend: memory.New()})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	caps, err := adapter.Capabilities(context.Background(), namespaces.Default)
	if err != nil {
		t.Fatalf("capabilities: %v", err)
	}
	if !caps.Scan || caps.Index {
		t.Fatalf("expected scan-only capabilities, got %+v", caps)
	}
}

type metaErrorBackend struct {
	storage.Backend
	key string
	err error
}

func (b metaErrorBackend) LoadMeta(ctx context.Context, namespace, key string) (storage.LoadMetaResult, error) {
	if key == b.key {
		return storage.LoadMetaResult{}, b.err
	}
	return b.Backend.LoadMeta(ctx, namespace, key)
}

type stateErrorBackend struct {
	storage.Backend
	key string
	err error
}

func (b stateErrorBackend) ReadState(ctx context.Context, namespace, key string) (storage.ReadStateResult, error) {
	if key == b.key {
		return storage.ReadStateResult{}, b.err
	}
	return b.Backend.ReadState(ctx, namespace, key)
}

type countingReadBackend struct {
	storage.Backend
	reads map[string]int
}

func (b *countingReadBackend) ReadState(ctx context.Context, namespace, key string) (storage.ReadStateResult, error) {
	b.reads[key]++
	return b.Backend.ReadState(ctx, namespace, key)
}

type capturedDocument struct {
	namespace string
	key       string
	version   int64
	doc       []byte
}

type capturingDocumentSink struct {
	rows []capturedDocument
}

func (s *capturingDocumentSink) OnDocument(_ context.Context, namespace, key string, version int64, reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	s.rows = append(s.rows, capturedDocument{
		namespace: namespace,
		key:       key,
		version:   version,
		doc:       append([]byte(nil), data...),
	})
	return nil
}

type drainingDocumentSink struct {
	chunkBytes int
	perChunk   time.Duration
	docs       int
}

func (s *drainingDocumentSink) OnDocument(_ context.Context, _ string, _ string, _ int64, reader io.Reader) error {
	chunk := s.chunkBytes
	if chunk <= 0 {
		chunk = 32 << 10
	}
	buf := make([]byte, chunk)
	for {
		n, err := reader.Read(buf)
		if n > 0 && s.perChunk > 0 {
			time.Sleep(s.perChunk)
		}
		if err == io.EOF {
			s.docs++
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func TestScanAdapterReturnsNonTransientMetaError(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "orders/1", map[string]any{"status": "open"})
	writeState(t, store, "default", "orders/2", map[string]any{"status": "open"})

	adapter, err := New(Config{Backend: metaErrorBackend{
		Backend: store,
		key:     "orders/1",
		err:     errors.New("disk read failed"),
	}})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	_, err = adapter.Query(ctx, search.Request{Namespace: "default", Limit: 10})
	if err == nil {
		t.Fatal("expected query error")
	}
	if !strings.Contains(err.Error(), "scan: load meta orders/1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScanAdapterSkipsTransientMetaError(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "orders/1", map[string]any{"status": "open"})
	writeState(t, store, "default", "orders/2", map[string]any{"status": "open"})

	adapter, err := New(Config{Backend: metaErrorBackend{
		Backend: store,
		key:     "orders/1",
		err:     storage.NewTransientError(errors.New("temporary backend issue")),
	}})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	result, err := adapter.Query(ctx, search.Request{Namespace: "default", Limit: 10})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Keys) != 1 || result.Keys[0] != "orders/2" {
		t.Fatalf("expected only non-failing key, got %v", result.Keys)
	}
}

func TestScanAdapterReturnsNonTransientStateError(t *testing.T) {
	store := memory.New()
	ctx := context.Background()
	writeState(t, store, "default", "orders/1", map[string]any{"status": "open"})
	writeState(t, store, "default", "orders/2", map[string]any{"status": "open"})

	sel, err := lql.ParseSelectorString(`and.eq{field=/status,value=open}`)
	if err != nil || sel.IsEmpty() {
		t.Fatalf("selector parse: %v", err)
	}

	adapter, err := New(Config{Backend: stateErrorBackend{
		Backend: store,
		key:     "orders/1",
		err:     errors.New("state read failed"),
	}})
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	_, err = adapter.Query(ctx, search.Request{
		Namespace: "default",
		Selector:  sel,
		Limit:     10,
	})
	if err == nil {
		t.Fatal("expected query error")
	}
	if !strings.Contains(err.Error(), "scan: load state orders/1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func writeState(t *testing.T, store storage.Backend, namespace, key string, doc map[string]any) {
	t.Helper()
	payload, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	res, err := store.WriteState(context.Background(), namespace, key, bytes.NewReader(payload), storage.PutStateOptions{})
	if err != nil {
		t.Fatalf("write state: %v", err)
	}
	meta := &storage.Meta{
		Version:             1,
		PublishedVersion:    1,
		StateETag:           res.NewETag,
		UpdatedAtUnix:       time.Now().Unix(),
		StatePlaintextBytes: res.BytesWritten,
		StateDescriptor:     append([]byte(nil), res.Descriptor...),
	}
	if _, err := store.StoreMeta(context.Background(), namespace, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}
}

func writeMetaOnly(t *testing.T, store storage.Backend, namespace, key string) {
	t.Helper()
	meta := &storage.Meta{
		Version:          1,
		PublishedVersion: 1,
	}
	if _, err := store.StoreMeta(context.Background(), namespace, key, meta, ""); err != nil {
		t.Fatalf("store meta: %v", err)
	}
}

func assertQueryMetadataInt(t *testing.T, metadata map[string]string, key string, want int64) {
	t.Helper()
	got := queryMetadataInt(t, metadata, key)
	if got != want {
		t.Fatalf("unexpected metadata %q: got=%d want=%d", key, got, want)
	}
}

func queryMetadataInt(t *testing.T, metadata map[string]string, key string) int64 {
	t.Helper()
	raw := strings.TrimSpace(metadata[key])
	if raw == "" {
		t.Fatalf("missing metadata key %q", key)
	}
	got, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse metadata %q=%q: %v", key, raw, err)
	}
	return got
}
