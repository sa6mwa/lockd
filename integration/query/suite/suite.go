package querysuite

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/xid"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	querydata "pkt.systems/lockd/integration/query/querydata"
	"pkt.systems/lockd/internal/core"
	indexer "pkt.systems/lockd/internal/search/index"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lql"
)

// ServerFactory starts a backend-specific lockd server for the query suite.
type ServerFactory func(testing.TB) *lockd.TestServer

type runConfig struct {
	datasetProfile querydata.DatasetProfile
}

// Option customizes the behavior of suite runners.
type Option func(*runConfig)

// WithDatasetProfile sets the dataset profile used for domain seeding.
func WithDatasetProfile(profile querydata.DatasetProfile) Option {
	return func(cfg *runConfig) {
		cfg.datasetProfile = profile
	}
}

// WithReducedDataset applies a lighter dataset profile (useful for slow backends).
func WithReducedDataset() Option {
	return WithDatasetProfile(querydata.DatasetReduced)
}

// WithExtendedDataset enables the superset dataset profile (used for stress
// scenarios or future selectors that need more permutations).
func WithExtendedDataset() Option {
	return WithDatasetProfile(querydata.DatasetExtended)
}

// RunSelectors seeds simple documents and ensures selector matching behaves consistently.
func RunSelectors(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 15*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		querydata.SeedState(ctx, t, ts.Client, "", "orders-open-1", map[string]any{"status": "open", "amount": 150.0, "region": "us"})
		querydata.SeedState(ctx, t, ts.Client, "", "orders-open-2", map[string]any{"status": "open", "amount": 80.0})
		querydata.SeedState(ctx, t, ts.Client, "", "orders-closed", map[string]any{"status": "closed", "amount": 200.0})
		flushNamespaces(ctx, t, ts.Client, namespaces.Default)

		selector, err := lql.ParseSelectorString(`
and.eq{field=/status,value=open},
and.range{field=/amount,gte=100,lt=200}`)
		if err != nil || selector.IsEmpty() {
			t.Fatalf("parse selector: %v", err)
		}

		resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
			Namespace: namespaces.Default,
			Selector:  selector,
			Limit:     10,
		})
		if len(resp.Keys) != 1 || resp.Keys[0] != "orders-open-1" {
			t.Fatalf("expected only orders-open-1, got %+v", resp.Keys)
		}
		if resp.Cursor != "" {
			t.Fatalf("expected empty cursor for single page, got %q", resp.Cursor)
		}
	})
}

// RunPagination verifies cursor pagination works across all backends.
func RunPagination(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 15*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		for i := 0; i < 4; i++ {
			key := fmt.Sprintf("batch-%02d", i)
			querydata.SeedState(ctx, t, ts.Client, querydata.PaginationNamespace, key, map[string]any{"status": "open", "index": i})
		}
		flushNamespaces(ctx, t, ts.Client, querydata.PaginationNamespace)
		selector, err := lql.ParseSelectorString(`eq{field=/status,value=open}`)
		if err != nil || selector.IsEmpty() {
			t.Fatalf("parse selector: %v", err)
		}

		var (
			cursor    string
			collected []string
		)
		seen := make(map[string]struct{})
		produced := 0
		for {
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: querydata.PaginationNamespace,
				Selector:  selector,
				Limit:     2,
				Cursor:    cursor,
			})
			for _, key := range resp.Keys {
				if _, ok := seen[key]; ok {
					t.Fatalf("duplicate key %s encountered during pagination", key)
				}
				seen[key] = struct{}{}
				collected = append(collected, key)
				produced++
			}
			if resp.Cursor == "" {
				break
			}
			if produced == 0 {
				t.Fatalf("pagination returned a cursor (%q) but no keys", resp.Cursor)
			}
			produced = 0
			cursor = resp.Cursor
			if len(collected) > 10 {
				t.Fatalf("pagination runaway: %+v", collected)
			}
		}
		expected := []string{"batch-00", "batch-01", "batch-02", "batch-03"}
		querydata.ExpectKeySet(t, collected, expected)
	})
}

// RunNamespaceIsolation ensures results stay scoped to the requested namespace.
func RunNamespaceIsolation(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 15*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		querydata.SeedState(ctx, t, ts.Client, "alpha", "job-1", map[string]any{"status": "open"})
		querydata.SeedState(ctx, t, ts.Client, "alpha", "job-2", map[string]any{"status": "open"})
		querydata.SeedState(ctx, t, ts.Client, "beta", "job-3", map[string]any{"status": "open"})
		flushNamespaces(ctx, t, ts.Client, "alpha", "beta")

		sel, err := lql.ParseSelectorString(`eq{field=/status,value=open}`)
		if err != nil || sel.IsEmpty() {
			t.Fatalf("parse selector: %v", err)
		}

		alpha := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
			Namespace: "alpha",
			Selector:  sel,
			Limit:     10,
		})
		querydata.ExpectKeySet(t, alpha.Keys, []string{"job-1", "job-2"})

		beta := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
			Namespace: "beta",
			Selector:  sel,
			Limit:     10,
		})
		querydata.ExpectKeySet(t, beta.Keys, []string{"job-3"})
	})
}

// RunPublicRead checks that public queries can be fetched via Client.Get without leases.
func RunPublicRead(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 15*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		key := "report-" + time.Now().Format("150405")
		querydata.SeedState(ctx, t, ts.Client, "", key, map[string]any{"status": "published", "payload": "value"})
		flushNamespaces(ctx, t, ts.Client, namespaces.Default)

		sel, err := lql.ParseSelectorString(`eq{field=/status,value=published}`)
		if err != nil || sel.IsEmpty() {
			t.Fatalf("parse selector: %v", err)
		}

		qr := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
			Namespace: namespaces.Default,
			Selector:  sel,
			Limit:     1,
		})
		if len(qr.Keys) != 1 || qr.Keys[0] != key {
			t.Fatalf("expected %s in query response, got %+v", key, qr.Keys)
		}

		stateResp, err := ts.Client.Get(ctx, key, lockdclient.WithGetNamespace(namespaces.Default))
		if err != nil {
			t.Fatalf("public get: %v", err)
		}
		if stateResp == nil || !stateResp.HasState {
			t.Fatalf("missing public payload")
		}
		defer stateResp.Close()
		reader := stateResp.Reader()
		if reader == nil {
			t.Fatalf("missing public reader")
		}
		var payload map[string]any
		if err := json.NewDecoder(reader).Decode(&payload); err != nil {
			t.Fatalf("decode public payload: %v", err)
		}
		if payload["payload"] != "value" {
			t.Fatalf("unexpected payload: %+v", payload)
		}
	})
}

// RunDocumentStreaming ensures /v1/query can stream documents (return=documents) across adapters.
func RunDocumentStreaming(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 15*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		querydata.SeedState(ctx, t, ts.Client, namespaces.Default, "doc-stream-1", map[string]any{"status": "staged", "region": "emea"})
		querydata.SeedState(ctx, t, ts.Client, namespaces.Default, "doc-stream-2", map[string]any{"status": "draft", "region": "amer"})
		flushNamespaces(ctx, t, ts.Client, namespaces.Default)

		sel, err := lql.ParseSelectorString(`/status="staged"`)
		if err != nil || sel.IsEmpty() {
			t.Fatalf("parse selector: %v", err)
		}

		rows := doQueryDocuments(t, httpClient, ts.URL(), api.QueryRequest{Namespace: namespaces.Default, Selector: sel})
		if len(rows) != 1 {
			t.Fatalf("expected 1 staged document, got %d", len(rows))
		}
		row := rows[0]
		if row.Key != "doc-stream-1" {
			t.Fatalf("unexpected key %s", row.Key)
		}
		var doc map[string]any
		if err := json.Unmarshal(row.Doc, &doc); err != nil {
			t.Fatalf("decode streamed doc: %v", err)
		}
		if doc["region"] != "emea" {
			t.Fatalf("unexpected document: %+v", doc)
		}
	})
}

// RunIndexRebuildUpgrade validates rebuild/cleanup behavior when a namespace
// upgrades from legacy v4 segments to v5 while preserving query correctness.
func RunIndexRebuildUpgrade(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 2*time.Minute, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		namespace := "q-rebuild-" + xid.New().String()
		keyA := "rebuild-doc-a"
		keyB := "rebuild-doc-b"
		querydata.SeedState(ctx, t, ts.Client, namespace, keyA, map[string]any{"status": "open", "kind": "ticket"})
		querydata.SeedState(ctx, t, ts.Client, namespace, keyB, map[string]any{"status": "open", "kind": "ticket"})
		flushNamespaces(ctx, t, ts.Client, namespace)

		crypto := rebuildCryptoFromServerConfig(t, ts.Config)
		idxStore := indexer.NewStore(ts.Backend(), crypto)
		idxManager := indexer.NewManager(idxStore, indexer.WriterOptions{
			FlushDocs:     1000,
			FlushInterval: time.Minute,
		})
		defer idxManager.Close(context.Background())

		legacy := indexer.NewSegment("legacy-v4-"+xid.New().String(), time.Now().UTC().Add(-time.Minute))
		legacy.Format = indexer.IndexFormatVersionV4
		legacy.Fields["/status"] = indexer.FieldBlock{Postings: map[string][]string{
			"open": {keyA},
		}}
		if _, _, err := idxStore.WriteSegment(ctx, namespace, legacy); err != nil {
			t.Fatalf("write legacy segment: %v", err)
		}
		manifest := indexer.NewManifest()
		manifest.Format = indexer.IndexFormatVersionV5
		manifest.Seq = 3
		manifest.UpdatedAt = legacy.CreatedAt
		manifest.Shards[0] = &indexer.Shard{
			ID: 0,
			Segments: []indexer.SegmentRef{
				{ID: legacy.ID, CreatedAt: legacy.CreatedAt, DocCount: legacy.DocCount()},
			},
		}
		if _, err := idxStore.SaveManifest(ctx, namespace, manifest, ""); err != nil {
			t.Fatalf("save seed manifest: %v", err)
		}

		defaultCfg := namespaces.DefaultConfig()
		if provider, ok := ts.Backend().(namespaces.ConfigProvider); ok && provider != nil {
			defaultCfg = provider.DefaultNamespaceConfig()
		}
		svc := core.New(core.Config{
			Store:                  ts.Backend(),
			Crypto:                 crypto,
			IndexManager:           idxManager,
			DefaultNamespace:       ts.Config.DefaultNamespace,
			DefaultNamespaceConfig: defaultCfg,
		})
		res, err := svc.RebuildIndex(ctx, namespace, core.IndexRebuildOptions{
			Mode:         "wait",
			Cleanup:      true,
			CleanupDelay: 0,
		})
		if err != nil {
			t.Fatalf("rebuild index: %v", err)
		}
		if !res.Rebuilt {
			t.Fatalf("expected rebuilt=true, got %+v", res)
		}

		manifestRes, err := idxStore.LoadManifest(ctx, namespace)
		if err != nil {
			t.Fatalf("load manifest: %v", err)
		}
		if manifestRes.Manifest == nil || manifestRes.Manifest.Format != indexer.IndexFormatVersionV5 {
			t.Fatalf("expected manifest format v5, got %+v", manifestRes.Manifest)
		}
		shard := manifestRes.Manifest.Shards[0]
		if shard == nil || len(shard.Segments) == 0 {
			t.Fatalf("expected rebuilt segments")
		}
		for _, ref := range shard.Segments {
			if ref.ID == legacy.ID {
				t.Fatalf("legacy v4 segment should be removed from manifest")
			}
			seg, err := idxStore.LoadSegment(ctx, namespace, ref.ID)
			if err != nil {
				t.Fatalf("load segment %s: %v", ref.ID, err)
			}
			if seg.Format != indexer.IndexFormatVersionV5 {
				t.Fatalf("expected segment format v5, got %d", seg.Format)
			}
		}
		if _, err := idxStore.LoadSegment(ctx, namespace, legacy.ID); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("expected legacy segment object deleted, got err=%v", err)
		}

		sel, err := lql.ParseSelectorString(`eq{field=/status,value=open}`)
		if err != nil || sel.IsEmpty() {
			t.Fatalf("parse selector: %v", err)
		}
		resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
			Namespace: namespace,
			Selector:  sel,
			Limit:     10,
		})
		querydata.ExpectKeySet(t, resp.Keys, []string{keyA, keyB})
	})
}

// RunLargeNamespaceLowMatchKeys validates key-return queries stay correct for
// large namespaces when selectors match very few documents.
func RunLargeNamespaceLowMatchKeys(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 3*time.Minute, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		namespace := "q-lowmatch-keys-" + xid.New().String()
		const targetTerm = "needle-token-42"
		totalDocs := largeNamespaceLowMatchTotalDocs(ts.Config.Store)
		targetKey := ""
		for i := 0; i < totalDocs; i++ {
			key := fmt.Sprintf("lowmatch-key-%03d", i)
			msgA := fmt.Sprintf("normal telemetry %03d", i)
			if i == totalDocs-7 {
				msgA = "contains needle-token-42 for exact low match"
				targetKey = key
			}
			querydata.SeedState(ctx, t, ts.Client, namespace, key, map[string]any{
				"kind": "work",
				"details": map[string]any{
					"a": map[string]any{"message": msgA},
					"b": map[string]any{"message": fmt.Sprintf("background %03d", i)},
				},
			})
		}
		flushNamespaces(ctx, t, ts.Client, namespace)

		sel, err := lql.ParseSelectorString(`
and.eq{field=/kind,value=work},
and.contains{field=/details/*/message,value=` + targetTerm + `}`)
		if err != nil || sel.IsEmpty() {
			t.Fatalf("parse low-match selector: %v", err)
		}
		resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
			Namespace: namespace,
			Selector:  sel,
			Limit:     16,
		})
		if len(resp.Keys) != 1 || resp.Keys[0] != targetKey {
			t.Fatalf("expected single key %q, got %+v", targetKey, resp.Keys)
		}
		if resp.Cursor != "" {
			t.Fatalf("expected empty cursor for low-match single result, got %q", resp.Cursor)
		}
	})
}

// RunLargeNamespaceLowMatchDocuments validates document-return streaming for
// large namespaces when selectors match very few documents.
func RunLargeNamespaceLowMatchDocuments(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 3*time.Minute, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		namespace := "q-lowmatch-docs-" + xid.New().String()
		const targetTerm = "needle-token-84"
		totalDocs := largeNamespaceLowMatchTotalDocs(ts.Config.Store)
		targetKey := ""
		for i := 0; i < totalDocs; i++ {
			key := fmt.Sprintf("lowmatch-doc-%03d", i)
			msgA := fmt.Sprintf("normal payload %03d", i)
			if i == totalDocs-11 {
				msgA = "contains needle-token-84 for document streaming"
				targetKey = key
			}
			querydata.SeedState(ctx, t, ts.Client, namespace, key, map[string]any{
				"kind": "work",
				"details": map[string]any{
					"a": map[string]any{"message": msgA},
					"b": map[string]any{"message": fmt.Sprintf("secondary %03d", i)},
				},
			})
		}
		flushNamespaces(ctx, t, ts.Client, namespace)

		sel, err := lql.ParseSelectorString(`
and.eq{field=/kind,value=work},
and.contains{field=/details/*/message,value=` + targetTerm + `}`)
		if err != nil || sel.IsEmpty() {
			t.Fatalf("parse low-match document selector: %v", err)
		}
		rows := doQueryDocuments(t, httpClient, ts.URL(), api.QueryRequest{
			Namespace: namespace,
			Selector:  sel,
			Limit:     16,
		})
		if len(rows) != 1 {
			t.Fatalf("expected one streamed document, got %d", len(rows))
		}
		if rows[0].Namespace != namespace {
			t.Fatalf("expected namespace %q, got %q", namespace, rows[0].Namespace)
		}
		if rows[0].Key != targetKey {
			t.Fatalf("expected key %q, got %q", targetKey, rows[0].Key)
		}
		var doc map[string]any
		if err := json.Unmarshal(rows[0].Doc, &doc); err != nil {
			t.Fatalf("decode streamed doc: %v", err)
		}
		if kind, _ := doc["kind"].(string); kind != "work" {
			t.Fatalf("unexpected streamed doc payload: %+v", doc)
		}
	})
}

// RunDocumentStreamingFlowControl validates cancellation and slow-reader
// behavior for return=documents streaming.
func RunDocumentStreamingFlowControl(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 2*time.Minute, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		namespace := "q-stream-flow-" + xid.New().String()
		totalDocs := documentStreamingFlowTotalDocs(ts.Config.Store)
		for i := 0; i < totalDocs; i++ {
			key := fmt.Sprintf("stream-flow-%03d", i)
			querydata.SeedState(ctx, t, ts.Client, namespace, key, map[string]any{
				"status": "open",
				"index":  i,
			})
		}
		flushNamespaces(ctx, t, ts.Client, namespace)

		sel, err := lql.ParseSelectorString(`eq{field=/status,value=open}`)
		if err != nil || sel.IsEmpty() {
			t.Fatalf("parse selector: %v", err)
		}
		reqBody, err := json.Marshal(api.QueryRequest{
			Namespace: namespace,
			Selector:  sel,
			Limit:     totalDocs,
			Return:    api.QueryReturnDocuments,
		})
		if err != nil {
			t.Fatalf("marshal query request: %v", err)
		}
		endpoint := fmt.Sprintf("%s/v1/query?return=%s", ts.URL(), api.QueryReturnDocuments)

		t.Run("cancel-mid-stream", func(t *testing.T) {
			reqCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			httpReq.Header.Set("Content-Type", "application/json")
			resp, err := httpClient.Do(httpReq)
			if err != nil {
				t.Fatalf("query request failed: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				data, _ := io.ReadAll(resp.Body)
				t.Fatalf("expected status 200, got %d: %s", resp.StatusCode, data)
			}
			scanner := bufio.NewScanner(resp.Body)
			count := 0
			for scanner.Scan() {
				count++
				if count >= 10 {
					cancel()
					break
				}
			}
			_ = resp.Body.Close()
			if count < 10 {
				t.Fatalf("expected at least 10 rows before cancellation, got %d", count)
			}
			verify := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespace,
				Selector:  sel,
				Limit:     1,
			})
			if len(verify.Keys) != 1 {
				t.Fatalf("expected server to remain responsive after cancellation, got keys=%v", verify.Keys)
			}
		})

		t.Run("slow-reader", func(t *testing.T) {
			httpReq, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(reqBody))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			httpReq.Header.Set("Content-Type", "application/json")
			resp, err := httpClient.Do(httpReq)
			if err != nil {
				t.Fatalf("query request failed: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				data, _ := io.ReadAll(resp.Body)
				t.Fatalf("expected status 200, got %d: %s", resp.StatusCode, data)
			}
			scanner := bufio.NewScanner(resp.Body)
			count := 0
			for scanner.Scan() {
				count++
				time.Sleep(2 * time.Millisecond)
			}
			if err := scanner.Err(); err != nil {
				t.Fatalf("scan stream: %v", err)
			}
			if count != totalDocs {
				t.Fatalf("expected %d rows from slow reader stream, got %d", totalDocs, count)
			}
		})
	})
}

func documentStreamingFlowTotalDocs(store string) int {
	switch {
	case isObjectStoreStore(store):
		// Object-store backends have substantially higher per-document seed
		// latency in CI; keep enough rows for flow-control assertions while
		// avoiding suite-level timeout pressure.
		return 24
	default:
		return 128
	}
}

func largeNamespaceLowMatchTotalDocs(store string) int {
	if isObjectStoreStore(store) {
		// Keep the low-match semantics while reducing fixture size for slower
		// object-store backends.
		return 48
	}
	return 384
}

func isObjectStoreStore(store string) bool {
	return strings.HasPrefix(store, "aws://") ||
		strings.HasPrefix(store, "azure://") ||
		strings.HasPrefix(store, "s3://")
}

// RunDomainDatasets seeds richer domain data (finance, firmware, SALUTE, flight) and evaluates selectors.
func RunDomainDatasets(t *testing.T, factory ServerFactory, opts ...Option) {
	cfg := runConfig{datasetProfile: querydata.DatasetFull}
	for _, opt := range opts {
		opt(&cfg)
	}
	shouldRun := func(name string) bool {
		if cfg.datasetProfile != querydata.DatasetReduced {
			return true
		}
		switch name {
		case "vouchers-unposted", "firmware-draining", "salute-opfor", "flight-manual-guidance":
			return true
		default:
			return false
		}
	}
	withServer(t, factory, 30*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		querydata.SeedVoucherData(ctx, t, ts.Client, cfg.datasetProfile)
		querydata.SeedFirmwareData(ctx, t, ts.Client, cfg.datasetProfile)
		querydata.SeedSaluteData(ctx, t, ts.Client, cfg.datasetProfile)
		querydata.SeedFlightData(ctx, t, ts.Client, cfg.datasetProfile)
		flushNamespaces(ctx, t, ts.Client, namespaces.Default)

		t.Run("vouchers-unposted", func(t *testing.T) {
			if !shouldRun("vouchers-unposted") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, err := lql.ParseSelectorString(`
and.eq{field=/voucher/book,value=GENERAL},
and.eq{field=/voucher/header/period,value=2025-11},
and.eq{field=/voucher/header/posted,value=false},
and.range{field=/voucher/lines/10/amount,gte=3000}`)
			if err != nil || sel.IsEmpty() {
				t.Fatalf("selector voucher: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			want := []string{"voucher-ap-2025-1101"}
			if !slices.Equal(resp.Keys, want) {
				t.Fatalf("expected %v, got %v", want, resp.Keys)
			}
		})

		t.Run("vouchers-posted", func(t *testing.T) {
			if !shouldRun("vouchers-posted") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, err := lql.ParseSelectorString(`
and.eq{field=/voucher/header/posted,value=true}`)
			if err != nil || sel.IsEmpty() {
				t.Fatalf("selector vouchers posted: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			querydata.ExpectKeySet(t, resp.Keys, []string{
				"voucher-ap-2025-1102",
				"voucher-bulk-00",
				"voucher-bulk-02",
				"voucher-bulk-04",
			})
		})

		t.Run("vouchers-unposted-nested-or", func(t *testing.T) {
			if !shouldRun("vouchers-unposted-nested-or") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, err := lql.ParseSelectorString(`
and.0.eq{field=/voucher/header/posted,value=false},
and.1.or.0.eq{field=/voucher/book,value=GENERAL},
and.1.or.1.eq{field=/voucher/book,value=US-LEDGER}`)
			if err != nil || sel.IsEmpty() {
				t.Fatalf("selector vouchers nested or: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			querydata.ExpectKeySet(t, resp.Keys, []string{
				"voucher-ap-2025-1101",
				"voucher-bulk-01",
				"voucher-bulk-03",
				"voucher-bulk-05",
				"voucher-us-2025-1103",
			})
		})

		t.Run("firmware-draining", func(t *testing.T) {
			if !shouldRun("firmware-draining") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, err := lql.ParseSelectorString(`
and.eq{field=/device/firmware/channel,value=stable},
and.range{field=/device/rollout/progress/percent,gte=50},
and.range{field=/device/telemetry/battery_mv,gte=3600},
and.eq{field=/device/rollout/progress/status,value=draining}`)
			if err != nil || sel.IsEmpty() {
				t.Fatalf("firmware selector: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			want := []string{"device-gw-2048"}
			if !slices.Equal(resp.Keys, want) {
				t.Fatalf("expected %v, got %v", want, resp.Keys)
			}
		})

		t.Run("firmware-draining-or-west", func(t *testing.T) {
			if !shouldRun("firmware-draining-or-west") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, err := lql.ParseSelectorString(`
and.eq{field=/device/rollout/progress/status,value=draining},
or.eq{field=/device/location/region,value=us-west}`)
			if err != nil || sel.IsEmpty() {
				t.Fatalf("firmware draining or west selector: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			querydata.ExpectKeySet(t, resp.Keys, []string{"device-gw-1024"})
		})

		t.Run("firmware-low-battery", func(t *testing.T) {
			if !shouldRun("firmware-low-battery") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, err := lql.ParseSelectorString(`
and.eq{field=/device/firmware/channel,value=stable},
and.range{field=/device/telemetry/battery_mv,lte=3600}`)
			if err != nil || sel.IsEmpty() {
				t.Fatalf("firmware low battery selector: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			querydata.ExpectKeySet(t, resp.Keys, []string{"device-gw-600", "device-gw-700", "device-gw-8192"})
		})

		t.Run("salute-opfor", func(t *testing.T) {
			if !shouldRun("salute-opfor") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, err := lql.ParseSelectorString(`
and.eq{field=/report/type,value=salute},
and.eq{field=/report/affiliation,value=opfor},
and.prefix{field=/report/location/grid,value=42SXD}`)
			if err != nil || sel.IsEmpty() {
				t.Fatalf("salute selector: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			want := []string{"salute-report-opfor"}
			if !slices.Equal(resp.Keys, want) {
				t.Fatalf("expected %v, got %v", want, resp.Keys)
			}
		})

		t.Run("salute-friendly-grid", func(t *testing.T) {
			if !shouldRun("salute-friendly-grid") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, err := lql.ParseSelectorString(`
and.eq{field=/report/affiliation,value=friendly},
and.prefix{field=/report/location/grid,value=42SY}`)
			if err != nil || sel.IsEmpty() {
				t.Fatalf("salute friendly selector: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			querydata.ExpectKeySet(t, resp.Keys, []string{"salute-report-friendly-south"})
		})

		t.Run("flight-critical", func(t *testing.T) {
			if !shouldRun("flight-critical") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, err := lql.ParseSelectorString(`
and.eq{field=/vehicle/mission,value=delta-heavy},
and.eq{field=/telemetry/stage,value=boost},
and.range{field=/telemetry/apoapsis_km,gte=150},
and.eq{field=/telemetry/guidance/mode,value=auto}`)
			if err != nil || sel.IsEmpty() {
				t.Fatalf("flight selector: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			want := []string{"flight-boost-02"}
			if !slices.Equal(resp.Keys, want) {
				t.Fatalf("expected %v, got %v", want, resp.Keys)
			}
		})

		t.Run("flight-manual-guidance", func(t *testing.T) {
			if !shouldRun("flight-manual-guidance") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, err := lql.ParseSelectorString(`
and.eq{field=/telemetry/guidance/mode,value=manual}`)
			if err != nil || sel.IsEmpty() {
				t.Fatalf("flight manual selector: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			querydata.ExpectKeySet(t, resp.Keys, []string{"flight-descent-01", "flight-entry-01", "flight-fairing-01"})
		})
	})
}

// RunTxSmoke verifies staged commit and rollback for a single key using the default
// acquire/release path. It keeps the payload small so it can run across all backends.
func RunTxSmoke(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 20*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		commitKey := fmt.Sprintf("tx-commit-%d", time.Now().UnixNano())
		rollbackKey := fmt.Sprintf("tx-rollback-%d", time.Now().UnixNano())

		// Commit: stage state + hidden metadata, then commit and verify state persisted.
		lease, err := ts.Client.Acquire(ctx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        commitKey,
			Owner:      "tx-smoke",
			TTLSeconds: 30,
		})
		if err != nil {
			t.Fatalf("acquire commit key: %v", err)
		}
		if err := lease.Save(ctx, map[string]any{"status": "open", "counter": 1}); err != nil {
			t.Fatalf("save commit payload: %v", err)
		}
		if _, err := lease.UpdateMetadata(ctx, lockdclient.MetadataOptions{QueryHidden: lockdclient.Bool(true)}); err != nil {
			t.Fatalf("set query_hidden: %v", err)
		}
		if err := lease.Release(ctx); err != nil {
			t.Fatalf("commit release: %v", err)
		}
		flushNamespaces(ctx, t, ts.Client, namespaces.Default)

		resp, err := ts.Client.Get(ctx, commitKey, lockdclient.WithGetNamespace(namespaces.Default))
		if err != nil {
			t.Fatalf("get committed key: %v", err)
		}
		doc, err := resp.Document()
		if err != nil {
			t.Fatalf("decode committed document: %v", err)
		}
		if v := doc.Body["counter"]; v != float64(1) { // JSON numbers decode to float64
			t.Fatalf("unexpected counter: %#v", v)
		}

		// Hidden metadata should suppress the key from selector queries.
		selector, err := lql.ParseSelectorString(`eq{field=/status,value=open}`)
		if err != nil || selector.IsEmpty() {
			t.Fatalf("parse selector: %v", err)
		}
		qr := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
			Namespace: namespaces.Default,
			Selector:  selector,
			Limit:     10,
		})
		if len(qr.Keys) != 0 {
			t.Fatalf("expected hidden key to be excluded, got %+v", qr.Keys)
		}

		// Rollback: stage state then rollback; key should disappear.
		rlease, err := ts.Client.Acquire(ctx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        rollbackKey,
			Owner:      "tx-smoke",
			TTLSeconds: 30,
		})
		if err != nil {
			t.Fatalf("acquire rollback key: %v", err)
		}
		if err := rlease.Save(ctx, map[string]any{"status": "temp"}); err != nil {
			t.Fatalf("save rollback payload: %v", err)
		}
		if err := rlease.ReleaseWithOptions(ctx, lockdclient.ReleaseOptions{Rollback: true}); err != nil {
			t.Fatalf("rollback release: %v", err)
		}
		if rbResp, err := ts.Client.Get(ctx, rollbackKey, lockdclient.WithGetNamespace(namespaces.Default)); err == nil {
			if rbResp.HasState {
				t.Fatalf("expected rollback key to have no state")
			}
		}

		// Cleanup committed key to avoid backend clutter.
		cleanupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if cl, err := ts.Client.Acquire(cleanupCtx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        commitKey,
			Owner:      "tx-smoke-cleanup",
			TTLSeconds: 10,
			BlockSecs:  1,
		}); err == nil {
			if _, rmErr := cl.Remove(cleanupCtx); rmErr != nil {
				t.Logf("cleanup remove %s: %v", commitKey, rmErr)
			}
			_ = cl.Release(cleanupCtx)
		}
	})
}

// RunTxnMultiKey validates commit and rollback decisions fanning out across multiple keys.
func RunTxnMultiKey(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 25*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		txnID := xid.New().String()
		keyA := fmt.Sprintf("xa-a-%d", time.Now().UnixNano())
		keyB := fmt.Sprintf("xa-b-%d", time.Now().UnixNano())

		leaseA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        keyA,
			Owner:      "xa-multi",
			TTLSeconds: 30,
			TxnID:      txnID,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("acquire keyA: %v", err)
		}
		leaseB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        keyB,
			Owner:      "xa-multi",
			TTLSeconds: 30,
			TxnID:      txnID,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("acquire keyB: %v", err)
		}
		if err := leaseA.Save(ctx, map[string]any{"value": "a"}); err != nil {
			t.Fatalf("save keyA: %v", err)
		}
		if err := leaseB.Save(ctx, map[string]any{"value": "b"}); err != nil {
			t.Fatalf("save keyB: %v", err)
		}

		// Commit once; decision should fan out to both participants.
		if err := leaseA.Release(ctx); err != nil {
			t.Fatalf("commit release: %v", err)
		}

		flushNamespaces(ctx, t, ts.Client, namespaces.Default)

		checkCommitted := func(key, expected string) {
			resp, err := ts.Client.Get(ctx, key, lockdclient.WithGetNamespace(namespaces.Default))
			if err != nil {
				t.Fatalf("get %s: %v", key, err)
			}
			doc, err := resp.Document()
			if err != nil {
				t.Fatalf("decode %s: %v", key, err)
			}
			if doc.Body["value"] != expected {
				t.Fatalf("unexpected value for %s: %#v", key, doc.Body["value"])
			}
		}
		checkCommitted(keyA, "a")
		checkCommitted(keyB, "b")

		// Ensure leases are cleared for all participants.
		reacquireCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := ts.Client.Acquire(reacquireCtx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        keyB,
			Owner:      "xa-multi-check",
			TTLSeconds: 10,
			BlockSecs:  lockdclient.BlockWaitForever,
		}); err != nil {
			t.Fatalf("reacquire keyB after commit: %v", err)
		}

		// Rollback path with a new txn id.
		txnRollback := xid.New().String()
		rollKeyA := fmt.Sprintf("xa-rb-a-%d", time.Now().UnixNano())
		rollKeyB := fmt.Sprintf("xa-rb-b-%d", time.Now().UnixNano())

		rbA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        rollKeyA,
			Owner:      "xa-multi",
			TTLSeconds: 30,
			TxnID:      txnRollback,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("acquire rollback A: %v", err)
		}
		rbB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        rollKeyB,
			Owner:      "xa-multi",
			TTLSeconds: 30,
			TxnID:      txnRollback,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("acquire rollback B: %v", err)
		}
		if err := rbA.Save(ctx, map[string]any{"value": "rollback-a"}); err != nil {
			t.Fatalf("save rollback A: %v", err)
		}
		if err := rbB.Save(ctx, map[string]any{"value": "rollback-b"}); err != nil {
			t.Fatalf("save rollback B: %v", err)
		}
		if err := rbA.ReleaseWithOptions(ctx, lockdclient.ReleaseOptions{Rollback: true}); err != nil {
			t.Fatalf("rollback decision: %v", err)
		}

		// Both keys should be absent after rollback.
		assertNoState := func(key string) {
			resp, err := ts.Client.Get(ctx, key, lockdclient.WithGetNamespace(namespaces.Default))
			if err != nil {
				t.Fatalf("get %s after rollback: %v", key, err)
			}
			if resp.HasState {
				t.Fatalf("expected %s to have no state after rollback", key)
			}
		}
		assertNoState(rollKeyA)
		assertNoState(rollKeyB)
	})
}

// RunTxnMultiKeyNamespaces commits and rolls back a transaction spanning two namespaces.
func RunTxnMultiKeyNamespaces(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 25*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		txnID := xid.New().String()
		nsA := "alpha"
		nsB := "beta"
		keyA := fmt.Sprintf("xa-ns-a-%d", time.Now().UnixNano())
		keyB := fmt.Sprintf("xa-ns-b-%d", time.Now().UnixNano())

		leaseA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
			Namespace:  nsA,
			Key:        keyA,
			Owner:      "xa-multi-ns",
			TTLSeconds: 30,
			TxnID:      txnID,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("acquire %s: %v", keyA, err)
		}
		leaseB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
			Namespace:  nsB,
			Key:        keyB,
			Owner:      "xa-multi-ns",
			TTLSeconds: 30,
			TxnID:      txnID,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("acquire %s: %v", keyB, err)
		}
		if err := leaseA.Save(ctx, map[string]any{"ns": nsA}); err != nil {
			t.Fatalf("save %s: %v", keyA, err)
		}
		if err := leaseB.Save(ctx, map[string]any{"ns": nsB}); err != nil {
			t.Fatalf("save %s: %v", keyB, err)
		}

		if err := leaseA.Release(ctx); err != nil {
			t.Fatalf("commit release: %v", err)
		}

		flushNamespaces(ctx, t, ts.Client, nsA, nsB)

		checkCommitted := func(ns, key, expected string) {
			resp, err := ts.Client.Get(ctx, key, lockdclient.WithGetNamespace(ns))
			if err != nil {
				t.Fatalf("get %s/%s: %v", ns, key, err)
			}
			doc, err := resp.Document()
			if err != nil {
				t.Fatalf("decode %s/%s: %v", ns, key, err)
			}
			if doc.Body["ns"] != expected {
				t.Fatalf("unexpected value for %s/%s: %#v", ns, key, doc.Body["ns"])
			}
		}
		checkCommitted(nsA, keyA, nsA)
		checkCommitted(nsB, keyB, nsB)

		// Ensure leases are cleared in both namespaces.
		reacquireCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := ts.Client.Acquire(reacquireCtx, api.AcquireRequest{
			Namespace:  nsB,
			Key:        keyB,
			Owner:      "xa-multi-ns-check",
			TTLSeconds: 5,
			BlockSecs:  lockdclient.BlockWaitForever,
		}); err != nil {
			t.Fatalf("reacquire %s/%s after commit: %v", nsB, keyB, err)
		}

		// Rollback path across namespaces.
		rbTxn := xid.New().String()
		rbAKey := fmt.Sprintf("xa-ns-rb-a-%d", time.Now().UnixNano())
		rbBKey := fmt.Sprintf("xa-ns-rb-b-%d", time.Now().UnixNano())
		rbLeaseA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
			Namespace:  nsA,
			Key:        rbAKey,
			Owner:      "xa-multi-ns",
			TTLSeconds: 30,
			TxnID:      rbTxn,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("acquire rollback A: %v", err)
		}
		rbLeaseB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
			Namespace:  nsB,
			Key:        rbBKey,
			Owner:      "xa-multi-ns",
			TTLSeconds: 30,
			TxnID:      rbTxn,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("acquire rollback B: %v", err)
		}
		if err := rbLeaseA.Save(ctx, map[string]any{"ns": nsA}); err != nil {
			t.Fatalf("save rollback A: %v", err)
		}
		if err := rbLeaseB.Save(ctx, map[string]any{"ns": nsB}); err != nil {
			t.Fatalf("save rollback B: %v", err)
		}
		if err := rbLeaseA.ReleaseWithOptions(ctx, lockdclient.ReleaseOptions{Rollback: true}); err != nil {
			t.Fatalf("rollback release: %v", err)
		}

		flushNamespaces(ctx, t, ts.Client, nsA, nsB)

		assertNoState := func(ns, key string) {
			resp, err := ts.Client.Get(ctx, key, lockdclient.WithGetNamespace(ns))
			if err != nil {
				t.Fatalf("get %s/%s after rollback: %v", ns, key, err)
			}
			if resp.HasState {
				t.Fatalf("expected no state for %s/%s after rollback", ns, key)
			}
		}
		assertNoState(nsA, rbAKey)
		assertNoState(nsB, rbBKey)
	})
}

// RunTxnSoak runs a short soak to flush out staged/txn leaks and stalled sweeper
// behavior. It alternates commit/rollback decisions across two keys per txn and
// asserts the backend is clean (.txns + .staging) once the loop completes.
func RunTxnSoak(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 45*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		const iterations = 24
		watchdog := time.NewTicker(4 * time.Second)
		defer watchdog.Stop()
		lastProgress := time.Now()

		for i := 0; i < iterations; i++ {
			select {
			case <-watchdog.C:
				if time.Since(lastProgress) > 8*time.Second {
					t.Fatalf("txn soak stalled after %d/%d iterations", i, iterations)
				}
			default:
			}
			txnID := xid.New().String()
			keyA := fmt.Sprintf("soak-a-%02d-%d", i, time.Now().UnixNano())
			keyB := fmt.Sprintf("soak-b-%02d-%d", i, time.Now().UnixNano())

			leaseA, err := ts.Client.Acquire(ctx, api.AcquireRequest{
				Namespace:  namespaces.Default,
				Key:        keyA,
				Owner:      "txn-soak",
				TTLSeconds: 25,
				TxnID:      txnID,
				BlockSecs:  lockdclient.BlockWaitForever,
			})
			if err != nil {
				t.Fatalf("acquire keyA: %v", err)
			}
			leaseB, err := ts.Client.Acquire(ctx, api.AcquireRequest{
				Namespace:  namespaces.Default,
				Key:        keyB,
				Owner:      "txn-soak",
				TTLSeconds: 25,
				TxnID:      txnID,
				BlockSecs:  lockdclient.BlockWaitForever,
			})
			if err != nil {
				t.Fatalf("acquire keyB: %v", err)
			}
			if err := leaseA.Save(ctx, map[string]any{"step": "A", "iter": i}); err != nil {
				t.Fatalf("save keyA: %v", err)
			}
			if err := leaseB.Save(ctx, map[string]any{"step": "B", "iter": i}); err != nil {
				t.Fatalf("save keyB: %v", err)
			}

			if i%2 == 0 {
				// Commit path.
				if err := leaseA.Release(ctx); err != nil {
					t.Fatalf("commit release: %v", err)
				}
				verifyCommitted := func(key, expectedStep string) {
					resp, err := ts.Client.Get(ctx, key, lockdclient.WithGetNamespace(namespaces.Default))
					if err != nil {
						t.Fatalf("get %s: %v", key, err)
					}
					doc, err := resp.Document()
					if err != nil {
						t.Fatalf("decode %s: %v", key, err)
					}
					if doc.Body["step"] != expectedStep {
						t.Fatalf("unexpected step for %s: %#v", key, doc.Body["step"])
					}
				}
				verifyCommitted(keyA, "A")
				verifyCommitted(keyB, "B")
			} else {
				// Rollback path.
				if err := leaseA.ReleaseWithOptions(ctx, lockdclient.ReleaseOptions{Rollback: true}); err != nil {
					t.Fatalf("rollback release: %v", err)
				}
				assertRolledBack := func(key string) {
					resp, err := ts.Client.Get(ctx, key, lockdclient.WithGetNamespace(namespaces.Default))
					if err != nil {
						t.Fatalf("get %s after rollback: %v", key, err)
					}
					if resp.HasState {
						t.Fatalf("expected no state for %s after rollback", key)
					}
				}
				assertRolledBack(keyA)
				assertRolledBack(keyB)
			}

			// Ensure leases clear promptly.
			reacquireCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if _, err := ts.Client.Acquire(reacquireCtx, api.AcquireRequest{
				Namespace:  namespaces.Default,
				Key:        keyB,
				Owner:      "txn-soak-reacquire",
				TTLSeconds: 5,
				BlockSecs:  1,
			}); err != nil {
				t.Fatalf("reacquire %s: %v", keyB, err)
			}
			cancel()
			lastProgress = time.Now()
		}

		flushNamespaces(ctx, t, ts.Client, namespaces.Default)

		waitCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		if err := waitForTxnRecordsDecided(waitCtx, ts.Backend()); err != nil {
			t.Fatalf("txn records leak: %v", err)
		}
		if err := waitForStagingEmpty(waitCtx, ts.Backend(), namespaces.Default); err != nil {
			t.Fatalf("staging leak: %v", err)
		}
	})
}

// RunTxnKeepAliveParity ensures keepalive enforces txn echoing for enlisted leases.
func RunTxnKeepAliveParity(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 20*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		txnID := xid.New().String()
		key := fmt.Sprintf("xa-keepalive-%d", time.Now().UnixNano())

		lease, err := ts.Client.Acquire(ctx, api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        key,
			Owner:      "txn-keepalive",
			TTLSeconds: 5,
			TxnID:      txnID,
			BlockSecs:  lockdclient.BlockWaitForever,
		})
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}

		_, err = ts.Client.KeepAlive(ctx, api.KeepAliveRequest{
			Namespace:  namespaces.Default,
			Key:        key,
			LeaseID:    lease.LeaseID,
			TTLSeconds: 10,
		})
		var apiErr *lockdclient.APIError
		if err == nil || !errors.As(err, &apiErr) || apiErr.Response.ErrorCode != "missing_txn" {
			t.Fatalf("expected missing_txn error, got %v", err)
		}

		_, err = ts.Client.KeepAlive(ctx, api.KeepAliveRequest{
			Namespace:  namespaces.Default,
			Key:        key,
			LeaseID:    lease.LeaseID,
			TTLSeconds: 10,
			TxnID:      xid.New().String(),
		})
		apiErr = nil
		if err == nil || !errors.As(err, &apiErr) || apiErr.Response.ErrorCode != "txn_mismatch" {
			t.Fatalf("expected txn_mismatch error, got %v", err)
		}

		resp, err := ts.Client.KeepAlive(ctx, api.KeepAliveRequest{
			Namespace:  namespaces.Default,
			Key:        key,
			LeaseID:    lease.LeaseID,
			TTLSeconds: 10,
			TxnID:      txnID,
		})
		if err != nil {
			t.Fatalf("keepalive with txn: %v", err)
		}
		if resp.ExpiresAt <= lease.ExpiresAt {
			t.Fatalf("expected expiry to extend beyond original, got %d (orig %d)", resp.ExpiresAt, lease.ExpiresAt)
		}
	})
}

// RunRawTxnSmoke exercises txn-aware operations via the raw HTTP API (no LeaseSession
// autofill) to ensure missing_txn enforcement and commit/rollback semantics hold.
func RunRawTxnSmoke(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 25*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		commitKey := fmt.Sprintf("raw-tx-commit-%d", time.Now().UnixNano())
		rollbackKey := fmt.Sprintf("raw-tx-rollback-%d", time.Now().UnixNano())

		commitLease := rawAcquire(t, httpClient, ts.URL(), api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        commitKey,
			Owner:      "raw-tx",
			TTLSeconds: 30,
		})
		expectMissingTxnOnUpdate(t, httpClient, ts.URL(), namespaces.Default, commitKey, commitLease.LeaseID, commitLease.FencingToken)
		rawUpdateOK(t, httpClient, ts.URL(), namespaces.Default, commitKey, commitLease.LeaseID, commitLease.FencingToken, commitLease.TxnID, `{"status":"raw-commit"}`)
		rawRelease(t, httpClient, ts.URL(), namespaces.Default, commitKey, commitLease.LeaseID, commitLease.TxnID, false, commitLease.FencingToken)
		verifyStatePresent(ctx, t, ts.Client, commitKey, namespaces.Default, "raw-commit")

		removeKey := fmt.Sprintf("raw-tx-remove-%d", time.Now().UnixNano())
		removeLease := rawAcquire(t, httpClient, ts.URL(), api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        removeKey,
			Owner:      "raw-tx",
			TTLSeconds: 30,
		})
		rawUpdateOK(t, httpClient, ts.URL(), namespaces.Default, removeKey, removeLease.LeaseID, removeLease.FencingToken, removeLease.TxnID, `{"status":"to-remove"}`)
		expectMissingTxnOnRemove(t, httpClient, ts.URL(), namespaces.Default, removeKey, removeLease.LeaseID, removeLease.FencingToken)
		rawRemoveOK(t, httpClient, ts.URL(), namespaces.Default, removeKey, removeLease.LeaseID, removeLease.FencingToken, removeLease.TxnID)
		rawRelease(t, httpClient, ts.URL(), namespaces.Default, removeKey, removeLease.LeaseID, removeLease.TxnID, false, removeLease.FencingToken)
		verifyStateAbsent(ctx, t, ts.Client, removeKey, namespaces.Default)

		rollbackLease := rawAcquire(t, httpClient, ts.URL(), api.AcquireRequest{
			Namespace:  namespaces.Default,
			Key:        rollbackKey,
			Owner:      "raw-tx",
			TTLSeconds: 30,
		})
		rawUpdateOK(t, httpClient, ts.URL(), namespaces.Default, rollbackKey, rollbackLease.LeaseID, rollbackLease.FencingToken, rollbackLease.TxnID, `{"status":"temporary"}`)
		rawRelease(t, httpClient, ts.URL(), namespaces.Default, rollbackKey, rollbackLease.LeaseID, rollbackLease.TxnID, true, rollbackLease.FencingToken)
		verifyStateAbsent(ctx, t, ts.Client, rollbackKey, namespaces.Default)
	})
}
func withServer(t testing.TB, factory ServerFactory, timeout time.Duration, fn func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client)) {
	t.Helper()
	ts := factory(t)
	if ts.Client == nil {
		t.Fatalf("test server missing client")
	}
	httpClient := newHTTPClient(t, ts)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	fn(ctx, ts, httpClient)
}

func newHTTPClient(t testing.TB, ts *lockd.TestServer) *http.Client {
	t.Helper()
	client, err := ts.NewHTTPClient()
	if err != nil {
		t.Fatalf("http client: %v", err)
	}
	return client
}

func doQuery(t testing.TB, httpClient *http.Client, baseURL string, req api.QueryRequest) api.QueryResponse {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal query request: %v", err)
	}
	httpReq, err := http.NewRequest(http.MethodPost, baseURL+"/v1/query", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("prepare http request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		t.Fatalf("query request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, data)
	}
	var out api.QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return out
}

type documentRow struct {
	Namespace string          `json:"ns"`
	Key       string          `json:"key"`
	Version   int64           `json:"ver,omitempty"`
	Doc       json.RawMessage `json:"doc"`
}

func doQueryDocuments(t testing.TB, httpClient *http.Client, baseURL string, req api.QueryRequest) []documentRow {
	t.Helper()
	req.Return = api.QueryReturnDocuments
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal query request: %v", err)
	}
	endpoint := fmt.Sprintf("%s/v1/query?return=%s", baseURL, api.QueryReturnDocuments)
	httpReq, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("prepare http request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		t.Fatalf("query request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, data)
	}
	scanner := bufio.NewScanner(resp.Body)
	rows := make([]documentRow, 0)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var row documentRow
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatalf("decode document row: %v", err)
		}
		rows = append(rows, row)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan documents: %v", err)
	}
	return rows
}

func flushNamespaces(ctx context.Context, t testing.TB, cli *lockdclient.Client, namespaces ...string) {
	querydata.FlushQueryNamespaces(ctx, t, cli, namespaces...)
}

func rawAcquire(t testing.TB, httpClient *http.Client, baseURL string, req api.AcquireRequest) api.AcquireResponse {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal acquire request: %v", err)
	}
	httpReq, err := http.NewRequest(http.MethodPost, baseURL+"/v1/acquire", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build acquire request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		t.Fatalf("acquire request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected acquire status %d: %s", resp.StatusCode, data)
	}
	var out api.AcquireResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode acquire: %v", err)
	}
	return out
}

func expectMissingTxnOnUpdate(t testing.TB, httpClient *http.Client, baseURL, namespace, key, leaseID string, fencing int64) {
	t.Helper()
	resp, err := rawUpdateRequest(httpClient, baseURL, namespace, key, leaseID, fencing, "", `{"status":"pending"}`)
	if err != nil {
		t.Fatalf("update request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400 missing_txn, got %d: %s", resp.StatusCode, data)
	}
	var apiErr api.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiErr); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if apiErr.ErrorCode != "missing_txn" {
		t.Fatalf("expected missing_txn, got %q (%+v)", apiErr.ErrorCode, apiErr)
	}
}

func rawUpdateOK(t testing.TB, httpClient *http.Client, baseURL, namespace, key, leaseID string, fencing int64, txnID string, body string) {
	t.Helper()
	resp, err := rawUpdateRequest(httpClient, baseURL, namespace, key, leaseID, fencing, txnID, body)
	if err != nil {
		t.Fatalf("update request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 update, got %d: %s", resp.StatusCode, data)
	}
	var out api.UpdateResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode update response: %v", err)
	}
	if out.NewVersion == 0 {
		t.Fatalf("expected version bump, got %+v", out)
	}
}

func expectMissingTxnOnRemove(t testing.TB, httpClient *http.Client, baseURL, namespace, key, leaseID string, fencing int64) {
	t.Helper()
	resp, err := rawRemoveRequest(httpClient, baseURL, namespace, key, leaseID, fencing, "")
	if err != nil {
		t.Fatalf("remove request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 400 missing_txn, got %d: %s", resp.StatusCode, data)
	}
	var apiErr api.ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiErr); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if apiErr.ErrorCode != "missing_txn" {
		t.Fatalf("expected missing_txn, got %q (%+v)", apiErr.ErrorCode, apiErr)
	}
}

func rawRemoveOK(t testing.TB, httpClient *http.Client, baseURL, namespace, key, leaseID string, fencing int64, txnID string) {
	t.Helper()
	resp, err := rawRemoveRequest(httpClient, baseURL, namespace, key, leaseID, fencing, txnID)
	if err != nil {
		t.Fatalf("remove request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 remove, got %d: %s", resp.StatusCode, data)
	}
	var out api.RemoveResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode remove response: %v", err)
	}
	if !out.Removed {
		t.Fatalf("expected removed=true, got %+v", out)
	}
}

func rawUpdateRequest(httpClient *http.Client, baseURL, namespace, key, leaseID string, fencing int64, txnID, body string) (*http.Response, error) {
	httpReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/update?key=%s&namespace=%s", baseURL, url.QueryEscape(key), url.QueryEscape(namespace)), bytes.NewReader([]byte(body)))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Lease-ID", leaseID)
	httpReq.Header.Set("X-Fencing-Token", fmt.Sprint(fencing))
	if strings.TrimSpace(txnID) != "" {
		httpReq.Header.Set("X-Txn-ID", txnID)
	}
	return httpClient.Do(httpReq)
}

func rawRemoveRequest(httpClient *http.Client, baseURL, namespace, key, leaseID string, fencing int64, txnID string) (*http.Response, error) {
	httpReq, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/remove?key=%s&namespace=%s", baseURL, url.QueryEscape(key), url.QueryEscape(namespace)), http.NoBody)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("X-Lease-ID", leaseID)
	httpReq.Header.Set("X-Fencing-Token", fmt.Sprint(fencing))
	if strings.TrimSpace(txnID) != "" {
		httpReq.Header.Set("X-Txn-ID", txnID)
	}
	return httpClient.Do(httpReq)
}

func rawRelease(t testing.TB, httpClient *http.Client, baseURL, namespace, key, leaseID, txnID string, rollback bool, fencing int64) {
	t.Helper()
	payload := map[string]any{
		"namespace": namespace,
		"key":       key,
		"lease_id":  leaseID,
		"txn_id":    txnID,
		"rollback":  rollback,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal release: %v", err)
	}
	httpReq, err := http.NewRequest(http.MethodPost, baseURL+"/v1/release", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build release: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Fencing-Token", fmt.Sprint(fencing))
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		t.Fatalf("release request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected release status %d: %s", resp.StatusCode, data)
	}
	var out api.ReleaseResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode release: %v", err)
	}
	if !out.Released {
		t.Fatalf("expected release=true, got %+v", out)
	}
}

func verifyStatePresent(ctx context.Context, t testing.TB, cli *lockdclient.Client, key, namespace, expectStatus string) {
	t.Helper()
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespace,
		Key:        key,
		Owner:      "raw-verify",
		TTLSeconds: 10,
		BlockSecs:  1,
	})
	if err != nil {
		t.Fatalf("acquire for verify: %v", err)
	}
	defer lease.Release(ctx) //nolint:errcheck
	var doc map[string]any
	if err := lease.Load(ctx, &doc); err != nil {
		t.Fatalf("load state: %v", err)
	}
	if len(doc) == 0 {
		t.Fatalf("expected state to exist for %s", key)
	}
	if status, ok := doc["status"]; !ok || status != expectStatus {
		t.Fatalf("expected status %q, got %+v", expectStatus, doc)
	}
}

func verifyStateAbsent(ctx context.Context, t testing.TB, cli *lockdclient.Client, key, namespace string) {
	t.Helper()
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  namespace,
		Key:        key,
		Owner:      "raw-verify",
		TTLSeconds: 5,
		BlockSecs:  1,
	})
	if err != nil {
		t.Fatalf("acquire for verify: %v", err)
	}
	defer lease.Release(ctx) //nolint:errcheck
	snap, err := lease.Get(ctx)
	if err != nil {
		t.Fatalf("get state: %v", err)
	}
	if snap != nil && snap.HasState {
		t.Fatalf("expected no state for %s, got %+v", key, snap)
	}
}

func waitForStagingEmpty(ctx context.Context, backend storage.Backend, namespace string) error {
	for {
		startAfter := ""
		stagedCount := 0
		for {
			list, err := backend.ListObjects(ctx, namespace, storage.ListOptions{StartAfter: startAfter, Limit: 256})
			if err != nil {
				return err
			}
			for _, obj := range list.Objects {
				if storage.IsStagingObjectKey(obj.Key) {
					stagedCount++
				}
			}
			if !list.Truncated || list.NextStartAfter == "" {
				break
			}
			startAfter = list.NextStartAfter
		}
		if stagedCount == 0 {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("bucket %s still has %d staging object(s)", namespace, stagedCount)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func waitForTxnRecordsDecided(ctx context.Context, backend storage.Backend) error {
	for {
		list, err := backend.ListObjects(ctx, ".txns", storage.ListOptions{})
		if err != nil {
			return err
		}
		pending := 0
		for _, obj := range list.Objects {
			objRes, err := backend.GetObject(ctx, ".txns", obj.Key)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					continue
				}
				return err
			}
			var rec core.TxnRecord
			decodeErr := json.NewDecoder(objRes.Reader).Decode(&rec)
			_ = objRes.Reader.Close()
			if decodeErr != nil {
				return decodeErr
			}
			allApplied := true
			for _, p := range rec.Participants {
				if !p.Applied {
					allApplied = false
					break
				}
			}
			if rec.State == "" || rec.State == core.TxnStatePending || !allApplied {
				pending++
				continue
			}
			if allApplied {
				pending++
			}
		}
		if pending == 0 {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("txn records still present: %d", pending)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func rebuildCryptoFromServerConfig(t testing.TB, cfg lockd.Config) *storage.Crypto {
	t.Helper()
	if !cfg.StorageEncryptionEnabled() {
		return nil
	}
	crypto, err := storage.NewCrypto(storage.CryptoConfig{
		Enabled:            true,
		RootKey:            cfg.MetadataRootKey,
		MetadataDescriptor: cfg.MetadataDescriptor,
		MetadataContext:    []byte(cfg.MetadataContext),
		Snappy:             cfg.StorageEncryptionSnappy,
		DisableBufferPool:  cfg.DisableKryptoPool,
	})
	if err != nil {
		t.Fatalf("build rebuild crypto: %v", err)
	}
	return crypto
}
