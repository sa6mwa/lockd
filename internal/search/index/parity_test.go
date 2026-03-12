package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/jsonpointer"
	"pkt.systems/lockd/internal/search"
	scanadapter "pkt.systems/lockd/internal/search/scan"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lql"
)

type parityHarness struct {
	index *Adapter
	scan  *scanadapter.Adapter
}

func TestIndexScanParitySyntheticCorpus(t *testing.T) {
	h := newParityHarness(t, buildSyntheticParityDocs(180))
	cases := []string{
		`eq{field=/status,value=open}`,
		`prefix{field=/owner,value=ali}`,
		`iprefix{field=/owner,value=ALI}`,
		`range{field=/metrics/amount,gte=100,lt=300}`,
		`in{field=/region,any=us|eu}`,
		`exists{/flags/priority}`,
		`contains{field=/logs/*/message,value=timeout}`,
		`icontains{field=/logs/.../message,value=TIMEOUT}`,
		`and.eq{field=/status,value=open},and.range{field=/metrics/amount,gte=100}`,
		`or.eq{field=/owner,value=alice},or.eq{field=/owner,value=bob}`,
		`and.icontains{field=/logs/.../message,value=timeout},and.not.eq{field=/status,value=closed}`,
	}
	for i, expr := range cases {
		t.Run(fmt.Sprintf("selector-%02d", i), func(t *testing.T) {
			h.assertParity(t, mustParseSelector(t, expr))
		})
	}
}

func TestIndexScanParityNestedRandomSelectors(t *testing.T) {
	h := newParityHarness(t, buildSyntheticParityDocs(220))
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 140; i++ {
		expr := randomSelectorExpr(rng, 0, 3)
		t.Run(fmt.Sprintf("random-%03d", i), func(t *testing.T) {
			h.assertParity(t, mustParseSelector(t, expr))
		})
	}
}

func TestIndexScanParityMixedFamilySelectors(t *testing.T) {
	h := newParityHarness(t, buildSyntheticParityDocs(220))
	cases := []string{
		`and.eq{field=/status,value=open},and.in{field=/region,any=us|eu},and.exists{/flags/priority},and.range{field=/metrics/amount,gte=100},and.contains{field=/logs/*/message,value=timeout}`,
		`and.iprefix{field=/owner,value=A},and.or.0.icontains{field=/logs/.../message,value=timeout},and.or.1.eq{field=/status,value=pending},and.not.eq{field=/region,value=apac}`,
		`or.0.eq{field=/status,value=closed},or.0.range{field=/metrics/amount,lt=100},or.1.eq{field=/status,value=open},or.1.contains{field=/logs/*/message,value=slow}`,
	}
	for i, expr := range cases {
		t.Run(fmt.Sprintf("mixed-%02d", i), func(t *testing.T) {
			h.assertParity(t, mustParseSelector(t, expr))
		})
	}
}

func TestIndexScanParityTemporalSelectors(t *testing.T) {
	now := time.Now().UTC()
	docs := map[string]map[string]any{
		"doc-date-only": {"timestamp": "2026-03-05"},
		"doc-old":       {"timestamp": "2026-03-05T10:27:59Z"},
		"doc-mid":       {"timestamp": "2026-03-05T10:28:30Z"},
		"doc-late":      {"timestamp": "2026-03-05T10:30:01Z"},
		"doc-future":    {"timestamp": now.Add(2 * time.Hour).Format(time.RFC3339Nano)},
		"doc-past":      {"timestamp": now.Add(-2 * time.Hour).Format(time.RFC3339Nano)},
		"doc-invalid":   {"timestamp": "not-a-date"},
	}
	h := newParityHarness(t, docs)
	cases := []string{
		`/timestamp>=2026-03-05T10:28:21Z,/timestamp<2026-03-05T10:30:00Z`,
		`date{f=/timestamp,a=2026-03-05,b=2026-03-06}`,
		`date{f=/timestamp,since=now}`,
	}
	for i, expr := range cases {
		t.Run(fmt.Sprintf("temporal-%02d", i), func(t *testing.T) {
			h.assertParity(t, mustParseSelector(t, expr))
		})
	}
}

func TestIndexScanParityMalformedAndEscapedPointers(t *testing.T) {
	docs := buildSyntheticParityDocs(40)
	docs["escaped-key"] = map[string]any{
		"meta": map[string]any{
			"x/y": "slash-value",
		},
		"status": "open",
	}
	h := newParityHarness(t, docs)

	t.Run("escaped-pointer", func(t *testing.T) {
		h.assertParity(t, mustParseSelector(t, `eq{field=/meta/x~1y,value=slash-value}`))
	})

	edgeCases := []string{
		`eq{field=/meta/~,value=x}`,
		`contains{field=/logs/~2/message,value=timeout}`,
		`exists{/bad/~}`,
	}
	for i, expr := range edgeCases {
		t.Run(fmt.Sprintf("edge-%02d", i), func(t *testing.T) {
			h.assertParity(t, mustParseSelector(t, expr))
		})
	}

	invalidCases := []string{
		`eq{field=meta/status,value=open}`,
		`contains{field=logs/*/message,value=timeout}`,
		`exists{flags/priority}`,
	}
	for i, expr := range invalidCases {
		t.Run(fmt.Sprintf("invalid-%02d", i), func(t *testing.T) {
			sel, err := lql.ParseSelectorString(expr)
			if err != nil {
				t.Fatalf("parse selector %q: %v", expr, err)
			}
			ctx := context.Background()
			req := search.Request{
				Namespace: namespaces.Default,
				Selector:  sel,
				Limit:     10_000,
				Engine:    search.EngineIndex,
			}
			_, errIdx := h.index.Query(ctx, req)
			req.Engine = search.EngineScan
			_, errScan := h.scan.Query(ctx, req)
			if (errIdx != nil) != (errScan != nil) {
				t.Fatalf("expected both adapters to agree on error presence: index=%v scan=%v", errIdx, errScan)
			}
			if errIdx == nil {
				t.Fatalf("expected invalid selector error")
			}
		})
	}
}

func TestIndexScanParityRecursiveNumericSegments(t *testing.T) {
	docs := buildSyntheticParityDocs(60)
	docs["doc-num-0"] = map[string]any{
		"events": map[string]any{
			"0": map[string]any{
				"message": "timeout stage zero",
				"code":    "E42",
			},
		},
	}
	docs["doc-num-10"] = map[string]any{
		"events": map[string]any{
			"10": map[string]any{
				"message": "timeout stage ten",
				"code":    "E42",
			},
		},
	}
	docs["doc-num-nested"] = map[string]any{
		"events": map[string]any{
			"group": map[string]any{
				"1": map[string]any{
					"message": "timeout nested one",
					"code":    "E43",
				},
			},
		},
	}
	h := newParityHarness(t, docs)

	cases := []string{
		`icontains{field=/events/.../message,value=TIMEOUT}`,
		`contains{field=/events/*/message,value=timeout}`,
		`eq{field=/events/.../code,value=E42}`,
	}
	for i, expr := range cases {
		t.Run(fmt.Sprintf("numeric-%02d", i), func(t *testing.T) {
			h.assertParity(t, mustParseSelector(t, expr))
		})
	}
}

func TestIndexScanParityStringTermValueIntentAfterJSONRoundTrip(t *testing.T) {
	docs := map[string]map[string]any{
		"doc-arr":     {"profile": []any{1, 2, 3}},
		"doc-str":     {"profile": "alpha"},
		"doc-missing": {"other": true},
	}
	h := newParityHarness(t, docs)
	cases := []struct {
		name string
		expr string
		want []string
	}{
		{name: "contains_omitted", expr: `contains{f=/profile}`, want: []string{"doc-arr", "doc-str"}},
		{name: "contains_explicit_empty", expr: `contains{f=/profile,v=""}`, want: []string{"doc-str"}},
		{name: "icontains_omitted", expr: `icontains{f=/profile}`, want: []string{"doc-arr", "doc-str"}},
		{name: "icontains_explicit_empty", expr: `icontains{f=/profile,v=""}`, want: []string{"doc-str"}},
		{name: "prefix_omitted", expr: `prefix{f=/profile}`, want: []string{"doc-arr", "doc-str"}},
		{name: "prefix_explicit_empty", expr: `prefix{f=/profile,v=""}`, want: []string{"doc-str"}},
		{name: "iprefix_omitted", expr: `iprefix{f=/profile}`, want: []string{"doc-arr", "doc-str"}},
		{name: "iprefix_explicit_empty", expr: `iprefix{f=/profile,v=""}`, want: []string{"doc-str"}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sel := selectorJSONRoundTrip(t, mustParseSelector(t, tc.expr))
			h.assertParity(t, sel)

			indexKeys, _ := h.collectPaged(t, sel, 16, search.EngineIndex)
			scanKeys, _ := h.collectPaged(t, sel, 16, search.EngineScan)
			sort.Strings(indexKeys)
			sort.Strings(scanKeys)
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			if !slices.Equal(indexKeys, want) {
				t.Fatalf("index keys mismatch for %q: got=%v want=%v", tc.expr, indexKeys, want)
			}
			if !slices.Equal(scanKeys, want) {
				t.Fatalf("scan keys mismatch for %q: got=%v want=%v", tc.expr, scanKeys, want)
			}
		})
	}
}

func TestIndexScanParityContainsAnyAfterJSONRoundTrip(t *testing.T) {
	docs := map[string]map[string]any{
		"doc-alpha": {"summary": "hjpijs signal"},
		"doc-beta": {
			"nested": map[string]any{
				"note": "HMM escalation",
			},
		},
		"doc-gamma": {"summary": "unrelated content"},
	}
	h := newParityHarness(t, docs)
	cases := []struct {
		name string
		expr string
		want []string
	}{
		{
			name: "contains_any_quoted",
			expr: `contains{f=/summary,a="hjpijs|missing"}`,
			want: []string{"doc-alpha"},
		},
		{
			name: "contains_any_unquoted",
			expr: `contains{f=/summary,a=hjpijs|missing}`,
			want: []string{"doc-alpha"},
		},
		{
			name: "icontains_any_recursive_quoted",
			expr: `icontains{f=/...,a="hjpijs|hmm"}`,
			want: []string{"doc-alpha", "doc-beta"},
		},
		{
			name: "icontains_any_recursive_unquoted",
			expr: `icontains{f=/...,a=hjpijs|hmm}`,
			want: []string{"doc-alpha", "doc-beta"},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			sel := selectorJSONRoundTrip(t, mustParseSelector(t, tc.expr))
			h.assertParity(t, sel)

			indexKeys, _ := h.collectPaged(t, sel, 16, search.EngineIndex)
			scanKeys, _ := h.collectPaged(t, sel, 16, search.EngineScan)
			sort.Strings(indexKeys)
			sort.Strings(scanKeys)
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			if !slices.Equal(indexKeys, want) {
				t.Fatalf("index keys mismatch for %q: got=%v want=%v", tc.expr, indexKeys, want)
			}
			if !slices.Equal(scanKeys, want) {
				t.Fatalf("scan keys mismatch for %q: got=%v want=%v", tc.expr, scanKeys, want)
			}
		})
	}
}

func TestIndexScanParityWildcardOrderingAndCursor(t *testing.T) {
	h := newParityHarness(t, buildSyntheticParityDocs(260))
	cases := []string{
		`contains{field=/logs/*/message,value=timeout}`,
		`icontains{field=/logs/.../message,value=TIMEOUT}`,
	}
	for i, expr := range cases {
		t.Run(fmt.Sprintf("ordering-%02d", i), func(t *testing.T) {
			sel := mustParseSelector(t, expr)
			indexKeysA, indexCursorsA := h.collectPaged(t, sel, 9, search.EngineIndex)
			indexKeysB, indexCursorsB := h.collectPaged(t, sel, 9, search.EngineIndex)
			scanKeys, _ := h.collectPaged(t, sel, 9, search.EngineScan)

			assertSortedUniqueKeys(t, indexKeysA)
			assertSortedUniqueKeys(t, scanKeys)
			if !slices.Equal(indexKeysA, indexKeysB) {
				t.Fatalf("index ordering not deterministic across runs\nrunA=%v\nrunB=%v", indexKeysA, indexKeysB)
			}
			if !slices.Equal(indexCursorsA, indexCursorsB) {
				t.Fatalf("index cursors not deterministic across runs\nrunA=%v\nrunB=%v", indexCursorsA, indexCursorsB)
			}
			if !slices.Equal(indexKeysA, scanKeys) {
				t.Fatalf("index/scan wildcard order mismatch\nindex=%v\nscan=%v", indexKeysA, scanKeys)
			}
		})
	}
}

func TestIndexScanParityPartialSegmentCoverage(t *testing.T) {
	docs := buildSyntheticParityDocs(180)
	keys := make([]string, 0, len(docs))
	for key := range docs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for i, key := range keys {
		doc := docs[key]
		if i%2 == 0 {
			delete(doc, "logs")
		}
		if i%3 == 0 {
			delete(doc, "flags")
		}
		if i%5 == 0 {
			delete(doc, "records")
		}
	}
	h := newParityHarnessSegmented(t, docs, 5)

	cases := []string{
		`exists{/logs/a/message}`,
		`contains{field=/logs/*/message,value=timeout}`,
		`exists{/flags/priority}`,
		`and.eq{field=/status,value=open},and.exists{/logs/a/message}`,
	}
	for i, expr := range cases {
		t.Run(fmt.Sprintf("partial-%02d", i), func(t *testing.T) {
			h.assertParity(t, mustParseSelector(t, expr))
		})
	}
}

func (h parityHarness) assertParity(t *testing.T, selector api.Selector) {
	t.Helper()
	ctx := context.Background()
	req := search.Request{
		Namespace: namespaces.Default,
		Selector:  selector,
		Limit:     10_000,
		Engine:    search.EngineIndex,
	}
	indexRes, indexErr := h.index.Query(ctx, req)
	req.Engine = search.EngineScan
	scanRes, scanErr := h.scan.Query(ctx, req)
	if (indexErr != nil) != (scanErr != nil) {
		t.Fatalf("mismatched error behavior: index=%v scan=%v", indexErr, scanErr)
	}
	if indexErr != nil || scanErr != nil {
		if !errors.Is(indexErr, scanErr) && indexErr.Error() != scanErr.Error() {
			t.Fatalf("mismatched errors: index=%v scan=%v", indexErr, scanErr)
		}
		return
	}
	got := append([]string(nil), indexRes.Keys...)
	want := append([]string(nil), scanRes.Keys...)
	sort.Strings(got)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Fatalf("selector parity mismatch\nindex=%v\nscan=%v", got, want)
	}
}

func (h parityHarness) collectPaged(t *testing.T, selector api.Selector, limit int, engine search.EngineHint) ([]string, []string) {
	t.Helper()
	ctx := context.Background()
	cursor := ""
	keys := make([]string, 0, 128)
	cursors := make([]string, 0, 16)
	seen := make(map[string]struct{})
	for i := 0; i < 512; i++ {
		req := search.Request{
			Namespace: namespaces.Default,
			Selector:  selector,
			Limit:     limit,
			Cursor:    cursor,
			Engine:    engine,
		}
		var (
			res search.Result
			err error
		)
		switch engine {
		case search.EngineIndex:
			res, err = h.index.Query(ctx, req)
		case search.EngineScan:
			res, err = h.scan.Query(ctx, req)
		default:
			t.Fatalf("unsupported engine %q", engine)
		}
		if err != nil {
			t.Fatalf("paged query failed for %q: %v", engine, err)
		}
		if len(res.Keys) == 0 && res.Cursor != "" {
			t.Fatalf("cursor %q returned without keys for %q", res.Cursor, engine)
		}
		for _, key := range res.Keys {
			if _, ok := seen[key]; ok {
				t.Fatalf("duplicate key %q in paged results for %q", key, engine)
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
		if res.Cursor == "" {
			return keys, cursors
		}
		cursors = append(cursors, res.Cursor)
		cursor = res.Cursor
	}
	t.Fatalf("paged query exceeded max page count for %q", engine)
	return nil, nil
}

func selectorJSONRoundTrip(t *testing.T, sel api.Selector) api.Selector {
	t.Helper()
	encoded, err := json.Marshal(sel)
	if err != nil {
		t.Fatalf("marshal selector: %v", err)
	}
	var out api.Selector
	if err := json.Unmarshal(encoded, &out); err != nil {
		t.Fatalf("unmarshal selector: %v", err)
	}
	return out
}

func assertSortedUniqueKeys(t *testing.T, keys []string) {
	t.Helper()
	if !slices.IsSorted(keys) {
		t.Fatalf("keys are not sorted: %v", keys)
	}
	for i := 1; i < len(keys); i++ {
		if keys[i-1] == keys[i] {
			t.Fatalf("duplicate adjacent key %q in sorted keys", keys[i])
		}
	}
}

func newParityHarness(t *testing.T, docs map[string]map[string]any) parityHarness {
	return newParityHarnessSegmented(t, docs, 1)
}

func newParityHarnessSegmented(t *testing.T, docs map[string]map[string]any, segmentCount int) parityHarness {
	t.Helper()
	if segmentCount <= 0 {
		segmentCount = 1
	}
	ctx := context.Background()
	mem := memory.New()
	store := NewStore(mem, nil)
	seed := time.Now().UnixNano()
	segments := make([]*Segment, 0, segmentCount)
	for i := 0; i < segmentCount; i++ {
		segments = append(segments, NewSegment(
			fmt.Sprintf("seg-parity-%d-%d", seed, i),
			time.Unix(1_700_100_000+int64(i), 0),
		))
	}

	keys := make([]string, 0, len(docs))
	for key := range docs {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for i, key := range keys {
		doc := docs[key]
		payload, err := json.Marshal(doc)
		if err != nil {
			t.Fatalf("marshal %s: %v", key, err)
		}
		stateRes, err := mem.WriteState(ctx, namespaces.Default, key, bytes.NewReader(payload), storage.PutStateOptions{})
		if err != nil {
			t.Fatalf("write state %s: %v", key, err)
		}
		meta := &storage.Meta{
			Version:             1,
			PublishedVersion:    1,
			StateETag:           stateRes.NewETag,
			StatePlaintextBytes: stateRes.BytesWritten,
			StateDescriptor:     append([]byte(nil), stateRes.Descriptor...),
		}
		if _, err := mem.StoreMeta(ctx, namespaces.Default, key, meta, ""); err != nil {
			t.Fatalf("store meta %s: %v", key, err)
		}

		terms, err := indexTermsFromJSON(payload)
		if err != nil {
			t.Fatalf("index terms %s: %v", key, err)
		}
		segment := segments[i%len(segments)]
		for field, fieldTerms := range terms {
			block := segment.Fields[field]
			if block.Postings == nil {
				block.Postings = make(map[string][]string)
			}
			for _, term := range fieldTerms {
				block.Postings[term] = append(block.Postings[term], key)
			}
			segment.Fields[field] = block
		}
	}

	manifest := NewManifest()
	manifest.Seq = 1
	manifest.Format = IndexFormatVersionV4
	refs := make([]SegmentRef, 0, len(segments))
	for _, segment := range segments {
		if segment.DocCount() == 0 {
			continue
		}
		if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
			t.Fatalf("write segment %s: %v", segment.ID, err)
		}
		refs = append(refs, SegmentRef{
			ID:        segment.ID,
			CreatedAt: segment.CreatedAt,
			DocCount:  segment.DocCount(),
		})
		if segment.CreatedAt.After(manifest.UpdatedAt) {
			manifest.UpdatedAt = segment.CreatedAt
		}
	}
	if len(refs) == 0 {
		t.Fatalf("expected at least one populated segment")
	}
	manifest.Shards[0] = &Shard{ID: 0, Segments: refs}
	if _, err := store.SaveManifest(ctx, namespaces.Default, manifest, ""); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	indexAdapter, err := NewAdapter(AdapterConfig{Store: store})
	if err != nil {
		t.Fatalf("new index adapter: %v", err)
	}
	scanAdapter, err := scanadapter.New(scanadapter.Config{Backend: mem})
	if err != nil {
		t.Fatalf("new scan adapter: %v", err)
	}
	return parityHarness{index: indexAdapter, scan: scanAdapter}
}

func indexTermsFromJSON(payload []byte) (map[string][]string, error) {
	var raw any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return nil, err
	}
	doc := Document{Key: "doc"}
	var walk func(path string, value any)
	walk = func(path string, value any) {
		switch v := value.(type) {
		case map[string]any:
			keys := make([]string, 0, len(v))
			for k := range v {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, key := range keys {
				walk(jsonpointer.Join(path, key), v[key])
			}
		case []any:
			for _, item := range v {
				walk(path, item)
			}
		case string:
			doc.AddString(path, v)
		case json.Number:
			doc.AddTerm(path, v.String())
		case bool:
			if v {
				doc.AddTerm(path, "true")
			} else {
				doc.AddTerm(path, "false")
			}
		}
	}
	walk("", raw)
	return doc.Fields, nil
}

func buildSyntheticParityDocs(count int) map[string]map[string]any {
	docs := make(map[string]map[string]any, count)
	statuses := []string{"open", "closed", "pending"}
	owners := []string{"alice", "bob", "carlos", "dina"}
	regions := []string{"us", "eu", "apac"}
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("doc-%04d", i)
		status := statuses[i%len(statuses)]
		owner := owners[i%len(owners)]
		region := regions[i%len(regions)]
		msgA := "normal operation"
		msgB := "all good"
		if i%4 == 0 {
			msgA = "timeout while syncing"
		}
		if i%7 == 0 {
			msgB = "slow link timeout"
		}
		doc := map[string]any{
			"status": status,
			"owner":  owner,
			"region": region,
			"metrics": map[string]any{
				"amount": i*11 + 5,
			},
			"flags": map[string]any{
				"priority": i%5 == 0,
			},
			"logs": map[string]any{
				"a": map[string]any{
					"message": msgA,
				},
				"b": map[string]any{
					"message": msgB,
				},
			},
			"records": []any{
				map[string]any{"status": status},
				map[string]any{"status": statuses[(i+1)%len(statuses)]},
			},
		}
		docs[key] = doc
	}
	return docs
}

func randomSelectorExpr(rng *rand.Rand, depth, maxDepth int) string {
	if depth >= maxDepth {
		return randomLeafSelectorExpr(rng)
	}
	switch rng.Intn(6) {
	case 0:
		return randomLeafSelectorExpr(rng)
	case 1:
		return aggregateSelectorExpr("and",
			randomSelectorExpr(rng, depth+1, maxDepth),
			randomSelectorExpr(rng, depth+1, maxDepth),
		)
	case 2:
		return aggregateSelectorExpr("or",
			randomSelectorExpr(rng, depth+1, maxDepth),
			randomSelectorExpr(rng, depth+1, maxDepth),
		)
	case 3:
		return prefixSelectorExpr("not", randomSelectorExpr(rng, depth+1, maxDepth))
	default:
		return aggregateSelectorExpr("and",
			randomLeafSelectorExpr(rng),
			aggregateSelectorExpr("or", randomLeafSelectorExpr(rng), randomLeafSelectorExpr(rng)),
		)
	}
}

func randomLeafSelectorExpr(rng *rand.Rand) string {
	switch rng.Intn(10) {
	case 0:
		statuses := []string{"open", "closed", "pending"}
		return fmt.Sprintf(`eq{field=/status,value=%s}`, statuses[rng.Intn(len(statuses))])
	case 1:
		owners := []string{"alice", "bob", "carlos", "dina"}
		return fmt.Sprintf(`prefix{field=/owner,value=%s}`, owners[rng.Intn(len(owners))][:1])
	case 2:
		regions := []string{"us", "eu", "apac"}
		if rng.Intn(2) == 0 {
			return fmt.Sprintf(`in{field=/region,any=%s}`, strings.Join(regions[:2+rng.Intn(2)], "|"))
		}
		return fmt.Sprintf(`in{field=/region,any="%s"}`, strings.Join(regions[:2+rng.Intn(2)], "|"))
	case 3:
		minVal := float64(rng.Intn(400))
		return fmt.Sprintf(`range{field=/metrics/amount,gte=%s}`, strconv.FormatFloat(minVal, 'f', -1, 64))
	case 4:
		return `exists{/flags/priority}`
	case 5:
		return `contains{field=/logs/*/message,value=timeout}`
	case 6:
		return `icontains{field=/logs/.../message,value=TIMEOUT}`
	case 7:
		if rng.Intn(2) == 0 {
			return `contains{field=/logs/*/message,any=timeout|slow}`
		}
		return `contains{field=/logs/*/message,any="timeout|slow"}`
	case 8:
		if rng.Intn(2) == 0 {
			return `icontains{field=/logs/.../message,any=TIMEOUT|SLOW}`
		}
		return `icontains{field=/logs/.../message,any="TIMEOUT|SLOW"}`
	default:
		return `eq{field=/records[]/status,value=open}`
	}
}

func aggregateSelectorExpr(op string, exprs ...string) string {
	clauses := make([]string, 0, len(exprs)*2)
	for i, expr := range exprs {
		clauses = append(clauses, splitAndPrefixSelectorExpr(op+"."+strconv.Itoa(i), expr)...)
	}
	return strings.Join(clauses, ",")
}

func prefixSelectorExpr(prefix, expr string) string {
	return strings.Join(splitAndPrefixSelectorExpr(prefix, expr), ",")
}

func splitAndPrefixSelectorExpr(prefix, expr string) []string {
	tokens := splitSelectorExprTokens(expr)
	out := make([]string, 0, len(tokens))
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		out = append(out, prefix+"."+token)
	}
	return out
}

func splitSelectorExprTokens(expr string) []string {
	var (
		tokens   []string
		start    int
		depth    int
		inQuotes bool
		quote    rune
	)
	for i, r := range expr {
		switch r {
		case '\'', '"':
			if !inQuotes {
				inQuotes = true
				quote = r
			} else if quote == r {
				inQuotes = false
				quote = 0
			}
		case '{':
			if !inQuotes {
				depth++
			}
		case '}':
			if !inQuotes && depth > 0 {
				depth--
			}
		case ',', '\n':
			if !inQuotes && depth == 0 {
				tokens = append(tokens, expr[start:i])
				start = i + 1
			}
		}
	}
	if start <= len(expr) {
		tokens = append(tokens, expr[start:])
	}
	return tokens
}
