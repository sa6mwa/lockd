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
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/jsonpointer"
	"pkt.systems/lockd/internal/search"
	scanadapter "pkt.systems/lockd/internal/search/scan"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/namespaces"
)

type parityHarness struct {
	index *Adapter
	scan  *scanadapter.Adapter
}

func TestIndexScanParitySyntheticCorpus(t *testing.T) {
	h := newParityHarness(t, buildSyntheticParityDocs(180))
	threeHundred := 300.0
	hundred := 100.0
	cases := []api.Selector{
		{Eq: &api.Term{Field: "/status", Value: "open"}},
		{Prefix: &api.Term{Field: "/owner", Value: "ali"}},
		{IPrefix: &api.Term{Field: "/owner", Value: "ALI"}},
		{Range: &api.RangeTerm{Field: "/metrics/amount", GTE: &hundred, LT: &threeHundred}},
		{In: &api.InTerm{Field: "/region", Any: []string{"us", "eu"}}},
		{Exists: "/flags/priority"},
		{Contains: &api.Term{Field: "/logs/*/message", Value: "timeout"}},
		{IContains: &api.Term{Field: "/logs/.../message", Value: "TIMEOUT"}},
		{
			And: []api.Selector{
				{Eq: &api.Term{Field: "/status", Value: "open"}},
				{Range: &api.RangeTerm{Field: "/metrics/amount", GTE: &hundred}},
			},
		},
		{
			Or: []api.Selector{
				{Eq: &api.Term{Field: "/owner", Value: "alice"}},
				{Eq: &api.Term{Field: "/owner", Value: "bob"}},
			},
		},
		{
			And: []api.Selector{
				{IContains: &api.Term{Field: "/logs/.../message", Value: "timeout"}},
				{
					Not: &api.Selector{
						Eq: &api.Term{Field: "/status", Value: "closed"},
					},
				},
			},
		},
	}
	for i, sel := range cases {
		t.Run(fmt.Sprintf("selector-%02d", i), func(t *testing.T) {
			h.assertParity(t, sel)
		})
	}
}

func TestIndexScanParityNestedRandomSelectors(t *testing.T) {
	h := newParityHarness(t, buildSyntheticParityDocs(220))
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 140; i++ {
		sel := randomSelector(rng, 0, 3)
		t.Run(fmt.Sprintf("random-%03d", i), func(t *testing.T) {
			h.assertParity(t, sel)
		})
	}
}

func TestIndexScanParityMixedFamilySelectors(t *testing.T) {
	h := newParityHarness(t, buildSyntheticParityDocs(220))
	hundred := 100.0
	cases := []api.Selector{
		{
			And: []api.Selector{
				{Eq: &api.Term{Field: "/status", Value: "open"}},
				{In: &api.InTerm{Field: "/region", Any: []string{"us", "eu"}}},
				{Exists: "/flags/priority"},
				{Range: &api.RangeTerm{Field: "/metrics/amount", GTE: &hundred}},
				{Contains: &api.Term{Field: "/logs/*/message", Value: "timeout"}},
			},
		},
		{
			And: []api.Selector{
				{IPrefix: &api.Term{Field: "/owner", Value: "A"}},
				{
					Or: []api.Selector{
						{IContains: &api.Term{Field: "/logs/.../message", Value: "timeout"}},
						{Eq: &api.Term{Field: "/status", Value: "pending"}},
					},
				},
				{
					Not: &api.Selector{
						Eq: &api.Term{Field: "/region", Value: "apac"},
					},
				},
			},
		},
		{
			Or: []api.Selector{
				{
					And: []api.Selector{
						{Eq: &api.Term{Field: "/status", Value: "closed"}},
						{Range: &api.RangeTerm{Field: "/metrics/amount", LT: &hundred}},
					},
				},
				{
					And: []api.Selector{
						{Eq: &api.Term{Field: "/status", Value: "open"}},
						{Contains: &api.Term{Field: "/logs/*/message", Value: "slow"}},
					},
				},
			},
		},
	}
	for i, sel := range cases {
		t.Run(fmt.Sprintf("mixed-%02d", i), func(t *testing.T) {
			h.assertParity(t, sel)
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
		h.assertParity(t, api.Selector{
			Eq: &api.Term{Field: "/meta/x~1y", Value: "slash-value"},
		})
	})

	edgeCases := []api.Selector{
		{Eq: &api.Term{Field: "/meta/~", Value: "x"}},
		{Contains: &api.Term{Field: "/logs/~2/message", Value: "timeout"}},
		{Exists: "/bad/~"},
	}
	for i, sel := range edgeCases {
		t.Run(fmt.Sprintf("edge-%02d", i), func(t *testing.T) {
			h.assertParity(t, sel)
		})
	}

	invalidCases := []api.Selector{
		{Eq: &api.Term{Field: "meta/status", Value: "open"}},
		{Contains: &api.Term{Field: "logs/*/message", Value: "timeout"}},
		{Exists: "flags/priority"},
	}
	for i, sel := range invalidCases {
		t.Run(fmt.Sprintf("invalid-%02d", i), func(t *testing.T) {
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

func newParityHarness(t *testing.T, docs map[string]map[string]any) parityHarness {
	t.Helper()
	ctx := context.Background()
	mem := memory.New()
	store := NewStore(mem, nil)
	segment := NewSegment(fmt.Sprintf("seg-parity-%d", time.Now().UnixNano()), time.Unix(1_700_100_000, 0))

	for key, doc := range docs {
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

	if _, _, err := store.WriteSegment(ctx, namespaces.Default, segment); err != nil {
		t.Fatalf("write segment: %v", err)
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

func randomSelector(rng *rand.Rand, depth, maxDepth int) api.Selector {
	if depth >= maxDepth {
		return randomLeafSelector(rng)
	}
	switch rng.Intn(6) {
	case 0:
		return randomLeafSelector(rng)
	case 1:
		return api.Selector{
			And: []api.Selector{
				randomSelector(rng, depth+1, maxDepth),
				randomSelector(rng, depth+1, maxDepth),
			},
		}
	case 2:
		return api.Selector{
			Or: []api.Selector{
				randomSelector(rng, depth+1, maxDepth),
				randomSelector(rng, depth+1, maxDepth),
			},
		}
	case 3:
		child := randomSelector(rng, depth+1, maxDepth)
		return api.Selector{Not: &child}
	default:
		return api.Selector{
			And: []api.Selector{
				randomLeafSelector(rng),
				{
					Or: []api.Selector{
						randomLeafSelector(rng),
						randomLeafSelector(rng),
					},
				},
			},
		}
	}
}

func randomLeafSelector(rng *rand.Rand) api.Selector {
	switch rng.Intn(8) {
	case 0:
		statuses := []string{"open", "closed", "pending"}
		return api.Selector{Eq: &api.Term{Field: "/status", Value: statuses[rng.Intn(len(statuses))]}}
	case 1:
		owners := []string{"alice", "bob", "carlos", "dina"}
		return api.Selector{Prefix: &api.Term{Field: "/owner", Value: owners[rng.Intn(len(owners))][:1]}}
	case 2:
		regions := []string{"us", "eu", "apac"}
		return api.Selector{In: &api.InTerm{Field: "/region", Any: regions[:2+rng.Intn(2)]}}
	case 3:
		minVal := float64(rng.Intn(400))
		return api.Selector{Range: &api.RangeTerm{Field: "/metrics/amount", GTE: &minVal}}
	case 4:
		return api.Selector{Exists: "/flags/priority"}
	case 5:
		return api.Selector{Contains: &api.Term{Field: "/logs/*/message", Value: "timeout"}}
	case 6:
		return api.Selector{IContains: &api.Term{Field: "/logs/.../message", Value: "TIMEOUT"}}
	default:
		return api.Selector{Eq: &api.Term{Field: "/records[]/status", Value: "open"}}
	}
}
