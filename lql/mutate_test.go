package lql

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/api"
)

const (
	financeTransactionJSON = `
{
  "transaction": {
    "id": "TRX-2025-00042",
    "batch": "AP-2025-11",
    "status": "pending",
    "amount": {
      "currency": "USD",
      "net": 12500.75,
      "fx_rate": 1.0000
    },
    "counterparty": {
      "id": "SUP-9912",
      "name": "Northwind Supplies",
      "country": "SE",
      "risk_score": 38
    },
    "entries": {
      "1000": {"type": "debit", "gl": "5000", "cost_center": "OPS", "amount": 12500.75},
      "2000": {"type": "credit", "gl": "2100", "cost_center": "OPS", "amount": 12500.75}
    },
    "approvals": {
      "required": 2,
      "completed": 1
    }
  }
}
`
	voucherDocumentJSON = `
{
  "voucher": {
    "id": "JV-2025-1101",
    "book": "GENERAL",
    "header": {
      "date": "2025-11-01",
      "period": "2025-11",
      "posted": false,
      "currency": "EUR"
    },
    "lines": {
      "10": {
        "account": "1510",
        "type": "debit",
        "amount": 3500,
        "dimensions": {"cost_center": "LON", "project": "RETROFIT"}
      },
      "20": {
        "account": "3010",
        "type": "credit",
        "amount": 3500,
        "dimensions": {"cost_center": "LON", "project": "RETROFIT"}
      }
    },
    "attachments": {"count": 2}
  }
}
`
	firmwareUpgradeJSON = `
{
  "device": {
    "id": "gw-2048",
    "fleet": "retail-pos",
    "location": {"region": "us-west", "site": "reno"},
    "firmware": {
      "channel": "stable",
      "current": {"version": "2.3.1", "build": "2025.10.29"},
      "target": {"version": "2.4.0", "build": "2025.11.05"}
    },
    "rollout": {
      "window": {"start": "2025-11-07T02:00:00Z", "end": "2025-11-07T05:00:00Z"},
      "progress": {"percent": 35, "status": "running"},
      "policy": {"max_failures": 3, "waves": 2}
    },
    "telemetry": {
      "battery_mv": 3800,
      "last_seen": "2025-11-08T04:11:00Z"
    }
  }
}
`
)

func TestParseMutationsBraceAndPointer(t *testing.T) {
	now := time.Date(2025, 10, 11, 1, 0, 0, 0, time.UTC)
	input := []string{
		"/state/progress=ready",
		"/state/metrics++",
		`/state/details{/owner="alice",/note="hi, world"}`,
		"/state/metrics=+3",
		"time:/state/updated=NOW",
		"rm:/state/legacy",
	}
	muts, err := ParseMutations(input, now)
	if err != nil {
		t.Fatalf("ParseMutations: %v", err)
	}
	if len(muts) != 7 {
		t.Fatalf("expected 7 mutations, got %d", len(muts))
	}
	assertMutation(t, muts[0], MutationSet, []string{"state", "progress"})
	assertMutation(t, muts[1], MutationIncrement, []string{"state", "metrics"})
	assertMutation(t, muts[2], MutationSet, []string{"state", "details", "owner"})
	assertMutation(t, muts[3], MutationSet, []string{"state", "details", "note"})
	assertMutation(t, muts[4], MutationIncrement, []string{"state", "metrics"})
	if muts[4].Delta != 3 {
		t.Fatalf("expected delta 3, got %v", muts[4].Delta)
	}
	assertMutation(t, muts[5], MutationSet, []string{"state", "updated"})
	if _, err := time.Parse(time.RFC3339Nano, muts[5].Value.(string)); err != nil {
		t.Fatalf("expected RFC3339 timestamp, got %v", muts[5].Value)
	}
	assertMutation(t, muts[6], MutationRemove, []string{"state", "legacy"})
}

func TestMutationsHelper(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	muts, err := Mutations(now,
		"/foo=bar,/count=1",
		"/count++",
	)
	if err != nil {
		t.Fatalf("Mutations: %v", err)
	}
	if len(muts) != 3 {
		t.Fatalf("unexpected mutation count %d", len(muts))
	}
	doc := map[string]any{}
	if err := ApplyMutations(doc, muts); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if doc["foo"] != "bar" {
		t.Fatalf("unexpected foo %v", doc["foo"])
	}
	var count float64
	switch v := doc["count"].(type) {
	case float64:
		count = v
	case int64:
		count = float64(v)
	default:
		t.Fatalf("unexpected type %T", v)
	}
	if count != 2 {
		t.Fatalf("unexpected count %v", count)
	}
}

func TestMutateHelper(t *testing.T) {
	doc := map[string]any{}
	if err := Mutate(doc, "/status=pending", "/status=done"); err != nil {
		t.Fatalf("mutate: %v", err)
	}
	if doc["status"] != "done" {
		t.Fatalf("unexpected status %v", doc["status"])
	}
	if err := Mutate(doc, "badexpr"); err == nil {
		t.Fatalf("expected error for invalid expr")
	}
}

func TestParseMutationsString(t *testing.T) {
	now := time.Date(2025, 10, 11, 1, 0, 0, 0, time.UTC)
	payload := `
/state/details{
  /owner = "alice"
  /note = "hi, world"
}
delete:/state/details/temporary
/state/count=+4
/state/count--
`
	muts, err := ParseMutationsString(payload, now)
	if err != nil {
		t.Fatalf("ParseMutationsString: %v", err)
	}
	if len(muts) != 5 {
		t.Fatalf("expected 5 mutations, got %d", len(muts))
	}
	assertMutation(t, muts[0], MutationSet, []string{"state", "details", "owner"})
	assertMutation(t, muts[1], MutationSet, []string{"state", "details", "note"})
	assertMutation(t, muts[2], MutationRemove, []string{"state", "details", "temporary"})
	assertMutation(t, muts[3], MutationIncrement, []string{"state", "count"})
	if muts[3].Delta != 4 {
		t.Fatalf("expected delta 4 got %v", muts[3].Delta)
	}
	assertMutation(t, muts[4], MutationIncrement, []string{"state", "count"})
	if muts[4].Delta != -1 {
		t.Fatalf("expected delta -1 got %v", muts[4].Delta)
	}
}

func TestParseMutationsStringError(t *testing.T) {
	now := time.Now()
	_, err := ParseMutationsString(`/state/details{/owner="alice"`, now)
	if err == nil {
		t.Fatalf("expected error for unterminated brace")
	}
}

func TestParseInlineAssignmentsNewlines(t *testing.T) {
	input := `
  owner = "alice"
  note = "hi, world"
`
	fields, err := parseInlineAssignments(input)
	if err != nil {
		t.Fatalf("parseInlineAssignments: %v", err)
	}
	if fields["owner"] != "\"alice\"" || fields["note"] != "\"hi, world\"" {
		t.Fatalf("unexpected assignments: %#v", fields)
	}
}

func TestParseInlineAssignmentsQuotedKeys(t *testing.T) {
	fields, err := parseInlineAssignments(`"hello key"="value,1",other=two`)
	if err != nil {
		t.Fatalf("parseInlineAssignments: %v", err)
	}
	if _, ok := fields["hello key"]; !ok {
		t.Fatalf("expected quoted key to be preserved without quotes: %#v", fields)
	}
	if fields["hello key"] != "\"value,1\"" {
		t.Fatalf("unexpected value: %#v", fields)
	}
	if fields["other"] != "two" {
		t.Fatalf("unexpected other: %#v", fields)
	}
}

func TestApplyMutationsJSON(t *testing.T) {
	jsonDoc := `{"state":{"count":1,"details":{"owner":"bob"}}}`
	dec := json.NewDecoder(strings.NewReader(jsonDoc))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	now := time.Date(2025, 10, 11, 2, 0, 0, 0, time.UTC)
	muts, err := ParseMutations([]string{
		"/state/count=+4",
		"/state/count--",
		`/state/details{/owner="alice",/note="updated"}`,
		"time:/state/updated=NOW",
		"rm:/state/details/legacy",
	}, now)
	if err != nil {
		t.Fatalf("ParseMutations: %v", err)
	}
	if err := ApplyMutations(doc, muts); err != nil {
		t.Fatalf("ApplyMutations: %v", err)
	}
	state := doc["state"].(map[string]any)
	if state["count"] != int64(4) {
		t.Fatalf("expected state.count=4 got %#v", state["count"])
	}
	details := state["details"].(map[string]any)
	if details["owner"] != "alice" || details["note"] != "updated" {
		t.Fatalf("unexpected details: %#v", details)
	}
	if _, ok := details["legacy"]; ok {
		t.Fatalf("expected legacy field removed: %#v", details)
	}
	if _, ok := state["updated"]; !ok {
		t.Fatalf("expected updated timestamp present")
	}
}

func TestParseMutationsQuotedPaths(t *testing.T) {
	now := time.Date(2025, 10, 11, 3, 0, 0, 0, time.UTC)
	doc := map[string]any{
		"data": map[string]any{
			"hello key": "world",
			"count":     int64(0),
		},
		"meta": map[string]any{
			"name": "helloworlder",
		},
	}
	muts, err := ParseMutations([]string{
		`/data{/hello key=Mars,/count++}`,
		`/meta{/name=hellomarser,/previous=world}`,
	}, now)
	if err != nil {
		t.Fatalf("ParseMutations: %v", err)
	}
	if err := ApplyMutations(doc, muts); err != nil {
		t.Fatalf("ApplyMutations: %v", err)
	}
	data := doc["data"].(map[string]any)
	if data["hello key"] != "Mars" {
		t.Fatalf("expected hello key Mars, got %#v", data["hello key"])
	}
	if data["count"] != int64(1) {
		t.Fatalf("expected count=1, got %#v", data["count"])
	}
	meta := doc["meta"].(map[string]any)
	if meta["name"] != "hellomarser" || meta["previous"] != "world" {
		t.Fatalf("unexpected meta: %#v", meta)
	}
}

func TestParseMutationsPointerInline(t *testing.T) {
	now := time.Date(2025, 10, 11, 3, 0, 0, 0, time.UTC)
	doc := map[string]any{
		"data": map[string]any{
			"hello key": "world",
			"count":     int64(0),
		},
		"meta": map[string]any{
			"name": "helloworlder",
		},
	}
	expr := `/data/hello key=Mars,/data/count++,/meta/name=hellomarser,/meta/previous=world`
	muts, err := ParseMutationsString(expr, now)
	if err != nil {
		t.Fatalf("ParseMutationsString: %v", err)
	}
	if err := ApplyMutations(doc, muts); err != nil {
		t.Fatalf("ApplyMutations: %v", err)
	}
	data := doc["data"].(map[string]any)
	if data["hello key"] != "Mars" || data["count"] != int64(1) {
		t.Fatalf("unexpected data: %#v", data)
	}
	meta := doc["meta"].(map[string]any)
	if meta["name"] != "hellomarser" || meta["previous"] != "world" {
		t.Fatalf("unexpected meta: %#v", meta)
	}
}

func TestSelectorFinanceTransaction(t *testing.T) {
	expr := `
and.eq{field=/transaction/status,value=pending},
and.range{field=/transaction/amount/net,gte=12000},
or.eq{field=/transaction/counterparty/country,value=SE},
or.1.eq{field=/transaction/counterparty/country,value=NO}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("ParseSelectorString: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatalf("expected selector")
	}
	if len(sel.Or) != 3 {
		t.Fatalf("expected or clauses with base selector, got %+v", sel)
	}
	base := sel.Or[0]
	if len(base.And) != 2 {
		t.Fatalf("expected base and clauses, got %+v", base)
	}
	clauses := selectorClausesByField(t, base.And)
	if eq := clauses["/transaction/status"]; eq.Eq == nil || eq.Eq.Value != "pending" {
		t.Fatalf("unexpected status clause %+v", eq)
	}
	if rng := clauses["/transaction/amount/net"]; rng.Range == nil || rng.Range.GTE == nil || *rng.Range.GTE != 12000 {
		t.Fatalf("unexpected range clause %+v", rng)
	}
	seen := make(map[string]bool)
	for _, clause := range sel.Or[1:] {
		if clause.Eq == nil || clause.Eq.Field != "/transaction/counterparty/country" {
			t.Fatalf("unexpected or clause %+v", clause)
		}
		seen[clause.Eq.Value] = true
	}
	if len(seen) != 2 || !seen["SE"] || !seen["NO"] {
		t.Fatalf("expected SE and NO branches, got %+v", seen)
	}
}

func TestFinanceTransactionMutations(t *testing.T) {
	doc := decodeJSONFixture(t, financeTransactionJSON)
	muts, err := ParseMutations([]string{
		"/transaction/status=posted",
		"/transaction/approvals/completed++",
		"/transaction/approvals/last_user=auditor-2",
		"/transaction/entries/2000/cost_center=HQ",
		"/transaction/counterparty/risk_score=+5",
	}, time.Now())
	if err != nil {
		t.Fatalf("ParseMutations: %v", err)
	}
	if err := ApplyMutations(doc, muts); err != nil {
		t.Fatalf("ApplyMutations: %v", err)
	}
	if status := getValue(t, doc, "transaction", "status"); status != "posted" {
		t.Fatalf("expected status posted, got %#v", status)
	}
	if val := numberValue(t, getValue(t, doc, "transaction", "approvals", "completed")); val != 2 {
		t.Fatalf("expected approvals 2, got %v", val)
	}
	if user := getValue(t, doc, "transaction", "approvals", "last_user"); user != "auditor-2" {
		t.Fatalf("expected last_user auditor-2 got %#v", user)
	}
	if cc := getValue(t, doc, "transaction", "entries", "2000", "cost_center"); cc != "HQ" {
		t.Fatalf("expected HQ cost center got %#v", cc)
	}
	if score := numberValue(t, getValue(t, doc, "transaction", "counterparty", "risk_score")); score != 43 {
		t.Fatalf("expected risk score 43 got %v", score)
	}
}

func TestSelectorVoucherDocument(t *testing.T) {
	expr := `
and.eq{field=/voucher/book,value=GENERAL},
and.eq{field=/voucher/header/currency,value=EUR},
and.eq{field=/voucher/header/period,value=2025-11},
and.range{field=/voucher/lines/20/amount,gte=3000},
or.eq{field=/voucher/lines/10/dimensions/cost_center,value=LON},
or.1.eq{field=/voucher/lines/10/dimensions/cost_center,value=AMS}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("ParseSelectorString: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatalf("expected selector")
	}
	if len(sel.Or) != 3 {
		t.Fatalf("expected 3 or nodes, got %+v", sel)
	}
	base := sel.Or[0]
	if len(base.And) != 4 {
		t.Fatalf("expected 4 and terms, got %+v", base)
	}
	clauses := selectorClausesByField(t, base.And)
	for _, field := range []string{"/voucher/book", "/voucher/header/currency", "/voucher/header/period"} {
		clause, ok := clauses[field]
		if !ok || clause.Eq == nil {
			t.Fatalf("missing eq clause for %s in %+v", field, clauses)
		}
	}
	if clause := clauses["/voucher/header/period"]; clause.Eq.Value != "2025-11" {
		t.Fatalf("unexpected period clause %+v", clause)
	}
	if rng := clauses["/voucher/lines/20/amount"]; rng.Range == nil || rng.Range.GTE == nil || *rng.Range.GTE != 3000 {
		t.Fatalf("unexpected range term %+v", rng)
	}
	regions := make(map[string]bool)
	for _, clause := range sel.Or[1:] {
		if clause.Eq == nil || clause.Eq.Field != "/voucher/lines/10/dimensions/cost_center" {
			t.Fatalf("unexpected region clause %+v", clause)
		}
		regions[clause.Eq.Value] = true
	}
	if len(regions) != 2 || !regions["LON"] || !regions["AMS"] {
		t.Fatalf("unexpected region set %+v", regions)
	}
}

func TestVoucherMutations(t *testing.T) {
	doc := decodeJSONFixture(t, voucherDocumentJSON)
	muts, err := ParseMutations([]string{
		"/voucher/header/posted=true",
		"/voucher/attachments/count++",
		"/voucher/lines/20/amount=+250",
		"/voucher/lines/30/account=ACC-2999",
		"/voucher/lines/30/type=credit",
		"/voucher/lines/30/amount=250",
		"/voucher/lines/30/dimensions/cost_center=HUB",
	}, time.Now())
	if err != nil {
		t.Fatalf("ParseMutations: %v", err)
	}
	if err := ApplyMutations(doc, muts); err != nil {
		t.Fatalf("ApplyMutations: %v", err)
	}
	if posted := getValue(t, doc, "voucher", "header", "posted"); posted != true {
		t.Fatalf("expected posted true, got %#v", posted)
	}
	if count := numberValue(t, getValue(t, doc, "voucher", "attachments", "count")); count != 3 {
		t.Fatalf("expected attachment count 3, got %v", count)
	}
	if amt := numberValue(t, getValue(t, doc, "voucher", "lines", "20", "amount")); amt != 3750 {
		t.Fatalf("expected line 20 amount 3750 got %v", amt)
	}
	line30 := getValue(t, doc, "voucher", "lines", "30").(map[string]any)
	if line30["account"] != "ACC-2999" || line30["type"] != "credit" {
		t.Fatalf("unexpected line30 header %#v", line30)
	}
	if amt := numberValue(t, line30["amount"]); amt != 250 {
		t.Fatalf("expected line30 amount 250 got %v", amt)
	}
	dims := line30["dimensions"].(map[string]any)
	if dims["cost_center"] != "HUB" {
		t.Fatalf("expected HUB cost center got %#v", dims["cost_center"])
	}
}

func TestSelectorIoTFirmware(t *testing.T) {
	expr := `
and.eq{field=/device/firmware/channel,value=stable},
and.eq{field=/device/fleet,value=retail-pos},
and.range{field=/device/rollout/progress/percent,gte=30},
and.range{field=/device/telemetry/battery_mv,gte=3600},
or.eq{field=/device/location/region,value=us-west},
or.1.eq{field=/device/location/region,value=ap-south}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("ParseSelectorString: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatalf("expected selector")
	}
	if len(sel.Or) != 3 {
		t.Fatalf("expected 3 or nodes, got %+v", sel)
	}
	base := sel.Or[0]
	if len(base.And) != 4 {
		t.Fatalf("expected four and clauses, got %+v", base)
	}
	clauses := selectorClausesByField(t, base.And)
	for _, field := range []string{"/device/firmware/channel", "/device/fleet"} {
		clause, ok := clauses[field]
		if !ok || clause.Eq == nil {
			t.Fatalf("missing eq clause for %s", field)
		}
	}
	if rng := clauses["/device/rollout/progress/percent"]; rng.Range == nil || rng.Range.GTE == nil || *rng.Range.GTE != 30 {
		t.Fatalf("unexpected rollout range %+v", rng)
	}
	if rng := clauses["/device/telemetry/battery_mv"]; rng.Range == nil || rng.Range.GTE == nil || *rng.Range.GTE != 3600 {
		t.Fatalf("unexpected telemetry range %+v", rng)
	}
	expectedRegions := map[string]bool{"us-west": false, "ap-south": false}
	for _, clause := range sel.Or[1:] {
		if clause.Eq == nil || clause.Eq.Field != "/device/location/region" {
			t.Fatalf("unexpected region clause %+v", clause)
		}
		if _, ok := expectedRegions[clause.Eq.Value]; ok {
			expectedRegions[clause.Eq.Value] = true
		}
	}
	for region, ok := range expectedRegions {
		if !ok {
			t.Fatalf("missing region %s in selector %+v", region, sel)
		}
	}
}

func TestIoTFirmwareMutations(t *testing.T) {
	doc := decodeJSONFixture(t, firmwareUpgradeJSON)
	muts, err := ParseMutations([]string{
		"/device/rollout/progress/percent=+15",
		"/device/rollout/progress/status=draining",
		"/device/rollout/policy/max_failures=+1",
		`/device/rollout/window/end="2025-11-07T06:00:00Z"`,
		"/device/firmware/target/version=2.4.1",
		`/device/telemetry/last_seen="2025-11-08T05:00:00Z"`,
		"/device/rollout/override_reason=site-maintenance",
	}, time.Now())
	if err != nil {
		t.Fatalf("ParseMutations: %v", err)
	}
	if err := ApplyMutations(doc, muts); err != nil {
		t.Fatalf("ApplyMutations: %v", err)
	}
	if pct := numberValue(t, getValue(t, doc, "device", "rollout", "progress", "percent")); pct != 50 {
		t.Fatalf("expected rollout percent 50 got %v", pct)
	}
	if status := getValue(t, doc, "device", "rollout", "progress", "status"); status != "draining" {
		t.Fatalf("unexpected rollout status %#v", status)
	}
	if maxFail := numberValue(t, getValue(t, doc, "device", "rollout", "policy", "max_failures")); maxFail != 4 {
		t.Fatalf("expected max_failures 4 got %v", maxFail)
	}
	if end := getValue(t, doc, "device", "rollout", "window", "end"); end != "2025-11-07T06:00:00Z" {
		t.Fatalf("unexpected window end %#v", end)
	}
	if version := getValue(t, doc, "device", "firmware", "target", "version"); version != "2.4.1" {
		t.Fatalf("unexpected target version %#v", version)
	}
	if seen := getValue(t, doc, "device", "telemetry", "last_seen"); seen != "2025-11-08T05:00:00Z" {
		t.Fatalf("unexpected last_seen %#v", seen)
	}
	if reason := getValue(t, doc, "device", "rollout", "override_reason"); reason != "site-maintenance" {
		t.Fatalf("expected override reason set, got %#v", reason)
	}
}

func TestApplyMutationsErrors(t *testing.T) {
	doc := map[string]any{"state": map[string]any{"count": "not-number"}}
	muts := []Mutation{
		{Path: []string{"state", "count"}, Kind: MutationIncrement, Delta: 1},
	}
	err := ApplyMutations(doc, muts)
	if err == nil {
		t.Fatalf("expected numeric error")
	}
}

func TestSplitPathErrors(t *testing.T) {
	if _, err := splitPath(`state.counter`); err == nil {
		t.Fatalf("expected error for legacy dotted path")
	}
	if _, err := splitPath(``); err == nil {
		t.Fatalf("expected error for empty path")
	}
}

func decodeJSONFixture(t *testing.T, payload string) map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(payload))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	return doc
}

func getValue(t *testing.T, root map[string]any, path ...string) any {
	t.Helper()
	var cur any = root
	for _, segment := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			t.Fatalf("path %v expected map, got %T", path, cur)
		}
		next, ok := m[segment]
		if !ok {
			t.Fatalf("missing segment %q in path %v", segment, path)
		}
		cur = next
	}
	return cur
}

func numberValue(t *testing.T, v any) float64 {
	t.Helper()
	switch val := v.(type) {
	case json.Number:
		f, err := val.Float64()
		if err != nil {
			t.Fatalf("parse number %v: %v", val, err)
		}
		return f
	case float32:
		return float64(val)
	case float64:
		return val
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case uint:
		return float64(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	default:
		t.Fatalf("unsupported number type %T", v)
		return 0
	}
}

func selectorClausesByField(t *testing.T, clauses []api.Selector) map[string]api.Selector {
	t.Helper()
	result := make(map[string]api.Selector)
	for _, clause := range clauses {
		switch {
		case clause.Eq != nil:
			result[clause.Eq.Field] = clause
		case clause.Range != nil:
			result[clause.Range.Field] = clause
		case clause.Exists != "":
			result[clause.Exists] = clause
		}
	}
	return result
}

func assertMutation(t *testing.T, mut Mutation, kind MutationKind, path []string) {
	t.Helper()
	if mut.Kind != kind {
		t.Fatalf("expected kind %v got %v (%+v)", kind, mut.Kind, mut)
	}
	if len(mut.Path) != len(path) {
		t.Fatalf("expected path %v got %v", path, mut.Path)
	}
	for i := range path {
		if mut.Path[i] != path[i] {
			t.Fatalf("expected path %v got %v", path, mut.Path)
		}
	}
}
