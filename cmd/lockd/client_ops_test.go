package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestParseMutations(t *testing.T) {
	now := time.Date(2025, 10, 11, 1, 0, 0, 0, time.UTC)
	muts, err := parseMutations([]string{
		"foo.bar=42",
		"foo.enabled=true",
		"path.value=hello",
		"path.list++",
		"path.sum=-3",
		"time:meta.timestamp=NOW",
	}, now)
	if err != nil {
		t.Fatalf("parseMutations: %v", err)
	}
	if len(muts) != 6 {
		t.Fatalf("expected 6 mutations, got %d", len(muts))
	}
	if muts[0].Kind != mutationSet || muts[0].Value != int64(42) {
		t.Fatalf("unexpected mutation[0]: %+v", muts[0])
	}
	if muts[3].Kind != mutationIncrement || muts[3].Delta != 1 {
		t.Fatalf("unexpected mutation[3]: %+v", muts[3])
	}
	if muts[4].Kind != mutationIncrement || muts[4].Delta != -3 {
		t.Fatalf("unexpected mutation[4]: %+v", muts[4])
	}
	if s, ok := muts[5].Value.(string); !ok || s == "" {
		t.Fatalf("expected timestamp string, got %#v", muts[5].Value)
	}
}

func TestApplyMutations(t *testing.T) {
	doc := map[string]any{}
	muts, err := parseMutations([]string{
		"counter=1",
		"counter++",
		"nested.value=hello",
		"nested.answer=41",
		"nested.answer=+1",
	}, time.Now())
	if err != nil {
		t.Fatalf("parseMutations: %v", err)
	}
	if err := applyMutations(doc, muts); err != nil {
		t.Fatalf("applyMutations: %v", err)
	}
	if v := doc["counter"]; v != int64(2) {
		t.Fatalf("expected counter=2 got %#v", v)
	}
	nested := doc["nested"].(map[string]any)
	if nested["value"] != "hello" {
		t.Fatalf("unexpected nested value: %#v", nested["value"])
	}
	if nested["answer"] != int64(42) {
		t.Fatalf("expected answer=42 got %#v", nested["answer"])
	}
}

func TestFormatStatePayload(t *testing.T) {
	jsonPayload := []byte(`{"foo":"bar","n":1}`)
	out, err := formatStatePayload(jsonPayload, stateFormatJSON)
	if err != nil {
		t.Fatalf("formatStatePayload json: %v", err)
	}
	if !bytes.Contains(out, []byte("\n")) {
		t.Fatalf("expected pretty JSON output, got %q", string(out))
	}
	yamlOut, err := formatStatePayload(jsonPayload, stateFormatYAML)
	if err != nil {
		t.Fatalf("formatStatePayload yaml: %v", err)
	}
	if !bytes.Contains(yamlOut, []byte("foo: bar")) {
		t.Fatalf("expected yaml output, got %q", string(yamlOut))
	}
}

func TestEnsureJSONBytes(t *testing.T) {
	payload := []byte(" {\"foo\": 1 } ")
	out, err := ensureJSONBytes(payload)
	if err != nil {
		t.Fatalf("ensureJSONBytes: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if doc["foo"] != float64(1) && doc["foo"] != int64(1) {
		t.Fatalf("unexpected foo: %#v", doc["foo"])
	}
}
