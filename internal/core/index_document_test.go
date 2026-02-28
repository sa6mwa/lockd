package core

import (
	"strings"
	"testing"

	indexer "pkt.systems/lockd/internal/search/index"
)

func TestBuildDocumentFromJSONAddsTrigramPostings(t *testing.T) {
	doc, err := buildDocumentFromJSON("k1", strings.NewReader(`{"message":"timeout"}`))
	if err != nil {
		t.Fatalf("buildDocumentFromJSON: %v", err)
	}
	raw := doc.Fields["/message"]
	if len(raw) != 1 || raw[0] != "timeout" {
		t.Fatalf("unexpected raw message terms: %+v", raw)
	}
	gramField := "__g3__:/message"
	grams := doc.Fields[gramField]
	if len(grams) == 0 {
		t.Fatalf("expected trigram postings on %q", gramField)
	}
	seen := make(map[string]struct{}, len(grams))
	for _, gram := range grams {
		seen[gram] = struct{}{}
	}
	for _, want := range []string{"tim", "ime", "meo", "eou", "out"} {
		if _, ok := seen[want]; !ok {
			t.Fatalf("missing trigram %q in %+v", want, grams)
		}
	}
}

func TestBuildDocumentFromJSONSkipsTokenizedPostingsByDefault(t *testing.T) {
	doc, err := buildDocumentFromJSON("k1", strings.NewReader(`{"message":"Timeout while syncing node-7"}`))
	if err != nil {
		t.Fatalf("buildDocumentFromJSON: %v", err)
	}
	tokenField := "__tok__:/message"
	tokens := doc.Fields[tokenField]
	if len(tokens) != 0 {
		t.Fatalf("expected no tokenized postings on %q by default, got %+v", tokenField, tokens)
	}
}

func TestBuildDocumentFromJSONWithPolicyBothAddsTokenizedPostings(t *testing.T) {
	policy := indexer.DefaultTextIndexPolicy()
	doc, err := buildDocumentFromJSONWithPolicy("k1", strings.NewReader(`{"message":"Timeout while syncing node-7"}`), policy)
	if err != nil {
		t.Fatalf("buildDocumentFromJSONWithPolicy: %v", err)
	}
	tokenField := "__tok__:/message"
	tokens := doc.Fields[tokenField]
	if len(tokens) == 0 {
		t.Fatalf("expected tokenized postings on %q", tokenField)
	}
	seen := make(map[string]struct{}, len(tokens))
	for _, tok := range tokens {
		seen[tok] = struct{}{}
	}
	for _, want := range []string{"timeout", "while", "syncing", "node", "7"} {
		if _, ok := seen[want]; !ok {
			t.Fatalf("missing token %q in %+v", want, tokens)
		}
	}
}

func TestBuildDocumentFromJSONWithPolicyTokenizedOnly(t *testing.T) {
	policy := indexer.DefaultTextIndexPolicy()
	policy.DefaultMode = indexer.TextFieldModeRaw
	policy.FieldModes = map[string]indexer.TextFieldMode{
		"/message": indexer.TextFieldModeTokenized,
	}
	doc, err := buildDocumentFromJSONWithPolicy("k1", strings.NewReader(`{"message":"timeout while syncing"}`), policy)
	if err != nil {
		t.Fatalf("buildDocumentFromJSONWithPolicy: %v", err)
	}
	if raw := doc.Fields["/message"]; len(raw) != 0 {
		t.Fatalf("expected tokenized-only field to skip raw postings, got %+v", raw)
	}
	if grams := doc.Fields["__g3__:/message"]; len(grams) != 0 {
		t.Fatalf("expected tokenized-only field to skip trigram postings, got %+v", grams)
	}
	if tokens := doc.Fields["__tok__:/message"]; len(tokens) == 0 {
		t.Fatalf("expected tokenized postings for /message")
	}
}

func TestBuildDocumentFromJSONWithPolicyAllText(t *testing.T) {
	policy := indexer.DefaultTextIndexPolicy()
	policy.EnableAllText = true
	doc, err := buildDocumentFromJSONWithPolicy("k1", strings.NewReader(`{"message":"timeout while syncing","service":"auth-api"}`), policy)
	if err != nil {
		t.Fatalf("buildDocumentFromJSONWithPolicy: %v", err)
	}
	all := doc.Fields["__tok__:_all_text"]
	if len(all) == 0 {
		t.Fatalf("expected all-text token postings")
	}
	seen := make(map[string]struct{}, len(all))
	for _, token := range all {
		seen[token] = struct{}{}
	}
	for _, want := range []string{"timeout", "while", "syncing", "auth", "api"} {
		if _, ok := seen[want]; !ok {
			t.Fatalf("missing all-text token %q in %+v", want, all)
		}
	}
}
