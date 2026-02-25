package core

import (
	"strings"
	"testing"
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
