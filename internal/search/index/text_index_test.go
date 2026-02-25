package index

import (
	"slices"
	"testing"
)

func TestSimpleTextAnalyzerTokens(t *testing.T) {
	tokens := simpleTextAnalyzer{}.Tokens("Timeout while syncing node-7")
	want := []string{"timeout", "while", "syncing", "node", "7"}
	if !slices.Equal(tokens, want) {
		t.Fatalf("unexpected tokens\ngot:  %v\nwant: %v", tokens, want)
	}
}

func TestTextIndexPolicyModeForField(t *testing.T) {
	policy := TextIndexPolicy{
		DefaultMode: TextFieldModeRaw,
		FieldModes: map[string]TextFieldMode{
			"/message": TextFieldModeTokenized,
		},
	}
	if got := policy.modeForField("/message"); got != TextFieldModeTokenized {
		t.Fatalf("modeForField(/message)=%s, want tokenized", got)
	}
	if got := policy.modeForField("/service"); got != TextFieldModeRaw {
		t.Fatalf("modeForField(/service)=%s, want raw", got)
	}
}

func TestTextIndexPolicySimpleStemming(t *testing.T) {
	policy := DefaultTextIndexPolicy()
	policy.EnableSimpleStemming = true
	toks := policy.normalized().Analyzer.Tokens("running tested boxes alarms")
	want := []string{"runn", "test", "box", "alarm"}
	if !slices.Equal(toks, want) {
		t.Fatalf("unexpected stemmed tokens\ngot:  %v\nwant: %v", toks, want)
	}
}
