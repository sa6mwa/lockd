package index

import "testing"

func TestDocumentAddStringPreservesRawValueAndTrigrams(t *testing.T) {
	var doc Document
	doc.AddString("/message", " Timeout ")

	raw := doc.Fields["/message"]
	if len(raw) != 1 || raw[0] != " timeout " {
		t.Fatalf("unexpected raw terms: %+v", raw)
	}
	grams := doc.Fields[containsGramField("/message")]
	if len(grams) == 0 {
		t.Fatalf("expected trigram terms")
	}
	if grams[0] != "tim" {
		t.Fatalf("unexpected first trigram: %q", grams[0])
	}
}

func TestDocumentAddStringDefaultsEmptyFieldName(t *testing.T) {
	var doc Document
	doc.AddString("", "abc")
	if len(doc.Fields["_"]) != 1 || doc.Fields["_"][0] != "abc" {
		t.Fatalf("unexpected default field terms: %+v", doc.Fields["_"])
	}
	if len(doc.Fields[containsGramField("_")]) == 0 {
		t.Fatalf("expected trigram terms on default field")
	}
}
