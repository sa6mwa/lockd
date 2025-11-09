package api

import "testing"

func TestTermUnmarshalBool(t *testing.T) {
	input := []byte(`{"field":"flag","value":true}`)
	var term Term
	if err := term.UnmarshalJSON(input); err != nil {
		t.Fatalf("unmarshal term: %v", err)
	}
	if term.Field != "flag" || term.Value != "true" {
		t.Fatalf("unexpected term %+v", term)
	}
}

