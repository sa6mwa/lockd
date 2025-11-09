package lql

import (
	"net/url"
	"testing"
)

func TestParseSelectorValuesSimple(t *testing.T) {
	values := url.Values{}
	values.Set("eq.field", "status")
	values.Set("eq.value", "open")
	sel, found, err := ParseSelectorValues(values)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !found {
		t.Fatal("expected selector")
	}
	if sel.Eq == nil || sel.Eq.Field != "status" || sel.Eq.Value != "open" {
		t.Fatalf("unexpected selector %+v", sel)
	}
}

func TestParseSelectorValuesBrace(t *testing.T) {
	values := url.Values{}
	values.Set("and.eq{field=status,value=open}", "")
	values.Set("or.eq{field=owner,value=alice}", "")
	values.Set("or.1.eq{field=owner,value=bob}", "")
	sel, found, err := ParseSelectorValues(values)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !found {
		t.Fatal("expected selector")
	}
	if sel.Or == nil || len(sel.Or) != 3 {
		t.Fatalf("unexpected selector %+v", sel)
	}
}

func TestParseSelectorString(t *testing.T) {
	expr := "eq{field=status,value=open},or.eq{field=owner,value=\"alice\"},or.1.eq{field=owner,value=bob}"
	sel, found, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !found {
		t.Fatal("expected selector")
	}
	if sel.Or == nil || len(sel.Or) != 3 {
		t.Fatalf("unexpected selector %+v", sel)
	}
}

func TestParseSelectorStringWhitespace(t *testing.T) {
	expr := `and.eq{
field="hello"
value="hi, world"},and.eq{field=status value="okili dokili"}`
	sel, found, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !found {
		t.Fatal("expected selector")
	}
	if len(sel.And) != 2 {
		t.Fatalf("unexpected selector %+v", sel)
	}
}

func TestParseSelectorStringInvalid(t *testing.T) {
	if _, _, err := ParseSelectorString("and.eq{field=status,value=open"); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestParseSelectorBooleanValue(t *testing.T) {
	expr := `and.eq{field=flag,value=true},and.eq{field=other,value=false}`
	sel, found, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !found || len(sel.And) != 2 {
		t.Fatalf("expected two and clauses, got %+v", sel)
	}
	expected := map[string]string{
		"flag":  "true",
		"other": "false",
	}
	for _, clause := range sel.And {
		if clause.Eq == nil {
			t.Fatalf("missing eq clause: %+v", clause)
		}
		got, ok := expected[clause.Eq.Field]
		if !ok {
			t.Fatalf("unexpected field %q", clause.Eq.Field)
		}
		if clause.Eq.Value != got {
			t.Fatalf("field %q expected %q got %q", clause.Eq.Field, got, clause.Eq.Value)
		}
		delete(expected, clause.Eq.Field)
	}
	if len(expected) != 0 {
		t.Fatalf("missing clauses for %v", expected)
	}
}

func TestParseSelectorStringRegression(t *testing.T) {
	inputs := map[string]string{
		"NegativeIndex":  "And.-1",
		"GarbageUnicode": "ANd.\u0970\x8b.30zz v1\nst",
		"HugeIndex":      "ANd.066666666000",
	}
	for name, input := range inputs {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("parse panicked for %q: %v", input, r)
				}
			}()
			_, _, _ = ParseSelectorString(input)
		})
	}
}
