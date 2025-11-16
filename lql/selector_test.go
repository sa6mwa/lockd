package lql

import (
	"net/url"
	"testing"
)

func TestParseSelectorValuesSimple(t *testing.T) {
	values := url.Values{}
	values.Set("eq.field", "/status")
	values.Set("eq.value", "open")
	sel, err := ParseSelectorValues(values)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if sel.Eq == nil || sel.Eq.Field != "/status" || sel.Eq.Value != "open" {
		t.Fatalf("unexpected selector %+v", sel)
	}
}

func TestParseSelectorValuesBrace(t *testing.T) {
	values := url.Values{}
	values.Set("and.eq{field=/status,value=open}", "")
	values.Set("or.eq{field=/owner,value=alice}", "")
	values.Set("or.1.eq{field=/owner,value=bob}", "")
	sel, err := ParseSelectorValues(values)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if sel.Or == nil || len(sel.Or) != 3 {
		t.Fatalf("unexpected selector %+v", sel)
	}
}

func TestParseSelectorString(t *testing.T) {
	expr := "eq{field=/status,value=open},or.eq{field=/owner,value=\"alice\"},or.1.eq{field=/owner,value=bob}"
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if sel.Or == nil || len(sel.Or) != 3 {
		t.Fatalf("unexpected selector %+v", sel)
	}
}

func TestParseSelectorStringWhitespace(t *testing.T) {
	expr := `and.eq{
field="/hello"
value="hi, world"},and.eq{field=/status value="okili dokili"}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() {
		t.Fatal("expected selector")
	}
	if len(sel.And) != 2 {
		t.Fatalf("unexpected selector %+v", sel)
	}
}

func TestParseSelectorStringInvalid(t *testing.T) {
	if _, err := ParseSelectorString("and.eq{field=/status,value=open"); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestParseSelectorBooleanValue(t *testing.T) {
	expr := `and.eq{field=/flag,value=true},and.eq{field=/other,value=false}`
	sel, err := ParseSelectorString(expr)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if sel.IsEmpty() || len(sel.And) != 2 {
		t.Fatalf("expected two and clauses, got %+v", sel)
	}
	expected := map[string]string{
		"/flag":  "true",
		"/other": "false",
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

func TestParseSelectorShorthand(t *testing.T) {
	t.Run("equality", func(t *testing.T) {
		sel, err := ParseSelectorString(`/status="open"`)
		if err != nil {
			t.Fatalf("parse shorthand eq: %v", err)
		}
		if sel.Eq == nil || sel.Eq.Field != "/status" || sel.Eq.Value != "open" {
			t.Fatalf("unexpected eq selector %+v", sel)
		}
	})

	t.Run("not equal", func(t *testing.T) {
		sel, err := ParseSelectorString(`/type!=critical`)
		if err != nil {
			t.Fatalf("parse shorthand !=: %v", err)
		}
		if sel.Not == nil || sel.Not.Eq == nil || sel.Not.Eq.Field != "/type" || sel.Not.Eq.Value != "critical" {
			t.Fatalf("unexpected not selector %+v", sel)
		}
	})

	t.Run("greater than", func(t *testing.T) {
		sel, err := ParseSelectorString(`/progress/count>10`)
		if err != nil {
			t.Fatalf("parse shorthand >: %v", err)
		}
		if sel.Range == nil || sel.Range.Field != "/progress/count" || sel.Range.GT == nil || *sel.Range.GT != 10 {
			t.Fatalf("unexpected range selector %+v", sel.Range)
		}
	})

	t.Run("gte with spaces", func(t *testing.T) {
		sel, err := ParseSelectorString("  /progress/count   >=   42  ")
		if err != nil {
			t.Fatalf("parse shorthand >=: %v", err)
		}
		if sel.Range == nil || sel.Range.GTE == nil || *sel.Range.GTE != 42 {
			t.Fatalf("unexpected gte selector %+v", sel.Range)
		}
	})

	t.Run("lte numeric", func(t *testing.T) {
		sel, err := ParseSelectorString(`/battery_mv<=3600`)
		if err != nil {
			t.Fatalf("parse shorthand <=: %v", err)
		}
		if sel.Range == nil || sel.Range.LTE == nil || *sel.Range.LTE != 3600 {
			t.Fatalf("unexpected lte selector %+v", sel.Range)
		}
	})

	t.Run("invalid missing value", func(t *testing.T) {
		if _, err := ParseSelectorString(`/count>=`); err == nil {
			t.Fatalf("expected error for missing value")
		}
	})
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
			_, _ = ParseSelectorString(input)
		})
	}
}
