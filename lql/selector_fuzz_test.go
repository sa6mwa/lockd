package lql

import "testing"

func FuzzParseSelectorString(f *testing.F) {
	seed := []string{
		"",
		"eq{field=status,value=open}",
		"and.eq{field=status,value=open},and.eq{field=owner,value=alice}",
		"or.eq{field=status,value=open},or.eq{field=status,value=processing}",
		"and.eq{field=status,value=open},or.eq{field=owner,value=\"alice\"}",
	}
	for _, s := range seed {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, expr string) {
		_, _, _ = ParseSelectorString(expr)
	})
}
