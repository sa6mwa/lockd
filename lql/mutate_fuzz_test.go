package lql

import (
	"testing"
	"time"
)

func FuzzParseMutationsString(f *testing.F) {
	now := time.Unix(1_750_000_000, 0)
	seeds := []string{
		`/counter=1`,
		`/state/count++`,
		`/state/count--`,
		`rm:/legacy/field`,
		`time:/state/updated=NOW`,
		`/state/details{/owner="alice",/note="hi"}`,
		`/state/strange key=value`,
		`/state{/hello key=world,/count=+2}`,
	}
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, expr string) {
		if len(expr) > 2048 {
			return
		}
		muts, err := ParseMutationsString(expr, now)
		if err != nil {
			return
		}
		doc := map[string]any{}
		_ = ApplyMutations(doc, muts)
	})
}
