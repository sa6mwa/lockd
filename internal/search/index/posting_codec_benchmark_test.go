package index

import (
	"fmt"
	"testing"
)

func BenchmarkAdaptivePostingDecodeSelectivity(b *testing.B) {
	const universe = 100_000
	selectivities := []float64{0.01, 0.05, 0.20, 0.50, 0.90}
	for _, sel := range selectivities {
		sel := sel
		docIDs := benchDocIDsForSelectivity(universe, sel)
		posting := newAdaptivePosting(docIDs)
		name := fmt.Sprintf("sel_%02d_repr_%d", int(sel*100), posting.encoding)
		b.Run(name, func(b *testing.B) {
			var sink int
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decoded := posting.decodeInto(nil)
				sink += len(decoded)
			}
			benchDocIDSetSize = sink
		})
	}
}

func BenchmarkAdaptivePostingUnionSelectivity(b *testing.B) {
	const universe = 100_000
	selectivities := []float64{0.01, 0.05, 0.20, 0.50, 0.90}
	for _, leftSel := range selectivities {
		leftSel := leftSel
		for _, rightSel := range selectivities {
			rightSel := rightSel
			left := newAdaptivePosting(benchDocIDsForSelectivity(universe, leftSel))
			right := newAdaptivePosting(benchDocIDsForSelectivity(universe, rightSel))
			name := fmt.Sprintf("l%02d_r%02d", int(leftSel*100), int(rightSel*100))
			b.Run(name, func(b *testing.B) {
				var sink int
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					lhs := left.decodeInto(nil)
					rhs := right.decodeInto(nil)
					out := unionDocIDsInto(nil, lhs, rhs)
					sink += len(out)
				}
				benchDocIDSetSize = sink
			})
		}
	}
}

func benchDocIDsForSelectivity(universe int, selectivity float64) docIDSet {
	if universe <= 0 {
		return nil
	}
	if selectivity <= 0 {
		selectivity = 0.01
	}
	if selectivity > 1 {
		selectivity = 1
	}
	count := int(float64(universe) * selectivity)
	if count < 1 {
		count = 1
	}
	step := universe / count
	if step < 1 {
		step = 1
	}
	out := make(docIDSet, 0, count)
	for i := 0; i < universe && len(out) < count; i += step {
		out = append(out, uint32(i))
	}
	for len(out) < count {
		out = append(out, uint32(len(out)))
	}
	return out
}
