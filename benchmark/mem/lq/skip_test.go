//go:build bench && (!mem || !lq)

package memlqbench

import "testing"

func TestMemLQBenchmarksRequireTags(t *testing.T) {
	t.Skip("mem queue benchmarks require -tags 'bench mem lq'")
}
