//go:build bench && !disk

package diskbench

import "testing"

func TestDiskBenchmarksRequireDiskTag(t *testing.T) {
	t.Skip("disk benchmarks require -tags 'bench disk'")
}
