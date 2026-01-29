//go:build bench && !nfs

package nfsbench

import "testing"

func TestNFSBenchmarksRequireTag(t *testing.T) {
	t.Skip("nfs benchmarks require -tags 'bench nfs'")
}
