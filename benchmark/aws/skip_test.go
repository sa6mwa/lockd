//go:build bench && !aws

package awsbench

import "testing"

func TestAWSBenchmarksRequireTag(t *testing.T) {
	t.Skip("aws benchmarks require -tags 'bench aws'")
}
