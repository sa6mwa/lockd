//go:build bench && !azure

package azurebench

import "testing"

func TestAzureBenchmarksRequireTag(t *testing.T) {
	t.Skip("azure benchmarks require -tags 'bench azure'")
}
