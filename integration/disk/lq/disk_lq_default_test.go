//go:build integration && disk && lq

package disklq

import "testing"

func TestDiskLQ(t *testing.T) {
	RunDiskLQScenarios(t)
}
