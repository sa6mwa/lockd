//go:build integration && disk && lq

package disklq

import "testing"

func TestDiskLQOnly(t *testing.T) {
	RunDiskLQScenarios(t)
}
