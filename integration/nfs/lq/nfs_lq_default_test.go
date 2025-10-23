//go:build integration && nfs && lq

package nfslq

import "testing"

func TestNFSLQ(t *testing.T) {
	RunNFSLQScenarios(t)
}
