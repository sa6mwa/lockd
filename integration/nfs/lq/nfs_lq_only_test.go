//go:build integration && nfs && lq

package nfslq

import "testing"

func TestNFSLQOnly(t *testing.T) {
	RunNFSLQScenarios(t)
}
