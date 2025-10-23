//go:build integration && nfs && lq

package nfslq

import "testing"

func RunNFSLQScenarios(t *testing.T) {
	t.Helper()
	t.Run("PollingBasics", runNFSQueuePollingBasics)
	t.Run("PollingIdleEnqueue", runNFSQueuePollingIdleEnqueueDoesNotPoll)
}
