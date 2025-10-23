//go:build integration && disk && lq

package disklq

import "testing"

func RunDiskLQScenarios(t *testing.T) {
	t.Helper()
	t.Run("FSNotifyBasics", runDiskQueueFSNotifyBasics)
	t.Run("FSNotifyIdleEnqueue", runDiskQueueFSNotifyIdleEnqueueDoesNotPoll)
	t.Run("PollingBasics", runDiskQueuePollingBasics)
	t.Run("PollingIdleEnqueue", runDiskQueuePollingIdleEnqueueDoesNotPoll)
	t.Run("PollingMultiConsumerContention", runDiskQueueMultiConsumerContention)
	t.Run("PollingNackVisibilityDelay", runDiskQueueNackVisibilityDelay)
	t.Run("PollingLeaseTimeoutHandoff", runDiskQueueLeaseTimeoutHandoff)
	t.Run("PollingMultiServerRouting", runDiskQueueMultiServerRouting)
	t.Run("PollingMultiServerFailoverClient", runDiskQueueMultiServerFailoverClient)
	t.Run("PollingHighFanInFanOutSingleServer", runDiskQueueHighFanInFanOutSingleServer)
	t.Run("SubscribeBasics", runDiskQueueSubscribeBasics)
	t.Run("SubscribeWithState", runDiskQueueSubscribeWithState)
}
