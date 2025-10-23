package queue

import "testing"

func TestIsQueueStateKey(t *testing.T) {
	cases := []struct {
		key     string
		isState bool
	}{
		{"q/orders/state/msg-123", true},
		{"q/orders/msg/msg-123", false},
		{"q//state/msg-123", false},
		{"state/msg-123", false},
		{"q/orders/state", false},
	}
	for _, tc := range cases {
		if got := IsQueueStateKey(tc.key); got != tc.isState {
			t.Fatalf("IsQueueStateKey(%q)=%v want %v", tc.key, got, tc.isState)
		}
	}
}
