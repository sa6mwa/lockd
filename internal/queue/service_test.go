package queue

import "testing"

func TestIsQueueStateKey(t *testing.T) {
	cases := []struct {
		key     string
		isState bool
	}{
		{"default/q/orders/state/msg-123", true},
		{"default/q/orders/msg/msg-123", false},
		{"default/q//state/msg-123", false},
		{"state/msg-123", false},
		{"default/q/orders/state", false},
	}
	for _, tc := range cases {
		if got := IsQueueStateKey(tc.key); got != tc.isState {
			t.Fatalf("IsQueueStateKey(%q)=%v want %v", tc.key, got, tc.isState)
		}
	}
}
