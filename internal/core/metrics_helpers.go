package core

import (
	"strings"

	"pkt.systems/lockd/internal/queue"
)

func metricResultLabel(err error) string {
	if err == nil {
		return "success"
	}
	return "error"
}

func leaseKindLabel(relKey string) string {
	clean := strings.TrimPrefix(relKey, "/")
	if _, ok := queue.ParseMessageLeaseKey(clean); ok {
		return "queue_message"
	}
	if _, ok := queue.ParseStateLeaseKey(clean); ok {
		return "queue_state"
	}
	return "lock"
}

func boolLabel(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
