package queue

import "strings"

// LeaseKeyParts captures the queue name and message id from a lease key.
type LeaseKeyParts struct {
	Queue string
	ID    string
}

// ParseMessageLeaseKey extracts the queue name and message id from a relative
// queue message lease key (e.g. "q/orders/msg/<id>"). Returns ok=false when the
// key does not match the expected layout.
func ParseMessageLeaseKey(rel string) (LeaseKeyParts, bool) {
	rel = strings.TrimPrefix(rel, "/")
	parts := strings.Split(rel, "/")
	if len(parts) != 4 {
		return LeaseKeyParts{}, false
	}
	if parts[0] != "q" || parts[2] != "msg" || parts[3] == "" {
		return LeaseKeyParts{}, false
	}
	return LeaseKeyParts{Queue: parts[1], ID: parts[3]}, true
}

// ParseStateLeaseKey extracts the queue name and message id from a relative
// queue state lease key (e.g. "q/orders/state/<id>").
func ParseStateLeaseKey(rel string) (LeaseKeyParts, bool) {
	rel = strings.TrimPrefix(rel, "/")
	parts := strings.Split(rel, "/")
	if len(parts) != 4 {
		return LeaseKeyParts{}, false
	}
	if parts[0] != "q" || parts[2] != "state" || parts[3] == "" {
		return LeaseKeyParts{}, false
	}
	return LeaseKeyParts{Queue: parts[1], ID: parts[3]}, true
}

// IsQueueMessageKey reports whether the relative key refers to a queue message lease.
func IsQueueMessageKey(rel string) bool {
	_, ok := ParseMessageLeaseKey(rel)
	return ok
}
