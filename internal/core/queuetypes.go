package core

import (
	"io"
	"time"
)

// QueueDequeueCommand requests deliveries from a queue.
type QueueDequeueCommand struct {
	Namespace    string
	Queue        string
	Owner        string
	TxnID        string
	Stateful     bool
	Visibility   time.Duration
	BlockSeconds int64
	PageSize     int
	StartAfter   string
	MaxConsumers int
	HasWatcher   bool
}

// QueueDelivery bundles message metadata and payload streams.
type QueueDelivery struct {
	Message            *QueueMessage
	Payload            io.ReadCloser
	PayloadContentType string
	PayloadBytes       int64
	NextCursor         string
	Finalize           func(success bool)
}

// QueueMessage mirrors api.Message but transport-neutral.
type QueueMessage struct {
	Namespace                string
	Queue                    string
	MessageID                string
	Attempts                 int
	MaxAttempts              int
	NotVisibleUntilUnix      int64
	VisibilityTimeoutSeconds int64
	Attributes               map[string]any
	PayloadContentType       string
	PayloadBytes             int64
	CorrelationID            string
	LeaseID                  string
	LeaseExpiresAtUnix       int64
	FencingToken             int64
	TxnID                    string
	MetaETag                 string
	StateETag                string
	StateLeaseID             string
	StateLeaseExpiresAtUnix  int64
	StateFencingToken        int64
	StateTxnID               string
}

// QueueDequeueResult contains deliveries and the next cursor.
type QueueDequeueResult struct {
	Deliveries []*QueueDelivery
	NextCursor string
}

// QueueAckCommand applies an acknowledgement.
type QueueAckCommand struct {
	Namespace         string
	Queue             string
	MessageID         string
	MetaETag          string
	StateETag         string
	LeaseID           string
	StateLeaseID      string
	Stateful          bool
	FencingToken      int64
	StateFencingToken int64
	TxnID             string
}

// QueueNackCommand returns a delivery to the queue.
type QueueNackCommand struct {
	Namespace         string
	Queue             string
	MessageID         string
	MetaETag          string
	StateETag         string
	LeaseID           string
	StateLeaseID      string
	Stateful          bool
	Delay             time.Duration
	LastError         any
	FencingToken      int64
	StateFencingToken int64
	TxnID             string
}

// QueueExtendCommand extends visibility for a delivery.
type QueueExtendCommand struct {
	Namespace         string
	Queue             string
	MessageID         string
	MetaETag          string
	StateETag         string
	LeaseID           string
	StateLeaseID      string
	Stateful          bool
	Visibility        time.Duration
	FencingToken      int64
	StateFencingToken int64
	TxnID             string
}
