package index

import "time"

const (
	// DefaultFlushDocs controls how many documents trigger a segment flush.
	DefaultFlushDocs = 1000
	// DefaultFlushInterval bounds how long we buffer before forcing a flush.
	DefaultFlushInterval = 30 * time.Second
)
