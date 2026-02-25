package index

import "time"

const (
	// DefaultFlushDocs controls how many documents trigger a segment flush.
	DefaultFlushDocs = 1000
	// DefaultFlushInterval bounds how long we buffer before forcing a flush.
	DefaultFlushInterval = 30 * time.Second
	// manifestSaveMaxRetries bounds CAS retry attempts when appending segments.
	manifestSaveMaxRetries = 5
	// manifestSaveRetryDelay is the base delay between CAS retries.
	manifestSaveRetryDelay = 15 * time.Millisecond
	// IndexFormatVersionLegacy represents the pre-enrichment format.
	IndexFormatVersionLegacy = 1
	// IndexFormatVersionV2 includes document metadata in segments.
	IndexFormatVersionV2 = 2
	// IndexFormatVersionV3 includes query-excluded hints in document metadata.
	IndexFormatVersionV3 = 3
	// IndexFormatVersionV4 adds trigram text postings for contains selectors.
	IndexFormatVersionV4 = 4
)
