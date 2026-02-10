package api

// IndexFlushRequest asks the server to flush pending index segments.
type IndexFlushRequest struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Mode controls operation behavior (for example async vs wait).
	Mode string `json:"mode,omitempty"`
}

// IndexFlushResponse reports the outcome of a flush invocation.
type IndexFlushResponse struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace"`
	// Mode controls operation behavior (for example async vs wait).
	Mode string `json:"mode"`
	// Accepted reports that the server accepted the request for asynchronous processing.
	Accepted bool `json:"accepted"`
	// Flushed reports that the requested index flush completed synchronously.
	Flushed bool `json:"flushed"`
	// Pending reports that flush work remains and will complete asynchronously.
	Pending bool `json:"pending"`
	// IndexSeq reports the index sequence observed by the operation.
	IndexSeq uint64 `json:"index_seq,omitempty"`
	// FlushID identifies the asynchronous flush operation when returned.
	FlushID string `json:"flush_id,omitempty"`
}
