package api

// IndexFlushRequest asks the server to flush pending index segments.
type IndexFlushRequest struct {
	Namespace string `json:"namespace,omitempty"`
	Mode      string `json:"mode,omitempty"`
}

// IndexFlushResponse reports the outcome of a flush invocation.
type IndexFlushResponse struct {
	Namespace string `json:"namespace"`
	Mode      string `json:"mode"`
	Accepted  bool   `json:"accepted"`
	Flushed   bool   `json:"flushed"`
	Pending   bool   `json:"pending"`
	IndexSeq  uint64 `json:"index_seq,omitempty"`
	FlushID   string `json:"flush_id,omitempty"`
}
