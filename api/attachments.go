package api

// AttachmentInfo describes an attachment stored alongside a state key.
type AttachmentInfo struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Size          int64  `json:"size"`
	ContentType   string `json:"content_type,omitempty"`
	CreatedAtUnix int64  `json:"created_at_unix,omitempty"`
	UpdatedAtUnix int64  `json:"updated_at_unix,omitempty"`
}

// AttachResponse acknowledges a staged attachment upload.
type AttachResponse struct {
	Attachment AttachmentInfo `json:"attachment"`
	Noop       bool           `json:"noop,omitempty"`
	Version    int64          `json:"version,omitempty"`
}

// AttachmentListResponse enumerates attachments for a key.
type AttachmentListResponse struct {
	Namespace   string           `json:"namespace,omitempty"`
	Key         string           `json:"key,omitempty"`
	Attachments []AttachmentInfo `json:"attachments"`
}

// DeleteAttachmentResponse acknowledges an attachment delete request.
type DeleteAttachmentResponse struct {
	Deleted bool  `json:"deleted"`
	Version int64 `json:"version,omitempty"`
}

// DeleteAllAttachmentsResponse acknowledges a delete-all attachments request.
type DeleteAllAttachmentsResponse struct {
	Deleted int   `json:"deleted"`
	Version int64 `json:"version,omitempty"`
}
