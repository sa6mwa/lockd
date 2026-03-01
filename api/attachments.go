package api

// AttachmentInfo describes an attachment stored alongside a state key.
type AttachmentInfo struct {
	// ID is the unique identifier for the referenced object.
	ID string `json:"id"`
	// Name is the human-readable identifier for the referenced object.
	Name string `json:"name"`
	// Size is the payload size in bytes.
	Size int64 `json:"size"`
	// PlaintextSHA256 is the SHA-256 checksum of the uploaded plaintext payload.
	PlaintextSHA256 string `json:"plaintext_sha256,omitempty"`
	// ContentType is the media type associated with the payload.
	ContentType string `json:"content_type,omitempty"`
	// CreatedAtUnix is the creation timestamp as Unix seconds.
	CreatedAtUnix int64 `json:"created_at_unix,omitempty"`
	// UpdatedAtUnix is the last update timestamp as Unix seconds.
	UpdatedAtUnix int64 `json:"updated_at_unix,omitempty"`
}

// AttachResponse acknowledges a staged attachment upload.
type AttachResponse struct {
	// Attachment contains metadata for the staged or retrieved attachment.
	Attachment AttachmentInfo `json:"attachment"`
	// Noop is true when the attach request reused existing content without creating a new revision.
	Noop bool `json:"noop,omitempty"`
	// Version is the lockd monotonic version for the target object.
	Version int64 `json:"version,omitempty"`
}

// AttachmentListResponse enumerates attachments for a key.
type AttachmentListResponse struct {
	// Namespace scopes the request or response to a lockd namespace.
	Namespace string `json:"namespace,omitempty"`
	// Key identifies the lock/state key within the namespace.
	Key string `json:"key,omitempty"`
	// Attachments enumerates attachments associated with the target key.
	Attachments []AttachmentInfo `json:"attachments"`
}

// DeleteAttachmentResponse acknowledges an attachment delete request.
type DeleteAttachmentResponse struct {
	// Deleted reports delete results for the requested attachment operation.
	Deleted bool `json:"deleted"`
	// Version is the lockd monotonic version for the target object.
	Version int64 `json:"version,omitempty"`
}

// DeleteAllAttachmentsResponse acknowledges a delete-all attachments request.
type DeleteAllAttachmentsResponse struct {
	// Deleted reports delete results for the requested attachment operation.
	Deleted int `json:"deleted"`
	// Version is the lockd monotonic version for the target object.
	Version int64 `json:"version,omitempty"`
}
