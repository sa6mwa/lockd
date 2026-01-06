package storage

import (
	"path"
	"strings"
)

// AttachmentObjectKey returns the object key for a committed attachment.
func AttachmentObjectKey(key, attachmentID string) string {
	key = strings.TrimPrefix(strings.TrimSpace(key), "/")
	attachmentID = strings.TrimSpace(attachmentID)
	return path.Join("state", key, "attachments", attachmentID)
}

// StagedAttachmentObjectKey returns the object key for a staged attachment payload.
func StagedAttachmentObjectKey(key, txnID, attachmentID string) string {
	key = strings.TrimPrefix(strings.TrimSpace(key), "/")
	txnID = strings.TrimSpace(txnID)
	attachmentID = strings.TrimSpace(attachmentID)
	return path.Join("state", key, ".staging", txnID, "attachments", attachmentID)
}

// AttachmentPrefix returns the object prefix that scopes committed attachments for a key.
func AttachmentPrefix(key string) string {
	key = strings.TrimPrefix(strings.TrimSpace(key), "/")
	return path.Join("state", key, "attachments") + "/"
}

// StagedAttachmentPrefix returns the object prefix for staged attachments in a txn.
func StagedAttachmentPrefix(key, txnID string) string {
	key = strings.TrimPrefix(strings.TrimSpace(key), "/")
	txnID = strings.TrimSpace(txnID)
	return path.Join("state", key, ".staging", txnID, "attachments") + "/"
}
