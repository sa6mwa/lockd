package mcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
)

type lockAcquireToolInput struct {
	Key         string `json:"key" jsonschema:"Key to lock"`
	Namespace   string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	Owner       string `json:"owner,omitempty" jsonschema:"Lock owner (defaults to oauth client id)"`
	TTLSeconds  int64  `json:"ttl_seconds,omitempty" jsonschema:"Lease TTL in seconds"`
	BlockSecond int64  `json:"block_seconds,omitempty" jsonschema:"Acquire wait: -1 no wait, 0 wait forever, >0 wait seconds"`
	IfNotExists bool   `json:"if_not_exists,omitempty" jsonschema:"Create-only acquire"`
	TxnID       string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
}

type lockAcquireToolOutput struct {
	Namespace     string `json:"namespace"`
	Key           string `json:"key"`
	LeaseID       string `json:"lease_id"`
	TxnID         string `json:"txn_id,omitempty"`
	Owner         string `json:"owner"`
	ExpiresAtUnix int64  `json:"expires_at_unix"`
	Version       int64  `json:"version"`
	StateETag     string `json:"state_etag,omitempty"`
	FencingToken  int64  `json:"fencing_token,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

func (s *server) handleLockAcquireTool(ctx context.Context, req *mcpsdk.CallToolRequest, input lockAcquireToolInput) (*mcpsdk.CallToolResult, lockAcquireToolOutput, error) {
	key := strings.TrimSpace(input.Key)
	if key == "" {
		return nil, lockAcquireToolOutput{}, fmt.Errorf("key is required")
	}
	clientID := ""
	if req != nil {
		clientID = requestClientID(req.Extra)
	}
	owner := strings.TrimSpace(input.Owner)
	if owner == "" {
		owner = defaultOwner(clientID)
	}
	ttl := input.TTLSeconds
	if ttl <= 0 {
		ttl = 30
	}
	block := input.BlockSecond
	if block == 0 {
		block = api.BlockNoWait
	}
	lease, err := s.upstream.Acquire(ctx, api.AcquireRequest{
		Namespace:   s.resolveNamespace(input.Namespace),
		Key:         key,
		TTLSeconds:  ttl,
		Owner:       owner,
		BlockSecs:   block,
		IfNotExists: input.IfNotExists,
		TxnID:       strings.TrimSpace(input.TxnID),
	})
	if err != nil {
		return nil, lockAcquireToolOutput{}, err
	}
	return nil, lockAcquireToolOutput{
		Namespace:     lease.Namespace,
		Key:           lease.Key,
		LeaseID:       lease.LeaseID,
		TxnID:         lease.TxnID,
		Owner:         lease.Owner,
		ExpiresAtUnix: lease.ExpiresAt,
		Version:       lease.Version,
		StateETag:     lease.StateETag,
		FencingToken:  lease.FencingToken,
		CorrelationID: lease.CorrelationID,
	}, nil
}

type lockKeepAliveToolInput struct {
	Key        string `json:"key" jsonschema:"Locked key"`
	Namespace  string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID    string `json:"lease_id" jsonschema:"Active lease id"`
	TTLSeconds int64  `json:"ttl_seconds" jsonschema:"TTL extension in seconds"`
	TxnID      string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
}

type lockKeepAliveToolOutput struct {
	ExpiresAtUnix int64 `json:"expires_at_unix"`
}

func (s *server) handleLockKeepAliveTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input lockKeepAliveToolInput) (*mcpsdk.CallToolResult, lockKeepAliveToolOutput, error) {
	if strings.TrimSpace(input.Key) == "" || strings.TrimSpace(input.LeaseID) == "" {
		return nil, lockKeepAliveToolOutput{}, fmt.Errorf("key and lease_id are required")
	}
	if input.TTLSeconds <= 0 {
		return nil, lockKeepAliveToolOutput{}, fmt.Errorf("ttl_seconds must be > 0")
	}
	resp, err := s.upstream.KeepAlive(ctx, api.KeepAliveRequest{
		Namespace:  s.resolveNamespace(input.Namespace),
		Key:        strings.TrimSpace(input.Key),
		LeaseID:    strings.TrimSpace(input.LeaseID),
		TTLSeconds: input.TTLSeconds,
		TxnID:      strings.TrimSpace(input.TxnID),
	})
	if err != nil {
		return nil, lockKeepAliveToolOutput{}, err
	}
	return nil, lockKeepAliveToolOutput{ExpiresAtUnix: resp.ExpiresAt}, nil
}

type lockReleaseToolInput struct {
	Key       string `json:"key" jsonschema:"Locked key"`
	Namespace string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID   string `json:"lease_id" jsonschema:"Active lease id"`
	TxnID     string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	Rollback  bool   `json:"rollback,omitempty" jsonschema:"Rollback staged changes instead of commit"`
}

type lockReleaseToolOutput struct {
	Released bool `json:"released"`
}

func (s *server) handleLockReleaseTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input lockReleaseToolInput) (*mcpsdk.CallToolResult, lockReleaseToolOutput, error) {
	if strings.TrimSpace(input.Key) == "" || strings.TrimSpace(input.LeaseID) == "" {
		return nil, lockReleaseToolOutput{}, fmt.Errorf("key and lease_id are required")
	}
	resp, err := s.upstream.Release(ctx, api.ReleaseRequest{
		Namespace: s.resolveNamespace(input.Namespace),
		Key:       strings.TrimSpace(input.Key),
		LeaseID:   strings.TrimSpace(input.LeaseID),
		TxnID:     strings.TrimSpace(input.TxnID),
		Rollback:  input.Rollback,
	})
	if err != nil {
		return nil, lockReleaseToolOutput{}, err
	}
	return nil, lockReleaseToolOutput{Released: resp.Released}, nil
}

type stateUpdateToolInput struct {
	Key           string `json:"key" jsonschema:"State key"`
	Namespace     string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID       string `json:"lease_id" jsonschema:"Active lease id"`
	TxnID         string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken  *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	IfETag        string `json:"if_etag,omitempty" jsonschema:"Conditional ETag guard"`
	IfVersion     string `json:"if_version,omitempty" jsonschema:"Conditional version guard"`
	QueryHidden   *bool  `json:"query_hidden,omitempty" jsonschema:"Optional query-hidden metadata mutation"`
	PayloadText   string `json:"payload_text,omitempty" jsonschema:"UTF-8 JSON payload text"`
	PayloadBase64 string `json:"payload_base64,omitempty" jsonschema:"Base64-encoded JSON payload bytes"`
}

type stateUpdateToolOutput struct {
	NewVersion   int64  `json:"new_version"`
	NewStateETag string `json:"new_state_etag"`
	Bytes        int64  `json:"bytes"`
	QueryHidden  *bool  `json:"query_hidden,omitempty"`
}

func (s *server) handleStateUpdateTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input stateUpdateToolInput) (*mcpsdk.CallToolResult, stateUpdateToolOutput, error) {
	if strings.TrimSpace(input.Key) == "" || strings.TrimSpace(input.LeaseID) == "" {
		return nil, stateUpdateToolOutput{}, fmt.Errorf("key and lease_id are required")
	}
	var payload []byte
	if strings.TrimSpace(input.PayloadBase64) != "" {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(input.PayloadBase64))
		if err != nil {
			return nil, stateUpdateToolOutput{}, fmt.Errorf("decode payload_base64: %w", err)
		}
		payload = decoded
	} else if strings.TrimSpace(input.PayloadText) != "" {
		payload = []byte(input.PayloadText)
	} else {
		payload = []byte("{}")
	}
	opts := lockdclient.UpdateOptions{
		Namespace:    s.resolveNamespace(input.Namespace),
		TxnID:        strings.TrimSpace(input.TxnID),
		FencingToken: input.FencingToken,
		IfETag:       strings.TrimSpace(input.IfETag),
		IfVersion:    strings.TrimSpace(input.IfVersion),
		Metadata: lockdclient.MetadataOptions{
			QueryHidden: input.QueryHidden,
			TxnID:       strings.TrimSpace(input.TxnID),
		},
	}
	resp, err := s.upstream.Update(ctx, strings.TrimSpace(input.Key), strings.TrimSpace(input.LeaseID), bytes.NewReader(payload), opts)
	if err != nil {
		return nil, stateUpdateToolOutput{}, err
	}
	return nil, stateUpdateToolOutput{
		NewVersion:   resp.NewVersion,
		NewStateETag: resp.NewStateETag,
		Bytes:        resp.BytesWritten,
		QueryHidden:  resp.Metadata.QueryHidden,
	}, nil
}

type stateMetadataToolInput struct {
	Key          string `json:"key" jsonschema:"State key"`
	Namespace    string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID      string `json:"lease_id" jsonschema:"Active lease id"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	IfETag       string `json:"if_etag,omitempty" jsonschema:"Conditional ETag guard"`
	IfVersion    string `json:"if_version,omitempty" jsonschema:"Conditional version guard"`
	QueryHidden  *bool  `json:"query_hidden" jsonschema:"Set true to hide, false to expose in queries"`
}

type stateMetadataToolOutput struct {
	Version     int64 `json:"version"`
	QueryHidden *bool `json:"query_hidden,omitempty"`
}

func (s *server) handleStateMetadataTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input stateMetadataToolInput) (*mcpsdk.CallToolResult, stateMetadataToolOutput, error) {
	if strings.TrimSpace(input.Key) == "" || strings.TrimSpace(input.LeaseID) == "" {
		return nil, stateMetadataToolOutput{}, fmt.Errorf("key and lease_id are required")
	}
	if input.QueryHidden == nil {
		return nil, stateMetadataToolOutput{}, fmt.Errorf("query_hidden is required")
	}
	opts := lockdclient.UpdateOptions{
		Namespace:    s.resolveNamespace(input.Namespace),
		TxnID:        strings.TrimSpace(input.TxnID),
		FencingToken: input.FencingToken,
		IfETag:       strings.TrimSpace(input.IfETag),
		IfVersion:    strings.TrimSpace(input.IfVersion),
		Metadata: lockdclient.MetadataOptions{
			QueryHidden: input.QueryHidden,
			TxnID:       strings.TrimSpace(input.TxnID),
		},
	}
	resp, err := s.upstream.UpdateMetadata(ctx, strings.TrimSpace(input.Key), strings.TrimSpace(input.LeaseID), opts)
	if err != nil {
		return nil, stateMetadataToolOutput{}, err
	}
	return nil, stateMetadataToolOutput{
		Version:     resp.Version,
		QueryHidden: resp.Metadata.QueryHidden,
	}, nil
}

type stateRemoveToolInput struct {
	Key          string `json:"key" jsonschema:"State key"`
	Namespace    string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID      string `json:"lease_id" jsonschema:"Active lease id"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	IfETag       string `json:"if_etag,omitempty" jsonschema:"Conditional ETag guard"`
	IfVersion    string `json:"if_version,omitempty" jsonschema:"Conditional version guard"`
}

type stateRemoveToolOutput struct {
	Removed    bool  `json:"removed"`
	NewVersion int64 `json:"new_version,omitempty"`
}

func (s *server) handleStateRemoveTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input stateRemoveToolInput) (*mcpsdk.CallToolResult, stateRemoveToolOutput, error) {
	if strings.TrimSpace(input.Key) == "" || strings.TrimSpace(input.LeaseID) == "" {
		return nil, stateRemoveToolOutput{}, fmt.Errorf("key and lease_id are required")
	}
	resp, err := s.upstream.Remove(ctx, strings.TrimSpace(input.Key), strings.TrimSpace(input.LeaseID), lockdclient.RemoveOptions{
		Namespace:    s.resolveNamespace(input.Namespace),
		TxnID:        strings.TrimSpace(input.TxnID),
		FencingToken: input.FencingToken,
		IfETag:       strings.TrimSpace(input.IfETag),
		IfVersion:    strings.TrimSpace(input.IfVersion),
	})
	if err != nil {
		return nil, stateRemoveToolOutput{}, err
	}
	return nil, stateRemoveToolOutput{Removed: resp.Removed, NewVersion: resp.NewVersion}, nil
}

type attachmentPutToolInput struct {
	Key              string `json:"key" jsonschema:"State key"`
	Namespace        string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID          string `json:"lease_id" jsonschema:"Active lease id"`
	TxnID            string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken     *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	Name             string `json:"name" jsonschema:"Attachment name"`
	ContentType      string `json:"content_type,omitempty" jsonschema:"Attachment content type"`
	MaxBytes         int64  `json:"max_bytes,omitempty" jsonschema:"Optional upload size cap"`
	PreventOverwrite bool   `json:"prevent_overwrite,omitempty" jsonschema:"Reject when attachment already exists"`
	PayloadText      string `json:"payload_text,omitempty" jsonschema:"UTF-8 payload text"`
	PayloadBase64    string `json:"payload_base64,omitempty" jsonschema:"Base64 payload bytes"`
}

type attachmentInfoOutput struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Size          int64  `json:"size"`
	ContentType   string `json:"content_type,omitempty"`
	CreatedAtUnix int64  `json:"created_at_unix,omitempty"`
	UpdatedAtUnix int64  `json:"updated_at_unix,omitempty"`
}

type attachmentPutToolOutput struct {
	Attachment attachmentInfoOutput `json:"attachment"`
	Noop       bool                 `json:"noop"`
	Version    int64                `json:"version"`
}

func (s *server) handleAttachmentPutTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input attachmentPutToolInput) (*mcpsdk.CallToolResult, attachmentPutToolOutput, error) {
	if strings.TrimSpace(input.Key) == "" || strings.TrimSpace(input.LeaseID) == "" || strings.TrimSpace(input.Name) == "" {
		return nil, attachmentPutToolOutput{}, fmt.Errorf("key, lease_id, and name are required")
	}
	var payload []byte
	if strings.TrimSpace(input.PayloadBase64) != "" {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(input.PayloadBase64))
		if err != nil {
			return nil, attachmentPutToolOutput{}, fmt.Errorf("decode payload_base64: %w", err)
		}
		payload = decoded
	} else {
		payload = []byte(input.PayloadText)
	}
	req := lockdclient.AttachRequest{
		Namespace:        s.resolveNamespace(input.Namespace),
		Key:              strings.TrimSpace(input.Key),
		LeaseID:          strings.TrimSpace(input.LeaseID),
		TxnID:            strings.TrimSpace(input.TxnID),
		FencingToken:     input.FencingToken,
		Name:             strings.TrimSpace(input.Name),
		Body:             bytes.NewReader(payload),
		ContentType:      strings.TrimSpace(input.ContentType),
		PreventOverwrite: input.PreventOverwrite,
	}
	if req.ContentType == "" {
		req.ContentType = "application/octet-stream"
	}
	if input.MaxBytes > 0 {
		maxBytes := input.MaxBytes
		req.MaxBytes = &maxBytes
	}
	resp, err := s.upstream.Attach(ctx, req)
	if err != nil {
		return nil, attachmentPutToolOutput{}, err
	}
	return nil, attachmentPutToolOutput{
		Attachment: toAttachmentInfoOutput(resp.Attachment),
		Noop:       resp.Noop,
		Version:    resp.Version,
	}, nil
}

type attachmentListToolInput struct {
	Key          string `json:"key" jsonschema:"State key"`
	Namespace    string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID      string `json:"lease_id,omitempty" jsonschema:"Optional lease id for protected listing"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	Public       bool   `json:"public,omitempty" jsonschema:"Allow public read listing"`
}

type attachmentListToolOutput struct {
	Namespace   string                 `json:"namespace"`
	Key         string                 `json:"key"`
	Attachments []attachmentInfoOutput `json:"attachments"`
}

func (s *server) handleAttachmentListTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input attachmentListToolInput) (*mcpsdk.CallToolResult, attachmentListToolOutput, error) {
	if strings.TrimSpace(input.Key) == "" {
		return nil, attachmentListToolOutput{}, fmt.Errorf("key is required")
	}
	resp, err := s.upstream.ListAttachments(ctx, lockdclient.ListAttachmentsRequest{
		Namespace:    s.resolveNamespace(input.Namespace),
		Key:          strings.TrimSpace(input.Key),
		LeaseID:      strings.TrimSpace(input.LeaseID),
		TxnID:        strings.TrimSpace(input.TxnID),
		FencingToken: input.FencingToken,
		Public:       input.Public,
	})
	if err != nil {
		return nil, attachmentListToolOutput{}, err
	}
	out := attachmentListToolOutput{
		Namespace:   resp.Namespace,
		Key:         resp.Key,
		Attachments: make([]attachmentInfoOutput, 0, len(resp.Attachments)),
	}
	for _, item := range resp.Attachments {
		out.Attachments = append(out.Attachments, toAttachmentInfoOutput(item))
	}
	return nil, out, nil
}

type attachmentGetToolInput struct {
	Key          string `json:"key" jsonschema:"State key"`
	Namespace    string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID      string `json:"lease_id,omitempty" jsonschema:"Optional lease id for protected retrieval"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	Public       bool   `json:"public,omitempty" jsonschema:"Allow public read retrieval"`
	ID           string `json:"id,omitempty" jsonschema:"Attachment id selector"`
	Name         string `json:"name,omitempty" jsonschema:"Attachment name selector"`
}

type attachmentGetToolOutput struct {
	Attachment    attachmentInfoOutput `json:"attachment"`
	PayloadBytes  int64                `json:"payload_bytes"`
	PayloadText   string               `json:"payload_text,omitempty"`
	PayloadBase64 string               `json:"payload_base64,omitempty"`
}

func (s *server) handleAttachmentGetTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input attachmentGetToolInput) (*mcpsdk.CallToolResult, attachmentGetToolOutput, error) {
	if strings.TrimSpace(input.Key) == "" {
		return nil, attachmentGetToolOutput{}, fmt.Errorf("key is required")
	}
	if strings.TrimSpace(input.ID) == "" && strings.TrimSpace(input.Name) == "" {
		return nil, attachmentGetToolOutput{}, fmt.Errorf("id or name is required")
	}
	att, err := s.upstream.GetAttachment(ctx, lockdclient.GetAttachmentRequest{
		Namespace:    s.resolveNamespace(input.Namespace),
		Key:          strings.TrimSpace(input.Key),
		LeaseID:      strings.TrimSpace(input.LeaseID),
		TxnID:        strings.TrimSpace(input.TxnID),
		FencingToken: input.FencingToken,
		Public:       input.Public,
		Selector: lockdclient.AttachmentSelector{
			ID:   strings.TrimSpace(input.ID),
			Name: strings.TrimSpace(input.Name),
		},
	})
	if err != nil {
		return nil, attachmentGetToolOutput{}, err
	}
	defer att.Close()
	payload, err := io.ReadAll(att)
	if err != nil {
		return nil, attachmentGetToolOutput{}, err
	}
	out := attachmentGetToolOutput{
		Attachment:   toAttachmentInfoOutput(att.AttachmentInfo),
		PayloadBytes: int64(len(payload)),
	}
	if len(payload) > 0 {
		out.PayloadBase64 = base64.StdEncoding.EncodeToString(payload)
		if utf8.Valid(payload) {
			out.PayloadText = string(payload)
		}
	}
	return nil, out, nil
}

type attachmentDeleteToolInput struct {
	Key          string `json:"key" jsonschema:"State key"`
	Namespace    string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID      string `json:"lease_id" jsonschema:"Active lease id"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	ID           string `json:"id,omitempty" jsonschema:"Attachment id selector"`
	Name         string `json:"name,omitempty" jsonschema:"Attachment name selector"`
}

type attachmentDeleteToolOutput struct {
	Deleted bool  `json:"deleted"`
	Version int64 `json:"version"`
}

func (s *server) handleAttachmentDeleteTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input attachmentDeleteToolInput) (*mcpsdk.CallToolResult, attachmentDeleteToolOutput, error) {
	if strings.TrimSpace(input.Key) == "" || strings.TrimSpace(input.LeaseID) == "" {
		return nil, attachmentDeleteToolOutput{}, fmt.Errorf("key and lease_id are required")
	}
	if strings.TrimSpace(input.ID) == "" && strings.TrimSpace(input.Name) == "" {
		return nil, attachmentDeleteToolOutput{}, fmt.Errorf("id or name is required")
	}
	resp, err := s.upstream.DeleteAttachment(ctx, lockdclient.DeleteAttachmentRequest{
		Namespace:    s.resolveNamespace(input.Namespace),
		Key:          strings.TrimSpace(input.Key),
		LeaseID:      strings.TrimSpace(input.LeaseID),
		TxnID:        strings.TrimSpace(input.TxnID),
		FencingToken: input.FencingToken,
		Selector: lockdclient.AttachmentSelector{
			ID:   strings.TrimSpace(input.ID),
			Name: strings.TrimSpace(input.Name),
		},
	})
	if err != nil {
		return nil, attachmentDeleteToolOutput{}, err
	}
	return nil, attachmentDeleteToolOutput{Deleted: resp.Deleted, Version: resp.Version}, nil
}

type attachmentDeleteAllToolInput struct {
	Key          string `json:"key" jsonschema:"State key"`
	Namespace    string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID      string `json:"lease_id" jsonschema:"Active lease id"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
}

type attachmentDeleteAllToolOutput struct {
	Deleted int   `json:"deleted"`
	Version int64 `json:"version"`
}

func (s *server) handleAttachmentDeleteAllTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input attachmentDeleteAllToolInput) (*mcpsdk.CallToolResult, attachmentDeleteAllToolOutput, error) {
	if strings.TrimSpace(input.Key) == "" || strings.TrimSpace(input.LeaseID) == "" {
		return nil, attachmentDeleteAllToolOutput{}, fmt.Errorf("key and lease_id are required")
	}
	resp, err := s.upstream.DeleteAllAttachments(ctx, lockdclient.DeleteAllAttachmentsRequest{
		Namespace:    s.resolveNamespace(input.Namespace),
		Key:          strings.TrimSpace(input.Key),
		LeaseID:      strings.TrimSpace(input.LeaseID),
		TxnID:        strings.TrimSpace(input.TxnID),
		FencingToken: input.FencingToken,
	})
	if err != nil {
		return nil, attachmentDeleteAllToolOutput{}, err
	}
	return nil, attachmentDeleteAllToolOutput{Deleted: resp.Deleted, Version: resp.Version}, nil
}

type namespaceGetToolInput struct {
	Namespace string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
}

type namespaceGetToolOutput struct {
	Namespace       string `json:"namespace"`
	PreferredEngine string `json:"preferred_engine"`
	FallbackEngine  string `json:"fallback_engine"`
	ETag            string `json:"etag,omitempty"`
}

func (s *server) handleNamespaceGetTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input namespaceGetToolInput) (*mcpsdk.CallToolResult, namespaceGetToolOutput, error) {
	result, err := s.upstream.GetNamespaceConfig(ctx, s.resolveNamespace(input.Namespace))
	if err != nil {
		return nil, namespaceGetToolOutput{}, err
	}
	if result.Config == nil {
		return nil, namespaceGetToolOutput{}, fmt.Errorf("namespace config not found")
	}
	return nil, namespaceGetToolOutput{
		Namespace:       result.Config.Namespace,
		PreferredEngine: result.Config.Query.PreferredEngine,
		FallbackEngine:  result.Config.Query.FallbackEngine,
		ETag:            result.ETag,
	}, nil
}

type namespaceUpdateToolInput struct {
	Namespace       string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	PreferredEngine string `json:"preferred_engine" jsonschema:"Preferred query engine"`
	FallbackEngine  string `json:"fallback_engine" jsonschema:"Fallback query engine"`
	IfMatch         string `json:"if_match,omitempty" jsonschema:"Optional If-Match ETag for CAS updates"`
}

type namespaceUpdateToolOutput struct {
	Namespace       string `json:"namespace"`
	PreferredEngine string `json:"preferred_engine"`
	FallbackEngine  string `json:"fallback_engine"`
	ETag            string `json:"etag,omitempty"`
}

func (s *server) handleNamespaceUpdateTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input namespaceUpdateToolInput) (*mcpsdk.CallToolResult, namespaceUpdateToolOutput, error) {
	ns := s.resolveNamespace(input.Namespace)
	result, err := s.upstream.UpdateNamespaceConfig(ctx, api.NamespaceConfigRequest{
		Namespace: ns,
		Query: &api.NamespaceQueryConfig{
			PreferredEngine: strings.TrimSpace(input.PreferredEngine),
			FallbackEngine:  strings.TrimSpace(input.FallbackEngine),
		},
	}, lockdclient.NamespaceConfigOptions{IfMatch: strings.TrimSpace(input.IfMatch)})
	if err != nil {
		return nil, namespaceUpdateToolOutput{}, err
	}
	if result.Config == nil {
		return nil, namespaceUpdateToolOutput{}, fmt.Errorf("namespace config not returned")
	}
	return nil, namespaceUpdateToolOutput{
		Namespace:       result.Config.Namespace,
		PreferredEngine: result.Config.Query.PreferredEngine,
		FallbackEngine:  result.Config.Query.FallbackEngine,
		ETag:            result.ETag,
	}, nil
}

type indexFlushToolInput struct {
	Namespace string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	Mode      string `json:"mode,omitempty" jsonschema:"Flush mode: async (default) or wait"`
}

type indexFlushToolOutput struct {
	Namespace string `json:"namespace"`
	Mode      string `json:"mode"`
	Accepted  bool   `json:"accepted"`
	Flushed   bool   `json:"flushed"`
	Pending   bool   `json:"pending"`
	IndexSeq  uint64 `json:"index_seq,omitempty"`
	FlushID   string `json:"flush_id,omitempty"`
}

func (s *server) handleIndexFlushTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input indexFlushToolInput) (*mcpsdk.CallToolResult, indexFlushToolOutput, error) {
	mode := strings.ToLower(strings.TrimSpace(input.Mode))
	var opts []lockdclient.FlushOption
	if mode == "wait" {
		opts = append(opts, lockdclient.WithFlushModeWait())
	} else {
		opts = append(opts, lockdclient.WithFlushModeAsync())
	}
	resp, err := s.upstream.FlushIndex(ctx, s.resolveNamespace(input.Namespace), opts...)
	if err != nil {
		return nil, indexFlushToolOutput{}, err
	}
	return nil, indexFlushToolOutput{
		Namespace: resp.Namespace,
		Mode:      resp.Mode,
		Accepted:  resp.Accepted,
		Flushed:   resp.Flushed,
		Pending:   resp.Pending,
		IndexSeq:  resp.IndexSeq,
		FlushID:   resp.FlushID,
	}, nil
}

func toAttachmentInfoOutput(info lockdclient.AttachmentInfo) attachmentInfoOutput {
	return attachmentInfoOutput{
		ID:            info.ID,
		Name:          info.Name,
		Size:          info.Size,
		ContentType:   info.ContentType,
		CreatedAtUnix: info.CreatedAtUnix,
		UpdatedAtUnix: info.UpdatedAtUnix,
	}
}
