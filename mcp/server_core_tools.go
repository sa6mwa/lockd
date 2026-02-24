package mcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
)

const (
	attachmentPutModeCreate  = "create"
	attachmentPutModeUpsert  = "upsert"
	attachmentPutModeReplace = "replace"
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
	IfVersion     *int64 `json:"if_version,omitempty" jsonschema:"Conditional version guard"`
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
	if strings.TrimSpace(input.PayloadBase64) != "" && input.PayloadText != "" {
		return nil, stateUpdateToolOutput{}, fmt.Errorf("payload_text and payload_base64 are mutually exclusive")
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
	if err := validateInlinePayloadBytes(int64(len(payload)), s.cfg.InlineMaxBytes, toolStateUpdate, toolStateWriteStreamBegin); err != nil {
		return nil, stateUpdateToolOutput{}, err
	}
	opts := lockdclient.UpdateOptions{
		Namespace:    s.resolveNamespace(input.Namespace),
		TxnID:        strings.TrimSpace(input.TxnID),
		FencingToken: input.FencingToken,
		IfETag:       strings.TrimSpace(input.IfETag),
		IfVersion:    input.IfVersion,
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

type stateStreamToolInput struct {
	Key       string `json:"key" jsonschema:"State key"`
	Namespace string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	Public    *bool  `json:"public,omitempty" jsonschema:"Read mode selector: true for public read (default), false for lease-bound read"`
	LeaseID   string `json:"lease_id,omitempty" jsonschema:"Lease ID required when public=false"`
}

type stateStreamToolOutput struct {
	Namespace             string `json:"namespace"`
	Key                   string `json:"key"`
	Found                 bool   `json:"found"`
	ETag                  string `json:"etag,omitempty"`
	Version               int64  `json:"version,omitempty"`
	DownloadURL           string `json:"download_url,omitempty"`
	DownloadMethod        string `json:"download_method,omitempty"`
	DownloadExpiresAtUnix int64  `json:"download_expires_at_unix,omitempty"`
}

func (s *server) handleStateStreamTool(ctx context.Context, req *mcpsdk.CallToolRequest, input stateStreamToolInput) (*mcpsdk.CallToolResult, stateStreamToolOutput, error) {
	if req == nil || req.Session == nil {
		return nil, stateStreamToolOutput{}, fmt.Errorf("state streaming requires an active MCP session")
	}
	key := strings.TrimSpace(input.Key)
	if key == "" {
		return nil, stateStreamToolOutput{}, fmt.Errorf("key is required")
	}
	leaseID := strings.TrimSpace(input.LeaseID)
	publicRead := resolvePublicReadMode(input.Public)
	if publicRead && leaseID != "" {
		return nil, stateStreamToolOutput{}, fmt.Errorf("lease_id must be empty when public=true")
	}
	if !publicRead && leaseID == "" {
		return nil, stateStreamToolOutput{}, fmt.Errorf("lease_id is required when public=false")
	}

	opts := []lockdclient.GetOption{}
	if ns := strings.TrimSpace(input.Namespace); ns != "" {
		opts = append(opts, lockdclient.WithGetNamespace(ns))
	}
	if leaseID != "" {
		opts = append(opts, lockdclient.WithGetLeaseID(leaseID))
	}
	if !publicRead {
		opts = append(opts, lockdclient.WithGetPublicDisabled(true))
	}
	transferCtx, cancelTransfer := context.WithCancel(context.Background())
	resp, err := s.upstream.Get(transferCtx, key, opts...)
	if err != nil {
		cancelTransfer()
		return nil, stateStreamToolOutput{}, err
	}

	out := stateStreamToolOutput{
		Namespace: resp.Namespace,
		Key:       resp.Key,
		Found:     resp.HasState,
		ETag:      resp.ETag,
	}
	if ver := strings.TrimSpace(resp.Version); ver != "" {
		parsed, parseErr := strconv.ParseInt(ver, 10, 64)
		if parseErr != nil {
			_ = resp.Close()
			cancelTransfer()
			return nil, stateStreamToolOutput{}, fmt.Errorf("invalid upstream version %q: %w", ver, parseErr)
		}
		out.Version = parsed
	}
	if !resp.HasState {
		_ = resp.Close()
		cancelTransfer()
		return nil, out, nil
	}
	reg, err := s.ensureTransferManager().RegisterDownload(req.Session, resp.Reader(), transferDownloadRequest{
		ContentType: "application/json",
		Cleanup:     cancelTransfer,
	})
	if err != nil {
		_ = resp.Close()
		cancelTransfer()
		return nil, stateStreamToolOutput{}, err
	}
	out.DownloadURL = s.transferURL(reg.ID)
	out.DownloadMethod = reg.Method
	out.DownloadExpiresAtUnix = reg.ExpiresAtUnix
	return nil, out, nil
}

type stateMetadataToolInput struct {
	Key          string `json:"key" jsonschema:"State key"`
	Namespace    string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID      string `json:"lease_id" jsonschema:"Active lease id"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	IfETag       string `json:"if_etag,omitempty" jsonschema:"Conditional ETag guard"`
	IfVersion    *int64 `json:"if_version,omitempty" jsonschema:"Conditional version guard"`
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
		IfVersion:    input.IfVersion,
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
	IfVersion    *int64 `json:"if_version,omitempty" jsonschema:"Conditional version guard"`
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
		IfVersion:    input.IfVersion,
	})
	if err != nil {
		return nil, stateRemoveToolOutput{}, err
	}
	return nil, stateRemoveToolOutput{Removed: resp.Removed, NewVersion: resp.NewVersion}, nil
}

type attachmentInfoOutput struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Size            int64  `json:"size"`
	PlaintextSHA256 string `json:"plaintext_sha256,omitempty"`
	ContentType     string `json:"content_type,omitempty"`
	CreatedAtUnix   int64  `json:"created_at_unix,omitempty"`
	UpdatedAtUnix   int64  `json:"updated_at_unix,omitempty"`
}

type attachmentPutToolOutput struct {
	Attachment attachmentInfoOutput `json:"attachment"`
	Noop       bool                 `json:"noop"`
	Version    int64                `json:"version"`
}

type attachmentPutToolInput struct {
	Key           string `json:"key" jsonschema:"State key"`
	Namespace     string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID       string `json:"lease_id" jsonschema:"Active lease id"`
	TxnID         string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken  *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	Name          string `json:"name" jsonschema:"Attachment name"`
	ContentType   string `json:"content_type,omitempty" jsonschema:"Attachment content type"`
	MaxBytes      int64  `json:"max_bytes,omitempty" jsonschema:"Optional upload size cap"`
	Mode          string `json:"mode,omitempty" jsonschema:"Write mode: create (default), upsert, or replace"`
	PayloadText   string `json:"payload_text,omitempty" jsonschema:"UTF-8 payload text"`
	PayloadBase64 string `json:"payload_base64,omitempty" jsonschema:"Base64-encoded payload bytes"`
}

func (s *server) handleAttachmentPutTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input attachmentPutToolInput) (*mcpsdk.CallToolResult, attachmentPutToolOutput, error) {
	key := strings.TrimSpace(input.Key)
	leaseID := strings.TrimSpace(input.LeaseID)
	name := strings.TrimSpace(input.Name)
	if key == "" || leaseID == "" || name == "" {
		return nil, attachmentPutToolOutput{}, fmt.Errorf("key, lease_id, and name are required")
	}
	if strings.TrimSpace(input.PayloadBase64) != "" && input.PayloadText != "" {
		return nil, attachmentPutToolOutput{}, fmt.Errorf("payload_text and payload_base64 are mutually exclusive")
	}
	if input.MaxBytes < 0 {
		return nil, attachmentPutToolOutput{}, fmt.Errorf("max_bytes must be >= 0")
	}
	mode, err := normalizeAttachmentPutMode(input.Mode)
	if err != nil {
		return nil, attachmentPutToolOutput{}, err
	}
	if mode == attachmentPutModeReplace {
		existing, listErr := s.upstream.ListAttachments(ctx, lockdclient.ListAttachmentsRequest{
			Namespace:    s.resolveNamespace(input.Namespace),
			Key:          key,
			LeaseID:      leaseID,
			TxnID:        strings.TrimSpace(input.TxnID),
			FencingToken: input.FencingToken,
			Public:       false,
		})
		if listErr != nil {
			return nil, attachmentPutToolOutput{}, fmt.Errorf("replace requires listing existing attachments: %w", listErr)
		}
		found := false
		for _, item := range existing.Attachments {
			if strings.TrimSpace(item.Name) == name {
				found = true
				break
			}
		}
		if !found {
			return nil, attachmentPutToolOutput{}, fmt.Errorf("attachment %q does not exist; replace mode requires an existing attachment", name)
		}
	}
	var payload []byte
	if strings.TrimSpace(input.PayloadBase64) != "" {
		decoded, decodeErr := base64.StdEncoding.DecodeString(strings.TrimSpace(input.PayloadBase64))
		if decodeErr != nil {
			return nil, attachmentPutToolOutput{}, fmt.Errorf("decode payload_base64: %w", decodeErr)
		}
		payload = decoded
	} else {
		payload = []byte(input.PayloadText)
	}
	if err := validateInlinePayloadBytes(int64(len(payload)), s.cfg.InlineMaxBytes, toolAttachmentsPut, toolAttachmentsWriteStreamBegin); err != nil {
		return nil, attachmentPutToolOutput{}, err
	}
	contentType := strings.TrimSpace(input.ContentType)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	var maxBytes *int64
	if input.MaxBytes > 0 {
		value := input.MaxBytes
		maxBytes = &value
	}
	resp, err := s.upstream.Attach(ctx, lockdclient.AttachRequest{
		Namespace:        s.resolveNamespace(input.Namespace),
		Key:              key,
		LeaseID:          leaseID,
		TxnID:            strings.TrimSpace(input.TxnID),
		FencingToken:     input.FencingToken,
		Name:             name,
		Body:             bytes.NewReader(payload),
		ContentType:      contentType,
		MaxBytes:         maxBytes,
		PreventOverwrite: mode == attachmentPutModeCreate,
	})
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
	LeaseID      string `json:"lease_id,omitempty" jsonschema:"Lease id required when public=false"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	Public       *bool  `json:"public,omitempty" jsonschema:"Read mode selector: true for public read (default), false for lease-bound read"`
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
	leaseID := strings.TrimSpace(input.LeaseID)
	publicRead := resolvePublicReadMode(input.Public)
	if publicRead && leaseID != "" {
		return nil, attachmentListToolOutput{}, fmt.Errorf("lease_id must be empty when public=true")
	}
	if !publicRead && leaseID == "" {
		return nil, attachmentListToolOutput{}, fmt.Errorf("lease_id is required when public=false")
	}
	resp, err := s.upstream.ListAttachments(ctx, lockdclient.ListAttachmentsRequest{
		Namespace:    s.resolveNamespace(input.Namespace),
		Key:          strings.TrimSpace(input.Key),
		LeaseID:      leaseID,
		TxnID:        strings.TrimSpace(input.TxnID),
		FencingToken: input.FencingToken,
		Public:       publicRead,
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
	LeaseID      string `json:"lease_id,omitempty" jsonschema:"Lease id required when public=false"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	Public       *bool  `json:"public,omitempty" jsonschema:"Read mode selector: true for public read (default), false for lease-bound read"`
	ID           string `json:"id,omitempty" jsonschema:"Attachment id selector"`
	Name         string `json:"name,omitempty" jsonschema:"Attachment name selector"`
	PayloadMode  string `json:"payload_mode,omitempty" jsonschema:"Payload mode: auto (default), inline, stream, or none"`
}

type attachmentGetToolOutput struct {
	Namespace             string               `json:"namespace"`
	Key                   string               `json:"key"`
	Attachment            attachmentInfoOutput `json:"attachment"`
	PayloadMode           string               `json:"payload_mode"`
	PayloadBytes          int64                `json:"payload_bytes"`
	PayloadSHA256         string               `json:"payload_sha256,omitempty"`
	PayloadText           string               `json:"payload_text,omitempty"`
	PayloadBase64         string               `json:"payload_base64,omitempty"`
	DownloadURL           string               `json:"download_url,omitempty"`
	DownloadMethod        string               `json:"download_method,omitempty"`
	DownloadExpiresAtUnix int64                `json:"download_expires_at_unix,omitempty"`
}

func (s *server) handleAttachmentGetTool(ctx context.Context, req *mcpsdk.CallToolRequest, input attachmentGetToolInput) (*mcpsdk.CallToolResult, attachmentGetToolOutput, error) {
	headIn := attachmentHeadToolInput{
		Key:          input.Key,
		Namespace:    input.Namespace,
		LeaseID:      input.LeaseID,
		TxnID:        input.TxnID,
		FencingToken: input.FencingToken,
		Public:       input.Public,
		ID:           input.ID,
		Name:         input.Name,
	}
	_, head, err := s.handleAttachmentHeadTool(ctx, nil, headIn)
	if err != nil {
		return nil, attachmentGetToolOutput{}, err
	}
	mode, err := parsePayloadMode(input.PayloadMode, payloadModeAuto)
	if err != nil {
		return nil, attachmentGetToolOutput{}, err
	}
	out := attachmentGetToolOutput{
		Namespace:     head.Namespace,
		Key:           head.Key,
		Attachment:    head.Attachment,
		PayloadMode:   string(payloadModeNone),
		PayloadBytes:  head.Attachment.Size,
		PayloadSHA256: strings.TrimSpace(head.Attachment.PlaintextSHA256),
	}
	switch mode {
	case payloadModeNone:
		return nil, out, nil
	case payloadModeInline:
		limit := normalizedInlineMaxBytes(s.cfg.InlineMaxBytes)
		if head.Attachment.Size > limit {
			return nil, attachmentGetToolOutput{}, inlinePayloadTooLargeError(toolAttachmentsGet, head.Attachment.Size, limit, toolAttachmentsStream)
		}
		readerCtx, cancel := context.WithCancel(context.Background())
		att, getErr := s.upstream.GetAttachment(readerCtx, lockdclient.GetAttachmentRequest{
			Namespace:    s.resolveNamespace(input.Namespace),
			Key:          strings.TrimSpace(input.Key),
			LeaseID:      strings.TrimSpace(input.LeaseID),
			TxnID:        strings.TrimSpace(input.TxnID),
			FencingToken: input.FencingToken,
			Public:       resolvePublicReadMode(input.Public),
			Selector: lockdclient.AttachmentSelector{
				ID:   strings.TrimSpace(input.ID),
				Name: strings.TrimSpace(input.Name),
			},
		})
		if getErr != nil {
			cancel()
			return nil, attachmentGetToolOutput{}, getErr
		}
		inline, inlineErr := readInlinePayloadStrict(att, s.cfg.InlineMaxBytes, toolAttachmentsGet, toolAttachmentsStream)
		_ = att.Close()
		cancel()
		if inlineErr != nil {
			return nil, attachmentGetToolOutput{}, inlineErr
		}
		out.PayloadMode = string(payloadModeInline)
		out.PayloadBytes = inline.Bytes
		out.PayloadText = inline.Text
		out.PayloadBase64 = inline.Base64
		return nil, out, nil
	case payloadModeStream:
		if req == nil || req.Session == nil {
			return nil, attachmentGetToolOutput{}, fmt.Errorf("payload_mode=stream requires an active MCP session")
		}
		_, streamOut, streamErr := s.handleAttachmentStreamTool(ctx, req, attachmentStreamToolInput{
			Key:          input.Key,
			Namespace:    input.Namespace,
			LeaseID:      input.LeaseID,
			TxnID:        input.TxnID,
			FencingToken: input.FencingToken,
			Public:       input.Public,
			ID:           input.ID,
			Name:         input.Name,
		})
		if streamErr != nil {
			return nil, attachmentGetToolOutput{}, streamErr
		}
		out.PayloadMode = string(payloadModeStream)
		out.DownloadURL = streamOut.DownloadURL
		out.DownloadMethod = streamOut.DownloadMethod
		out.DownloadExpiresAtUnix = streamOut.DownloadExpiresAtUnix
		return nil, out, nil
	case payloadModeAuto:
		limit := normalizedInlineMaxBytes(s.cfg.InlineMaxBytes)
		if head.Attachment.Size <= limit {
			return s.handleAttachmentGetTool(ctx, req, attachmentGetToolInput{
				Key:          input.Key,
				Namespace:    input.Namespace,
				LeaseID:      input.LeaseID,
				TxnID:        input.TxnID,
				FencingToken: input.FencingToken,
				Public:       input.Public,
				ID:           input.ID,
				Name:         input.Name,
				PayloadMode:  string(payloadModeInline),
			})
		}
		return s.handleAttachmentGetTool(ctx, req, attachmentGetToolInput{
			Key:          input.Key,
			Namespace:    input.Namespace,
			LeaseID:      input.LeaseID,
			TxnID:        input.TxnID,
			FencingToken: input.FencingToken,
			Public:       input.Public,
			ID:           input.ID,
			Name:         input.Name,
			PayloadMode:  string(payloadModeStream),
		})
	default:
		return nil, attachmentGetToolOutput{}, fmt.Errorf("unsupported payload_mode %q", mode)
	}
}

type attachmentHeadToolInput struct {
	Key          string `json:"key" jsonschema:"State key"`
	Namespace    string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID      string `json:"lease_id,omitempty" jsonschema:"Lease id required when public=false"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	Public       *bool  `json:"public,omitempty" jsonschema:"Read mode selector: true for public read (default), false for lease-bound read"`
	ID           string `json:"id,omitempty" jsonschema:"Attachment id selector"`
	Name         string `json:"name,omitempty" jsonschema:"Attachment name selector"`
}

type attachmentHeadToolOutput struct {
	Namespace  string               `json:"namespace"`
	Key        string               `json:"key"`
	Attachment attachmentInfoOutput `json:"attachment"`
}

func (s *server) handleAttachmentHeadTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input attachmentHeadToolInput) (*mcpsdk.CallToolResult, attachmentHeadToolOutput, error) {
	key := strings.TrimSpace(input.Key)
	if key == "" {
		return nil, attachmentHeadToolOutput{}, fmt.Errorf("key is required")
	}
	id := strings.TrimSpace(input.ID)
	name := strings.TrimSpace(input.Name)
	if id == "" && name == "" {
		return nil, attachmentHeadToolOutput{}, fmt.Errorf("id or name is required")
	}
	leaseID := strings.TrimSpace(input.LeaseID)
	publicRead := resolvePublicReadMode(input.Public)
	if publicRead && leaseID != "" {
		return nil, attachmentHeadToolOutput{}, fmt.Errorf("lease_id must be empty when public=true")
	}
	if !publicRead && leaseID == "" {
		return nil, attachmentHeadToolOutput{}, fmt.Errorf("lease_id is required when public=false")
	}

	list, err := s.upstream.ListAttachments(ctx, lockdclient.ListAttachmentsRequest{
		Namespace:    s.resolveNamespace(input.Namespace),
		Key:          key,
		LeaseID:      leaseID,
		TxnID:        strings.TrimSpace(input.TxnID),
		FencingToken: input.FencingToken,
		Public:       publicRead,
	})
	if err != nil {
		return nil, attachmentHeadToolOutput{}, err
	}

	for _, item := range list.Attachments {
		if id != "" && strings.TrimSpace(item.ID) != id {
			continue
		}
		if name != "" && strings.TrimSpace(item.Name) != name {
			continue
		}
		return nil, attachmentHeadToolOutput{
			Namespace:  list.Namespace,
			Key:        list.Key,
			Attachment: toAttachmentInfoOutput(item),
		}, nil
	}
	return nil, attachmentHeadToolOutput{}, fmt.Errorf("attachment not found")
}

type attachmentChecksumToolInput struct {
	Key          string `json:"key" jsonschema:"State key"`
	Namespace    string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID      string `json:"lease_id,omitempty" jsonschema:"Lease id required when public=false"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	Public       *bool  `json:"public,omitempty" jsonschema:"Read mode selector: true for public read (default), false for lease-bound read"`
	ID           string `json:"id,omitempty" jsonschema:"Attachment id selector"`
	Name         string `json:"name,omitempty" jsonschema:"Attachment name selector"`
}

type attachmentChecksumToolOutput struct {
	Namespace       string `json:"namespace"`
	Key             string `json:"key"`
	AttachmentID    string `json:"attachment_id"`
	AttachmentName  string `json:"attachment_name"`
	PlaintextSHA256 string `json:"plaintext_sha256,omitempty"`
}

func (s *server) handleAttachmentChecksumTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input attachmentChecksumToolInput) (*mcpsdk.CallToolResult, attachmentChecksumToolOutput, error) {
	_, head, err := s.handleAttachmentHeadTool(ctx, nil, attachmentHeadToolInput(input))
	if err != nil {
		return nil, attachmentChecksumToolOutput{}, err
	}
	return nil, attachmentChecksumToolOutput{
		Namespace:       head.Namespace,
		Key:             head.Key,
		AttachmentID:    head.Attachment.ID,
		AttachmentName:  head.Attachment.Name,
		PlaintextSHA256: strings.TrimSpace(head.Attachment.PlaintextSHA256),
	}, nil
}

type attachmentStreamToolInput struct {
	Key          string `json:"key" jsonschema:"State key"`
	Namespace    string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID      string `json:"lease_id,omitempty" jsonschema:"Lease id required when public=false"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	Public       *bool  `json:"public,omitempty" jsonschema:"Read mode selector: true for public read (default), false for lease-bound read"`
	ID           string `json:"id,omitempty" jsonschema:"Attachment id selector"`
	Name         string `json:"name,omitempty" jsonschema:"Attachment name selector"`
}

type attachmentStreamToolOutput struct {
	Namespace             string               `json:"namespace"`
	Key                   string               `json:"key"`
	Attachment            attachmentInfoOutput `json:"attachment"`
	DownloadURL           string               `json:"download_url,omitempty"`
	DownloadMethod        string               `json:"download_method,omitempty"`
	DownloadExpiresAtUnix int64                `json:"download_expires_at_unix,omitempty"`
}

func (s *server) handleAttachmentStreamTool(ctx context.Context, req *mcpsdk.CallToolRequest, input attachmentStreamToolInput) (*mcpsdk.CallToolResult, attachmentStreamToolOutput, error) {
	if req == nil || req.Session == nil {
		return nil, attachmentStreamToolOutput{}, fmt.Errorf("attachment streaming requires an active MCP session")
	}
	key := strings.TrimSpace(input.Key)
	if key == "" {
		return nil, attachmentStreamToolOutput{}, fmt.Errorf("key is required")
	}
	selectorID := strings.TrimSpace(input.ID)
	selectorName := strings.TrimSpace(input.Name)
	if selectorID == "" && selectorName == "" {
		return nil, attachmentStreamToolOutput{}, fmt.Errorf("id or name is required")
	}
	leaseID := strings.TrimSpace(input.LeaseID)
	publicRead := resolvePublicReadMode(input.Public)
	if publicRead && leaseID != "" {
		return nil, attachmentStreamToolOutput{}, fmt.Errorf("lease_id must be empty when public=true")
	}
	if !publicRead && leaseID == "" {
		return nil, attachmentStreamToolOutput{}, fmt.Errorf("lease_id is required when public=false")
	}
	transferCtx, cancelTransfer := context.WithCancel(context.Background())
	att, err := s.upstream.GetAttachment(transferCtx, lockdclient.GetAttachmentRequest{
		Namespace:    s.resolveNamespace(input.Namespace),
		Key:          key,
		LeaseID:      leaseID,
		TxnID:        strings.TrimSpace(input.TxnID),
		FencingToken: input.FencingToken,
		Public:       publicRead,
		Selector: lockdclient.AttachmentSelector{
			ID:   selectorID,
			Name: selectorName,
		},
	})
	if err != nil {
		cancelTransfer()
		return nil, attachmentStreamToolOutput{}, err
	}
	reg, err := s.ensureTransferManager().RegisterDownload(req.Session, att, transferDownloadRequest{
		ContentType:   strings.TrimSpace(att.ContentType),
		ContentLength: att.Size,
		Filename:      att.Name,
		Headers: map[string]string{
			"X-Lockd-Attachment-ID":               strings.TrimSpace(att.ID),
			"X-Lockd-Attachment-Name":             strings.TrimSpace(att.Name),
			"X-Lockd-Attachment-Plaintext-SHA256": strings.TrimSpace(att.PlaintextSHA256),
		},
		Cleanup: cancelTransfer,
	})
	if err != nil {
		_ = att.Close()
		cancelTransfer()
		return nil, attachmentStreamToolOutput{}, err
	}
	return nil, attachmentStreamToolOutput{
		Namespace:             s.resolveNamespace(input.Namespace),
		Key:                   key,
		Attachment:            toAttachmentInfoOutput(att.AttachmentInfo),
		DownloadURL:           s.transferURL(reg.ID),
		DownloadMethod:        reg.Method,
		DownloadExpiresAtUnix: reg.ExpiresAtUnix,
	}, nil
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
		ID:              info.ID,
		Name:            info.Name,
		Size:            info.Size,
		PlaintextSHA256: strings.TrimSpace(info.PlaintextSHA256),
		ContentType:     info.ContentType,
		CreatedAtUnix:   info.CreatedAtUnix,
		UpdatedAtUnix:   info.UpdatedAtUnix,
	}
}

func normalizeAttachmentPutMode(raw string) (string, error) {
	mode := strings.ToLower(strings.TrimSpace(raw))
	if mode == "" {
		mode = attachmentPutModeCreate
	}
	switch mode {
	case attachmentPutModeCreate, attachmentPutModeUpsert, attachmentPutModeReplace:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid mode %q (expected create|upsert|replace)", raw)
	}
}
