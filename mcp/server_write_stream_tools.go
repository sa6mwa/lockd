package mcp

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	lockdclient "pkt.systems/lockd/client"
)

type writeStreamBeginOutput struct {
	StreamID      string `json:"stream_id"`
	MaxChunkBytes int64  `json:"max_chunk_bytes"`
}

type writeStreamAppendInput struct {
	StreamID    string `json:"stream_id" jsonschema:"Write stream identifier returned by begin"`
	ChunkBase64 string `json:"chunk_base64" jsonschema:"Base64-encoded payload chunk bytes"`
}

type writeStreamAppendOutput struct {
	StreamID      string `json:"stream_id"`
	BytesAppended int64  `json:"bytes_appended"`
	TotalBytes    int64  `json:"total_bytes"`
}

type writeStreamCommitInput struct {
	StreamID string `json:"stream_id" jsonschema:"Write stream identifier returned by begin"`
}

type writeStreamAbortInput struct {
	StreamID string `json:"stream_id" jsonschema:"Write stream identifier returned by begin"`
	Reason   string `json:"reason,omitempty" jsonschema:"Optional abort reason for diagnostics"`
}

type writeStreamAbortOutput struct {
	StreamID      string `json:"stream_id"`
	Aborted       bool   `json:"aborted"`
	BytesReceived int64  `json:"bytes_received"`
}

type stateWriteStreamBeginInput struct {
	Key          string `json:"key" jsonschema:"State key"`
	Namespace    string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID      string `json:"lease_id" jsonschema:"Active lease id"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	IfETag       string `json:"if_etag,omitempty" jsonschema:"Conditional ETag guard"`
	IfVersion    *int64 `json:"if_version,omitempty" jsonschema:"Conditional version guard"`
	QueryHidden  *bool  `json:"query_hidden,omitempty" jsonschema:"Optional query-hidden metadata mutation"`
}

type stateWriteStreamCommitOutput struct {
	StreamID      string `json:"stream_id"`
	BytesReceived int64  `json:"bytes_received"`
	NewVersion    int64  `json:"new_version"`
	NewStateETag  string `json:"new_state_etag"`
	Bytes         int64  `json:"bytes"`
	QueryHidden   *bool  `json:"query_hidden,omitempty"`
}

func (s *server) handleStateWriteStreamBeginTool(_ context.Context, req *mcpsdk.CallToolRequest, input stateWriteStreamBeginInput) (*mcpsdk.CallToolResult, writeStreamBeginOutput, error) {
	if req == nil || req.Session == nil {
		return nil, writeStreamBeginOutput{}, fmt.Errorf("state write stream begin requires an active MCP session")
	}
	key := strings.TrimSpace(input.Key)
	leaseID := strings.TrimSpace(input.LeaseID)
	if key == "" || leaseID == "" {
		return nil, writeStreamBeginOutput{}, fmt.Errorf("key and lease_id are required")
	}
	namespace := s.resolveNamespace(input.Namespace)
	opts := lockdclient.UpdateOptions{
		Namespace:    namespace,
		TxnID:        strings.TrimSpace(input.TxnID),
		FencingToken: input.FencingToken,
		IfETag:       strings.TrimSpace(input.IfETag),
		IfVersion:    input.IfVersion,
		Metadata: lockdclient.MetadataOptions{
			QueryHidden: input.QueryHidden,
			TxnID:       strings.TrimSpace(input.TxnID),
		},
	}
	mgr := s.ensureWriteStreamManager()
	streamID, err := mgr.Begin(req.Session, writeStreamKindState, func(runCtx context.Context, reader io.Reader) (any, error) {
		resp, err := s.upstream.UpdateStream(runCtx, key, leaseID, reader, opts)
		if err != nil {
			return nil, err
		}
		return stateUpdateToolOutput{
			NewVersion:   resp.NewVersion,
			NewStateETag: resp.NewStateETag,
			Bytes:        resp.BytesWritten,
			QueryHidden:  resp.Metadata.QueryHidden,
		}, nil
	})
	if err != nil {
		return nil, writeStreamBeginOutput{}, err
	}
	return nil, writeStreamBeginOutput{StreamID: streamID, MaxChunkBytes: normalizedInlineMaxBytes(s.cfg.InlineMaxBytes)}, nil
}

func (s *server) handleStateWriteStreamAppendTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamAppendInput) (*mcpsdk.CallToolResult, writeStreamAppendOutput, error) {
	if req == nil || req.Session == nil {
		return nil, writeStreamAppendOutput{}, fmt.Errorf("state write stream append requires an active MCP session")
	}
	chunk, err := decodeWriteStreamChunk(input.ChunkBase64, s.cfg.InlineMaxBytes)
	if err != nil {
		return nil, writeStreamAppendOutput{}, err
	}
	mgr := s.ensureWriteStreamManager()
	appended, total, err := mgr.Append(req.Session, input.StreamID, writeStreamKindState, chunk)
	if err != nil {
		return nil, writeStreamAppendOutput{}, err
	}
	return nil, writeStreamAppendOutput{StreamID: strings.TrimSpace(input.StreamID), BytesAppended: appended, TotalBytes: total}, nil
}

func (s *server) handleStateWriteStreamCommitTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamCommitInput) (*mcpsdk.CallToolResult, stateWriteStreamCommitOutput, error) {
	if req == nil || req.Session == nil {
		return nil, stateWriteStreamCommitOutput{}, fmt.Errorf("state write stream commit requires an active MCP session")
	}
	mgr := s.ensureWriteStreamManager()
	result, bytes, err := mgr.Commit(req.Session, input.StreamID, writeStreamKindState)
	if err != nil {
		return nil, stateWriteStreamCommitOutput{}, err
	}
	stateOut, ok := result.(stateUpdateToolOutput)
	if !ok {
		return nil, stateWriteStreamCommitOutput{}, fmt.Errorf("state write stream %s returned invalid result type", strings.TrimSpace(input.StreamID))
	}
	return nil, stateWriteStreamCommitOutput{
		StreamID:      strings.TrimSpace(input.StreamID),
		BytesReceived: bytes,
		NewVersion:    stateOut.NewVersion,
		NewStateETag:  stateOut.NewStateETag,
		Bytes:         stateOut.Bytes,
		QueryHidden:   stateOut.QueryHidden,
	}, nil
}

func (s *server) handleStateWriteStreamAbortTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamAbortInput) (*mcpsdk.CallToolResult, writeStreamAbortOutput, error) {
	if req == nil || req.Session == nil {
		return nil, writeStreamAbortOutput{}, fmt.Errorf("state write stream abort requires an active MCP session")
	}
	mgr := s.ensureWriteStreamManager()
	bytes, err := mgr.Abort(req.Session, input.StreamID, writeStreamKindState, strings.TrimSpace(input.Reason))
	if err != nil {
		return nil, writeStreamAbortOutput{}, err
	}
	return nil, writeStreamAbortOutput{StreamID: strings.TrimSpace(input.StreamID), Aborted: true, BytesReceived: bytes}, nil
}

type queueWriteStreamBeginInput struct {
	Queue             string         `json:"queue,omitempty" jsonschema:"Queue name (defaults to lockd.agent.bus)"`
	Namespace         string         `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	ContentType       string         `json:"content_type,omitempty" jsonschema:"Payload content type"`
	DelaySeconds      int64          `json:"delay_seconds,omitempty" jsonschema:"Initial invisibility delay"`
	VisibilitySeconds int64          `json:"visibility_seconds,omitempty" jsonschema:"Visibility timeout for dequeued lease"`
	TTLSeconds        int64          `json:"ttl_seconds,omitempty" jsonschema:"Message retention TTL in seconds"`
	MaxAttempts       int            `json:"max_attempts,omitempty" jsonschema:"Maximum failed attempts before terminal handling"`
	Attributes        map[string]any `json:"attributes,omitempty" jsonschema:"Message attributes metadata"`
}

type queueWriteStreamCommitOutput struct {
	StreamID          string `json:"stream_id"`
	BytesReceived     int64  `json:"bytes_received"`
	Namespace         string `json:"namespace"`
	Queue             string `json:"queue"`
	MessageID         string `json:"message_id"`
	Attempts          int    `json:"attempts"`
	MaxAttempts       int    `json:"max_attempts"`
	FailureAttempts   int    `json:"failure_attempts,omitempty"`
	NotVisibleUntil   int64  `json:"not_visible_until_unix"`
	VisibilitySeconds int64  `json:"visibility_timeout_seconds"`
	PayloadBytes      int64  `json:"payload_bytes"`
	CorrelationID     string `json:"correlation_id,omitempty"`
}

func (s *server) handleQueueWriteStreamBeginTool(_ context.Context, req *mcpsdk.CallToolRequest, input queueWriteStreamBeginInput) (*mcpsdk.CallToolResult, writeStreamBeginOutput, error) {
	if req == nil || req.Session == nil {
		return nil, writeStreamBeginOutput{}, fmt.Errorf("queue write stream begin requires an active MCP session")
	}
	queue := s.resolveQueue(input.Queue)
	namespace := s.resolveNamespace(input.Namespace)
	contentType := strings.TrimSpace(input.ContentType)
	if contentType == "" {
		contentType = "application/json"
	}
	options := lockdclient.EnqueueOptions{
		Namespace:   namespace,
		Delay:       time.Duration(input.DelaySeconds) * time.Second,
		Visibility:  time.Duration(input.VisibilitySeconds) * time.Second,
		TTL:         time.Duration(input.TTLSeconds) * time.Second,
		MaxAttempts: input.MaxAttempts,
		Attributes:  input.Attributes,
		ContentType: contentType,
	}
	mgr := s.ensureWriteStreamManager()
	streamID, err := mgr.Begin(req.Session, writeStreamKindQueue, func(runCtx context.Context, reader io.Reader) (any, error) {
		resp, err := s.upstream.Enqueue(runCtx, queue, reader, options)
		if err != nil {
			return nil, err
		}
		return queueEnqueueToolOutput{
			Namespace:         resp.Namespace,
			Queue:             resp.Queue,
			MessageID:         resp.MessageID,
			Attempts:          resp.Attempts,
			MaxAttempts:       resp.MaxAttempts,
			FailureAttempts:   resp.FailureAttempts,
			NotVisibleUntil:   resp.NotVisibleUntilUnix,
			VisibilitySeconds: resp.VisibilityTimeoutSeconds,
			PayloadBytes:      resp.PayloadBytes,
			CorrelationID:     resp.CorrelationID,
		}, nil
	})
	if err != nil {
		return nil, writeStreamBeginOutput{}, err
	}
	return nil, writeStreamBeginOutput{StreamID: streamID, MaxChunkBytes: normalizedInlineMaxBytes(s.cfg.InlineMaxBytes)}, nil
}

func (s *server) handleQueueWriteStreamAppendTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamAppendInput) (*mcpsdk.CallToolResult, writeStreamAppendOutput, error) {
	if req == nil || req.Session == nil {
		return nil, writeStreamAppendOutput{}, fmt.Errorf("queue write stream append requires an active MCP session")
	}
	chunk, err := decodeWriteStreamChunk(input.ChunkBase64, s.cfg.InlineMaxBytes)
	if err != nil {
		return nil, writeStreamAppendOutput{}, err
	}
	mgr := s.ensureWriteStreamManager()
	appended, total, err := mgr.Append(req.Session, input.StreamID, writeStreamKindQueue, chunk)
	if err != nil {
		return nil, writeStreamAppendOutput{}, err
	}
	return nil, writeStreamAppendOutput{StreamID: strings.TrimSpace(input.StreamID), BytesAppended: appended, TotalBytes: total}, nil
}

func (s *server) handleQueueWriteStreamCommitTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamCommitInput) (*mcpsdk.CallToolResult, queueWriteStreamCommitOutput, error) {
	if req == nil || req.Session == nil {
		return nil, queueWriteStreamCommitOutput{}, fmt.Errorf("queue write stream commit requires an active MCP session")
	}
	mgr := s.ensureWriteStreamManager()
	result, bytes, err := mgr.Commit(req.Session, input.StreamID, writeStreamKindQueue)
	if err != nil {
		return nil, queueWriteStreamCommitOutput{}, err
	}
	queueOut, ok := result.(queueEnqueueToolOutput)
	if !ok {
		return nil, queueWriteStreamCommitOutput{}, fmt.Errorf("queue write stream %s returned invalid result type", strings.TrimSpace(input.StreamID))
	}
	return nil, queueWriteStreamCommitOutput{
		StreamID:          strings.TrimSpace(input.StreamID),
		BytesReceived:     bytes,
		Namespace:         queueOut.Namespace,
		Queue:             queueOut.Queue,
		MessageID:         queueOut.MessageID,
		Attempts:          queueOut.Attempts,
		MaxAttempts:       queueOut.MaxAttempts,
		FailureAttempts:   queueOut.FailureAttempts,
		NotVisibleUntil:   queueOut.NotVisibleUntil,
		VisibilitySeconds: queueOut.VisibilitySeconds,
		PayloadBytes:      queueOut.PayloadBytes,
		CorrelationID:     queueOut.CorrelationID,
	}, nil
}

func (s *server) handleQueueWriteStreamAbortTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamAbortInput) (*mcpsdk.CallToolResult, writeStreamAbortOutput, error) {
	if req == nil || req.Session == nil {
		return nil, writeStreamAbortOutput{}, fmt.Errorf("queue write stream abort requires an active MCP session")
	}
	mgr := s.ensureWriteStreamManager()
	bytes, err := mgr.Abort(req.Session, input.StreamID, writeStreamKindQueue, strings.TrimSpace(input.Reason))
	if err != nil {
		return nil, writeStreamAbortOutput{}, err
	}
	return nil, writeStreamAbortOutput{StreamID: strings.TrimSpace(input.StreamID), Aborted: true, BytesReceived: bytes}, nil
}

type attachmentsWriteStreamBeginInput struct {
	Key          string `json:"key" jsonschema:"State key"`
	Namespace    string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	LeaseID      string `json:"lease_id" jsonschema:"Active lease id"`
	TxnID        string `json:"txn_id,omitempty" jsonschema:"Optional XA transaction id"`
	FencingToken *int64 `json:"fencing_token,omitempty" jsonschema:"Optional fencing token override"`
	Name         string `json:"name" jsonschema:"Attachment name"`
	ContentType  string `json:"content_type,omitempty" jsonschema:"Attachment content type"`
	MaxBytes     int64  `json:"max_bytes,omitempty" jsonschema:"Optional upload size cap"`
	Mode         string `json:"mode,omitempty" jsonschema:"Write mode: create (default), upsert, or replace"`
}

type attachmentsWriteStreamCommitOutput struct {
	StreamID      string               `json:"stream_id"`
	BytesReceived int64                `json:"bytes_received"`
	Attachment    attachmentInfoOutput `json:"attachment"`
	Noop          bool                 `json:"noop"`
	Version       int64                `json:"version"`
}

func (s *server) handleAttachmentsWriteStreamBeginTool(ctx context.Context, req *mcpsdk.CallToolRequest, input attachmentsWriteStreamBeginInput) (*mcpsdk.CallToolResult, writeStreamBeginOutput, error) {
	if req == nil || req.Session == nil {
		return nil, writeStreamBeginOutput{}, fmt.Errorf("attachments write stream begin requires an active MCP session")
	}
	key := strings.TrimSpace(input.Key)
	leaseID := strings.TrimSpace(input.LeaseID)
	name := strings.TrimSpace(input.Name)
	if key == "" || leaseID == "" || name == "" {
		return nil, writeStreamBeginOutput{}, fmt.Errorf("key, lease_id, and name are required")
	}
	if input.MaxBytes < 0 {
		return nil, writeStreamBeginOutput{}, fmt.Errorf("max_bytes must be >= 0")
	}
	mode, err := normalizeAttachmentPutMode(input.Mode)
	if err != nil {
		return nil, writeStreamBeginOutput{}, err
	}
	namespace := s.resolveNamespace(input.Namespace)
	txnID := strings.TrimSpace(input.TxnID)
	if mode == attachmentPutModeReplace {
		existing, listErr := s.upstream.ListAttachments(ctx, lockdclient.ListAttachmentsRequest{
			Namespace:    namespace,
			Key:          key,
			LeaseID:      leaseID,
			TxnID:        txnID,
			FencingToken: input.FencingToken,
			Public:       false,
		})
		if listErr != nil {
			return nil, writeStreamBeginOutput{}, fmt.Errorf("replace requires listing existing attachments: %w", listErr)
		}
		found := false
		for _, item := range existing.Attachments {
			if strings.TrimSpace(item.Name) == name {
				found = true
				break
			}
		}
		if !found {
			return nil, writeStreamBeginOutput{}, fmt.Errorf("attachment %q does not exist; replace mode requires an existing attachment", name)
		}
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
	mgr := s.ensureWriteStreamManager()
	streamID, err := mgr.Begin(req.Session, writeStreamKindAttachment, func(runCtx context.Context, reader io.Reader) (any, error) {
		resp, err := s.upstream.Attach(runCtx, lockdclient.AttachRequest{
			Namespace:        namespace,
			Key:              key,
			LeaseID:          leaseID,
			TxnID:            txnID,
			FencingToken:     input.FencingToken,
			Name:             name,
			Body:             reader,
			ContentType:      contentType,
			MaxBytes:         maxBytes,
			PreventOverwrite: mode == attachmentPutModeCreate,
		})
		if err != nil {
			return nil, err
		}
		return attachmentPutToolOutput{
			Attachment: toAttachmentInfoOutput(resp.Attachment),
			Noop:       resp.Noop,
			Version:    resp.Version,
		}, nil
	})
	if err != nil {
		return nil, writeStreamBeginOutput{}, err
	}
	return nil, writeStreamBeginOutput{StreamID: streamID, MaxChunkBytes: normalizedInlineMaxBytes(s.cfg.InlineMaxBytes)}, nil
}

func (s *server) handleAttachmentsWriteStreamAppendTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamAppendInput) (*mcpsdk.CallToolResult, writeStreamAppendOutput, error) {
	if req == nil || req.Session == nil {
		return nil, writeStreamAppendOutput{}, fmt.Errorf("attachments write stream append requires an active MCP session")
	}
	chunk, err := decodeWriteStreamChunk(input.ChunkBase64, s.cfg.InlineMaxBytes)
	if err != nil {
		return nil, writeStreamAppendOutput{}, err
	}
	mgr := s.ensureWriteStreamManager()
	appended, total, err := mgr.Append(req.Session, input.StreamID, writeStreamKindAttachment, chunk)
	if err != nil {
		return nil, writeStreamAppendOutput{}, err
	}
	return nil, writeStreamAppendOutput{StreamID: strings.TrimSpace(input.StreamID), BytesAppended: appended, TotalBytes: total}, nil
}

func (s *server) handleAttachmentsWriteStreamCommitTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamCommitInput) (*mcpsdk.CallToolResult, attachmentsWriteStreamCommitOutput, error) {
	if req == nil || req.Session == nil {
		return nil, attachmentsWriteStreamCommitOutput{}, fmt.Errorf("attachments write stream commit requires an active MCP session")
	}
	mgr := s.ensureWriteStreamManager()
	result, bytes, err := mgr.Commit(req.Session, input.StreamID, writeStreamKindAttachment)
	if err != nil {
		return nil, attachmentsWriteStreamCommitOutput{}, err
	}
	attOut, ok := result.(attachmentPutToolOutput)
	if !ok {
		return nil, attachmentsWriteStreamCommitOutput{}, fmt.Errorf("attachments write stream %s returned invalid result type", strings.TrimSpace(input.StreamID))
	}
	return nil, attachmentsWriteStreamCommitOutput{
		StreamID:      strings.TrimSpace(input.StreamID),
		BytesReceived: bytes,
		Attachment:    attOut.Attachment,
		Noop:          attOut.Noop,
		Version:       attOut.Version,
	}, nil
}

func (s *server) handleAttachmentsWriteStreamAbortTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamAbortInput) (*mcpsdk.CallToolResult, writeStreamAbortOutput, error) {
	if req == nil || req.Session == nil {
		return nil, writeStreamAbortOutput{}, fmt.Errorf("attachments write stream abort requires an active MCP session")
	}
	mgr := s.ensureWriteStreamManager()
	bytes, err := mgr.Abort(req.Session, input.StreamID, writeStreamKindAttachment, strings.TrimSpace(input.Reason))
	if err != nil {
		return nil, writeStreamAbortOutput{}, err
	}
	return nil, writeStreamAbortOutput{StreamID: strings.TrimSpace(input.StreamID), Aborted: true, BytesReceived: bytes}, nil
}

func (s *server) ensureWriteStreamManager() *writeStreamManager {
	if s.writeStreams != nil {
		return s.writeStreams
	}
	s.writeStreams = newWriteStreamManager(s.writeStreamLog)
	return s.writeStreams
}

func decodeWriteStreamChunk(chunkBase64 string, maxBytes int64) ([]byte, error) {
	encoded := strings.TrimSpace(chunkBase64)
	if encoded == "" {
		return nil, fmt.Errorf("chunk_base64 is required")
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode chunk_base64: %w", err)
	}
	limit := normalizedInlineMaxBytes(maxBytes)
	if int64(len(decoded)) > limit {
		return nil, fmt.Errorf("decoded chunk size %d exceeds mcp.inline_max_bytes=%d", len(decoded), limit)
	}
	return decoded, nil
}
