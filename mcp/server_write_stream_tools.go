package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	lockdclient "pkt.systems/lockd/client"
)

type writeStreamBeginOutput struct {
	StreamID            string `json:"stream_id"`
	UploadURL           string `json:"upload_url,omitempty"`
	UploadMethod        string `json:"upload_method,omitempty"`
	UploadExpiresAtUnix int64  `json:"upload_expires_at_unix,omitempty"`
}

type writeStreamCommitInput struct {
	StreamID       string `json:"stream_id" jsonschema:"Write stream identifier returned by begin"`
	ExpectedSHA256 string `json:"expected_sha256,omitempty" jsonschema:"Optional expected lowercase hex SHA-256 of uploaded plaintext bytes"`
	ExpectedBytes  *int64 `json:"expected_bytes,omitempty" jsonschema:"Optional expected plaintext byte count of uploaded data"`
}

type writeStreamAbortInput struct {
	StreamID string `json:"stream_id" jsonschema:"Write stream identifier returned by begin"`
	Reason   string `json:"reason,omitempty" jsonschema:"Optional abort reason for diagnostics"`
}

type writeStreamStatusInput struct {
	StreamID string `json:"stream_id" jsonschema:"Write stream identifier returned by begin"`
}

type writeStreamStatusOutput struct {
	StreamID                  string `json:"stream_id"`
	BytesReceived             int64  `json:"bytes_received"`
	PayloadSHA256             string `json:"payload_sha256,omitempty"`
	UploadCapabilityAvailable bool   `json:"upload_capability_available"`
	UploadExpiresAtUnix       int64  `json:"upload_expires_at_unix,omitempty"`
	UploadCompleted           bool   `json:"upload_completed"`
	UploadInProgress          bool   `json:"upload_in_progress"`
	Committing                bool   `json:"committing"`
	Aborted                   bool   `json:"aborted"`
	CanCommit                 bool   `json:"can_commit"`
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

type writeStreamTransferOutput struct {
	UploadURL           string
	UploadMethod        string
	UploadExpiresAtUnix int64
}

func normalizeExpectedSHA256(raw string) (string, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return "", nil
	}
	if len(value) != 64 {
		return "", fmt.Errorf("expected_sha256 must be 64 hex characters")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return "", fmt.Errorf("expected_sha256 must be valid hex")
	}
	return hex.EncodeToString(decoded), nil
}

func validateWriteStreamCommitExpectations(input writeStreamCommitInput, status writeStreamStatus) error {
	if input.ExpectedBytes != nil && *input.ExpectedBytes < 0 {
		return fmt.Errorf("expected_bytes must be >= 0")
	}
	if input.ExpectedBytes != nil && *input.ExpectedBytes != status.BytesReceived {
		return fmt.Errorf("expected_bytes mismatch: expected=%d actual=%d", *input.ExpectedBytes, status.BytesReceived)
	}
	expectedSHA, err := normalizeExpectedSHA256(input.ExpectedSHA256)
	if err != nil {
		return err
	}
	if expectedSHA == "" {
		return nil
	}
	actualSHA := strings.TrimSpace(status.PayloadSHA256)
	if actualSHA == "" {
		empty := sha256.Sum256(nil)
		actualSHA = hex.EncodeToString(empty[:])
	}
	if expectedSHA != actualSHA {
		return fmt.Errorf("expected_sha256 mismatch: expected=%s actual=%s", expectedSHA, actualSHA)
	}
	return nil
}

func (s *server) writeStreamStatus(req *mcpsdk.CallToolRequest, kind writeStreamKind, streamID string) (writeStreamStatusOutput, error) {
	if req == nil || req.Session == nil {
		return writeStreamStatusOutput{}, fmt.Errorf("%s write stream status requires an active MCP session", kind)
	}
	normalizedStreamID := strings.TrimSpace(streamID)
	if normalizedStreamID == "" {
		return writeStreamStatusOutput{}, fmt.Errorf("stream_id is required")
	}
	status, err := s.ensureWriteStreamManager().Status(req.Session, normalizedStreamID, kind)
	if err != nil {
		return writeStreamStatusOutput{}, err
	}
	upload := s.ensureTransferManager().WriteStreamUploadStatus(req.Session, normalizedStreamID)
	canCommit := !status.Aborted && !status.Committing && !status.UploadInProgress && status.UploadCompleted
	return writeStreamStatusOutput{
		StreamID:                  normalizedStreamID,
		BytesReceived:             status.BytesReceived,
		PayloadSHA256:             strings.TrimSpace(status.PayloadSHA256),
		UploadCapabilityAvailable: upload.Available,
		UploadExpiresAtUnix:       upload.ExpiresAtUnix,
		UploadCompleted:           status.UploadCompleted,
		UploadInProgress:          status.UploadInProgress,
		Committing:                status.Committing,
		Aborted:                   status.Aborted,
		CanCommit:                 canCommit,
	}, nil
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
	transfer, err := s.registerWriteStreamUploadCapability(req.Session, streamID, writeStreamKindState)
	if err != nil {
		_, _ = mgr.Abort(req.Session, streamID, writeStreamKindState, "transfer capability setup failed")
		return nil, writeStreamBeginOutput{}, err
	}
	return nil, writeStreamBeginOutput{
		StreamID:            streamID,
		UploadURL:           transfer.UploadURL,
		UploadMethod:        transfer.UploadMethod,
		UploadExpiresAtUnix: transfer.UploadExpiresAtUnix,
	}, nil
}

func (s *server) handleStateWriteStreamCommitTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamCommitInput) (*mcpsdk.CallToolResult, stateWriteStreamCommitOutput, error) {
	if req == nil || req.Session == nil {
		return nil, stateWriteStreamCommitOutput{}, fmt.Errorf("state write stream commit requires an active MCP session")
	}
	mgr := s.ensureWriteStreamManager()
	commitStatus, err := mgr.Status(req.Session, input.StreamID, writeStreamKindState)
	if err != nil {
		return nil, stateWriteStreamCommitOutput{}, err
	}
	if err := validateWriteStreamCommitExpectations(input, commitStatus); err != nil {
		return nil, stateWriteStreamCommitOutput{}, err
	}
	result, status, err := mgr.Commit(req.Session, input.StreamID, writeStreamKindState)
	if err != nil {
		return nil, stateWriteStreamCommitOutput{}, err
	}
	s.ensureTransferManager().RevokeWriteStream(req.Session, input.StreamID)
	stateOut, ok := result.(stateUpdateToolOutput)
	if !ok {
		return nil, stateWriteStreamCommitOutput{}, fmt.Errorf("state write stream %s returned invalid result type", strings.TrimSpace(input.StreamID))
	}
	return nil, stateWriteStreamCommitOutput{
		StreamID:      strings.TrimSpace(input.StreamID),
		BytesReceived: status.BytesReceived,
		NewVersion:    stateOut.NewVersion,
		NewStateETag:  stateOut.NewStateETag,
		Bytes:         stateOut.Bytes,
		QueryHidden:   stateOut.QueryHidden,
	}, nil
}

func (s *server) handleStateWriteStreamStatusTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamStatusInput) (*mcpsdk.CallToolResult, writeStreamStatusOutput, error) {
	status, err := s.writeStreamStatus(req, writeStreamKindState, input.StreamID)
	if err != nil {
		return nil, writeStreamStatusOutput{}, err
	}
	return nil, status, nil
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
	s.ensureTransferManager().RevokeWriteStream(req.Session, input.StreamID)
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
	transfer, err := s.registerWriteStreamUploadCapability(req.Session, streamID, writeStreamKindQueue)
	if err != nil {
		_, _ = mgr.Abort(req.Session, streamID, writeStreamKindQueue, "transfer capability setup failed")
		return nil, writeStreamBeginOutput{}, err
	}
	return nil, writeStreamBeginOutput{
		StreamID:            streamID,
		UploadURL:           transfer.UploadURL,
		UploadMethod:        transfer.UploadMethod,
		UploadExpiresAtUnix: transfer.UploadExpiresAtUnix,
	}, nil
}

func (s *server) handleQueueWriteStreamCommitTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamCommitInput) (*mcpsdk.CallToolResult, queueWriteStreamCommitOutput, error) {
	if req == nil || req.Session == nil {
		return nil, queueWriteStreamCommitOutput{}, fmt.Errorf("queue write stream commit requires an active MCP session")
	}
	mgr := s.ensureWriteStreamManager()
	commitStatus, err := mgr.Status(req.Session, input.StreamID, writeStreamKindQueue)
	if err != nil {
		return nil, queueWriteStreamCommitOutput{}, err
	}
	if err := validateWriteStreamCommitExpectations(input, commitStatus); err != nil {
		return nil, queueWriteStreamCommitOutput{}, err
	}
	result, status, err := mgr.Commit(req.Session, input.StreamID, writeStreamKindQueue)
	if err != nil {
		return nil, queueWriteStreamCommitOutput{}, err
	}
	s.ensureTransferManager().RevokeWriteStream(req.Session, input.StreamID)
	queueOut, ok := result.(queueEnqueueToolOutput)
	if !ok {
		return nil, queueWriteStreamCommitOutput{}, fmt.Errorf("queue write stream %s returned invalid result type", strings.TrimSpace(input.StreamID))
	}
	return nil, queueWriteStreamCommitOutput{
		StreamID:          strings.TrimSpace(input.StreamID),
		BytesReceived:     status.BytesReceived,
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

func (s *server) handleQueueWriteStreamStatusTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamStatusInput) (*mcpsdk.CallToolResult, writeStreamStatusOutput, error) {
	status, err := s.writeStreamStatus(req, writeStreamKindQueue, input.StreamID)
	if err != nil {
		return nil, writeStreamStatusOutput{}, err
	}
	return nil, status, nil
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
	s.ensureTransferManager().RevokeWriteStream(req.Session, input.StreamID)
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
	transfer, err := s.registerWriteStreamUploadCapability(req.Session, streamID, writeStreamKindAttachment)
	if err != nil {
		_, _ = mgr.Abort(req.Session, streamID, writeStreamKindAttachment, "transfer capability setup failed")
		return nil, writeStreamBeginOutput{}, err
	}
	return nil, writeStreamBeginOutput{
		StreamID:            streamID,
		UploadURL:           transfer.UploadURL,
		UploadMethod:        transfer.UploadMethod,
		UploadExpiresAtUnix: transfer.UploadExpiresAtUnix,
	}, nil
}

func (s *server) handleAttachmentsWriteStreamCommitTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamCommitInput) (*mcpsdk.CallToolResult, attachmentsWriteStreamCommitOutput, error) {
	if req == nil || req.Session == nil {
		return nil, attachmentsWriteStreamCommitOutput{}, fmt.Errorf("attachments write stream commit requires an active MCP session")
	}
	mgr := s.ensureWriteStreamManager()
	commitStatus, err := mgr.Status(req.Session, input.StreamID, writeStreamKindAttachment)
	if err != nil {
		return nil, attachmentsWriteStreamCommitOutput{}, err
	}
	if err := validateWriteStreamCommitExpectations(input, commitStatus); err != nil {
		return nil, attachmentsWriteStreamCommitOutput{}, err
	}
	result, status, err := mgr.Commit(req.Session, input.StreamID, writeStreamKindAttachment)
	if err != nil {
		return nil, attachmentsWriteStreamCommitOutput{}, err
	}
	s.ensureTransferManager().RevokeWriteStream(req.Session, input.StreamID)
	attOut, ok := result.(attachmentPutToolOutput)
	if !ok {
		return nil, attachmentsWriteStreamCommitOutput{}, fmt.Errorf("attachments write stream %s returned invalid result type", strings.TrimSpace(input.StreamID))
	}
	return nil, attachmentsWriteStreamCommitOutput{
		StreamID:      strings.TrimSpace(input.StreamID),
		BytesReceived: status.BytesReceived,
		Attachment:    attOut.Attachment,
		Noop:          attOut.Noop,
		Version:       attOut.Version,
	}, nil
}

func (s *server) handleAttachmentsWriteStreamStatusTool(_ context.Context, req *mcpsdk.CallToolRequest, input writeStreamStatusInput) (*mcpsdk.CallToolResult, writeStreamStatusOutput, error) {
	status, err := s.writeStreamStatus(req, writeStreamKindAttachment, input.StreamID)
	if err != nil {
		return nil, writeStreamStatusOutput{}, err
	}
	return nil, status, nil
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
	s.ensureTransferManager().RevokeWriteStream(req.Session, input.StreamID)
	return nil, writeStreamAbortOutput{StreamID: strings.TrimSpace(input.StreamID), Aborted: true, BytesReceived: bytes}, nil
}

func (s *server) ensureWriteStreamManager() *writeStreamManager {
	if s.writeStreams != nil {
		return s.writeStreams
	}
	s.writeStreams = newWriteStreamManager(s.writeStreamLog)
	return s.writeStreams
}

func (s *server) registerWriteStreamUploadCapability(session *mcpsdk.ServerSession, streamID string, kind writeStreamKind) (writeStreamTransferOutput, error) {
	manager := s.ensureTransferManager()
	writeStreams := s.ensureWriteStreamManager()
	reg, err := manager.RegisterUpload(session, streamID, func(ctx context.Context, reader io.Reader) (int64, error) {
		_, total, uploadErr := writeStreams.Upload(session, streamID, kind, reader)
		return total, uploadErr
	})
	if err != nil {
		return writeStreamTransferOutput{}, err
	}
	return writeStreamTransferOutput{
		UploadURL:           s.transferURL(reg.ID),
		UploadMethod:        reg.Method,
		UploadExpiresAtUnix: reg.ExpiresAtUnix,
	}, nil
}
