package mcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
)

type queueEnqueueToolInput struct {
	Queue             string         `json:"queue,omitempty" jsonschema:"Queue name (defaults to lockd.agent.bus)"`
	Namespace         string         `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	PayloadText       string         `json:"payload_text,omitempty" jsonschema:"UTF-8 payload text"`
	PayloadBase64     string         `json:"payload_base64,omitempty" jsonschema:"Base64-encoded payload bytes"`
	ContentType       string         `json:"content_type,omitempty" jsonschema:"Payload content type"`
	DelaySeconds      int64          `json:"delay_seconds,omitempty" jsonschema:"Initial invisibility delay"`
	VisibilitySeconds int64          `json:"visibility_seconds,omitempty" jsonschema:"Visibility timeout for dequeued lease"`
	TTLSeconds        int64          `json:"ttl_seconds,omitempty" jsonschema:"Message retention TTL in seconds"`
	MaxAttempts       int            `json:"max_attempts,omitempty" jsonschema:"Maximum failed attempts before terminal handling"`
	Attributes        map[string]any `json:"attributes,omitempty" jsonschema:"Message attributes metadata"`
}

type queueEnqueueToolOutput struct {
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

func (s *server) handleQueueEnqueueTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input queueEnqueueToolInput) (*mcpsdk.CallToolResult, queueEnqueueToolOutput, error) {
	queue := s.resolveQueue(input.Queue)
	namespace := s.resolveNamespace(input.Namespace)

	var payload []byte
	if input.PayloadBase64 != "" {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(input.PayloadBase64))
		if err != nil {
			return nil, queueEnqueueToolOutput{}, fmt.Errorf("decode payload_base64: %w", err)
		}
		payload = decoded
	} else {
		payload = []byte(input.PayloadText)
	}
	contentType := strings.TrimSpace(input.ContentType)
	if contentType == "" {
		contentType = "application/json"
	}

	resp, err := s.upstream.Enqueue(ctx, queue, bytes.NewReader(payload), lockdclient.EnqueueOptions{
		Namespace:   namespace,
		Delay:       time.Duration(input.DelaySeconds) * time.Second,
		Visibility:  time.Duration(input.VisibilitySeconds) * time.Second,
		TTL:         time.Duration(input.TTLSeconds) * time.Second,
		MaxAttempts: input.MaxAttempts,
		Attributes:  input.Attributes,
		ContentType: contentType,
	})
	if err != nil {
		return nil, queueEnqueueToolOutput{}, err
	}
	return nil, queueEnqueueToolOutput{
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
}

type queueDequeueToolInput struct {
	Queue             string `json:"queue,omitempty" jsonschema:"Queue name (defaults to lockd.agent.bus)"`
	Namespace         string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	Owner             string `json:"owner,omitempty" jsonschema:"Consumer owner ID (defaults to oauth client id)"`
	BlockSecond       int64  `json:"block_seconds,omitempty" jsonschema:"Long-poll wait; -1 no wait, 0 wait forever, >0 wait seconds"`
	Stateful          bool   `json:"stateful,omitempty" jsonschema:"Acquire workflow state lease alongside dequeued message"`
	VisibilitySeconds int64  `json:"visibility_seconds,omitempty" jsonschema:"Optional visibility timeout override in seconds"`
	PageSize          int    `json:"page_size,omitempty" jsonschema:"Optional dequeue page size hint"`
	StartAfter        string `json:"start_after,omitempty" jsonschema:"Optional dequeue cursor start-after message id"`
	TxnID             string `json:"txn_id,omitempty" jsonschema:"Optional transaction id to bind dequeue operations"`
}

type queueDequeueToolOutput struct {
	Namespace             string `json:"namespace"`
	Queue                 string `json:"queue"`
	Found                 bool   `json:"found"`
	MessageID             string `json:"message_id,omitempty"`
	Attempts              int    `json:"attempts,omitempty"`
	MaxAttempts           int    `json:"max_attempts,omitempty"`
	FailureAttempts       int    `json:"failure_attempts,omitempty"`
	LeaseID               string `json:"lease_id,omitempty"`
	LeaseExpiresAtUnix    int64  `json:"lease_expires_at_unix,omitempty"`
	FencingToken          int64  `json:"fencing_token,omitempty"`
	MetaETag              string `json:"meta_etag,omitempty"`
	TxnID                 string `json:"txn_id,omitempty"`
	Cursor                string `json:"cursor,omitempty"`
	NotVisibleUntilUnix   int64  `json:"not_visible_until_unix,omitempty"`
	VisibilitySeconds     int64  `json:"visibility_timeout_seconds,omitempty"`
	CorrelationID         string `json:"correlation_id,omitempty"`
	ContentType           string `json:"content_type,omitempty"`
	PayloadBytes          int64  `json:"payload_bytes,omitempty"`
	PayloadText           string `json:"payload_text,omitempty"`
	PayloadBase64         string `json:"payload_base64,omitempty"`
	StateLeaseID          string `json:"state_lease_id,omitempty"`
	StateLeaseExpiresUnix int64  `json:"state_lease_expires_at_unix,omitempty"`
	StateFencingToken     int64  `json:"state_fencing_token,omitempty"`
	StateETag             string `json:"state_etag,omitempty"`
}

func (s *server) handleQueueDequeueTool(ctx context.Context, req *mcpsdk.CallToolRequest, input queueDequeueToolInput) (*mcpsdk.CallToolResult, queueDequeueToolOutput, error) {
	queue := s.resolveQueue(input.Queue)
	namespace := s.resolveNamespace(input.Namespace)
	clientID := ""
	if req != nil {
		clientID = requestClientID(req.Extra)
	}
	owner := strings.TrimSpace(input.Owner)
	if owner == "" {
		owner = defaultOwner(clientID)
	}
	blockSeconds := input.BlockSecond
	if blockSeconds == 0 {
		blockSeconds = api.BlockNoWait
	}
	opts := lockdclient.DequeueOptions{
		Namespace:    namespace,
		Owner:        owner,
		BlockSeconds: blockSeconds,
		TxnID:        strings.TrimSpace(input.TxnID),
	}
	if input.VisibilitySeconds > 0 {
		opts.Visibility = time.Duration(input.VisibilitySeconds) * time.Second
	}
	if input.PageSize > 0 {
		opts.PageSize = input.PageSize
	}
	if startAfter := strings.TrimSpace(input.StartAfter); startAfter != "" {
		opts.StartAfter = startAfter
	}
	var (
		msg *lockdclient.QueueMessage
		err error
	)
	if input.Stateful {
		msg, err = s.upstream.DequeueWithState(ctx, queue, opts)
	} else {
		msg, err = s.upstream.Dequeue(ctx, queue, opts)
	}
	if err != nil {
		var apiErr *lockdclient.APIError
		if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
			return nil, queueDequeueToolOutput{
				Namespace: namespace,
				Queue:     queue,
				Found:     false,
			}, nil
		}
		return nil, queueDequeueToolOutput{}, err
	}
	if msg == nil {
		return nil, queueDequeueToolOutput{
			Namespace: namespace,
			Queue:     queue,
			Found:     false,
		}, nil
	}

	reader, err := msg.PayloadReader()
	if err != nil {
		return nil, queueDequeueToolOutput{}, err
	}
	payload, readErr := io.ReadAll(reader)
	_ = reader.Close()
	_ = msg.ClosePayload()
	if readErr != nil {
		return nil, queueDequeueToolOutput{}, readErr
	}

	out := queueDequeueToolOutput{
		Namespace:           msg.Namespace(),
		Queue:               msg.Queue(),
		Found:               true,
		MessageID:           msg.MessageID(),
		Attempts:            msg.Attempts(),
		MaxAttempts:         msg.MaxAttempts(),
		FailureAttempts:     msg.FailureAttempts(),
		LeaseID:             msg.LeaseID(),
		LeaseExpiresAtUnix:  msg.LeaseExpiresAt(),
		FencingToken:        msg.FencingToken(),
		MetaETag:            msg.MetaETag(),
		TxnID:               msg.TxnID(),
		Cursor:              msg.Cursor(),
		NotVisibleUntilUnix: msg.NotVisibleUntil().Unix(),
		VisibilitySeconds:   int64(msg.VisibilityTimeout().Seconds()),
		CorrelationID:       msg.CorrelationID(),
		ContentType:         msg.ContentType(),
		PayloadBytes:        msg.PayloadSize(),
	}
	if state := msg.StateHandle(); state != nil {
		out.StateLeaseID = state.LeaseID()
		out.StateLeaseExpiresUnix = state.LeaseExpiresAt()
		out.StateFencingToken = state.FencingToken()
		out.StateETag = state.ETag()
	}
	if len(payload) > 0 {
		out.PayloadBase64 = base64.StdEncoding.EncodeToString(payload)
		if utf8.Valid(payload) {
			out.PayloadText = string(payload)
		}
	}
	return nil, out, nil
}

type queueAckToolInput struct {
	Namespace         string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	Queue             string `json:"queue" jsonschema:"Queue name"`
	MessageID         string `json:"message_id" jsonschema:"Dequeued message id"`
	LeaseID           string `json:"lease_id" jsonschema:"Dequeued lease id"`
	TxnID             string `json:"txn_id,omitempty" jsonschema:"Optional transaction id"`
	FencingToken      int64  `json:"fencing_token" jsonschema:"Dequeued fencing token"`
	MetaETag          string `json:"meta_etag" jsonschema:"Dequeued meta etag"`
	StateETag         string `json:"state_etag,omitempty" jsonschema:"Optional state etag"`
	StateLeaseID      string `json:"state_lease_id,omitempty" jsonschema:"Optional state lease id"`
	StateFencingToken int64  `json:"state_fencing_token,omitempty" jsonschema:"Optional state fencing token"`
}

type queueAckToolOutput struct {
	Acked         bool   `json:"acked"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

func (s *server) handleQueueAckTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input queueAckToolInput) (*mcpsdk.CallToolResult, queueAckToolOutput, error) {
	req := api.AckRequest{
		Namespace:         s.resolveNamespace(input.Namespace),
		Queue:             strings.TrimSpace(input.Queue),
		MessageID:         strings.TrimSpace(input.MessageID),
		LeaseID:           strings.TrimSpace(input.LeaseID),
		TxnID:             strings.TrimSpace(input.TxnID),
		FencingToken:      input.FencingToken,
		MetaETag:          strings.TrimSpace(input.MetaETag),
		StateETag:         strings.TrimSpace(input.StateETag),
		StateLeaseID:      strings.TrimSpace(input.StateLeaseID),
		StateFencingToken: input.StateFencingToken,
	}
	if req.Queue == "" || req.MessageID == "" || req.LeaseID == "" || req.MetaETag == "" {
		return nil, queueAckToolOutput{}, fmt.Errorf("queue, message_id, lease_id, and meta_etag are required")
	}
	resp, err := s.upstream.QueueAck(ctx, req)
	if err != nil {
		return nil, queueAckToolOutput{}, err
	}
	return nil, queueAckToolOutput{
		Acked:         resp.Acked,
		CorrelationID: resp.CorrelationID,
	}, nil
}

type queueNackToolInput struct {
	Namespace         string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	Queue             string `json:"queue" jsonschema:"Queue name"`
	MessageID         string `json:"message_id" jsonschema:"Dequeued message id"`
	LeaseID           string `json:"lease_id" jsonschema:"Dequeued lease id"`
	TxnID             string `json:"txn_id,omitempty" jsonschema:"Optional transaction id"`
	FencingToken      int64  `json:"fencing_token" jsonschema:"Dequeued fencing token"`
	MetaETag          string `json:"meta_etag" jsonschema:"Dequeued meta etag"`
	StateETag         string `json:"state_etag,omitempty" jsonschema:"Optional state etag"`
	StateLeaseID      string `json:"state_lease_id,omitempty" jsonschema:"Optional state lease id"`
	StateFencingToken int64  `json:"state_fencing_token,omitempty" jsonschema:"Optional state fencing token"`
	DelaySeconds      int64  `json:"delay_seconds,omitempty" jsonschema:"Visibility delay before message becomes available again"`
	Reason            string `json:"reason,omitempty" jsonschema:"Optional failure reason detail"`
}

type queueNackToolOutput struct {
	Requeued      bool   `json:"requeued"`
	MetaETag      string `json:"meta_etag,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

func (s *server) handleQueueNackTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input queueNackToolInput) (*mcpsdk.CallToolResult, queueNackToolOutput, error) {
	req := api.NackRequest{
		Namespace:         s.resolveNamespace(input.Namespace),
		Queue:             strings.TrimSpace(input.Queue),
		MessageID:         strings.TrimSpace(input.MessageID),
		LeaseID:           strings.TrimSpace(input.LeaseID),
		TxnID:             strings.TrimSpace(input.TxnID),
		FencingToken:      input.FencingToken,
		MetaETag:          strings.TrimSpace(input.MetaETag),
		StateETag:         strings.TrimSpace(input.StateETag),
		StateLeaseID:      strings.TrimSpace(input.StateLeaseID),
		StateFencingToken: input.StateFencingToken,
		DelaySeconds:      input.DelaySeconds,
		Intent:            api.NackIntentFailure,
	}
	if reason := strings.TrimSpace(input.Reason); reason != "" {
		req.LastError = map[string]any{"detail": reason}
	}
	if req.Queue == "" || req.MessageID == "" || req.LeaseID == "" || req.MetaETag == "" {
		return nil, queueNackToolOutput{}, fmt.Errorf("queue, message_id, lease_id, and meta_etag are required")
	}
	resp, err := s.upstream.QueueNack(ctx, req)
	if err != nil {
		return nil, queueNackToolOutput{}, err
	}
	return nil, queueNackToolOutput{
		Requeued:      resp.Requeued,
		MetaETag:      resp.MetaETag,
		CorrelationID: resp.CorrelationID,
	}, nil
}

type queueDeferToolInput struct {
	Namespace         string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	Queue             string `json:"queue" jsonschema:"Queue name"`
	MessageID         string `json:"message_id" jsonschema:"Dequeued message id"`
	LeaseID           string `json:"lease_id" jsonschema:"Dequeued lease id"`
	TxnID             string `json:"txn_id,omitempty" jsonschema:"Optional transaction id"`
	FencingToken      int64  `json:"fencing_token" jsonschema:"Dequeued fencing token"`
	MetaETag          string `json:"meta_etag" jsonschema:"Dequeued meta etag"`
	StateETag         string `json:"state_etag,omitempty" jsonschema:"Optional state etag"`
	StateLeaseID      string `json:"state_lease_id,omitempty" jsonschema:"Optional state lease id"`
	StateFencingToken int64  `json:"state_fencing_token,omitempty" jsonschema:"Optional state fencing token"`
	DelaySeconds      int64  `json:"delay_seconds,omitempty" jsonschema:"Visibility delay before message becomes available again"`
}

type queueDeferToolOutput struct {
	Requeued      bool   `json:"requeued"`
	MetaETag      string `json:"meta_etag,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

func (s *server) handleQueueDeferTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input queueDeferToolInput) (*mcpsdk.CallToolResult, queueDeferToolOutput, error) {
	req := api.NackRequest{
		Namespace:         s.resolveNamespace(input.Namespace),
		Queue:             strings.TrimSpace(input.Queue),
		MessageID:         strings.TrimSpace(input.MessageID),
		LeaseID:           strings.TrimSpace(input.LeaseID),
		TxnID:             strings.TrimSpace(input.TxnID),
		FencingToken:      input.FencingToken,
		MetaETag:          strings.TrimSpace(input.MetaETag),
		StateETag:         strings.TrimSpace(input.StateETag),
		StateLeaseID:      strings.TrimSpace(input.StateLeaseID),
		StateFencingToken: input.StateFencingToken,
		DelaySeconds:      input.DelaySeconds,
		Intent:            api.NackIntentDefer,
	}
	if req.Queue == "" || req.MessageID == "" || req.LeaseID == "" || req.MetaETag == "" {
		return nil, queueDeferToolOutput{}, fmt.Errorf("queue, message_id, lease_id, and meta_etag are required")
	}
	resp, err := s.upstream.QueueNack(ctx, req)
	if err != nil {
		return nil, queueDeferToolOutput{}, err
	}
	return nil, queueDeferToolOutput{
		Requeued:      resp.Requeued,
		MetaETag:      resp.MetaETag,
		CorrelationID: resp.CorrelationID,
	}, nil
}

type queueExtendToolInput struct {
	Namespace         string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	Queue             string `json:"queue" jsonschema:"Queue name"`
	MessageID         string `json:"message_id" jsonschema:"Dequeued message id"`
	LeaseID           string `json:"lease_id" jsonschema:"Dequeued lease id"`
	TxnID             string `json:"txn_id,omitempty" jsonschema:"Optional transaction id"`
	FencingToken      int64  `json:"fencing_token" jsonschema:"Dequeued fencing token"`
	MetaETag          string `json:"meta_etag" jsonschema:"Dequeued meta etag"`
	StateLeaseID      string `json:"state_lease_id,omitempty" jsonschema:"Optional state lease id"`
	StateFencingToken int64  `json:"state_fencing_token,omitempty" jsonschema:"Optional state fencing token"`
	ExtendBySeconds   int64  `json:"extend_by_seconds,omitempty" jsonschema:"Lease extension in seconds"`
}

type queueExtendToolOutput struct {
	LeaseExpiresAtUnix  int64  `json:"lease_expires_at_unix"`
	VisibilitySeconds   int64  `json:"visibility_timeout_seconds"`
	MetaETag            string `json:"meta_etag,omitempty"`
	StateLeaseExpiresAt int64  `json:"state_lease_expires_at_unix,omitempty"`
	CorrelationID       string `json:"correlation_id,omitempty"`
}

func (s *server) handleQueueExtendTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input queueExtendToolInput) (*mcpsdk.CallToolResult, queueExtendToolOutput, error) {
	req := api.ExtendRequest{
		Namespace:         s.resolveNamespace(input.Namespace),
		Queue:             strings.TrimSpace(input.Queue),
		MessageID:         strings.TrimSpace(input.MessageID),
		LeaseID:           strings.TrimSpace(input.LeaseID),
		TxnID:             strings.TrimSpace(input.TxnID),
		FencingToken:      input.FencingToken,
		MetaETag:          strings.TrimSpace(input.MetaETag),
		StateLeaseID:      strings.TrimSpace(input.StateLeaseID),
		StateFencingToken: input.StateFencingToken,
		ExtendBySeconds:   input.ExtendBySeconds,
	}
	if req.Queue == "" || req.MessageID == "" || req.LeaseID == "" || req.MetaETag == "" {
		return nil, queueExtendToolOutput{}, fmt.Errorf("queue, message_id, lease_id, and meta_etag are required")
	}
	resp, err := s.upstream.QueueExtend(ctx, req)
	if err != nil {
		return nil, queueExtendToolOutput{}, err
	}
	return nil, queueExtendToolOutput{
		LeaseExpiresAtUnix:  resp.LeaseExpiresAtUnix,
		VisibilitySeconds:   resp.VisibilityTimeoutSeconds,
		MetaETag:            resp.MetaETag,
		StateLeaseExpiresAt: resp.StateLeaseExpiresAtUnix,
		CorrelationID:       resp.CorrelationID,
	}, nil
}

type queueSubscribeToolInput struct {
	Namespace string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	Queue     string `json:"queue,omitempty" jsonschema:"Queue name (defaults to lockd.agent.bus)"`
}

type queueSubscribeToolOutput struct {
	Namespace  string `json:"namespace"`
	Queue      string `json:"queue"`
	Subscribed bool   `json:"subscribed"`
}

func (s *server) handleQueueSubscribeTool(ctx context.Context, req *mcpsdk.CallToolRequest, input queueSubscribeToolInput) (*mcpsdk.CallToolResult, queueSubscribeToolOutput, error) {
	if req == nil || req.Session == nil {
		return nil, queueSubscribeToolOutput{}, fmt.Errorf("queue subscriptions require an active MCP session")
	}
	namespace := s.resolveNamespace(input.Namespace)
	queue := s.resolveQueue(input.Queue)
	subscribed, err := s.subscriptions.Subscribe(ctx, req.Session, requestClientID(req.Extra), namespace, queue)
	if err != nil {
		return nil, queueSubscribeToolOutput{}, err
	}
	return nil, queueSubscribeToolOutput{
		Namespace:  namespace,
		Queue:      queue,
		Subscribed: subscribed,
	}, nil
}

type queueUnsubscribeToolInput struct {
	Namespace string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	Queue     string `json:"queue,omitempty" jsonschema:"Queue name (defaults to lockd.agent.bus)"`
}

type queueUnsubscribeToolOutput struct {
	Namespace    string `json:"namespace"`
	Queue        string `json:"queue"`
	Unsubscribed bool   `json:"unsubscribed"`
}

func (s *server) handleQueueUnsubscribeTool(_ context.Context, req *mcpsdk.CallToolRequest, input queueUnsubscribeToolInput) (*mcpsdk.CallToolResult, queueUnsubscribeToolOutput, error) {
	if req == nil || req.Session == nil {
		return nil, queueUnsubscribeToolOutput{}, fmt.Errorf("queue subscriptions require an active MCP session")
	}
	namespace := s.resolveNamespace(input.Namespace)
	queue := s.resolveQueue(input.Queue)
	unsubscribed := s.subscriptions.Unsubscribe(req.Session, namespace, queue)
	return nil, queueUnsubscribeToolOutput{
		Namespace:    namespace,
		Queue:        queue,
		Unsubscribed: unsubscribed,
	}, nil
}
