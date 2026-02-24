package mcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

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
	if strings.TrimSpace(input.PayloadBase64) != "" && input.PayloadText != "" {
		return nil, queueEnqueueToolOutput{}, fmt.Errorf("payload_text and payload_base64 are mutually exclusive")
	}

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
	if err := validateInlinePayloadBytes(int64(len(payload)), s.cfg.InlineMaxBytes, toolQueueEnqueue, toolQueueWriteStreamBegin); err != nil {
		return nil, queueEnqueueToolOutput{}, err
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
	Cursor            string `json:"cursor,omitempty" jsonschema:"Optional continuation cursor from previous dequeue response"`
	TxnID             string `json:"txn_id,omitempty" jsonschema:"Optional transaction id to bind dequeue operations"`
	PayloadMode       string `json:"payload_mode,omitempty" jsonschema:"Payload mode: auto (default), inline, stream, or none"`
	StateMode         string `json:"state_mode,omitempty" jsonschema:"State payload mode when stateful=true: auto, inline, stream, or none (default)"`
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
	NextCursor            string `json:"next_cursor,omitempty"`
	NotVisibleUntilUnix   int64  `json:"not_visible_until_unix,omitempty"`
	VisibilitySeconds     int64  `json:"visibility_timeout_seconds,omitempty"`
	CorrelationID         string `json:"correlation_id,omitempty"`
	ContentType           string `json:"content_type,omitempty"`
	PayloadBytes          int64  `json:"payload_bytes,omitempty"`
	PayloadMode           string `json:"payload_mode"`
	PayloadText           string `json:"payload_text,omitempty"`
	PayloadBase64         string `json:"payload_base64,omitempty"`
	PayloadDownloadURL    string `json:"payload_download_url,omitempty"`
	PayloadDownloadMethod string `json:"payload_download_method,omitempty"`
	PayloadDownloadExpiry int64  `json:"payload_download_expires_at_unix,omitempty"`
	StateLeaseID          string `json:"state_lease_id,omitempty"`
	StateLeaseExpiresUnix int64  `json:"state_lease_expires_at_unix,omitempty"`
	StateFencingToken     int64  `json:"state_fencing_token,omitempty"`
	StateETag             string `json:"state_etag,omitempty"`
	StateVersion          int64  `json:"state_version,omitempty"`
	StateFound            bool   `json:"state_found,omitempty"`
	StatePayloadMode      string `json:"state_payload_mode,omitempty"`
	StatePayloadBytes     int64  `json:"state_payload_bytes,omitempty"`
	StatePayloadText      string `json:"state_payload_text,omitempty"`
	StatePayloadBase64    string `json:"state_payload_base64,omitempty"`
	StateDownloadURL      string `json:"state_download_url,omitempty"`
	StateDownloadMethod   string `json:"state_download_method,omitempty"`
	StateDownloadExpiry   int64  `json:"state_download_expires_at_unix,omitempty"`
}

func (s *server) handleQueueDequeueTool(_ context.Context, req *mcpsdk.CallToolRequest, input queueDequeueToolInput) (*mcpsdk.CallToolResult, queueDequeueToolOutput, error) {
	payloadMode, err := parsePayloadMode(input.PayloadMode, payloadModeAuto)
	if err != nil {
		return nil, queueDequeueToolOutput{}, err
	}
	stateMode, err := parsePayloadModeField(input.StateMode, payloadModeNone, "state_mode")
	if err != nil {
		return nil, queueDequeueToolOutput{}, err
	}
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
	if cursor := strings.TrimSpace(input.Cursor); cursor != "" {
		opts.StartAfter = cursor
	}
	var msg *lockdclient.QueueMessage
	dequeueCtx, cancelDequeue := context.WithCancel(context.Background())
	if input.Stateful {
		msg, err = s.upstream.DequeueWithState(dequeueCtx, queue, opts)
	} else {
		msg, err = s.upstream.Dequeue(dequeueCtx, queue, opts)
	}
	if err != nil {
		cancelDequeue()
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
		cancelDequeue()
		return nil, queueDequeueToolOutput{
			Namespace: namespace,
			Queue:     queue,
			Found:     false,
		}, nil
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
		NextCursor:          msg.Cursor(),
		NotVisibleUntilUnix: msg.NotVisibleUntil().Unix(),
		VisibilitySeconds:   int64(msg.VisibilityTimeout().Seconds()),
		CorrelationID:       msg.CorrelationID(),
		ContentType:         msg.ContentType(),
		PayloadBytes:        msg.PayloadSize(),
		PayloadMode:         string(payloadModeNone),
	}
	if state := msg.StateHandle(); state != nil {
		out.StateLeaseID = state.LeaseID()
		out.StateLeaseExpiresUnix = state.LeaseExpiresAt()
		out.StateFencingToken = state.FencingToken()
		out.StateETag = state.ETag()
		out.StatePayloadMode = string(payloadModeNone)
	}
	if err := s.applyQueuePayloadMode(req, msg, &out, payloadMode, cancelDequeue); err != nil {
		_ = msg.ClosePayload()
		cancelDequeue()
		return nil, queueDequeueToolOutput{}, err
	}
	if input.Stateful && msg.StateHandle() != nil {
		if err := s.applyQueueStatePayloadMode(req, msg, &out, stateMode); err != nil {
			return nil, queueDequeueToolOutput{}, err
		}
	}
	return nil, out, nil
}

func (s *server) applyQueuePayloadMode(req *mcpsdk.CallToolRequest, msg *lockdclient.QueueMessage, out *queueDequeueToolOutput, mode payloadMode, cancelDequeue context.CancelFunc) error {
	if msg == nil || out == nil {
		return nil
	}
	if out.PayloadBytes <= 0 {
		out.PayloadMode = string(payloadModeNone)
		_ = msg.ClosePayload()
		cancelDequeue()
		return nil
	}
	switch mode {
	case payloadModeNone:
		out.PayloadMode = string(payloadModeNone)
		_ = msg.ClosePayload()
		cancelDequeue()
		return nil
	case payloadModeInline:
		limit := normalizedInlineMaxBytes(s.cfg.InlineMaxBytes)
		if out.PayloadBytes > limit {
			return inlinePayloadTooLargeError(toolQueueDequeue, out.PayloadBytes, limit, "lockd.queue.dequeue(payload_mode=stream)")
		}
		reader, err := msg.PayloadReader()
		if err != nil {
			return err
		}
		inline, err := readInlinePayloadStrict(reader, s.cfg.InlineMaxBytes, toolQueueDequeue, "lockd.queue.dequeue(payload_mode=stream)")
		_ = reader.Close()
		_ = msg.ClosePayload()
		cancelDequeue()
		if err != nil {
			return err
		}
		out.PayloadMode = string(payloadModeInline)
		out.PayloadBytes = inline.Bytes
		out.PayloadText = inline.Text
		out.PayloadBase64 = inline.Base64
		return nil
	case payloadModeAuto:
		limit := normalizedInlineMaxBytes(s.cfg.InlineMaxBytes)
		if out.PayloadBytes <= limit {
			return s.applyQueuePayloadMode(req, msg, out, payloadModeInline, cancelDequeue)
		}
		return s.applyQueuePayloadMode(req, msg, out, payloadModeStream, cancelDequeue)
	case payloadModeStream:
		if req == nil || req.Session == nil {
			return fmt.Errorf("payload_mode=stream requires an active MCP session")
		}
		reader, err := msg.PayloadReader()
		if err != nil {
			return err
		}
		reg, err := s.ensureTransferManager().RegisterDownload(req.Session, reader, transferDownloadRequest{
			ContentType:   strings.TrimSpace(out.ContentType),
			ContentLength: out.PayloadBytes,
			Filename:      out.MessageID,
			Headers: map[string]string{
				"X-Lockd-Queue":         out.Queue,
				"X-Lockd-Namespace":     out.Namespace,
				"X-Lockd-Queue-Message": out.MessageID,
			},
			Cleanup: cancelDequeue,
		})
		if err != nil {
			_ = reader.Close()
			return err
		}
		out.PayloadMode = string(payloadModeStream)
		out.PayloadDownloadURL = s.transferURL(reg.ID)
		out.PayloadDownloadMethod = reg.Method
		out.PayloadDownloadExpiry = reg.ExpiresAtUnix
		return nil
	default:
		return fmt.Errorf("unsupported payload_mode %q", mode)
	}
}

func (s *server) applyQueueStatePayloadMode(req *mcpsdk.CallToolRequest, msg *lockdclient.QueueMessage, out *queueDequeueToolOutput, mode payloadMode) error {
	if msg == nil || out == nil {
		return nil
	}
	stateHandle := msg.StateHandle()
	if stateHandle == nil {
		out.StatePayloadMode = string(payloadModeNone)
		return nil
	}
	out.StatePayloadMode = string(payloadModeNone)
	if mode == payloadModeNone {
		return nil
	}
	if mode == payloadModeStream && (req == nil || req.Session == nil) {
		return fmt.Errorf("state_mode=stream requires an active MCP session")
	}
	snapshot, err := stateHandle.Get(context.Background())
	if err != nil {
		return err
	}
	if snapshot == nil {
		out.StateFound = false
		return nil
	}
	out.StateFound = snapshot.HasState
	out.StateVersion = snapshot.Version
	if strings.TrimSpace(snapshot.ETag) != "" {
		out.StateETag = snapshot.ETag
	}
	if !snapshot.HasState || snapshot.Reader == nil {
		_ = snapshot.Close()
		return nil
	}
	switch mode {
	case payloadModeInline:
		inline, inlineErr := readInlinePayloadStrict(snapshot.Reader, s.cfg.InlineMaxBytes, toolQueueDequeue, "lockd.queue.dequeue(state_mode=stream)")
		_ = snapshot.Close()
		if inlineErr != nil {
			return inlineErr
		}
		out.StatePayloadMode = string(payloadModeInline)
		out.StatePayloadBytes = inline.Bytes
		out.StatePayloadText = inline.Text
		out.StatePayloadBase64 = inline.Base64
		return nil
	case payloadModeAuto:
		inline, tooLarge, inlineErr := readInlinePayloadAuto(snapshot.Reader, s.cfg.InlineMaxBytes)
		_ = snapshot.Close()
		if inlineErr != nil {
			return inlineErr
		}
		if !tooLarge {
			out.StatePayloadMode = string(payloadModeInline)
			out.StatePayloadBytes = inline.Bytes
			out.StatePayloadText = inline.Text
			out.StatePayloadBase64 = inline.Base64
			return nil
		}
		if req == nil || req.Session == nil {
			limit := normalizedInlineMaxBytes(s.cfg.InlineMaxBytes)
			return inlinePayloadTooLargeError(toolQueueDequeue, limit+1, limit, "lockd.queue.dequeue(state_mode=stream)")
		}
		return s.applyQueueStatePayloadMode(req, msg, out, payloadModeStream)
	case payloadModeStream:
		refetch, refetchErr := stateHandle.Get(context.Background())
		if refetchErr != nil {
			_ = snapshot.Close()
			return refetchErr
		}
		_ = snapshot.Close()
		if refetch == nil || !refetch.HasState || refetch.Reader == nil {
			if refetch != nil {
				_ = refetch.Close()
			}
			out.StateFound = false
			out.StatePayloadMode = string(payloadModeNone)
			return nil
		}
		reg, regErr := s.ensureTransferManager().RegisterDownload(req.Session, refetch.Reader, transferDownloadRequest{
			ContentType: "application/json",
			Headers: map[string]string{
				"X-Lockd-Queue-State-Queue":   out.Queue,
				"X-Lockd-Queue-State-Message": out.MessageID,
				"X-Lockd-Queue-State-ETag":    out.StateETag,
			},
			Cleanup: func() {
				_ = refetch.Close()
			},
		})
		if regErr != nil {
			_ = refetch.Close()
			return regErr
		}
		out.StatePayloadMode = string(payloadModeStream)
		out.StateDownloadURL = s.transferURL(reg.ID)
		out.StateDownloadMethod = reg.Method
		out.StateDownloadExpiry = reg.ExpiresAtUnix
		return nil
	default:
		_ = snapshot.Close()
		return fmt.Errorf("unsupported state_mode %q", mode)
	}
}

type queueWatchToolInput struct {
	Queue           string `json:"queue,omitempty" jsonschema:"Queue name (defaults to lockd.agent.bus)"`
	Namespace       string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	DurationSeconds int64  `json:"duration_seconds,omitempty" jsonschema:"Maximum watch duration in seconds (default: 30)"`
	MaxEvents       int    `json:"max_events,omitempty" jsonschema:"Maximum events to return before stopping (default: 1)"`
}

type queueStatsToolInput struct {
	Queue     string `json:"queue,omitempty" jsonschema:"Queue name (defaults to lockd.agent.bus)"`
	Namespace string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
}

type queueStatsToolOutput struct {
	Namespace               string `json:"namespace"`
	Queue                   string `json:"queue"`
	WaitingConsumers        int    `json:"waiting_consumers"`
	PendingCandidates       int    `json:"pending_candidates"`
	TotalConsumers          int    `json:"total_consumers"`
	HasActiveWatcher        bool   `json:"has_active_watcher"`
	Available               bool   `json:"available"`
	HeadMessageID           string `json:"head_message_id,omitempty"`
	HeadEnqueuedAtUnix      int64  `json:"head_enqueued_at_unix,omitempty"`
	HeadNotVisibleUntilUnix int64  `json:"head_not_visible_until_unix,omitempty"`
	HeadAgeSeconds          int64  `json:"head_age_seconds,omitempty"`
	CorrelationID           string `json:"correlation_id,omitempty"`
}

func (s *server) handleQueueStatsTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input queueStatsToolInput) (*mcpsdk.CallToolResult, queueStatsToolOutput, error) {
	namespace := s.resolveNamespace(input.Namespace)
	queue := s.resolveQueue(input.Queue)
	resp, err := s.upstream.QueueStats(ctx, queue, lockdclient.QueueStatsOptions{
		Namespace: namespace,
	})
	if err != nil {
		return nil, queueStatsToolOutput{}, err
	}
	return nil, queueStatsToolOutput{
		Namespace:               resp.Namespace,
		Queue:                   resp.Queue,
		WaitingConsumers:        resp.WaitingConsumers,
		PendingCandidates:       resp.PendingCandidates,
		TotalConsumers:          resp.TotalConsumers,
		HasActiveWatcher:        resp.HasActiveWatcher,
		Available:               resp.Available,
		HeadMessageID:           resp.HeadMessageID,
		HeadEnqueuedAtUnix:      resp.HeadEnqueuedAtUnix,
		HeadNotVisibleUntilUnix: resp.HeadNotVisibleUntilUnix,
		HeadAgeSeconds:          resp.HeadAgeSeconds,
		CorrelationID:           resp.CorrelationID,
	}, nil
}

type queueWatchEventOutput struct {
	Namespace     string `json:"namespace"`
	Queue         string `json:"queue"`
	Available     bool   `json:"available"`
	HeadMessageID string `json:"head_message_id,omitempty"`
	ChangedAtUnix int64  `json:"changed_at_unix,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

type queueWatchToolOutput struct {
	Namespace  string                  `json:"namespace"`
	Queue      string                  `json:"queue"`
	Events     []queueWatchEventOutput `json:"events"`
	EventCount int                     `json:"event_count"`
	StopReason string                  `json:"stop_reason"`
}

func (s *server) handleQueueWatchTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input queueWatchToolInput) (*mcpsdk.CallToolResult, queueWatchToolOutput, error) {
	namespace := s.resolveNamespace(input.Namespace)
	queue := s.resolveQueue(input.Queue)
	duration := input.DurationSeconds
	if duration <= 0 {
		duration = 30
	}
	maxEvents := input.MaxEvents
	if maxEvents <= 0 {
		maxEvents = 1
	}

	out := queueWatchToolOutput{
		Namespace:  namespace,
		Queue:      queue,
		Events:     make([]queueWatchEventOutput, 0, maxEvents),
		StopReason: "timeout",
	}

	watchCtx, cancel := context.WithTimeout(ctx, time.Duration(duration)*time.Second)
	defer cancel()
	stopErr := errors.New("lockd-mcp-watch-stop")
	err := s.upstream.WatchQueue(watchCtx, queue, lockdclient.WatchQueueOptions{
		Namespace: namespace,
	}, func(_ context.Context, ev lockdclient.QueueWatchEvent) error {
		out.Events = append(out.Events, queueWatchEventOutput{
			Namespace:     ev.Namespace,
			Queue:         ev.Queue,
			Available:     ev.Available,
			HeadMessageID: ev.HeadMessageID,
			ChangedAtUnix: ev.ChangedAt.Unix(),
			CorrelationID: ev.CorrelationID,
		})
		if len(out.Events) >= maxEvents {
			out.StopReason = "max_events"
			return stopErr
		}
		return nil
	})

	out.EventCount = len(out.Events)
	switch {
	case err == nil:
		return nil, out, nil
	case errors.Is(err, stopErr):
		return nil, out, nil
	case errors.Is(err, context.DeadlineExceeded), errors.Is(watchCtx.Err(), context.DeadlineExceeded):
		out.StopReason = "timeout"
		return nil, out, nil
	case errors.Is(err, context.Canceled), errors.Is(watchCtx.Err(), context.Canceled):
		out.StopReason = "context_canceled"
		return nil, out, nil
	default:
		return nil, queueWatchToolOutput{}, err
	}
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
