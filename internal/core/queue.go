package core

import (
	"context"
	"errors"
	"io"
	"time"

	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/lockd/internal/queue"
)

// QueueEnqueueCommand describes a transport-neutral enqueue request.
type QueueEnqueueCommand struct {
	Namespace      string
	Queue          string
	Payload        io.Reader
	DelaySeconds   int64
	VisibilitySecs int64
	TTLSeconds     int64
	MaxAttempts    int
	Attributes     map[string]any
	ContentType    string
}

// QueueEnqueueResult surfaces details of the enqueued message.
type QueueEnqueueResult struct {
	Namespace           string
	Queue               string
	MessageID           string
	Attempts            int
	MaxAttempts         int
	NotVisibleUntilUnix int64
	VisibilityTimeout   int64
	PayloadBytes        int64
	CorrelationID       string
}

// Enqueue inserts a message into the queue backend.
func (s *Service) Enqueue(ctx context.Context, cmd QueueEnqueueCommand) (*QueueEnqueueResult, error) {
	start := s.clock.Now()
	if err := s.maybeThrottleQueue(ctx, qrf.KindQueueProducer); err != nil {
		return nil, err
	}
	qsvc, ok := s.queueProvider.(*queue.Service)
	if !ok || qsvc == nil {
		return nil, Failure{Code: "queue_disabled", Detail: "queue service not configured", HTTPStatus: 501}
	}
	if cmd.Queue == "" {
		return nil, Failure{Code: "missing_queue", Detail: "queue is required", HTTPStatus: 400}
	}
	ns, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: 400}
	}
	s.observeNamespace(ns)

	opts := queue.EnqueueOptions{
		Delay:       secondsToDuration(cmd.DelaySeconds),
		Visibility:  secondsToDuration(cmd.VisibilitySecs),
		TTL:         secondsToDuration(cmd.TTLSeconds),
		MaxAttempts: cmd.MaxAttempts,
		Attributes:  cmd.Attributes,
		ContentType: cmd.ContentType,
	}

	plan := s.newWritePlan(ctx)
	commitCtx := plan.Context()
	msg, err := qsvc.Enqueue(commitCtx, ns, cmd.Queue, cmd.Payload, opts)
	if err != nil {
		return nil, plan.Wait(classifyQueueError(err))
	}
	if waitErr := plan.Wait(nil); waitErr != nil {
		return nil, waitErr
	}
	if s.queueDispatcher != nil {
		s.queueDispatcher.Notify(ns, cmd.Queue)
	}
	if s.queueMetrics != nil {
		duration := s.clock.Now().Sub(start)
		s.queueMetrics.recordEnqueue(ctx, msg.Namespace, msg.Queue, msg.PayloadBytes, duration)
	}
	return &QueueEnqueueResult{
		Namespace:           msg.Namespace,
		Queue:               msg.Queue,
		MessageID:           msg.ID,
		Attempts:            msg.Attempts,
		MaxAttempts:         msg.MaxAttempts,
		NotVisibleUntilUnix: msg.NotVisibleUntil.Unix(),
		VisibilityTimeout:   int64(msg.Visibility.Seconds()),
		PayloadBytes:        msg.PayloadBytes,
		CorrelationID:       msg.CorrelationID,
	}, nil
}

func secondsToDuration(v int64) time.Duration {
	if v <= 0 {
		return 0
	}
	return time.Duration(v) * time.Second
}

func classifyQueueError(err error) error {
	var failure Failure
	switch {
	case errors.Is(err, queue.ErrTooManyConsumers):
		failure = Failure{Code: "queue_busy", Detail: "too many consumers", HTTPStatus: 503}
	default:
		return err
	}
	return failure
}
