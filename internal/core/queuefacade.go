package core

import (
	"context"

	"pkt.systems/lockd/internal/qrf"
	"pkt.systems/lockd/internal/queue"
)

const maxQueueDequeueBatch = 64

// Dequeue fetches one or more deliveries from the queue using the dispatcher.
// It mirrors the HTTP handler semantics but is transport-neutral so gRPC and
// future transports can share the same behaviour.
func (s *Service) Dequeue(ctx context.Context, cmd QueueDequeueCommand) (*QueueDequeueResult, error) {
	if err := s.maybeThrottleQueue(qrf.KindQueueConsumer); err != nil {
		return nil, err
	}
	qsvc, ok := s.queueProvider.(*queue.Service)
	if !ok || qsvc == nil {
		return nil, Failure{Code: "queue_disabled", Detail: "queue service not configured", HTTPStatus: 501}
	}
	disp, _ := s.queueDispatcher.(*queue.Dispatcher)

	// Basic validation mirrors the former HTTP handler.
	if cmd.Queue == "" {
		return nil, Failure{Code: "missing_queue", Detail: "queue is required", HTTPStatus: 400}
	}
	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: 400}
	}
	s.observeNamespace(namespace)

	owner := cmd.Owner
	if owner == "" {
		return nil, Failure{Code: "missing_owner", Detail: "owner is required", HTTPStatus: 400}
	}

	pageSize := cmd.PageSize
	if pageSize <= 0 {
		pageSize = 1
	}
	if pageSize > maxQueueDequeueBatch {
		pageSize = maxQueueDequeueBatch
	}

	visibility := cmd.Visibility
	if visibility <= 0 {
		visibility = s.resolveTTL(0)
	}

	deliveries, nextCursor, err := s.consumeQueueBatch(
		ctx,
		qsvc,
		disp,
		namespace,
		cmd.Queue,
		owner,
		visibility,
		cmd.Stateful,
		cmd.BlockSeconds,
		pageSize,
	)
	if err != nil {
		return nil, err
	}

	result := &QueueDequeueResult{
		Deliveries: deliveries,
		NextCursor: nextCursor,
	}
	return result, nil
}
