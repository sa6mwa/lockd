package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path"
	"strings"

	"pkt.systems/lockd/internal/queue"
	"pkt.systems/lockd/internal/storage"
)

const txnQueueDecisionNamespace = ".txn-queue-decisions"

type txnQueueDecisionList struct {
	Items         []txnQueueDecisionItem `json:"items,omitempty"`
	UpdatedAtUnix int64                  `json:"updated_at_unix,omitempty"`
}

type txnQueueDecisionItem struct {
	TxnID         string   `json:"txn_id"`
	State         TxnState `json:"state"`
	Key           string   `json:"key"`
	CreatedAtUnix int64    `json:"created_at_unix,omitempty"`
}

func queueDecisionListKey(namespace, queueName string) string {
	return path.Join("q", namespace, queueName, "decisions.json")
}

func (s *Service) loadQueueDecisionList(ctx context.Context, namespace, queueName string) (*txnQueueDecisionList, string, error) {
	obj, err := s.store.GetObject(ctx, txnQueueDecisionNamespace, queueDecisionListKey(namespace, queueName))
	if err != nil {
		return nil, "", err
	}
	defer obj.Reader.Close()
	var list txnQueueDecisionList
	if err := json.NewDecoder(obj.Reader).Decode(&list); err != nil {
		return nil, "", err
	}
	etag := ""
	if obj.Info != nil {
		etag = obj.Info.ETag
	}
	return &list, etag, nil
}

func (s *Service) putQueueDecisionList(ctx context.Context, namespace, queueName string, list *txnQueueDecisionList, expectedETag string) (string, error) {
	if list == nil {
		return "", nil
	}
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(list); err != nil {
		return "", err
	}
	info, err := s.store.PutObject(ctx, txnQueueDecisionNamespace, queueDecisionListKey(namespace, queueName), bytes.NewReader(buf.Bytes()), storage.PutObjectOptions{
		ExpectedETag: expectedETag,
		ContentType:  storage.ContentTypeJSON,
	})
	if err != nil {
		return "", err
	}
	if info != nil {
		return info.ETag, nil
	}
	return "", nil
}

func (s *Service) deleteQueueDecisionList(ctx context.Context, namespace, queueName string, expectedETag string) error {
	return s.store.DeleteObject(ctx, txnQueueDecisionNamespace, queueDecisionListKey(namespace, queueName), storage.DeleteObjectOptions{
		ExpectedETag:   expectedETag,
		IgnoreNotFound: true,
	})
}

func (s *Service) recordQueueDecisionWorklist(ctx context.Context, rec *TxnRecord) {
	if s == nil || rec == nil {
		return
	}
	if rec.State == "" || rec.State == TxnStatePending {
		return
	}
	if s.queueProvider == nil {
		return
	}
	itemsByQueue := map[string][]txnQueueDecisionItem{}
	now := s.clock.Now().Unix()
	for _, p := range rec.Participants {
		if p.Namespace == "" || strings.HasPrefix(p.Namespace, ".") {
			continue
		}
		queueName := ""
		if parts, ok := queue.ParseMessageLeaseKey(p.Key); ok {
			queueName = parts.Queue
		} else if parts, ok := queue.ParseStateLeaseKey(p.Key); ok {
			queueName = parts.Queue
		} else {
			continue
		}
		key := strings.TrimPrefix(p.Key, "/")
		item := txnQueueDecisionItem{
			TxnID:         rec.TxnID,
			State:         rec.State,
			Key:           key,
			CreatedAtUnix: now,
		}
		queueKey := p.Namespace + "/" + queueName
		itemsByQueue[queueKey] = append(itemsByQueue[queueKey], item)
	}
	for queueKey, items := range itemsByQueue {
		if len(items) == 0 {
			continue
		}
		parts := strings.SplitN(queueKey, "/", 2)
		if len(parts) != 2 {
			continue
		}
		if err := s.enqueueQueueDecisionItems(ctx, parts[0], parts[1], items); err != nil && s.logger != nil {
			s.logger.Warn("txn.queue.decisions.record_failed",
				"namespace", parts[0],
				"queue", parts[1],
				"txn_id", rec.TxnID,
				"error", err,
			)
		}
	}
}

func (s *Service) enqueueQueueDecisionItems(ctx context.Context, namespace, queueName string, items []txnQueueDecisionItem) error {
	if s == nil || s.store == nil || len(items) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := s.clock.Now().Unix()
	keyFor := func(item txnQueueDecisionItem) string {
		return item.TxnID + ":" + item.Key
	}
	for {
		list, etag, err := s.loadQueueDecisionList(ctx, namespace, queueName)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				list = &txnQueueDecisionList{}
				etag = ""
			} else if errors.Is(err, storage.ErrNotImplemented) {
				return nil
			} else {
				return err
			}
		}
		existing := make(map[string]struct{}, len(list.Items))
		for _, item := range list.Items {
			existing[keyFor(item)] = struct{}{}
		}
		added := 0
		for _, item := range items {
			if item.TxnID == "" || item.Key == "" {
				continue
			}
			k := keyFor(item)
			if _, ok := existing[k]; ok {
				continue
			}
			list.Items = append(list.Items, item)
			existing[k] = struct{}{}
			added++
		}
		if added == 0 {
			return nil
		}
		list.UpdatedAtUnix = now
		if _, err := s.putQueueDecisionList(ctx, namespace, queueName, list, etag); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			if errors.Is(err, storage.ErrNotImplemented) {
				return nil
			}
			return err
		}
		s.clearQueueDecisionEmpty(namespace + "/" + queueName)
		return nil
	}
}

func (s *Service) maybeApplyQueueDecisionWorklist(ctx context.Context, namespace, queueName string) (int, error) {
	if s == nil || s.queueProvider == nil || s.store == nil {
		return 0, nil
	}
	maxApply := s.queueDecisionMaxApply
	if maxApply <= 0 {
		return 0, nil
	}
	cacheKey := namespace + "/" + queueName
	if !s.shouldCheckQueueDecision(cacheKey, s.clock.Now()) {
		return 0, nil
	}
	list, etag, err := s.loadQueueDecisionList(ctx, namespace, queueName)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) || errors.Is(err, storage.ErrNotImplemented) {
			s.markQueueDecisionEmpty(cacheKey, s.clock.Now())
			return 0, nil
		}
		return 0, err
	}
	if list == nil || len(list.Items) == 0 {
		s.markQueueDecisionEmpty(cacheKey, s.clock.Now())
		return 0, nil
	}
	s.clearQueueDecisionEmpty(cacheKey)
	applyCount := 0
	limit := len(list.Items)
	if limit > maxApply {
		limit = maxApply
	}
	processed := make(map[string]struct{}, limit)
	for i := 0; i < limit; i++ {
		item := list.Items[i]
		if item.TxnID == "" || item.Key == "" {
			processed[item.TxnID+":"+item.Key] = struct{}{}
			continue
		}
		commit := item.State == TxnStateCommit
		if err := s.applyTxnDecisionToKey(ctx, namespace, item.Key, item.TxnID, commit, true); err != nil {
			if isQueueMessageLeaseMismatch(err) || errors.Is(err, storage.ErrNotFound) {
				processed[item.TxnID+":"+item.Key] = struct{}{}
				continue
			}
			return applyCount, err
		}
		processed[item.TxnID+":"+item.Key] = struct{}{}
		applyCount++
	}
	if len(processed) == 0 {
		return applyCount, nil
	}
	for attempt := 0; attempt < 3; attempt++ {
		current := list
		currentETag := etag
		if attempt > 0 {
			var loadErr error
			current, currentETag, loadErr = s.loadQueueDecisionList(ctx, namespace, queueName)
			if loadErr != nil {
				if errors.Is(loadErr, storage.ErrNotFound) || errors.Is(loadErr, storage.ErrNotImplemented) {
					return applyCount, nil
				}
				return applyCount, loadErr
			}
		}
		remaining := current.Items[:0]
		for _, item := range current.Items {
			if _, ok := processed[item.TxnID+":"+item.Key]; ok {
				continue
			}
			remaining = append(remaining, item)
		}
		current.Items = remaining
		current.UpdatedAtUnix = s.clock.Now().Unix()
		if len(current.Items) == 0 {
			if err := s.deleteQueueDecisionList(ctx, namespace, queueName, currentETag); err != nil {
				if errors.Is(err, storage.ErrCASMismatch) {
					continue
				}
				if errors.Is(err, storage.ErrNotImplemented) {
					return applyCount, nil
				}
				return applyCount, err
			}
			s.markQueueDecisionEmpty(cacheKey, s.clock.Now())
			return applyCount, nil
		}
		if _, err := s.putQueueDecisionList(ctx, namespace, queueName, current, currentETag); err != nil {
			if errors.Is(err, storage.ErrCASMismatch) {
				continue
			}
			if errors.Is(err, storage.ErrNotImplemented) {
				return applyCount, nil
			}
			return applyCount, err
		}
		return applyCount, nil
	}
	return applyCount, nil
}
