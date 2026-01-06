package queue

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	lockdproto "pkt.systems/lockd/internal/proto"
)

const (
	metaFileExtension = ".pb"
)

func marshalMessageDocument(doc *messageDocument) ([]byte, error) {
	if doc == nil {
		return nil, fmt.Errorf("queue: message document is nil")
	}
	msg := &lockdproto.QueueMessageMeta{
		Type:                     doc.Type,
		Queue:                    doc.Queue,
		Id:                       doc.ID,
		EnqueuedAtUnix:           doc.EnqueuedAt.Unix(),
		UpdatedAtUnix:            doc.UpdatedAt.Unix(),
		Attempts:                 int32(doc.Attempts),
		MaxAttempts:              int32(doc.MaxAttempts),
		NotVisibleUntilUnix:      doc.NotVisibleUntil.Unix(),
		VisibilityTimeoutSeconds: doc.VisibilityTimeout,
		PayloadBytes:             doc.PayloadBytes,
		PayloadContentType:       doc.PayloadContentType,
		CorrelationId:            doc.CorrelationID,
		LeaseId:                  doc.LeaseID,
		LeaseFencingToken:        doc.LeaseFencingToken,
		LeaseTxnId:               doc.LeaseTxnID,
	}
	if len(doc.PayloadDescriptor) > 0 {
		msg.PayloadDescriptor = append([]byte(nil), doc.PayloadDescriptor...)
	}
	if len(doc.MetaDescriptor) > 0 {
		msg.MetaDescriptor = append([]byte(nil), doc.MetaDescriptor...)
	}
	if doc.Attributes != nil {
		structVal, err := structpb.NewStruct(doc.Attributes)
		if err != nil {
			return nil, fmt.Errorf("queue: encode attributes: %w", err)
		}
		msg.Attributes = structVal
	}
	if doc.LastError != nil {
		val, err := structpb.NewValue(doc.LastError)
		if err != nil {
			return nil, fmt.Errorf("queue: encode last_error: %w", err)
		}
		msg.LastError = val
	}
	if !doc.EnqueuedAt.IsZero() {
		msg.EnqueuedAtUnix = doc.EnqueuedAt.Unix()
	}
	if !doc.UpdatedAt.IsZero() {
		msg.UpdatedAtUnix = doc.UpdatedAt.Unix()
	}
	if !doc.NotVisibleUntil.IsZero() {
		msg.NotVisibleUntilUnix = doc.NotVisibleUntil.Unix()
	}
	if doc.ExpiresAt != nil && !doc.ExpiresAt.IsZero() {
		value := doc.ExpiresAt.Unix()
		msg.ExpiresAtUnix = &value
	}
	return proto.Marshal(msg)
}

func unmarshalMessageDocument(payload []byte) (*messageDocument, error) {
	var meta lockdproto.QueueMessageMeta
	if err := proto.Unmarshal(payload, &meta); err != nil {
		return nil, fmt.Errorf("queue: decode message meta protobuf: %w", err)
	}
	doc := &messageDocument{
		Type:               meta.GetType(),
		Queue:              meta.GetQueue(),
		ID:                 meta.GetId(),
		Attempts:           int(meta.GetAttempts()),
		MaxAttempts:        int(meta.GetMaxAttempts()),
		PayloadBytes:       meta.GetPayloadBytes(),
		PayloadContentType: meta.GetPayloadContentType(),
		VisibilityTimeout:  meta.GetVisibilityTimeoutSeconds(),
		CorrelationID:      meta.GetCorrelationId(),
		LeaseID:            meta.GetLeaseId(),
		LeaseFencingToken:  meta.GetLeaseFencingToken(),
		LeaseTxnID:         meta.GetLeaseTxnId(),
	}
	if meta.GetAttributes() != nil {
		doc.Attributes = meta.GetAttributes().AsMap()
	}
	if meta.GetLastError() != nil {
		doc.LastError = meta.GetLastError().AsInterface()
	}
	if meta.GetEnqueuedAtUnix() != 0 {
		doc.EnqueuedAt = time.Unix(meta.GetEnqueuedAtUnix(), 0).UTC()
	}
	if meta.GetUpdatedAtUnix() != 0 {
		doc.UpdatedAt = time.Unix(meta.GetUpdatedAtUnix(), 0).UTC()
	}
	if meta.GetNotVisibleUntilUnix() != 0 {
		doc.NotVisibleUntil = time.Unix(meta.GetNotVisibleUntilUnix(), 0).UTC()
	}
	if meta.ExpiresAtUnix != nil {
		tm := time.Unix(meta.GetExpiresAtUnix(), 0).UTC()
		doc.ExpiresAt = &tm
	}
	if len(meta.GetPayloadDescriptor()) > 0 {
		doc.PayloadDescriptor = append([]byte(nil), meta.GetPayloadDescriptor()...)
	}
	if len(meta.GetMetaDescriptor()) > 0 {
		doc.MetaDescriptor = append([]byte(nil), meta.GetMetaDescriptor()...)
	}
	return doc, nil
}
