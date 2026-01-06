package storage

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	lockdproto "pkt.systems/lockd/internal/proto"
)

// MarshalMeta encodes meta into its protobuf representation and encrypts it when crypto is enabled.
func MarshalMeta(meta *Meta, crypto *Crypto) ([]byte, error) {
	if meta == nil {
		meta = &Meta{}
	}
	payload, err := proto.Marshal(metaToProto(meta))
	if err != nil {
		return nil, fmt.Errorf("storage: encode meta protobuf: %w", err)
	}
	if crypto == nil || !crypto.Enabled() {
		return payload, nil
	}
	ciphertext, err := crypto.EncryptMetadata(payload)
	if err != nil {
		return nil, err
	}
	return ciphertext, nil
}

// UnmarshalMeta decrypts (when necessary) and decodes the protobuf payload into a Meta instance.
func UnmarshalMeta(payload []byte, crypto *Crypto) (*Meta, error) {
	var err error
	if crypto != nil && crypto.Enabled() {
		payload, err = crypto.DecryptMetadata(payload)
		if err != nil {
			return nil, err
		}
	}
	var msg lockdproto.LockMeta
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return nil, fmt.Errorf("storage: decode meta protobuf: %w", err)
	}
	return metaFromProto(&msg), nil
}

// MarshalMetaRecord encodes an ETag and meta pair for storage backends that persist both values together.
func MarshalMetaRecord(etag string, meta *Meta, crypto *Crypto) ([]byte, error) {
	record := &lockdproto.MetaRecord{
		Etag: etag,
		Meta: metaToProto(meta),
	}
	payload, err := proto.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("storage: encode meta record protobuf: %w", err)
	}
	if crypto == nil || !crypto.Enabled() {
		return payload, nil
	}
	ciphertext, err := crypto.EncryptMetadata(payload)
	if err != nil {
		return nil, err
	}
	return ciphertext, nil
}

// UnmarshalMetaRecord decodes (and decrypts when necessary) a meta record payload into its ETag and Meta components.
func UnmarshalMetaRecord(payload []byte, crypto *Crypto) (MetaRecord, error) {
	var err error
	if crypto != nil && crypto.Enabled() {
		payload, err = crypto.DecryptMetadata(payload)
		if err != nil {
			return MetaRecord{}, err
		}
	}
	var record lockdproto.MetaRecord
	if err := proto.Unmarshal(payload, &record); err != nil {
		return MetaRecord{}, fmt.Errorf("storage: decode meta record protobuf: %w", err)
	}
	return MetaRecord{ETag: record.GetEtag(), Meta: metaFromProto(record.GetMeta())}, nil
}

func metaToProto(meta *Meta) *lockdproto.LockMeta {
	if meta == nil {
		return &lockdproto.LockMeta{}
	}
	pm := &lockdproto.LockMeta{
		Version:                   meta.Version,
		PublishedVersion:          meta.PublishedVersion,
		StateEtag:                 meta.StateETag,
		UpdatedAtUnix:             meta.UpdatedAtUnix,
		FencingToken:              meta.FencingToken,
		StatePlaintextBytes:       meta.StatePlaintextBytes,
		StagedTxnId:               meta.StagedTxnID,
		StagedStateEtag:           meta.StagedStateETag,
		StagedStateDescriptor:     meta.StagedStateDescriptor,
		StagedStatePlaintextBytes: meta.StagedStatePlaintextBytes,
		StagedVersion:             meta.StagedVersion,
		StagedRemove:              meta.StagedRemove,
	}
	if meta.Lease != nil {
		pm.Lease = &lockdproto.Lease{
			LeaseId:       meta.Lease.ID,
			Owner:         meta.Lease.Owner,
			ExpiresAtUnix: meta.Lease.ExpiresAtUnix,
			FencingToken:  meta.Lease.FencingToken,
			TxnId:         meta.Lease.TxnID,
		}
	}
	if len(meta.StateDescriptor) > 0 {
		pm.StateDescriptor = append([]byte(nil), meta.StateDescriptor...)
	}
	if len(meta.Attributes) > 0 {
		attrs := make(map[string]interface{}, len(meta.Attributes))
		for k, v := range meta.Attributes {
			attrs[k] = v
		}
		if s, err := structpb.NewStruct(attrs); err == nil {
			pm.Attributes = s
		}
	}
	if len(meta.StagedAttributes) > 0 {
		pm.StagedAttributes = make(map[string]string, len(meta.StagedAttributes))
		for k, v := range meta.StagedAttributes {
			pm.StagedAttributes[k] = v
		}
	}
	if len(meta.Attachments) > 0 {
		pm.Attachments = make([]*lockdproto.Attachment, 0, len(meta.Attachments))
		for _, att := range meta.Attachments {
			pm.Attachments = append(pm.Attachments, &lockdproto.Attachment{
				Id:             att.ID,
				Name:           att.Name,
				Size:           att.Size,
				PlaintextBytes: att.PlaintextBytes,
				ContentType:    att.ContentType,
				Descriptor_:    append([]byte(nil), att.Descriptor...),
				CreatedAtUnix:  att.CreatedAtUnix,
				UpdatedAtUnix:  att.UpdatedAtUnix,
			})
		}
	}
	if len(meta.StagedAttachments) > 0 {
		pm.StagedAttachments = make([]*lockdproto.StagedAttachment, 0, len(meta.StagedAttachments))
		for _, att := range meta.StagedAttachments {
			pm.StagedAttachments = append(pm.StagedAttachments, &lockdproto.StagedAttachment{
				Id:               att.ID,
				Name:             att.Name,
				Size:             att.Size,
				PlaintextBytes:   att.PlaintextBytes,
				ContentType:      att.ContentType,
				StagedDescriptor: append([]byte(nil), att.StagedDescriptor...),
				CreatedAtUnix:    att.CreatedAtUnix,
				UpdatedAtUnix:    att.UpdatedAtUnix,
			})
		}
	}
	if len(meta.StagedAttachmentDeletes) > 0 {
		pm.StagedAttachmentDeletes = append([]string(nil), meta.StagedAttachmentDeletes...)
	}
	if meta.StagedAttachmentsClear {
		pm.StagedAttachmentsClear = true
	}
	return pm
}

func metaFromProto(pm *lockdproto.LockMeta) *Meta {
	if pm == nil {
		return &Meta{}
	}
	meta := &Meta{
		Version:                   pm.GetVersion(),
		PublishedVersion:          pm.GetPublishedVersion(),
		StateETag:                 pm.GetStateEtag(),
		UpdatedAtUnix:             pm.GetUpdatedAtUnix(),
		FencingToken:              pm.GetFencingToken(),
		StagedTxnID:               pm.GetStagedTxnId(),
		StagedStateETag:           pm.GetStagedStateEtag(),
		StagedStateDescriptor:     pm.GetStagedStateDescriptor(),
		StagedStatePlaintextBytes: pm.GetStagedStatePlaintextBytes(),
		StagedVersion:             pm.GetStagedVersion(),
		StagedRemove:              pm.GetStagedRemove(),
	}
	if lease := pm.GetLease(); lease != nil {
		meta.Lease = &Lease{
			ID:            lease.GetLeaseId(),
			Owner:         lease.GetOwner(),
			ExpiresAtUnix: lease.GetExpiresAtUnix(),
			FencingToken:  lease.GetFencingToken(),
			TxnID:         lease.GetTxnId(),
		}
	}
	if desc := pm.GetStateDescriptor(); len(desc) > 0 {
		meta.StateDescriptor = append([]byte(nil), desc...)
	}
	meta.StatePlaintextBytes = pm.GetStatePlaintextBytes()
	if attrs := pm.GetAttributes(); attrs != nil {
		meta.Attributes = make(map[string]string, len(attrs.Fields))
		for k, v := range attrs.Fields {
			meta.Attributes[k] = v.GetStringValue()
		}
	}
	if attrs := pm.GetStagedAttributes(); attrs != nil {
		meta.StagedAttributes = make(map[string]string, len(attrs))
		for k, v := range attrs {
			meta.StagedAttributes[k] = v
		}
	}
	if atts := pm.GetAttachments(); len(atts) > 0 {
		meta.Attachments = make([]Attachment, 0, len(atts))
		for _, att := range atts {
			if att == nil {
				continue
			}
			plaintextBytes := att.GetPlaintextBytes()
			if plaintextBytes == 0 {
				plaintextBytes = att.GetSize()
			}
			meta.Attachments = append(meta.Attachments, Attachment{
				ID:             att.GetId(),
				Name:           att.GetName(),
				Size:           att.GetSize(),
				PlaintextBytes: plaintextBytes,
				ContentType:    att.GetContentType(),
				Descriptor:     append([]byte(nil), att.GetDescriptor_()...),
				CreatedAtUnix:  att.GetCreatedAtUnix(),
				UpdatedAtUnix:  att.GetUpdatedAtUnix(),
			})
		}
	}
	if atts := pm.GetStagedAttachments(); len(atts) > 0 {
		meta.StagedAttachments = make([]StagedAttachment, 0, len(atts))
		for _, att := range atts {
			if att == nil {
				continue
			}
			plaintextBytes := att.GetPlaintextBytes()
			if plaintextBytes == 0 {
				plaintextBytes = att.GetSize()
			}
			meta.StagedAttachments = append(meta.StagedAttachments, StagedAttachment{
				ID:               att.GetId(),
				Name:             att.GetName(),
				Size:             att.GetSize(),
				PlaintextBytes:   plaintextBytes,
				ContentType:      att.GetContentType(),
				StagedDescriptor: append([]byte(nil), att.GetStagedDescriptor()...),
				CreatedAtUnix:    att.GetCreatedAtUnix(),
				UpdatedAtUnix:    att.GetUpdatedAtUnix(),
			})
		}
	}
	if deletes := pm.GetStagedAttachmentDeletes(); len(deletes) > 0 {
		meta.StagedAttachmentDeletes = append([]string(nil), deletes...)
	}
	meta.StagedAttachmentsClear = pm.GetStagedAttachmentsClear()
	return meta
}
