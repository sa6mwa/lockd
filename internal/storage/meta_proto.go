package storage

import (
	"fmt"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	lockdproto "pkt.systems/lockd/internal/proto"
)

// MarshalMeta encodes meta into its protobuf representation and encrypts it when crypto is enabled.
func MarshalMeta(meta *Meta, crypto *Crypto) ([]byte, error) {
	if meta == nil {
		meta = &Meta{}
	}
	pm := borrowLockMeta()
	fillLockMeta(meta, pm)
	payload, err := proto.Marshal(pm)
	releaseLockMeta(pm)
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
	record := borrowMetaRecord()
	pm := borrowLockMeta()
	fillLockMeta(meta, pm)
	record.Etag = etag
	record.Meta = pm
	payload, err := proto.Marshal(record)
	releaseLockMeta(pm)
	releaseMetaRecord(record)
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

// UnmarshalMetaRecordSummary decodes only the query-relevant metadata fields.
func UnmarshalMetaRecordSummary(payload []byte, crypto *Crypto) (LoadMetaSummaryResult, error) {
	var err error
	if crypto != nil && crypto.Enabled() {
		payload, err = crypto.DecryptMetadata(payload)
		if err != nil {
			return LoadMetaSummaryResult{}, err
		}
	}
	var record lockdproto.MetaRecord
	if err := proto.Unmarshal(payload, &record); err != nil {
		return LoadMetaSummaryResult{}, fmt.Errorf("storage: decode meta record protobuf: %w", err)
	}
	pm := record.GetMeta()
	summary := &MetaSummary{}
	if pm != nil {
		summary.Version = pm.GetVersion()
		summary.PublishedVersion = pm.GetPublishedVersion()
		summary.StateETag = pm.GetStateEtag()
		summary.StatePlaintextBytes = pm.GetStatePlaintextBytes()
		if desc := pm.GetStateDescriptor(); len(desc) > 0 {
			summary.StateDescriptor = append([]byte(nil), desc...)
		}
		if attrs := pm.GetAttributes(); attrs != nil {
			if value, ok := attrs.Fields[MetaAttributeQueryExclude]; ok {
				switch strings.ToLower(value.GetStringValue()) {
				case "true", "1", "yes", "y", "on":
					summary.QueryExcluded = true
				}
			}
		}
	}
	return LoadMetaSummaryResult{
		Meta: summary,
		ETag: record.GetEtag(),
	}, nil
}

var lockMetaPool = sync.Pool{
	New: func() any {
		return &lockdproto.LockMeta{}
	},
}

var metaRecordPool = sync.Pool{
	New: func() any {
		return &lockdproto.MetaRecord{}
	},
}

func borrowLockMeta() *lockdproto.LockMeta {
	pm := lockMetaPool.Get().(*lockdproto.LockMeta)
	*pm = lockdproto.LockMeta{}
	return pm
}

func releaseLockMeta(pm *lockdproto.LockMeta) {
	if pm == nil {
		return
	}
	*pm = lockdproto.LockMeta{}
	lockMetaPool.Put(pm)
}

func borrowMetaRecord() *lockdproto.MetaRecord {
	record := metaRecordPool.Get().(*lockdproto.MetaRecord)
	*record = lockdproto.MetaRecord{}
	return record
}

func releaseMetaRecord(record *lockdproto.MetaRecord) {
	if record == nil {
		return
	}
	*record = lockdproto.MetaRecord{}
	metaRecordPool.Put(record)
}

func fillLockMeta(meta *Meta, pm *lockdproto.LockMeta) {
	if meta == nil || pm == nil {
		return
	}
	pm.Version = meta.Version
	pm.PublishedVersion = meta.PublishedVersion
	pm.StateEtag = meta.StateETag
	pm.UpdatedAtUnix = meta.UpdatedAtUnix
	pm.FencingToken = meta.FencingToken
	pm.StatePlaintextBytes = meta.StatePlaintextBytes
	pm.StagedTxnId = meta.StagedTxnID
	pm.StagedStateEtag = meta.StagedStateETag
	pm.StagedStateDescriptor = meta.StagedStateDescriptor
	pm.StagedStatePlaintextBytes = meta.StagedStatePlaintextBytes
	pm.StagedVersion = meta.StagedVersion
	pm.StagedRemove = meta.StagedRemove
	if meta.Lease != nil {
		pm.Lease = &lockdproto.Lease{
			LeaseId:       meta.Lease.ID,
			Owner:         meta.Lease.Owner,
			ExpiresAtUnix: meta.Lease.ExpiresAtUnix,
			FencingToken:  meta.Lease.FencingToken,
			TxnId:         meta.Lease.TxnID,
			TxnExplicit:   meta.Lease.TxnExplicit,
		}
	}
	if len(meta.StateDescriptor) > 0 {
		pm.StateDescriptor = append([]byte(nil), meta.StateDescriptor...)
	}
	if len(meta.Attributes) > 0 {
		fields := make(map[string]*structpb.Value, len(meta.Attributes))
		for k, v := range meta.Attributes {
			fields[k] = structpb.NewStringValue(v)
		}
		pm.Attributes = &structpb.Struct{Fields: fields}
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
				Id:              att.ID,
				Name:            att.Name,
				Size:            att.Size,
				PlaintextBytes:  att.PlaintextBytes,
				PlaintextSha256: att.PlaintextSHA256,
				ContentType:     att.ContentType,
				Descriptor_:     append([]byte(nil), att.Descriptor...),
				CreatedAtUnix:   att.CreatedAtUnix,
				UpdatedAtUnix:   att.UpdatedAtUnix,
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
				PlaintextSha256:  att.PlaintextSHA256,
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
			TxnExplicit:   lease.GetTxnExplicit(),
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
				ID:              att.GetId(),
				Name:            att.GetName(),
				Size:            att.GetSize(),
				PlaintextBytes:  plaintextBytes,
				PlaintextSHA256: att.GetPlaintextSha256(),
				ContentType:     att.GetContentType(),
				Descriptor:      append([]byte(nil), att.GetDescriptor_()...),
				CreatedAtUnix:   att.GetCreatedAtUnix(),
				UpdatedAtUnix:   att.GetUpdatedAtUnix(),
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
				PlaintextSHA256:  att.GetPlaintextSha256(),
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
