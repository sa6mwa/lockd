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
func UnmarshalMetaRecord(payload []byte, crypto *Crypto) (string, *Meta, error) {
	var err error
	if crypto != nil && crypto.Enabled() {
		payload, err = crypto.DecryptMetadata(payload)
		if err != nil {
			return "", nil, err
		}
	}
	var record lockdproto.MetaRecord
	if err := proto.Unmarshal(payload, &record); err != nil {
		return "", nil, fmt.Errorf("storage: decode meta record protobuf: %w", err)
	}
	return record.GetEtag(), metaFromProto(record.GetMeta()), nil
}

func metaToProto(meta *Meta) *lockdproto.LockMeta {
	if meta == nil {
		return &lockdproto.LockMeta{}
	}
	pm := &lockdproto.LockMeta{
		Version:             meta.Version,
		PublishedVersion:    meta.PublishedVersion,
		StateEtag:           meta.StateETag,
		UpdatedAtUnix:       meta.UpdatedAtUnix,
		FencingToken:        meta.FencingToken,
		StatePlaintextBytes: meta.StatePlaintextBytes,
	}
	if meta.Lease != nil {
		pm.Lease = &lockdproto.Lease{
			LeaseId:       meta.Lease.ID,
			Owner:         meta.Lease.Owner,
			ExpiresAtUnix: meta.Lease.ExpiresAtUnix,
			FencingToken:  meta.Lease.FencingToken,
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
	return pm
}

func metaFromProto(pm *lockdproto.LockMeta) *Meta {
	if pm == nil {
		return &Meta{}
	}
	meta := &Meta{
		Version:          pm.GetVersion(),
		PublishedVersion: pm.GetPublishedVersion(),
		StateETag:        pm.GetStateEtag(),
		UpdatedAtUnix:    pm.GetUpdatedAtUnix(),
		FencingToken:     pm.GetFencingToken(),
	}
	if lease := pm.GetLease(); lease != nil {
		meta.Lease = &Lease{
			ID:            lease.GetLeaseId(),
			Owner:         lease.GetOwner(),
			ExpiresAtUnix: lease.GetExpiresAtUnix(),
			FencingToken:  lease.GetFencingToken(),
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
	return meta
}
