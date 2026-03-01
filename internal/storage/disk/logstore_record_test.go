package disk

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

func TestDecodeMetaRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		recType logRecordType
		meta    recordMeta
	}{
		{
			name:    "meta put",
			recType: logRecordMetaPut,
			meta: recordMeta{
				gen:        7,
				modifiedAt: 11,
				etag:       uuid.NewString(),
			},
		},
		{
			name:    "meta delete",
			recType: logRecordMetaDelete,
			meta: recordMeta{
				gen:        8,
				modifiedAt: 12,
			},
		},
		{
			name:    "state put",
			recType: logRecordStatePut,
			meta: recordMeta{
				gen:           9,
				modifiedAt:    13,
				etag:          hex.EncodeToString([]byte("0123456789abcdef0123456789abcdef")),
				plaintextSize: 1234,
				cipherSize:    5678,
				descriptor:    []byte("descriptor-state"),
			},
		},
		{
			name:    "state link",
			recType: logRecordStateLink,
			meta: recordMeta{
				gen:           10,
				modifiedAt:    14,
				etag:          hex.EncodeToString([]byte("fedcba9876543210fedcba9876543210")),
				plaintextSize: 2234,
				cipherSize:    6678,
				descriptor:    []byte("descriptor-link"),
			},
		},
		{
			name:    "state delete",
			recType: logRecordStateDelete,
			meta: recordMeta{
				gen:        11,
				modifiedAt: 15,
			},
		},
		{
			name:    "object put",
			recType: logRecordObjectPut,
			meta: recordMeta{
				gen:           12,
				modifiedAt:    16,
				etag:          hex.EncodeToString([]byte("aabbccddeeff00112233445566778899")),
				plaintextSize: 777,
				contentType:   "application/json",
				descriptor:    []byte("descriptor-object"),
			},
		},
		{
			name:    "object delete",
			recType: logRecordObjectDelete,
			meta: recordMeta{
				gen:        13,
				modifiedAt: 17,
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			etagBytes, err := etagBytesForRecord(tc.recType, tc.meta.etag)
			if err != nil {
				t.Fatalf("etag bytes: %v", err)
			}
			encoded, _, _, err := encodeMetaInto(nil, tc.recType, tc.meta, etagBytes)
			if err != nil {
				t.Fatalf("encode meta: %v", err)
			}
			decoded, err := decodeMeta(tc.recType, encoded)
			if err != nil {
				t.Fatalf("decode meta: %v", err)
			}

			if decoded.gen != tc.meta.gen {
				t.Fatalf("gen mismatch: got %d want %d", decoded.gen, tc.meta.gen)
			}
			if decoded.modifiedAt != tc.meta.modifiedAt {
				t.Fatalf("modifiedAt mismatch: got %d want %d", decoded.modifiedAt, tc.meta.modifiedAt)
			}
			if decoded.etag != tc.meta.etag {
				t.Fatalf("etag mismatch: got %q want %q", decoded.etag, tc.meta.etag)
			}
			if decoded.plaintextSize != tc.meta.plaintextSize {
				t.Fatalf("plaintextSize mismatch: got %d want %d", decoded.plaintextSize, tc.meta.plaintextSize)
			}
			if decoded.cipherSize != tc.meta.cipherSize {
				t.Fatalf("cipherSize mismatch: got %d want %d", decoded.cipherSize, tc.meta.cipherSize)
			}
			if decoded.contentType != tc.meta.contentType {
				t.Fatalf("contentType mismatch: got %q want %q", decoded.contentType, tc.meta.contentType)
			}
			if !reflect.DeepEqual(decoded.descriptor, tc.meta.descriptor) {
				t.Fatalf("descriptor mismatch: got %v want %v", decoded.descriptor, tc.meta.descriptor)
			}
		})
	}
}

func TestDecodeMetaRejectsTruncatedData(t *testing.T) {
	tests := []struct {
		name    string
		recType logRecordType
	}{
		{name: "meta put", recType: logRecordMetaPut},
		{name: "meta delete", recType: logRecordMetaDelete},
		{name: "state put", recType: logRecordStatePut},
		{name: "state link", recType: logRecordStateLink},
		{name: "state delete", recType: logRecordStateDelete},
		{name: "object put", recType: logRecordObjectPut},
		{name: "object delete", recType: logRecordObjectDelete},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeMeta(tc.recType, []byte{1, 2, 3}); err == nil {
				t.Fatalf("expected decode error")
			}
		})
	}
}
