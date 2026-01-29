package disk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash/crc32"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"pkt.systems/lockd/internal/storage"
)

func TestLogstoreRefreshSeesState(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()

	store1 := newLogStore(root, time.Now, nil, 0, 0, false)
	ns1, err := store1.namespace("default")
	if err != nil {
		t.Fatalf("namespace store1: %v", err)
	}
	if err := ns1.refresh(); err != nil {
		t.Fatalf("refresh store1: %v", err)
	}
	if _, err := ns1.appendRecord(ctx, logRecordStatePut, "key", recordMeta{gen: 1}, strings.NewReader("initial")); err != nil {
		t.Fatalf("append record: %v", err)
	}

	store2 := newLogStore(root, time.Now, nil, 0, 0, false)
	ns2, err := store2.namespace("default")
	if err != nil {
		t.Fatalf("namespace store2: %v", err)
	}
	if err := ns2.refresh(); err != nil {
		t.Fatalf("refresh store2: %v", err)
	}
	if ns2.stateIndex["key"] == nil {
		t.Fatalf("expected refreshed state index entry")
	}
}

func TestLogstoreCloseAfterCommit(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()

	store := newLogStore(root, time.Now, nil, 0, 0, true)
	ns, err := store.namespace("default")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	if err := ns.refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if _, err := ns.appendRecord(ctx, logRecordStatePut, "key", recordMeta{gen: 1}, strings.NewReader("initial")); err != nil {
		t.Fatalf("append record: %v", err)
	}
	ns.mu.Lock()
	active := ns.active
	ns.mu.Unlock()
	if active != nil {
		t.Fatalf("expected active segment closed after commit")
	}
}

func TestLogstoreSingleWriterRefreshesMarkers(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()

	writer := newLogStore(root, time.Now, nil, 0, 0, false)
	writer.setSingleWriter(true)
	writerNS, err := writer.namespace("default")
	if err != nil {
		t.Fatalf("namespace writer: %v", err)
	}
	if err := writerNS.refresh(); err != nil {
		t.Fatalf("refresh writer: %v", err)
	}

	reader := newLogStore(root, time.Now, nil, 0, 0, false)
	readerNS, err := reader.namespace("default")
	if err != nil {
		t.Fatalf("namespace reader: %v", err)
	}
	if err := readerNS.refresh(); err != nil {
		t.Fatalf("refresh reader: %v", err)
	}

	if _, err := writerNS.appendRecord(ctx, logRecordStatePut, "key", recordMeta{gen: 1}, strings.NewReader("initial")); err != nil {
		t.Fatalf("append record: %v", err)
	}
	if err := readerNS.refresh(); err != nil {
		t.Fatalf("refresh reader after write: %v", err)
	}
	if readerNS.stateIndex["key"] == nil {
		t.Fatalf("expected refreshed state index entry after single-writer update")
	}
}

func TestLogstoreFlushAppendsSingle(t *testing.T) {
	root := t.TempDir()
	store := newLogStore(root, time.Now, nil, 0, 0, false)
	ns, err := store.namespace("default")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	if err := ns.refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	meta := recordMeta{
		gen:        1,
		modifiedAt: time.Now().Unix(),
		etag:       uuid.NewString(),
	}
	record, payloadStartRel := buildInlineRecord(t, logRecordMetaPut, "key", meta, nil)
	req := &appendRequest{
		recType:         logRecordMetaPut,
		key:             "key",
		meta:            meta,
		record:          record,
		recordBuf:       record,
		payloadLen:      0,
		payloadStartRel: payloadStartRel,
		written:         make(chan *recordRef, 1),
		done:            make(chan error, 1),
		ready:           make(chan struct{}),
		group:           storage.NewCommitGroup(),
		noSync:          true,
	}
	ready := req.ready
	req.group.Add(req.done)
	ns.flushAppends([]*appendRequest{req})

	ref := <-req.written
	if ref == nil {
		t.Fatalf("expected record ref")
	}
	select {
	case <-ready:
	default:
		t.Fatalf("expected ready channel to be closed")
	}
	if err := req.group.Wait(); err != nil {
		t.Fatalf("commit error: %v", err)
	}

	ns.mu.Lock()
	active := ns.active
	metaRef := ns.metaIndex["key"]
	ns.mu.Unlock()
	if metaRef == nil {
		t.Fatalf("expected meta index entry")
	}
	if active == nil {
		t.Fatalf("expected active segment")
	}
	if ref.offset != 0 {
		t.Fatalf("expected offset 0, got %d", ref.offset)
	}
	if ref.payloadOffset != payloadStartRel {
		t.Fatalf("expected payload offset %d, got %d", payloadStartRel, ref.payloadOffset)
	}

	data, err := os.ReadFile(active.path)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if len(data) < len(record) {
		t.Fatalf("expected record length %d, got %d", len(record), len(data))
	}
	if !bytes.Equal(data[:len(record)], record) {
		t.Fatalf("record bytes mismatch")
	}
}

func TestLogstoreAppendRecordStatePutETag(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()

	store := newLogStore(root, time.Now, nil, 0, 0, false)
	ns, err := store.namespace("default")
	if err != nil {
		t.Fatalf("namespace: %v", err)
	}
	if err := ns.refresh(); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	payload := "payload-contents"
	ref, err := ns.appendRecord(ctx, logRecordStatePut, "key", recordMeta{gen: 1}, strings.NewReader(payload))
	if err != nil {
		t.Fatalf("append record: %v", err)
	}
	if ref == nil {
		t.Fatalf("expected record ref")
	}
	sum := sha256.Sum256([]byte(payload))
	expected := hex.EncodeToString(sum[:])
	if ref.meta.etag != expected {
		t.Fatalf("expected etag %s, got %s", expected, ref.meta.etag)
	}
}

func buildInlineRecord(t *testing.T, recType logRecordType, key string, meta recordMeta, payload []byte) ([]byte, int64) {
	t.Helper()
	etagBytes, err := etagBytesForRecord(recType, meta.etag)
	if err != nil {
		t.Fatalf("etag bytes: %v", err)
	}
	metaBytes, _, _, err := encodeMeta(recType, meta, etagBytes)
	if err != nil {
		t.Fatalf("encode meta: %v", err)
	}
	header := recordHeader{
		recType:    recType,
		keyLen:     uint32(len(key)),
		metaLen:    uint32(len(metaBytes)),
		payloadLen: uint32(len(payload)),
		payloadCRC: crc32.ChecksumIEEE(payload),
	}
	var headerBuf [logHeaderSize]byte
	encodeHeader(headerBuf[:], header)
	recordLen := logHeaderSize + len(key) + len(metaBytes) + len(payload)
	record := make([]byte, recordLen)
	copy(record, headerBuf[:])
	copy(record[logHeaderSize:], key)
	copy(record[logHeaderSize+len(key):], metaBytes)
	if len(payload) > 0 {
		copy(record[logHeaderSize+len(key)+len(metaBytes):], payload)
	}
	return record, int64(logHeaderSize + len(key) + len(metaBytes))
}
