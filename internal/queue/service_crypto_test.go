package queue

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"pkt.systems/kryptograf"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
)

func TestQueueEnqueueEncryptsMetadataAndPayload(t *testing.T) {
	ctx := context.Background()
	root := kryptograf.MustGenerateRootKey()
	kg := kryptograf.New(root)
	material, err := kg.MintDEK([]byte("test-meta-context"))
	if err != nil {
		t.Fatalf("mint metadata dek: %v", err)
	}
	metaDescriptor := material.Descriptor
	material.Zero()

	crypto, err := storage.NewCrypto(storage.CryptoConfig{
		Enabled:            true,
		RootKey:            root,
		MetadataDescriptor: metaDescriptor,
		MetadataContext:    []byte("test-meta-context"),
	})
	if err != nil {
		t.Fatalf("init crypto: %v", err)
	}

	store := memory.NewWithConfig(memory.Config{Crypto: crypto})
	t.Cleanup(func() { _ = store.Close() })

	svc, err := New(store, clock.Real{}, Config{Crypto: crypto})
	if err != nil {
		t.Fatalf("init queue service: %v", err)
	}

	payload := []byte(`{"hello":"world"}`)
	msg, err := svc.Enqueue(ctx, "orders", bytes.NewReader(payload), EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	metaReader, metaInfo, err := store.GetObject(ctx, msg.MetadataObject)
	if err != nil {
		t.Fatalf("get metadata object: %v", err)
	}
	if metaInfo == nil {
		t.Fatalf("expected metadata info")
	}
	if metaInfo.ContentType != storage.ContentTypeProtobufEncrypted {
		t.Fatalf("metadata content type = %s want %s", metaInfo.ContentType, storage.ContentTypeProtobufEncrypted)
	}
	if len(metaInfo.Descriptor) == 0 {
		t.Fatalf("expected metadata descriptor to be populated")
	}
	metaMat, err := crypto.MaterialFromDescriptor(storage.QueueMetaContext(msg.MetadataObject), metaInfo.Descriptor)
	if err != nil {
		metaReader.Close()
		t.Fatalf("reconstruct metadata material: %v", err)
	}
	metaDecr, err := crypto.DecryptReaderForMaterial(metaReader, metaMat)
	if err != nil {
		metaReader.Close()
		t.Fatalf("decrypt metadata: %v", err)
	}
	metaBytes, err := io.ReadAll(metaDecr)
	if err != nil {
		metaDecr.Close()
		t.Fatalf("read decrypted metadata: %v", err)
	}
	if err := metaDecr.Close(); err != nil {
		t.Fatalf("close decrypted metadata: %v", err)
	}
	doc, err := unmarshalMessageDocument(metaBytes)
	if err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	if doc.ID != msg.ID || doc.Queue != msg.Queue {
		t.Fatalf("decoded document mismatch: got id=%s queue=%s want id=%s queue=%s", doc.ID, doc.Queue, msg.ID, msg.Queue)
	}

	payloadReader, payloadInfo, err := store.GetObject(ctx, msg.PayloadObject)
	if err != nil {
		t.Fatalf("get payload object: %v", err)
	}
	if payloadInfo == nil {
		t.Fatalf("expected payload info")
	}
	if payloadInfo.ContentType != storage.ContentTypeOctetStreamEncrypted {
		t.Fatalf("payload content type = %s want %s", payloadInfo.ContentType, storage.ContentTypeOctetStreamEncrypted)
	}
	if len(payloadInfo.Descriptor) == 0 {
		t.Fatalf("expected payload descriptor")
	}
	payloadMat, err := crypto.MaterialFromDescriptor(storage.QueuePayloadContext(msg.PayloadObject), payloadInfo.Descriptor)
	if err != nil {
		payloadReader.Close()
		t.Fatalf("reconstruct payload material: %v", err)
	}
	payloadDecr, err := crypto.DecryptReaderForMaterial(payloadReader, payloadMat)
	if err != nil {
		payloadReader.Close()
		t.Fatalf("decrypt payload: %v", err)
	}
	decrypted, err := io.ReadAll(payloadDecr)
	if err != nil {
		payloadDecr.Close()
		t.Fatalf("read decrypted payload: %v", err)
	}
	if err := payloadDecr.Close(); err != nil {
		t.Fatalf("close decrypted payload: %v", err)
	}
	if !bytes.Equal(decrypted, payload) {
		t.Fatalf("payload mismatch: got %q want %q", decrypted, payload)
	}
}

func TestQueueEnqueueWithoutCryptoStoresPlaintext(t *testing.T) {
	ctx := context.Background()
	store := memory.NewWithConfig(memory.Config{})
	t.Cleanup(func() { _ = store.Close() })

	svc, err := New(store, clock.Real{}, Config{})
	if err != nil {
		t.Fatalf("init queue service: %v", err)
	}

	payload := []byte("plain payload")
	msg, err := svc.Enqueue(ctx, "orders", bytes.NewReader(payload), EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	metaReader, metaInfo, err := store.GetObject(ctx, msg.MetadataObject)
	if err != nil {
		t.Fatalf("get metadata object: %v", err)
	}
	if metaInfo == nil {
		t.Fatalf("expected metadata info")
	}
	if metaInfo.ContentType != storage.ContentTypeProtobuf {
		t.Fatalf("metadata content type = %s want %s", metaInfo.ContentType, storage.ContentTypeProtobuf)
	}
	if len(metaInfo.Descriptor) != 0 {
		t.Fatalf("expected metadata descriptor to be empty")
	}
	metaBytes, err := io.ReadAll(metaReader)
	if err != nil {
		metaReader.Close()
		t.Fatalf("read metadata: %v", err)
	}
	if err := metaReader.Close(); err != nil {
		t.Fatalf("close metadata reader: %v", err)
	}
	doc, err := unmarshalMessageDocument(metaBytes)
	if err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	if doc.ID != msg.ID {
		t.Fatalf("decoded doc id = %s want %s", doc.ID, msg.ID)
	}

	payloadReader, payloadInfo, err := store.GetObject(ctx, msg.PayloadObject)
	if err != nil {
		t.Fatalf("get payload object: %v", err)
	}
	if payloadInfo == nil {
		t.Fatalf("expected payload info")
	}
	if payloadInfo.ContentType != storage.ContentTypeOctetStream {
		t.Fatalf("payload content type = %s want %s", payloadInfo.ContentType, storage.ContentTypeOctetStream)
	}
	if len(payloadInfo.Descriptor) != 0 {
		t.Fatalf("expected payload descriptor to be empty")
	}
	plainPayload, err := io.ReadAll(payloadReader)
	if err != nil {
		payloadReader.Close()
		t.Fatalf("read payload: %v", err)
	}
	if err := payloadReader.Close(); err != nil {
		t.Fatalf("close payload reader: %v", err)
	}
	if !bytes.Equal(plainPayload, payload) {
		t.Fatalf("payload mismatch: got %q want %q", plainPayload, payload)
	}
}

func TestQueueGetMessageMissingDescriptor(t *testing.T) {
	ctx := context.Background()
	store, svc := newEncryptedQueueService(t)

	result, err := svc.Enqueue(ctx, "orders", bytes.NewReader([]byte("payload")), EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var tampered bool
	svc.store = &tamperBackend{
		Backend: svc.store,
		getObjectHook: func(ctx context.Context, key string, reader io.ReadCloser, info *storage.ObjectInfo) (io.ReadCloser, *storage.ObjectInfo, error) {
			if !tampered && strings.HasSuffix(key, ".pb") && strings.Contains(key, "/msg/") {
				tampered = true
				infoCopy := *info
				infoCopy.Descriptor = nil
				return reader, &infoCopy, nil
			}
			return reader, info, nil
		},
	}

	if _, _, err := svc.GetMessage(ctx, result.Queue, result.ID); err == nil || !strings.Contains(err.Error(), "missing metadata descriptor") {
		if err == nil {
			t.Fatalf("expected missing descriptor error")
		}
		t.Fatalf("unexpected error: %v", err)
	}

	_ = store.Close()
}

func TestQueueGetPayloadMissingDescriptor(t *testing.T) {
	ctx := context.Background()
	store, svc := newEncryptedQueueService(t)

	result, err := svc.Enqueue(ctx, "orders", bytes.NewReader([]byte("payload-two")), EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var tampered bool
	svc.store = &tamperBackend{
		Backend: svc.store,
		getObjectHook: func(ctx context.Context, key string, reader io.ReadCloser, info *storage.ObjectInfo) (io.ReadCloser, *storage.ObjectInfo, error) {
			if !tampered && strings.HasSuffix(key, ".bin") && strings.Contains(key, "/msg/") {
				tampered = true
				infoCopy := *info
				infoCopy.Descriptor = nil
				return reader, &infoCopy, nil
			}
			return reader, info, nil
		},
	}

	if _, _, err := svc.GetPayload(ctx, result.Queue, result.ID); err == nil || !strings.Contains(err.Error(), "missing payload descriptor") {
		if err == nil {
			t.Fatalf("expected missing descriptor error")
		}
		t.Fatalf("unexpected error: %v", err)
	}

	_ = store.Close()
}

func TestQueueCopyObjectMissingDescriptor(t *testing.T) {
	ctx := context.Background()
	store, svc := newEncryptedQueueService(t)

	result, err := svc.Enqueue(ctx, "orders", bytes.NewReader([]byte("payload-three")), EnqueueOptions{})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	payloadKey := result.PayloadObject
	dlqKey := messageDLQPayloadPath(result.Queue, result.ID)

	var tampered bool
	svc.store = &tamperBackend{
		Backend: svc.store,
		getObjectHook: func(ctx context.Context, key string, reader io.ReadCloser, info *storage.ObjectInfo) (io.ReadCloser, *storage.ObjectInfo, error) {
			if !tampered && key == payloadKey {
				tampered = true
				infoCopy := *info
				infoCopy.Descriptor = nil
				return reader, &infoCopy, nil
			}
			return reader, info, nil
		},
	}

	err = svc.copyObject(ctx, payloadKey, dlqKey, true, storage.QueuePayloadContext(payloadKey), storage.QueuePayloadContext(dlqKey))
	if err == nil || !strings.Contains(err.Error(), "missing descriptor") {
		t.Fatalf("expected descriptor error, got %v", err)
	}

	_ = store.Close()
}

type tamperBackend struct {
	storage.Backend
	getObjectHook func(context.Context, string, io.ReadCloser, *storage.ObjectInfo) (io.ReadCloser, *storage.ObjectInfo, error)
}

func (t *tamperBackend) GetObject(ctx context.Context, key string) (io.ReadCloser, *storage.ObjectInfo, error) {
	reader, info, err := t.Backend.GetObject(ctx, key)
	if err != nil {
		return reader, info, err
	}
	if t.getObjectHook == nil {
		return reader, info, nil
	}
	return t.getObjectHook(ctx, key, reader, info)
}

func newEncryptedQueueService(t *testing.T) (*memory.Store, *Service) {
	t.Helper()
	crypto := mustNewTestCrypto(t, false)
	store := memory.NewWithConfig(memory.Config{Crypto: crypto})
	svc, err := New(store, clock.Real{}, Config{Crypto: crypto})
	if err != nil {
		t.Fatalf("init queue service: %v", err)
	}
	return store, svc
}

func mustNewTestCrypto(t *testing.T, snappy bool) *storage.Crypto {
	t.Helper()
	root := kryptograf.MustGenerateRootKey()
	kg := kryptograf.New(root)
	material, err := kg.MintDEK([]byte("queue-meta"))
	if err != nil {
		t.Fatalf("mint metadata material: %v", err)
	}
	desc := material.Descriptor
	material.Zero()
	crypto, err := storage.NewCrypto(storage.CryptoConfig{
		Enabled:            true,
		RootKey:            root,
		MetadataDescriptor: desc,
		MetadataContext:    []byte("queue-meta"),
		Snappy:             snappy,
	})
	if err != nil {
		t.Fatalf("init crypto: %v", err)
	}
	return crypto
}
