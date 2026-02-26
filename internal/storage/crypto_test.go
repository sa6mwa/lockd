package storage

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"pkt.systems/kryptograf"
)

func TestNewCryptoRequiresMaterial(t *testing.T) {
	root := kryptograf.MustGenerateRootKey()
	descMaterial, err := kryptograf.New(root).MintDEK([]byte("meta-context"))
	if err != nil {
		t.Fatalf("mint metadata material: %v", err)
	}
	descriptor := descMaterial.Descriptor
	descMaterial.Zero()

	_, err = NewCrypto(CryptoConfig{
		Enabled:            true,
		MetadataDescriptor: descriptor,
		MetadataContext:    []byte("meta-context"),
	})
	if err == nil {
		t.Fatalf("expected error when root key missing")
	}

	_, err = NewCrypto(CryptoConfig{
		Enabled:         true,
		RootKey:         root,
		MetadataContext: []byte("meta-context"),
	})
	if err == nil {
		t.Fatalf("expected error when descriptor missing")
	}

	_, err = NewCrypto(CryptoConfig{
		Enabled:            true,
		RootKey:            root,
		MetadataDescriptor: descriptor,
	})
	if err == nil {
		t.Fatalf("expected error when metadata context missing")
	}

	crypto, err := NewCrypto(CryptoConfig{Enabled: false})
	if err != nil {
		t.Fatalf("disabled crypto should not error: %v", err)
	}
	if crypto != nil {
		t.Fatalf("disabled crypto should return nil helper")
	}
}

func TestCryptoMetadataRoundTrip(t *testing.T) {
	crypto := mustNewTestCrypto(t, false)

	plaintext := []byte("hello metadata")
	ciphertext, err := crypto.EncryptMetadata(plaintext)
	if err != nil {
		t.Fatalf("encrypt metadata: %v", err)
	}
	if bytes.Equal(ciphertext, plaintext) {
		t.Fatalf("expected encrypted payload to differ from plaintext")
	}
	decrypted, err := crypto.DecryptMetadata(ciphertext)
	if err != nil {
		t.Fatalf("decrypt metadata: %v", err)
	}
	if !bytes.Equal(decrypted, plaintext) {
		t.Fatalf("metadata mismatch: got %q want %q", decrypted, plaintext)
	}
}

func TestCryptoMintMaterialRoundTrip(t *testing.T) {
	crypto := mustNewTestCrypto(t, false)

	minted, err := crypto.MintMaterial("state:test-key")
	if err != nil {
		t.Fatalf("mint material: %v", err)
	}
	mat := minted.Material
	descriptor := minted.Descriptor
	if len(descriptor) == 0 {
		t.Fatalf("expected descriptor bytes")
	}

	const payload = "encrypted payload body"
	var buf bytes.Buffer
	writer, err := crypto.EncryptWriterForMaterial(&buf, mat)
	if err != nil {
		t.Fatalf("encrypt writer: %v", err)
	}
	if _, err := io.WriteString(writer, payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	reconstructed, err := crypto.MaterialFromDescriptor("state:test-key", descriptor)
	if err != nil {
		t.Fatalf("material from descriptor: %v", err)
	}
	reader, err := crypto.DecryptReaderForMaterial(bytes.NewReader(buf.Bytes()), reconstructed)
	if err != nil {
		t.Fatalf("decrypt reader: %v", err)
	}
	decoded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read decrypted payload: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close decrypted reader: %v", err)
	}
	if string(decoded) != payload {
		t.Fatalf("payload mismatch: got %q want %q", decoded, payload)
	}
}

func TestCryptoMaterialContextMismatch(t *testing.T) {
	crypto := mustNewTestCrypto(t, false)

	minted, err := crypto.MintMaterial("queue-meta:q/orders/msg/1.pb")
	if err != nil {
		t.Fatalf("mint material: %v", err)
	}
	descriptor := minted.Descriptor

	if _, err := crypto.MaterialFromDescriptor("queue-meta:q/other/msg.pb", descriptor); err == nil {
		t.Fatalf("expected context mismatch to error")
	}
}

func TestCryptoMaterialCorruptedDescriptor(t *testing.T) {
	crypto := mustNewTestCrypto(t, false)

	minted, err := crypto.MintMaterial("queue-payload:q/orders/msg/1.bin")
	if err != nil {
		t.Fatalf("mint material: %v", err)
	}
	descriptor := minted.Descriptor
	corrupted := append([]byte(nil), descriptor...)
	corrupted[0] ^= 0xFF

	if _, err := crypto.MaterialFromDescriptor("queue-payload:q/orders/msg/1.bin", corrupted); err == nil {
		t.Fatalf("expected corrupted descriptor to error")
	}
}

func TestCryptoSnappyRoundTrip(t *testing.T) {
	crypto := mustNewTestCrypto(t, true)

	minted, err := crypto.MintMaterial("state:snappy")
	if err != nil {
		t.Fatalf("mint material: %v", err)
	}
	mat := minted.Material
	descriptor := minted.Descriptor

	data := bytes.Repeat([]byte("snappy-data-"), 64)
	var buf bytes.Buffer
	writer, err := crypto.EncryptWriterForMaterial(&buf, mat)
	if err != nil {
		t.Fatalf("encrypt writer: %v", err)
	}
	if _, err := writer.Write(data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	reconstructed, err := crypto.MaterialFromDescriptor("state:snappy", descriptor)
	if err != nil {
		t.Fatalf("material from descriptor: %v", err)
	}
	reader, err := crypto.DecryptReaderForMaterial(bytes.NewReader(buf.Bytes()), reconstructed)
	if err != nil {
		t.Fatalf("decrypt reader: %v", err)
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read decrypted: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close reader: %v", err)
	}
	if !bytes.Equal(out, data) {
		t.Fatalf("snappy payload mismatch")
	}
}

func TestCryptoMaterialDescriptorCacheBounded(t *testing.T) {
	crypto := mustNewTestCrypto(t, false)
	crypto.materialCacheCap = 2
	crypto.materialCache = make(map[materialCacheKey]kryptograf.Material, crypto.materialCacheCap)

	m1, err := crypto.MintMaterial("state:cache-1")
	if err != nil {
		t.Fatalf("mint material 1: %v", err)
	}
	m2, err := crypto.MintMaterial("state:cache-2")
	if err != nil {
		t.Fatalf("mint material 2: %v", err)
	}
	m3, err := crypto.MintMaterial("state:cache-3")
	if err != nil {
		t.Fatalf("mint material 3: %v", err)
	}

	r1, err := crypto.MaterialFromDescriptor("state:cache-1", m1.Descriptor)
	if err != nil {
		t.Fatalf("reconstruct 1: %v", err)
	}
	r1b, err := crypto.MaterialFromDescriptor("state:cache-1", m1.Descriptor)
	if err != nil {
		t.Fatalf("reconstruct 1 (cache hit): %v", err)
	}
	if r1 != r1b {
		t.Fatalf("expected cached material to match")
	}
	if got := len(crypto.materialCache); got != 1 {
		t.Fatalf("cache size after repeated same descriptor = %d, want 1", got)
	}

	if _, err := crypto.MaterialFromDescriptor("state:cache-2", m2.Descriptor); err != nil {
		t.Fatalf("reconstruct 2: %v", err)
	}
	if got := len(crypto.materialCache); got != 2 {
		t.Fatalf("cache size after second descriptor = %d, want 2", got)
	}

	if _, err := crypto.MaterialFromDescriptor("state:cache-3", m3.Descriptor); err != nil {
		t.Fatalf("reconstruct 3: %v", err)
	}
	if got := len(crypto.materialCache); got != 1 {
		t.Fatalf("cache size after bounded flush = %d, want 1", got)
	}
}

func mustNewTestCrypto(t *testing.T, snappy bool) *Crypto {
	t.Helper()
	cfg := mustNewTestCryptoConfig(t, snappy)
	crypto, err := NewCrypto(cfg)
	if err != nil {
		t.Fatalf("init crypto: %v", err)
	}
	return crypto
}

func mustNewTestCryptoConfig(t *testing.T, snappy bool) CryptoConfig {
	t.Helper()
	root := kryptograf.MustGenerateRootKey()
	kg := kryptograf.New(root)
	material, err := kg.MintDEK([]byte("meta-context"))
	if err != nil {
		t.Fatalf("mint metadata material: %v", err)
	}
	desc := material.Descriptor
	material.Zero()
	return CryptoConfig{
		Enabled:            true,
		RootKey:            root,
		MetadataDescriptor: desc,
		MetadataContext:    []byte("meta-context"),
		Snappy:             snappy,
	}
}

func TestCryptoBufferPoolToggle(t *testing.T) {
	t.Run("enabled_by_default", func(t *testing.T) {
		cfg := mustNewTestCryptoConfig(t, false)
		calls := exerciseCryptoPool(t, cfg)
		if calls == 0 {
			t.Fatalf("expected buffer pool to be used by default")
		}
	})

	t.Run("disabled_via_config", func(t *testing.T) {
		cfg := mustNewTestCryptoConfig(t, false)
		cfg.DisableBufferPool = true
		calls := exerciseCryptoPool(t, cfg)
		if calls != 0 {
			t.Fatalf("expected buffer pool to be disabled via config; got %d allocations", calls)
		}
	})
}

// exerciseCryptoPool encrypts and decrypts metadata while counting pool.Get calls.
func exerciseCryptoPool(t *testing.T, cfg CryptoConfig) int32 {
	t.Helper()
	var gets int32
	cryptoBufferPool = sync.Pool{
		New: func() any {
			atomic.AddInt32(&gets, 1)
			return make([]byte, 1024)
		},
	}
	t.Cleanup(func() {
		cryptoBufferPool = sync.Pool{}
	})
	crypto, err := NewCrypto(cfg)
	if err != nil {
		t.Fatalf("init crypto: %v", err)
	}
	payload := bytes.Repeat([]byte("pool-check-"), 32)
	ciphertext, err := crypto.EncryptMetadata(payload)
	if err != nil {
		t.Fatalf("encrypt metadata: %v", err)
	}
	plaintext, err := crypto.DecryptMetadata(ciphertext)
	if err != nil {
		t.Fatalf("decrypt metadata: %v", err)
	}
	if !bytes.Equal(plaintext, payload) {
		t.Fatalf("round trip mismatch")
	}
	return atomic.LoadInt32(&gets)
}
