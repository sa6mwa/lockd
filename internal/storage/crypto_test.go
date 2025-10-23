package storage

import (
	"bytes"
	"io"
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

	mat, descriptor, err := crypto.MintMaterial("state:test-key")
	if err != nil {
		t.Fatalf("mint material: %v", err)
	}
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

	_, descriptor, err := crypto.MintMaterial("queue-meta:q/orders/msg/1.pb")
	if err != nil {
		t.Fatalf("mint material: %v", err)
	}

	if _, err := crypto.MaterialFromDescriptor("queue-meta:q/other/msg.pb", descriptor); err == nil {
		t.Fatalf("expected context mismatch to error")
	}
}

func TestCryptoMaterialCorruptedDescriptor(t *testing.T) {
	crypto := mustNewTestCrypto(t, false)

	_, descriptor, err := crypto.MintMaterial("queue-payload:q/orders/msg/1.bin")
	if err != nil {
		t.Fatalf("mint material: %v", err)
	}
	corrupted := append([]byte(nil), descriptor...)
	corrupted[0] ^= 0xFF

	if _, err := crypto.MaterialFromDescriptor("queue-payload:q/orders/msg/1.bin", corrupted); err == nil {
		t.Fatalf("expected corrupted descriptor to error")
	}
}

func TestCryptoSnappyRoundTrip(t *testing.T) {
	crypto := mustNewTestCrypto(t, true)

	mat, descriptor, err := crypto.MintMaterial("state:snappy")
	if err != nil {
		t.Fatalf("mint material: %v", err)
	}

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

func mustNewTestCrypto(t *testing.T, snappy bool) *Crypto {
	t.Helper()
	root := kryptograf.MustGenerateRootKey()
	kg := kryptograf.New(root)
	material, err := kg.MintDEK([]byte("meta-context"))
	if err != nil {
		t.Fatalf("mint metadata material: %v", err)
	}
	desc := material.Descriptor
	material.Zero()
	crypto, err := NewCrypto(CryptoConfig{
		Enabled:            true,
		RootKey:            root,
		MetadataDescriptor: desc,
		MetadataContext:    []byte("meta-context"),
		Snappy:             snappy,
	})
	if err != nil {
		t.Fatalf("init crypto: %v", err)
	}
	return crypto
}
