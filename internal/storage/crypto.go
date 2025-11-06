package storage

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"pkt.systems/kryptograf"
	"pkt.systems/kryptograf/keymgmt"
)

// CryptoConfig drives the creation of a Crypto helper for storage encryption.
type CryptoConfig struct {
	Enabled            bool
	RootKey            keymgmt.RootKey
	MetadataDescriptor keymgmt.Descriptor
	MetadataContext    []byte
	Snappy             bool
}

// Crypto encapsulates kryptograf helpers for encrypting metadata, state, and queue payloads.
type Crypto struct {
	enabled          bool
	kg               kryptograf.Kryptograf
	metadataMaterial kryptograf.Material
	metadataContext  []byte
}

// NewCrypto initialises a Crypto helper according to cfg. When encryption is disabled the returned value is nil.
func NewCrypto(cfg CryptoConfig) (*Crypto, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.MetadataContext == nil || len(cfg.MetadataContext) == 0 {
		return nil, fmt.Errorf("storage crypto: metadata context required when encryption enabled")
	}
	if cfg.MetadataDescriptor == (keymgmt.Descriptor{}) {
		return nil, fmt.Errorf("storage crypto: metadata descriptor required when encryption enabled")
	}
	if cfg.RootKey == (keymgmt.RootKey{}) {
		return nil, fmt.Errorf("storage crypto: root key required when encryption enabled")
	}
	kg := kryptograf.New(cfg.RootKey)
	if cfg.Snappy {
		kg = kg.WithSnappy()
	}
	mat, err := kg.ReconstructDEK(cfg.MetadataContext, cfg.MetadataDescriptor)
	if err != nil {
		return nil, fmt.Errorf("storage crypto: reconstruct metadata DEK: %w", err)
	}
	return &Crypto{
		enabled:          true,
		kg:               kg,
		metadataMaterial: mat,
		metadataContext:  append([]byte(nil), cfg.MetadataContext...),
	}, nil
}

// Enabled reports whether encryption is active.
func (c *Crypto) Enabled() bool {
	return c != nil && c.enabled
}

// EncryptMetadata encrypts the provided plaintext using the configured metadata material.
func (c *Crypto) EncryptMetadata(plaintext []byte) ([]byte, error) {
	if !c.Enabled() {
		return plaintext, nil
	}
	var buf bytes.Buffer
	writer, err := c.kg.EncryptWriter(&buf, c.metadataMaterial)
	if err != nil {
		return nil, fmt.Errorf("storage crypto: encrypt metadata: %w", err)
	}
	if _, err := writer.Write(plaintext); err != nil {
		writer.Close()
		return nil, fmt.Errorf("storage crypto: encrypt metadata write: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("storage crypto: encrypt metadata close: %w", err)
	}
	return buf.Bytes(), nil
}

// DecryptMetadata decrypts the metadata payload using the configured metadata material.
func (c *Crypto) DecryptMetadata(ciphertext []byte) ([]byte, error) {
	if !c.Enabled() {
		return ciphertext, nil
	}
	reader, err := c.kg.DecryptReader(bytes.NewReader(ciphertext), c.metadataMaterial)
	if err != nil {
		return nil, fmt.Errorf("storage crypto: decrypt metadata: %w", err)
	}
	defer reader.Close()
	plaintext, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("storage crypto: decrypt metadata read: %w", err)
	}
	return plaintext, nil
}

// MintMaterial derives a new material for the supplied context and returns it along with its descriptor bytes.
func (c *Crypto) MintMaterial(context string) (kryptograf.Material, []byte, error) {
	if !c.Enabled() {
		return kryptograf.Material{}, nil, nil
	}
	mat, err := c.kg.MintDEK([]byte(context))
	if err != nil {
		return kryptograf.Material{}, nil, fmt.Errorf("storage crypto: mint material for %q: %w", context, err)
	}
	descBytes, err := mat.Descriptor.MarshalBinary()
	if err != nil {
		mat.Zero()
		return kryptograf.Material{}, nil, fmt.Errorf("storage crypto: marshal descriptor for %q: %w", context, err)
	}
	return mat, descBytes, nil
}

// MaterialFromDescriptor reconstructs a material for the supplied context and descriptor bytes.
func (c *Crypto) MaterialFromDescriptor(context string, descriptor []byte) (kryptograf.Material, error) {
	if !c.Enabled() {
		return kryptograf.Material{}, nil
	}
	var desc keymgmt.Descriptor
	if err := desc.UnmarshalBinary(descriptor); err != nil {
		return kryptograf.Material{}, fmt.Errorf("storage crypto: decode descriptor for %q: %w", context, err)
	}
	mat, err := c.kg.ReconstructDEK([]byte(context), desc)
	if err != nil {
		return kryptograf.Material{}, fmt.Errorf("storage crypto: reconstruct material for %q: %w", context, err)
	}
	return mat, nil
}

// StateObjectContext returns the encryption context used for a lock state object.
func StateObjectContext(key string) string {
	return "state:" + key
}

func queueObjectContext(prefix, namespace, relPath string) string {
	rel := strings.TrimPrefix(relPath, "/")
	if namespace == "" {
		return prefix + rel
	}
	return prefix + namespace + "/" + rel
}

// QueueMetaContext returns the encryption context for queue metadata objects.
func QueueMetaContext(namespace, relPath string) string {
	return queueObjectContext("queue-meta:", namespace, relPath)
}

// QueuePayloadContext returns the encryption context for queue payload objects.
func QueuePayloadContext(namespace, relPath string) string {
	return queueObjectContext("queue-payload:", namespace, relPath)
}

// EncryptWriterForMaterial wraps dst with an encrypting writer using the provided material.
func (c *Crypto) EncryptWriterForMaterial(dst io.Writer, mat kryptograf.Material) (io.WriteCloser, error) {
	if !c.Enabled() {
		return nopCloser{Writer: dst}, nil
	}
	writer, err := c.kg.EncryptWriter(dst, mat)
	if err != nil {
		mat.Zero()
		return nil, err
	}
	return &materialWriteCloser{WriteCloser: writer, material: mat}, nil
}

// DecryptReaderForMaterial wraps src with a decrypting reader using the provided material.
func (c *Crypto) DecryptReaderForMaterial(src io.Reader, mat kryptograf.Material) (io.ReadCloser, error) {
	if !c.Enabled() {
		return io.NopCloser(src), nil
	}
	reader, err := c.kg.DecryptReader(src, mat)
	if err != nil {
		mat.Zero()
		return nil, err
	}
	return &materialReadCloser{ReadCloser: reader, material: mat}, nil
}

type materialWriteCloser struct {
	io.WriteCloser
	material kryptograf.Material
}

func (m *materialWriteCloser) Close() error {
	err := m.WriteCloser.Close()
	m.material.Zero()
	return err
}

type materialReadCloser struct {
	io.ReadCloser
	material kryptograf.Material
}

func (m *materialReadCloser) Close() error {
	err := m.ReadCloser.Close()
	m.material.Zero()
	return err
}

type nopCloser struct {
	Writer io.Writer
}

func (n nopCloser) Write(p []byte) (int, error) {
	return n.Writer.Write(p)
}

func (n nopCloser) Close() error { return nil }

// Kryptograf exposes the underlying kryptograf instance.
