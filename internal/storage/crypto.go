package storage

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"

	"pkt.systems/kryptograf"
	kryptocipher "pkt.systems/kryptograf/cipher"
	"pkt.systems/kryptograf/keymgmt"
)

// CryptoConfig drives the creation of a Crypto helper for storage encryption.
type CryptoConfig struct {
	Enabled            bool
	RootKey            keymgmt.RootKey
	MetadataDescriptor keymgmt.Descriptor
	MetadataContext    []byte
	Snappy             bool
	DisableBufferPool  bool
}

// Crypto encapsulates kryptograf helpers for encrypting metadata, state, and queue payloads.
type Crypto struct {
	enabled          bool
	kg               kryptograf.Kryptograf
	metadataMaterial kryptograf.Material
	metadataContext  []byte
	materialCacheCap int
	materialCacheMu  sync.RWMutex
	materialCache    map[materialCacheKey]kryptograf.Material
	cipherCacheCap   int
	cipherCacheMu    sync.RWMutex
	cipherCache      map[keymgmt.Descriptor]kryptocipher.Cipher
}

const defaultMaterialCacheEntries = 1024
const defaultDecryptSourceReadBufferSize = 8 * 1024

type materialCacheKey struct {
	context    string
	descriptor keymgmt.Descriptor
}

// MaterialResult captures minted crypto material and its descriptor bytes.
type MaterialResult struct {
	Material   kryptograf.Material
	Descriptor []byte
}

var cryptoBufferPool sync.Pool
var cryptoSourceReadBufferPool = sync.Pool{
	New: func() any {
		return bufio.NewReaderSize(bytes.NewReader(nil), defaultDecryptSourceReadBufferSize)
	},
}

// NewCrypto initialises a Crypto helper according to cfg. When encryption is disabled the returned value is nil.
func NewCrypto(cfg CryptoConfig) (*Crypto, error) {
	const defaultStreamChunkSize = 8 * 1024 // reduces per-writer buffer overhead vs kryptograf default (64 KiB)
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
	kg := kryptograf.New(cfg.RootKey).WithChunkSize(defaultStreamChunkSize)
	if !cfg.DisableBufferPool {
		kg = kg.WithOptions(
			kryptograf.WithBufferPool(&cryptoBufferPool),
			kryptograf.WithSourceReadBufferPool(&cryptoSourceReadBufferPool),
		)
	}
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
		materialCacheCap: defaultMaterialCacheEntries,
		materialCache:    make(map[materialCacheKey]kryptograf.Material, defaultMaterialCacheEntries),
		cipherCacheCap:   defaultMaterialCacheEntries,
		cipherCache:      make(map[keymgmt.Descriptor]kryptocipher.Cipher, defaultMaterialCacheEntries),
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
	if len(plaintext) > 0 {
		// Pre-grow to reduce reallocations for small metadata payloads.
		buf.Grow(len(plaintext) + 256)
	}
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
func (c *Crypto) MintMaterial(context string) (MaterialResult, error) {
	if !c.Enabled() {
		return MaterialResult{}, nil
	}
	mat, err := c.kg.MintDEK([]byte(context))
	if err != nil {
		return MaterialResult{}, fmt.Errorf("storage crypto: mint material for %q: %w", context, err)
	}
	descBytes, err := mat.Descriptor.MarshalBinary()
	if err != nil {
		mat.Zero()
		return MaterialResult{}, fmt.Errorf("storage crypto: marshal descriptor for %q: %w", context, err)
	}
	return MaterialResult{Material: mat, Descriptor: descBytes}, nil
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
	cacheKey := materialCacheKey{context: context, descriptor: desc}
	if mat, ok := c.materialCacheGet(cacheKey); ok {
		return mat, nil
	}
	mat, err := c.kg.ReconstructDEK([]byte(context), desc)
	if err != nil {
		return kryptograf.Material{}, fmt.Errorf("storage crypto: reconstruct material for %q: %w", context, err)
	}
	c.materialCachePut(cacheKey, mat)
	return mat, nil
}

func (c *Crypto) materialCacheGet(key materialCacheKey) (kryptograf.Material, bool) {
	if c == nil || c.materialCacheCap <= 0 {
		return kryptograf.Material{}, false
	}
	c.materialCacheMu.RLock()
	defer c.materialCacheMu.RUnlock()
	if len(c.materialCache) == 0 {
		return kryptograf.Material{}, false
	}
	mat, ok := c.materialCache[key]
	return mat, ok
}

func (c *Crypto) materialCachePut(key materialCacheKey, material kryptograf.Material) {
	if c == nil || c.materialCacheCap <= 0 {
		return
	}
	c.materialCacheMu.Lock()
	defer c.materialCacheMu.Unlock()
	if c.materialCache == nil {
		c.materialCache = make(map[materialCacheKey]kryptograf.Material, c.materialCacheCap)
	}
	if _, exists := c.materialCache[key]; exists {
		c.materialCache[key] = material
		return
	}
	if len(c.materialCache) >= c.materialCacheCap {
		for cacheKey, cached := range c.materialCache {
			cached.Zero()
			delete(c.materialCache, cacheKey)
		}
	}
	c.materialCache[key] = material
}

// StateObjectContext returns the encryption context used for a lock state object.
func StateObjectContext(key string) string {
	return "state:" + key
}

// AttachmentObjectContext returns the encryption context used for attachment objects.
func AttachmentObjectContext(key string) string {
	return "attachment:" + key
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
	cachedCipher, err := c.cipherForMaterial(mat)
	if err != nil {
		mat.Zero()
		return nil, err
	}
	writer, err := c.kg.EncryptWriter(dst, mat, kryptograf.WithCipher(func([]byte) (kryptocipher.Cipher, error) {
		return cachedCipher, nil
	}))
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
	cachedCipher, err := c.cipherForMaterial(mat)
	if err != nil {
		mat.Zero()
		return nil, err
	}
	reader, err := c.kg.DecryptReader(src, mat, kryptograf.WithCipher(func([]byte) (kryptocipher.Cipher, error) {
		return cachedCipher, nil
	}), kryptograf.WithSourceReadBuffer(defaultDecryptSourceReadBufferSize))
	if err != nil {
		mat.Zero()
		return nil, err
	}
	return &materialReadCloser{ReadCloser: reader, material: mat}, nil
}

func (c *Crypto) cipherForMaterial(mat kryptograf.Material) (kryptocipher.Cipher, error) {
	if !c.Enabled() {
		return nil, nil
	}
	key := mat.Descriptor
	if cached, ok := c.cipherCacheGet(key); ok {
		return cached, nil
	}
	factory := kryptocipher.AESGCM()
	cipherImpl, err := factory(mat.Key.Bytes())
	if err != nil {
		return nil, err
	}
	c.cipherCachePut(key, cipherImpl)
	return cipherImpl, nil
}

func (c *Crypto) cipherCacheGet(key keymgmt.Descriptor) (kryptocipher.Cipher, bool) {
	if c == nil || c.cipherCacheCap <= 0 {
		return nil, false
	}
	c.cipherCacheMu.RLock()
	defer c.cipherCacheMu.RUnlock()
	if len(c.cipherCache) == 0 {
		return nil, false
	}
	cached, ok := c.cipherCache[key]
	return cached, ok
}

func (c *Crypto) cipherCachePut(key keymgmt.Descriptor, cipherImpl kryptocipher.Cipher) {
	if c == nil || c.cipherCacheCap <= 0 {
		return
	}
	c.cipherCacheMu.Lock()
	defer c.cipherCacheMu.Unlock()
	if c.cipherCache == nil {
		c.cipherCache = make(map[keymgmt.Descriptor]kryptocipher.Cipher, c.cipherCacheCap)
	}
	if _, exists := c.cipherCache[key]; exists {
		c.cipherCache[key] = cipherImpl
		return
	}
	if len(c.cipherCache) >= c.cipherCacheCap {
		for cacheKey := range c.cipherCache {
			delete(c.cipherCache, cacheKey)
		}
	}
	c.cipherCache[key] = cipherImpl
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
