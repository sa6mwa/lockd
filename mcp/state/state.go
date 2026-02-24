package state

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"pkt.systems/kryptograf"
	"pkt.systems/kryptograf/keymgmt"
	"pkt.systems/lockd"
)

const (
	// DefaultFileName is the default MCP OAuth state file name under the lockd config directory.
	DefaultFileName = "mcp.pem"

	stateBlockType    = "LOCKD-MCP-STATE"
	descriptorName    = "lockd/mcp/state"
	descriptorContext = "lockd/mcp/state"
)

var (
	// ErrNotBootstrapped indicates the MCP state file is missing or does not contain bootstrap state.
	ErrNotBootstrapped = errors.New("mcp state not bootstrapped")
	// ErrClientNotFound indicates a requested OAuth client ID was not found.
	ErrClientNotFound = errors.New("oauth client not found")
)

// Data is the persisted MCP OAuth state payload.
type Data struct {
	Version   int               `json:"version"`
	Issuer    string            `json:"issuer"`
	Clients   map[string]Client `json:"clients"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// Client is a confidential OAuth client record.
type Client struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	SecretSalt string    `json:"secret_salt"`
	SecretHash string    `json:"secret_hash"`
	Scopes     []string  `json:"scopes,omitempty"`
	Revoked    bool      `json:"revoked"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type cryptoMaterial struct {
	Root     keymgmt.RootKey
	Material kryptograf.Material
}

// BootstrapRequest defines MCP state bootstrap parameters.
type BootstrapRequest struct {
	Path              string
	Issuer            string
	InitialClientName string
	InitialScopes     []string
	Force             bool
	Now               time.Time
}

// BootstrapResponse contains credentials for the bootstrap client.
type BootstrapResponse struct {
	Path         string
	Issuer       string
	ClientID     string
	ClientSecret string
}

// DefaultPath returns the default MCP state path under the lockd config directory.
func DefaultPath() (string, error) {
	dir, err := lockd.DefaultConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, DefaultFileName), nil
}

// Bootstrap creates or replaces the MCP state file and emits initial client credentials.
func Bootstrap(req BootstrapRequest) (BootstrapResponse, error) {
	path := strings.TrimSpace(req.Path)
	if path == "" {
		var err error
		path, err = DefaultPath()
		if err != nil {
			return BootstrapResponse{}, fmt.Errorf("resolve default state path: %w", err)
		}
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return BootstrapResponse{}, fmt.Errorf("resolve state path: %w", err)
	}
	if !req.Force {
		if _, err := os.Stat(path); err == nil {
			return BootstrapResponse{}, fmt.Errorf("state file %s already exists (use --force to overwrite)", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return BootstrapResponse{}, fmt.Errorf("stat state file: %w", err)
		}
	}

	now := req.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	data := NewData(strings.TrimSpace(req.Issuer), now)
	name := strings.TrimSpace(req.InitialClientName)
	if name == "" {
		name = "default"
	}
	client, secret, err := data.AddClient(name, req.InitialScopes, now)
	if err != nil {
		return BootstrapResponse{}, err
	}
	if err := Save(path, data); err != nil {
		return BootstrapResponse{}, err
	}
	return BootstrapResponse{
		Path:         path,
		Issuer:       data.Issuer,
		ClientID:     client.ID,
		ClientSecret: secret,
	}, nil
}

// NewData constructs a normalized state payload.
func NewData(issuer string, now time.Time) *Data {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return &Data{
		Version:   1,
		Issuer:    strings.TrimSpace(issuer),
		Clients:   make(map[string]Client),
		UpdatedAt: now.UTC(),
	}
}

// Clone returns a deep copy of d.
func (d *Data) Clone() *Data {
	if d == nil {
		return nil
	}
	out := &Data{
		Version:   d.Version,
		Issuer:    d.Issuer,
		UpdatedAt: d.UpdatedAt,
		Clients:   make(map[string]Client, len(d.Clients)),
	}
	for id, c := range d.Clients {
		copied := c
		copied.Scopes = append([]string(nil), c.Scopes...)
		out.Clients[id] = copied
	}
	return out
}

// Normalize validates and repairs structural defaults.
func (d *Data) Normalize() {
	if d.Version <= 0 {
		d.Version = 1
	}
	d.Issuer = strings.TrimSpace(d.Issuer)
	if d.Clients == nil {
		d.Clients = make(map[string]Client)
	}
	for id, c := range d.Clients {
		c.ID = strings.TrimSpace(c.ID)
		if c.ID == "" {
			c.ID = id
		}
		c.Name = strings.TrimSpace(c.Name)
		c.SecretSalt = strings.TrimSpace(c.SecretSalt)
		c.SecretHash = strings.TrimSpace(c.SecretHash)
		c.Scopes = normalizeScopes(c.Scopes)
		d.Clients[c.ID] = c
		if c.ID != id {
			delete(d.Clients, id)
		}
	}
}

// AddClient creates a confidential client and returns the new record and cleartext secret.
func (d *Data) AddClient(name string, scopes []string, now time.Time) (Client, string, error) {
	if d == nil {
		return Client{}, "", fmt.Errorf("state required")
	}
	d.Normalize()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return Client{}, "", fmt.Errorf("client name required")
	}
	id := uuid.NewString()
	secret, salt, hash, err := newSecretMaterial()
	if err != nil {
		return Client{}, "", err
	}
	client := Client{
		ID:         id,
		Name:       name,
		SecretSalt: salt,
		SecretHash: hash,
		Scopes:     normalizeScopes(scopes),
		CreatedAt:  now.UTC(),
		UpdatedAt:  now.UTC(),
	}
	d.Clients[id] = client
	d.UpdatedAt = now.UTC()
	return client, secret, nil
}

// RotateClientSecret rotates the secret for clientID and returns the new cleartext secret.
func (d *Data) RotateClientSecret(clientID string, now time.Time) (string, error) {
	if d == nil {
		return "", fmt.Errorf("state required")
	}
	d.Normalize()
	clientID = strings.TrimSpace(clientID)
	client, ok := d.Clients[clientID]
	if !ok {
		return "", ErrClientNotFound
	}
	secret, salt, hash, err := newSecretMaterial()
	if err != nil {
		return "", err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	client.SecretSalt = salt
	client.SecretHash = hash
	client.UpdatedAt = now.UTC()
	d.Clients[clientID] = client
	d.UpdatedAt = now.UTC()
	return secret, nil
}

// RemoveClient deletes clientID.
func (d *Data) RemoveClient(clientID string, now time.Time) error {
	if d == nil {
		return fmt.Errorf("state required")
	}
	d.Normalize()
	clientID = strings.TrimSpace(clientID)
	if _, ok := d.Clients[clientID]; !ok {
		return ErrClientNotFound
	}
	delete(d.Clients, clientID)
	if now.IsZero() {
		now = time.Now().UTC()
	}
	d.UpdatedAt = now.UTC()
	return nil
}

// RevokeClient toggles revocation state for clientID.
func (d *Data) RevokeClient(clientID string, revoked bool, now time.Time) error {
	if d == nil {
		return fmt.Errorf("state required")
	}
	d.Normalize()
	clientID = strings.TrimSpace(clientID)
	client, ok := d.Clients[clientID]
	if !ok {
		return ErrClientNotFound
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	client.Revoked = revoked
	client.UpdatedAt = now.UTC()
	d.Clients[clientID] = client
	d.UpdatedAt = now.UTC()
	return nil
}

// UpdateClientName renames clientID.
func (d *Data) UpdateClientName(clientID, name string, now time.Time) error {
	if d == nil {
		return fmt.Errorf("state required")
	}
	d.Normalize()
	clientID = strings.TrimSpace(clientID)
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("client name required")
	}
	client, ok := d.Clients[clientID]
	if !ok {
		return ErrClientNotFound
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	client.Name = name
	client.UpdatedAt = now.UTC()
	d.Clients[clientID] = client
	d.UpdatedAt = now.UTC()
	return nil
}

// UpdateClientScopes replaces client scopes.
func (d *Data) UpdateClientScopes(clientID string, scopes []string, now time.Time) error {
	if d == nil {
		return fmt.Errorf("state required")
	}
	d.Normalize()
	clientID = strings.TrimSpace(clientID)
	client, ok := d.Clients[clientID]
	if !ok {
		return ErrClientNotFound
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	client.Scopes = normalizeScopes(scopes)
	client.UpdatedAt = now.UTC()
	d.Clients[clientID] = client
	d.UpdatedAt = now.UTC()
	return nil
}

// SetIssuer updates the OAuth issuer value.
func (d *Data) SetIssuer(issuer string, now time.Time) {
	if d == nil {
		return
	}
	d.Normalize()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	d.Issuer = strings.TrimSpace(issuer)
	d.UpdatedAt = now.UTC()
}

// ListClients returns clients sorted by ID.
func (d *Data) ListClients() []Client {
	if d == nil {
		return nil
	}
	d.Normalize()
	ids := make([]string, 0, len(d.Clients))
	for id := range d.Clients {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]Client, 0, len(ids))
	for _, id := range ids {
		client := d.Clients[id]
		client.Scopes = append([]string(nil), client.Scopes...)
		out = append(out, client)
	}
	return out
}

// VerifyClientSecret verifies a confidential client secret against stored hash material.
func (d *Data) VerifyClientSecret(clientID, secret string) (*Client, bool) {
	if d == nil {
		return nil, false
	}
	d.Normalize()
	clientID = strings.TrimSpace(clientID)
	client, ok := d.Clients[clientID]
	if !ok || client.Revoked {
		return nil, false
	}
	saltBytes, err := base64.RawURLEncoding.DecodeString(client.SecretSalt)
	if err != nil {
		return nil, false
	}
	expected, err := base64.RawURLEncoding.DecodeString(client.SecretHash)
	if err != nil {
		return nil, false
	}
	sum := sha256.Sum256(append(saltBytes, []byte(secret)...))
	if subtle.ConstantTimeCompare(sum[:], expected) != 1 {
		return nil, false
	}
	copied := client
	copied.Scopes = append([]string(nil), client.Scopes...)
	return &copied, true
}

// Load reads and decrypts MCP state from path.
func Load(path string) (*Data, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		var err error
		path, err = DefaultPath()
		if err != nil {
			return nil, fmt.Errorf("resolve default state path: %w", err)
		}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotBootstrapped
		}
		return nil, fmt.Errorf("read mcp state file: %w", err)
	}

	ciphertext, err := findStateBlock(raw)
	if err != nil {
		return nil, err
	}
	material, err := extractMaterial(raw)
	if err != nil {
		return nil, err
	}
	plaintext, err := decryptPayload(ciphertext, material)
	if err != nil {
		return nil, err
	}
	var data Data
	if err := json.Unmarshal(plaintext, &data); err != nil {
		return nil, fmt.Errorf("decode mcp state: %w", err)
	}
	data.Normalize()
	return &data, nil
}

// Save encrypts and writes MCP state to path.
func Save(path string, data *Data) error {
	if data == nil {
		return fmt.Errorf("state payload required")
	}
	path = strings.TrimSpace(path)
	if path == "" {
		var err error
		path, err = DefaultPath()
		if err != nil {
			return fmt.Errorf("resolve default state path: %w", err)
		}
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve state path: %w", err)
	}
	data = data.Clone()
	data.Normalize()

	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("encode mcp state: %w", err)
	}

	existing, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read existing state file: %w", err)
	}
	material, basePEM, err := ensureMaterial(existing)
	if err != nil {
		return err
	}
	ciphertext, err := encryptPayload(payload, material)
	if err != nil {
		return err
	}
	updated, err := upsertStateBlock(basePEM, ciphertext)
	if err != nil {
		return err
	}
	if err := writeAtomic(path, updated, 0o600); err != nil {
		return err
	}
	return nil
}

func extractMaterial(pemBytes []byte) (cryptoMaterial, error) {
	store, err := keymgmt.LoadPEM(pemBytes)
	if err != nil {
		return cryptoMaterial{}, fmt.Errorf("load key material: %w", err)
	}
	root, ok, err := store.RootKey()
	if err != nil {
		return cryptoMaterial{}, fmt.Errorf("read root key: %w", err)
	}
	if !ok {
		return cryptoMaterial{}, fmt.Errorf("%w: root key missing", ErrNotBootstrapped)
	}
	desc, ok, err := store.Descriptor(descriptorName)
	if err != nil {
		return cryptoMaterial{}, fmt.Errorf("read descriptor: %w", err)
	}
	if !ok {
		return cryptoMaterial{}, fmt.Errorf("%w: descriptor missing", ErrNotBootstrapped)
	}
	kg := kryptograf.New(root)
	mat, err := kg.ReconstructDEK([]byte(descriptorContext), desc)
	if err != nil {
		return cryptoMaterial{}, fmt.Errorf("reconstruct state DEK: %w", err)
	}
	return cryptoMaterial{Root: root, Material: mat}, nil
}

func ensureMaterial(existing []byte) (cryptoMaterial, []byte, error) {
	var out []byte
	store, err := keymgmt.LoadPEMInto(existing, &out)
	if err != nil {
		return cryptoMaterial{}, nil, fmt.Errorf("load state key bundle: %w", err)
	}
	root, err := store.EnsureRootKey()
	if err != nil {
		return cryptoMaterial{}, nil, fmt.Errorf("ensure root key: %w", err)
	}
	mat, err := store.EnsureDescriptor(descriptorName, root, []byte(descriptorContext))
	if err != nil {
		return cryptoMaterial{}, nil, fmt.Errorf("ensure descriptor: %w", err)
	}
	if err := store.Commit(); err != nil {
		return cryptoMaterial{}, nil, fmt.Errorf("commit key material: %w", err)
	}
	if len(out) == 0 {
		out = existing
	}
	if len(out) == 0 {
		raw, err := store.Bytes()
		if err != nil {
			return cryptoMaterial{}, nil, fmt.Errorf("serialize key material: %w", err)
		}
		out = raw
	}
	return cryptoMaterial{Root: root, Material: mat}, out, nil
}

func encryptPayload(plaintext []byte, material cryptoMaterial) ([]byte, error) {
	defer material.Material.Zero()
	kg := kryptograf.New(material.Root)
	var buf bytes.Buffer
	writer, err := kg.EncryptWriter(&buf, material.Material)
	if err != nil {
		return nil, fmt.Errorf("encrypt state payload: %w", err)
	}
	if _, err := writer.Write(plaintext); err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("encrypt state payload write: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("encrypt state payload close: %w", err)
	}
	return buf.Bytes(), nil
}

func decryptPayload(ciphertext []byte, material cryptoMaterial) ([]byte, error) {
	defer material.Material.Zero()
	kg := kryptograf.New(material.Root)
	reader, err := kg.DecryptReader(bytes.NewReader(ciphertext), material.Material)
	if err != nil {
		return nil, fmt.Errorf("decrypt state payload: %w", err)
	}
	defer reader.Close()
	plaintext, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("decrypt state payload read: %w", err)
	}
	return plaintext, nil
}

func findStateBlock(data []byte) ([]byte, error) {
	rest := data
	for len(rest) > 0 {
		block, next := pem.Decode(rest)
		if block == nil {
			return nil, fmt.Errorf("%w: invalid PEM", ErrNotBootstrapped)
		}
		if block.Type == stateBlockType {
			return append([]byte(nil), block.Bytes...), nil
		}
		rest = next
	}
	return nil, fmt.Errorf("%w: missing %s block", ErrNotBootstrapped, stateBlockType)
}

func upsertStateBlock(basePEM []byte, ciphertext []byte) ([]byte, error) {
	blocks, err := decodePEMBlocks(basePEM)
	if err != nil {
		return nil, err
	}
	filtered := make([]*pem.Block, 0, len(blocks)+1)
	for _, block := range blocks {
		if block.Type == stateBlockType {
			continue
		}
		filtered = append(filtered, block)
	}
	filtered = append(filtered, &pem.Block{Type: stateBlockType, Bytes: ciphertext})
	return encodePEMBlocks(filtered)
}

func decodePEMBlocks(data []byte) ([]*pem.Block, error) {
	if len(data) == 0 {
		return nil, nil
	}
	rest := data
	blocks := make([]*pem.Block, 0, 8)
	for len(rest) > 0 {
		block, next := pem.Decode(rest)
		if block == nil {
			return nil, fmt.Errorf("parse PEM blocks: invalid PEM data")
		}
		blocks = append(blocks, block)
		rest = next
	}
	return blocks, nil
}

func encodePEMBlocks(blocks []*pem.Block) ([]byte, error) {
	var buf bytes.Buffer
	for _, block := range blocks {
		if err := pem.Encode(&buf, block); err != nil {
			return nil, fmt.Errorf("encode PEM block %s: %w", block.Type, err)
		}
	}
	return buf.Bytes(), nil
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return fmt.Errorf("write state file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace state file: %w", err)
	}
	return nil
}

func newSecretMaterial() (secret, salt, hash string, err error) {
	secret, err = randomToken(32)
	if err != nil {
		return "", "", "", fmt.Errorf("generate client secret: %w", err)
	}
	salt, err = randomToken(16)
	if err != nil {
		return "", "", "", fmt.Errorf("generate client salt: %w", err)
	}
	saltBytes, err := base64.RawURLEncoding.DecodeString(salt)
	if err != nil {
		return "", "", "", fmt.Errorf("decode generated salt: %w", err)
	}
	sum := sha256.Sum256(append(saltBytes, []byte(secret)...))
	hash = base64.RawURLEncoding.EncodeToString(sum[:])
	return secret, salt, hash, nil
}

func randomToken(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func normalizeScopes(scopes []string) []string {
	if len(scopes) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(scopes))
	out := make([]string, 0, len(scopes))
	for _, raw := range scopes {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil
	}
	return out
}
