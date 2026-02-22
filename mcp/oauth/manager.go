package oauth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/oauthex"

	"pkt.systems/lockd/mcp/state"
	"pkt.systems/pslog"
)

const (
	defaultAccessTokenTTL  = 15 * time.Minute
	defaultRefreshTokenTTL = 30 * 24 * time.Hour
	defaultCodeTTL         = 2 * time.Minute
)

// ManagerConfig configures OAuth manager behavior.
type ManagerConfig struct {
	StatePath      string
	RefreshStore   string
	AccessTokenTTL time.Duration
	RefreshTTL     time.Duration
	CodeTTL        time.Duration
	Logger         pslog.Logger
}

// AddClientRequest defines add-client parameters.
type AddClientRequest struct {
	Name   string
	Scopes []string
}

// AddClientResponse contains newly issued client credentials.
type AddClientResponse struct {
	ClientID     string
	ClientSecret string
}

// UpdateClientRequest defines mutable client fields.
type UpdateClientRequest struct {
	ClientID string
	Name     string
	Scopes   []string
}

// TokenResponse is the OAuth token response payload.
type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
	Scope        string `json:"scope,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

// AuthServerMetadata is RFC 8414-compatible authorization server metadata.
type AuthServerMetadata struct {
	Issuer                            string   `json:"issuer"`
	AuthorizationEndpoint             string   `json:"authorization_endpoint"`
	TokenEndpoint                     string   `json:"token_endpoint"`
	ResponseTypesSupported            []string `json:"response_types_supported"`
	GrantTypesSupported               []string `json:"grant_types_supported"`
	TokenEndpointAuthMethodsSupported []string `json:"token_endpoint_auth_methods_supported"`
	CodeChallengeMethodsSupported     []string `json:"code_challenge_methods_supported"`
	ScopesSupported                   []string `json:"scopes_supported,omitempty"`
}

// Manager provides OAuth client administration and HTTP auth handlers.
type Manager struct {
	path           string
	refreshStore   string
	accessTokenTTL time.Duration
	refreshTTL     time.Duration
	codeTTL        time.Duration
	logger         pslog.Logger

	mu           sync.RWMutex
	state        *state.Data
	stateMTime   time.Time
	stateSize    int64
	accessToken  map[string]accessToken
	refreshToken map[string]refreshToken
	authCode     map[string]authorizationCode
}

type accessToken struct {
	Token     string
	ClientID  string
	Scopes    []string
	ExpiresAt time.Time
}

type authorizationCode struct {
	Code        string
	ClientID    string
	RedirectURI string
	Scopes      []string
	ExpiresAt   time.Time
}

type refreshToken struct {
	Token     string    `json:"token"`
	ClientID  string    `json:"client_id"`
	Scopes    []string  `json:"scopes,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
}

type refreshStoreFile struct {
	Version int                     `json:"version"`
	Tokens  map[string]refreshToken `json:"tokens"`
}

// NewManager creates a manager backed by the configured state file.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	path := strings.TrimSpace(cfg.StatePath)
	if path == "" {
		var err error
		path, err = state.DefaultPath()
		if err != nil {
			return nil, err
		}
	}
	path, err := filepathAbs(path)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		path:           path,
		refreshStore:   strings.TrimSpace(cfg.RefreshStore),
		accessTokenTTL: cfg.AccessTokenTTL,
		refreshTTL:     cfg.RefreshTTL,
		codeTTL:        cfg.CodeTTL,
		logger:         cfg.Logger,
		accessToken:    make(map[string]accessToken),
		refreshToken:   make(map[string]refreshToken),
		authCode:       make(map[string]authorizationCode),
	}
	if m.refreshStore == "" {
		m.refreshStore = defaultRefreshStorePath(path)
	}
	if m.accessTokenTTL <= 0 {
		m.accessTokenTTL = defaultAccessTokenTTL
	}
	if m.refreshTTL <= 0 {
		m.refreshTTL = defaultRefreshTokenTTL
	}
	if m.codeTTL <= 0 {
		m.codeTTL = defaultCodeTTL
	}
	if m.logger == nil {
		m.logger = pslog.NewStructured(context.Background(), os.Stderr).With("app", "lockd")
	}
	if err := m.reload(); err != nil {
		return nil, err
	}
	if err := m.loadRefreshStore(); err != nil {
		return nil, err
	}
	return m, nil
}

// StatePath returns the backing state file path.
func (m *Manager) StatePath() string {
	return m.path
}

// ReloadIfChanged refreshes state from disk when file metadata changes.
func (m *Manager) ReloadIfChanged() error {
	info, err := os.Stat(m.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return state.ErrNotBootstrapped
		}
		return fmt.Errorf("stat oauth state: %w", err)
	}

	m.mu.RLock()
	unchanged := info.ModTime().Equal(m.stateMTime) && info.Size() == m.stateSize
	m.mu.RUnlock()
	if unchanged {
		return nil
	}
	return m.reload()
}

// Snapshot returns a copy of the current state.
func (m *Manager) Snapshot() *state.Data {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state == nil {
		return nil
	}
	return m.state.Clone()
}

// ListClients returns all clients sorted by ID.
func (m *Manager) ListClients() ([]state.Client, error) {
	if err := m.ReloadIfChanged(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state == nil {
		return nil, state.ErrNotBootstrapped
	}
	return m.state.ListClients(), nil
}

// Issuer returns the configured OAuth issuer.
func (m *Manager) Issuer() (string, error) {
	if err := m.ReloadIfChanged(); err != nil {
		return "", err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state == nil {
		return "", state.ErrNotBootstrapped
	}
	return strings.TrimSpace(m.state.Issuer), nil
}

// SetIssuer updates issuer and persists state.
func (m *Manager) SetIssuer(issuer string) error {
	issuer = strings.TrimSpace(issuer)
	if issuer == "" {
		return fmt.Errorf("issuer required")
	}
	_, err := url.ParseRequestURI(issuer)
	if err != nil {
		return fmt.Errorf("invalid issuer %q: %w", issuer, err)
	}
	return m.mutate(func(d *state.Data, now time.Time) error {
		d.SetIssuer(issuer, now)
		return nil
	})
}

// AddClient creates a confidential client and persists it.
func (m *Manager) AddClient(req AddClientRequest) (AddClientResponse, error) {
	resp := AddClientResponse{}
	err := m.mutate(func(d *state.Data, now time.Time) error {
		client, secret, err := d.AddClient(req.Name, req.Scopes, now)
		if err != nil {
			return err
		}
		resp.ClientID = client.ID
		resp.ClientSecret = secret
		return nil
	})
	return resp, err
}

// RemoveClient removes a client.
func (m *Manager) RemoveClient(clientID string) error {
	return m.mutate(func(d *state.Data, now time.Time) error {
		return d.RemoveClient(clientID, now)
	})
}

// RevokeClient sets client revocation status.
func (m *Manager) RevokeClient(clientID string, revoked bool) error {
	return m.mutate(func(d *state.Data, now time.Time) error {
		return d.RevokeClient(clientID, revoked, now)
	})
}

// UpdateClient updates mutable client fields.
func (m *Manager) UpdateClient(req UpdateClientRequest) error {
	return m.mutate(func(d *state.Data, now time.Time) error {
		if strings.TrimSpace(req.Name) != "" {
			if err := d.UpdateClientName(req.ClientID, req.Name, now); err != nil {
				return err
			}
		}
		if req.Scopes != nil {
			if err := d.UpdateClientScopes(req.ClientID, req.Scopes, now); err != nil {
				return err
			}
		}
		return nil
	})
}

// RotateClientSecret rotates client secret and persists it.
func (m *Manager) RotateClientSecret(clientID string) (string, error) {
	var secret string
	err := m.mutate(func(d *state.Data, now time.Time) error {
		var err error
		secret, err = d.RotateClientSecret(clientID, now)
		return err
	})
	return secret, err
}

// VerifyToken implements go-sdk/auth.TokenVerifier.
func (m *Manager) VerifyToken(ctx context.Context, token string, _ *http.Request) (*mcpauth.TokenInfo, error) {
	if err := m.ReloadIfChanged(); err != nil {
		return nil, fmt.Errorf("%w: reload oauth state: %v", mcpauth.ErrInvalidToken, err)
	}
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return nil, fmt.Errorf("%w: empty token", mcpauth.ErrInvalidToken)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupLocked(time.Now().UTC())
	entry, ok := m.accessToken[trimmed]
	if !ok {
		return nil, fmt.Errorf("%w: token not found", mcpauth.ErrInvalidToken)
	}
	if m.state == nil {
		return nil, fmt.Errorf("%w: state unavailable", mcpauth.ErrInvalidToken)
	}
	client, ok := m.state.Clients[entry.ClientID]
	if !ok || client.Revoked {
		delete(m.accessToken, trimmed)
		return nil, fmt.Errorf("%w: client revoked or missing", mcpauth.ErrInvalidToken)
	}

	return &mcpauth.TokenInfo{
		Scopes:     append([]string(nil), entry.Scopes...),
		Expiration: entry.ExpiresAt,
		UserID:     entry.ClientID,
	}, nil
}

// HandleAuthorize serves the OAuth authorize endpoint.
func (m *Manager) HandleAuthorize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := m.ReloadIfChanged(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	responseType := strings.TrimSpace(r.URL.Query().Get("response_type"))
	if responseType != "code" {
		http.Error(w, "unsupported response_type", http.StatusBadRequest)
		return
	}
	clientID := strings.TrimSpace(r.URL.Query().Get("client_id"))
	if clientID == "" {
		http.Error(w, "client_id required", http.StatusBadRequest)
		return
	}
	redirectURI := strings.TrimSpace(r.URL.Query().Get("redirect_uri"))
	if redirectURI == "" {
		http.Error(w, "redirect_uri required", http.StatusBadRequest)
		return
	}
	if _, err := url.ParseRequestURI(redirectURI); err != nil {
		http.Error(w, "invalid redirect_uri", http.StatusBadRequest)
		return
	}
	requestedScopes := splitScope(r.URL.Query().Get("scope"))
	stateParam := r.URL.Query().Get("state")

	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupLocked(now)
	if m.state == nil {
		http.Error(w, "oauth not bootstrapped", http.StatusUnauthorized)
		return
	}
	client, ok := m.state.Clients[clientID]
	if !ok || client.Revoked {
		http.Error(w, "invalid client_id", http.StatusUnauthorized)
		return
	}
	effectiveScopes, ok := allowedScopes(client.Scopes, requestedScopes)
	if !ok {
		http.Error(w, "invalid scope", http.StatusBadRequest)
		return
	}
	code, err := randomToken(32)
	if err != nil {
		http.Error(w, "failed to issue code", http.StatusInternalServerError)
		return
	}
	m.authCode[code] = authorizationCode{
		Code:        code,
		ClientID:    clientID,
		RedirectURI: redirectURI,
		Scopes:      effectiveScopes,
		ExpiresAt:   now.Add(m.codeTTL),
	}

	u, err := url.Parse(redirectURI)
	if err != nil {
		http.Error(w, "invalid redirect_uri", http.StatusBadRequest)
		return
	}
	q := u.Query()
	q.Set("code", code)
	if stateParam != "" {
		q.Set("state", stateParam)
	}
	u.RawQuery = q.Encode()
	http.Redirect(w, r, u.String(), http.StatusFound)
}

// HandleToken serves OAuth token issuance.
func (m *Manager) HandleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := m.ReloadIfChanged(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}

	clientID, clientSecret := clientCredentials(r)
	if clientID == "" || clientSecret == "" {
		http.Error(w, "client authentication required", http.StatusUnauthorized)
		return
	}

	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupLocked(now)
	if m.state == nil {
		http.Error(w, "oauth not bootstrapped", http.StatusUnauthorized)
		return
	}
	client, ok := m.state.VerifyClientSecret(clientID, clientSecret)
	if !ok {
		http.Error(w, "invalid client credentials", http.StatusUnauthorized)
		return
	}

	grantType := strings.TrimSpace(r.Form.Get("grant_type"))
	scopes := splitScope(r.Form.Get("scope"))
	switch grantType {
	case "client_credentials":
		effectiveScopes, scopeOK := allowedScopes(client.Scopes, scopes)
		if !scopeOK {
			http.Error(w, "invalid scope", http.StatusBadRequest)
			return
		}
		resp, err := m.issueTokenLocked(clientID, effectiveScopes, false, now)
		if err != nil {
			http.Error(w, "failed to issue token", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	case "authorization_code":
		code := strings.TrimSpace(r.Form.Get("code"))
		if code == "" {
			http.Error(w, "code required", http.StatusBadRequest)
			return
		}
		entry, ok := m.authCode[code]
		if !ok || entry.ExpiresAt.Before(now) {
			http.Error(w, "invalid code", http.StatusBadRequest)
			return
		}
		redirectURI := strings.TrimSpace(r.Form.Get("redirect_uri"))
		if redirectURI == "" || redirectURI != entry.RedirectURI {
			http.Error(w, "redirect_uri mismatch", http.StatusBadRequest)
			return
		}
		if entry.ClientID != clientID {
			http.Error(w, "client mismatch", http.StatusBadRequest)
			return
		}
		delete(m.authCode, code)
		resp, err := m.issueTokenLocked(clientID, entry.Scopes, true, now)
		if err != nil {
			http.Error(w, "failed to issue token", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	case "refresh_token":
		refresh := strings.TrimSpace(r.Form.Get("refresh_token"))
		if refresh == "" {
			http.Error(w, "refresh_token required", http.StatusBadRequest)
			return
		}
		rt, ok := m.refreshToken[refresh]
		if !ok || !rt.ExpiresAt.After(now) {
			http.Error(w, "invalid refresh_token", http.StatusUnauthorized)
			return
		}
		if rt.ClientID != clientID {
			http.Error(w, "client mismatch", http.StatusBadRequest)
			return
		}
		delete(m.refreshToken, refresh)
		effectiveScopes := rt.Scopes
		if len(scopes) > 0 {
			var scopeOK bool
			effectiveScopes, scopeOK = allowedScopes(rt.Scopes, scopes)
			if !scopeOK {
				http.Error(w, "invalid scope", http.StatusBadRequest)
				return
			}
		}
		resp, err := m.issueTokenLocked(clientID, effectiveScopes, true, now)
		if err != nil {
			http.Error(w, "failed to issue token", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	default:
		http.Error(w, "unsupported grant_type", http.StatusBadRequest)
	}
}

// HandleAuthServerMetadata serves RFC8414-style authorization server metadata.
func (m *Manager) HandleAuthServerMetadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := m.ReloadIfChanged(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	issuer, err := m.Issuer()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if issuer == "" {
		http.Error(w, "issuer is not configured", http.StatusBadRequest)
		return
	}

	meta := AuthServerMetadata{
		Issuer:                            issuer,
		AuthorizationEndpoint:             joinURL(issuer, "/authorize"),
		TokenEndpoint:                     joinURL(issuer, "/token"),
		ResponseTypesSupported:            []string{"code"},
		GrantTypesSupported:               []string{"authorization_code", "client_credentials"},
		TokenEndpointAuthMethodsSupported: []string{"client_secret_basic", "client_secret_post"},
		CodeChallengeMethodsSupported:     []string{"S256"},
		ScopesSupported:                   m.scopesSupported(),
	}
	writeJSON(w, http.StatusOK, meta)
}

// ProtectedResourceMetadata returns MCP protected-resource metadata payload.
func (m *Manager) ProtectedResourceMetadata(resourceID string) (*oauthex.ProtectedResourceMetadata, error) {
	if err := m.ReloadIfChanged(); err != nil {
		return nil, err
	}
	issuer, err := m.Issuer()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(resourceID) == "" {
		resourceID = joinURL(issuer, "/mcp")
	}
	return &oauthex.ProtectedResourceMetadata{
		Resource:             resourceID,
		AuthorizationServers: []string{issuer},
		ScopesSupported:      m.scopesSupported(),
		BearerMethodsSupported: []string{
			"header",
		},
	}, nil
}

func (m *Manager) issueTokenLocked(clientID string, scopes []string, includeRefresh bool, now time.Time) (TokenResponse, error) {
	token, err := randomToken(32)
	if err != nil {
		return TokenResponse{}, err
	}
	expiresAt := now.Add(m.accessTokenTTL)
	m.accessToken[token] = accessToken{
		Token:     token,
		ClientID:  clientID,
		Scopes:    append([]string(nil), scopes...),
		ExpiresAt: expiresAt,
	}
	resp := TokenResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresIn:   int64(m.accessTokenTTL.Seconds()),
		Scope:       strings.Join(scopes, " "),
	}
	if includeRefresh {
		refresh, err := randomToken(32)
		if err != nil {
			return TokenResponse{}, err
		}
		m.refreshToken[refresh] = refreshToken{
			Token:     refresh,
			ClientID:  clientID,
			Scopes:    append([]string(nil), scopes...),
			ExpiresAt: now.Add(m.refreshTTL),
		}
		resp.RefreshToken = refresh
		if err := m.saveRefreshStoreLocked(); err != nil {
			return TokenResponse{}, err
		}
	}
	return resp, nil
}

func (m *Manager) cleanupLocked(now time.Time) {
	refreshChanged := false
	for token, info := range m.accessToken {
		if !info.ExpiresAt.After(now) {
			delete(m.accessToken, token)
		}
	}
	for code, info := range m.authCode {
		if !info.ExpiresAt.After(now) {
			delete(m.authCode, code)
		}
	}
	for token, info := range m.refreshToken {
		if !info.ExpiresAt.After(now) {
			delete(m.refreshToken, token)
			refreshChanged = true
		}
	}
	if refreshChanged {
		_ = m.saveRefreshStoreLocked()
	}
}

func (m *Manager) scopesSupported() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state == nil {
		return nil
	}
	set := make(map[string]struct{})
	for _, client := range m.state.Clients {
		for _, scope := range client.Scopes {
			set[scope] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for scope := range set {
		out = append(out, scope)
	}
	sort.Strings(out)
	return out
}

func (m *Manager) mutate(fn func(*state.Data, time.Time) error) error {
	current, err := state.Load(m.path)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	working := current.Clone()
	if err := fn(working, now); err != nil {
		return err
	}
	if err := state.Save(m.path, working); err != nil {
		return err
	}
	info, err := os.Stat(m.path)
	if err != nil {
		return fmt.Errorf("stat saved state: %w", err)
	}

	m.mu.Lock()
	m.state = working
	m.stateMTime = info.ModTime()
	m.stateSize = info.Size()
	m.mu.Unlock()
	return nil
}

func (m *Manager) reload() error {
	loaded, err := state.Load(m.path)
	if err != nil {
		return err
	}
	info, err := os.Stat(m.path)
	if err != nil {
		return fmt.Errorf("stat oauth state: %w", err)
	}
	m.mu.Lock()
	m.state = loaded
	m.stateMTime = info.ModTime()
	m.stateSize = info.Size()
	m.mu.Unlock()
	m.logger.Debug("oauth state reloaded", "path", m.path, "clients", len(loaded.Clients))
	return nil
}

func (m *Manager) loadRefreshStore() error {
	raw, err := os.ReadFile(m.refreshStore)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read refresh-token store: %w", err)
	}
	var store refreshStoreFile
	if err := json.Unmarshal(raw, &store); err != nil {
		return fmt.Errorf("decode refresh-token store: %w", err)
	}
	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	if store.Tokens == nil {
		store.Tokens = make(map[string]refreshToken)
	}
	for token, entry := range store.Tokens {
		if !entry.ExpiresAt.After(now) {
			continue
		}
		entry.Token = token
		entry.Scopes = append([]string(nil), entry.Scopes...)
		m.refreshToken[token] = entry
	}
	return nil
}

func (m *Manager) saveRefreshStoreLocked() error {
	payload := refreshStoreFile{
		Version: 1,
		Tokens:  make(map[string]refreshToken, len(m.refreshToken)),
	}
	for token, entry := range m.refreshToken {
		entry.Token = token
		entry.Scopes = append([]string(nil), entry.Scopes...)
		payload.Tokens[token] = entry
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode refresh-token store: %w", err)
	}
	if err := writeFileAtomic(m.refreshStore, encoded, 0o600); err != nil {
		return fmt.Errorf("persist refresh-token store: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func splitScope(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Fields(raw)
	if len(parts) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(parts))
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, ok := set[p]; ok {
			continue
		}
		set[p] = struct{}{}
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func allowedScopes(allowed, requested []string) ([]string, bool) {
	if len(allowed) == 0 {
		if len(requested) == 0 {
			return nil, true
		}
		return requested, true
	}
	if len(requested) == 0 {
		return append([]string(nil), allowed...), true
	}
	allowSet := make(map[string]struct{}, len(allowed))
	for _, scope := range allowed {
		allowSet[scope] = struct{}{}
	}
	for _, scope := range requested {
		if _, ok := allowSet[scope]; !ok {
			return nil, false
		}
	}
	return requested, true
}

func clientCredentials(r *http.Request) (string, string) {
	if id, secret, ok := r.BasicAuth(); ok {
		if strings.TrimSpace(id) != "" && strings.TrimSpace(secret) != "" {
			return strings.TrimSpace(id), strings.TrimSpace(secret)
		}
	}
	return strings.TrimSpace(r.Form.Get("client_id")), strings.TrimSpace(r.Form.Get("client_secret"))
}

func randomToken(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func joinURL(baseURL, p string) string {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return p
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return baseURL + p
	}
	u.Path = path.Join(u.Path, strings.TrimPrefix(p, "/"))
	return u.String()
}

func filepathAbs(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", fmt.Errorf("resolve path %q: %w", p, err)
	}
	return abs, nil
}

func defaultRefreshStorePath(statePath string) string {
	dir := filepath.Dir(statePath)
	return filepath.Join(dir, "mcp-auth-store.json")
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
