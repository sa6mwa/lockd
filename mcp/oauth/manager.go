package oauth

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	"pkt.systems/kryptograf"
	"pkt.systems/kryptograf/keymgmt"
	"pkt.systems/lockd/mcp/preset"
	"pkt.systems/lockd/mcp/state"
	"pkt.systems/pslog"
)

const (
	defaultAccessTokenTTL       = 15 * time.Minute
	defaultRefreshTokenTTL      = 24 * time.Hour
	defaultAuthorizationCodeTTL = 2 * time.Minute

	defaultTokenStoreVersion = 1
	tokenStoreContext        = "lockd/mcp/oauth/token-store"
	tokenStoreFileName       = "mcp-token-store.enc.json"

	// TokenInfoExtraDefaultNamespace carries an optional client namespace override
	// in auth.TokenInfo.Extra for MCP request handlers.
	TokenInfoExtraDefaultNamespace = "lockd_default_namespace"
)

var (
	// ErrClientInactive indicates the OAuth client is missing or revoked.
	ErrClientInactive = errors.New("oauth client missing or revoked")
)

// ManagerConfig configures OAuth manager behavior.
type ManagerConfig struct {
	StatePath            string
	TokenStore           string
	AccessTokenTTL       time.Duration
	RefreshTokenTTL      time.Duration
	AuthorizationCodeTTL time.Duration
	Logger               pslog.Logger
}

// AddClientRequest defines add-client parameters.
type AddClientRequest struct {
	Name string
	// Namespace optionally sets a per-client default namespace used by MCP tools
	// when the tool input omits namespace.
	Namespace    string
	LockdPreset  bool
	Presets      []preset.Definition
	Scopes       []string
	RedirectURIs []string
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
	// Namespace updates the optional per-client default namespace when non-nil.
	// Use pointer to distinguish "leave unchanged" (nil) from "clear" ("").
	Namespace    *string
	LockdPreset  *bool
	Presets      []preset.Definition
	Scopes       []string
	RedirectURIs []string
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
	AuthorizationEndpoint             string   `json:"authorization_endpoint,omitempty"`
	TokenEndpoint                     string   `json:"token_endpoint"`
	RegistrationEndpoint              string   `json:"registration_endpoint,omitempty"`
	ResponseTypesSupported            []string `json:"response_types_supported,omitempty"`
	GrantTypesSupported               []string `json:"grant_types_supported"`
	TokenEndpointAuthMethodsSupported []string `json:"token_endpoint_auth_methods_supported"`
	CodeChallengeMethodsSupported     []string `json:"code_challenge_methods_supported,omitempty"`
	ScopesSupported                   []string `json:"scopes_supported,omitempty"`
}

// OpenIDConfigurationMetadata exposes an OIDC-style discovery alias.
type OpenIDConfigurationMetadata struct {
	Issuer                            string   `json:"issuer"`
	AuthorizationEndpoint             string   `json:"authorization_endpoint,omitempty"`
	TokenEndpoint                     string   `json:"token_endpoint"`
	RegistrationEndpoint              string   `json:"registration_endpoint,omitempty"`
	ResponseTypesSupported            []string `json:"response_types_supported,omitempty"`
	GrantTypesSupported               []string `json:"grant_types_supported,omitempty"`
	TokenEndpointAuthMethodsSupported []string `json:"token_endpoint_auth_methods_supported,omitempty"`
	CodeChallengeMethodsSupported     []string `json:"code_challenge_methods_supported,omitempty"`
	ScopesSupported                   []string `json:"scopes_supported,omitempty"`
}

// Manager provides OAuth client administration and HTTP auth handlers.
type Manager struct {
	path                 string
	tokenStore           string
	accessTokenTTL       time.Duration
	refreshTokenTTL      time.Duration
	authorizationCodeTTL time.Duration
	logger               pslog.Logger

	mu                sync.RWMutex
	state             *state.Data
	stateMTime        time.Time
	stateSize         int64
	accessToken       map[string]accessToken
	refreshToken      map[string]refreshToken
	authorizationCode map[string]authorizationCode
}

type accessToken struct {
	Token     string
	ClientID  string
	Scopes    []string
	Resource  string
	ExpiresAt time.Time
}

type accessTokenRecord struct {
	ClientID  string    `json:"client_id"`
	Scopes    []string  `json:"scopes,omitempty"`
	Resource  string    `json:"resource,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
}

type refreshToken struct {
	Token     string
	ClientID  string
	Scopes    []string
	Resource  string
	ExpiresAt time.Time
}

type refreshTokenRecord struct {
	ClientID  string    `json:"client_id"`
	Scopes    []string  `json:"scopes,omitempty"`
	Resource  string    `json:"resource,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
}

type authorizationCode struct {
	ClientID            string
	RedirectURI         string
	Scopes              []string
	Resource            string
	PKCEChallenge       string
	PKCEChallengeMethod string
	ExpiresAt           time.Time
}

type tokenStoreFile struct {
	Version    int    `json:"version"`
	Descriptor string `json:"descriptor"`
	Ciphertext string `json:"ciphertext"`
}

type tokenStorePayload struct {
	Version       int                           `json:"version"`
	AccessTokens  map[string]accessTokenRecord  `json:"access_tokens,omitempty"`
	RefreshTokens map[string]refreshTokenRecord `json:"refresh_tokens,omitempty"`
	LegacyTokens  map[string]accessTokenRecord  `json:"tokens,omitempty"`
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
		path:                 path,
		tokenStore:           strings.TrimSpace(cfg.TokenStore),
		accessTokenTTL:       cfg.AccessTokenTTL,
		refreshTokenTTL:      cfg.RefreshTokenTTL,
		authorizationCodeTTL: cfg.AuthorizationCodeTTL,
		logger:               cfg.Logger,
		accessToken:          make(map[string]accessToken),
		refreshToken:         make(map[string]refreshToken),
		authorizationCode:    make(map[string]authorizationCode),
	}
	if m.tokenStore == "" {
		m.tokenStore = defaultTokenStorePath(path)
	}
	if m.accessTokenTTL <= 0 {
		m.accessTokenTTL = defaultAccessTokenTTL
	}
	if m.refreshTokenTTL <= 0 {
		m.refreshTokenTTL = defaultRefreshTokenTTL
	}
	if m.authorizationCodeTTL <= 0 {
		m.authorizationCodeTTL = defaultAuthorizationCodeTTL
	}
	if m.logger == nil {
		m.logger = pslog.NewStructured(context.Background(), os.Stderr).With("app", "lockd")
	}
	if err := m.reload(); err != nil {
		return nil, err
	}
	if err := m.loadTokenStore(); err != nil {
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

// ClientNamespace returns the optional default namespace configured on a client.
func (m *Manager) ClientNamespace(clientID string) (string, error) {
	clientID = strings.TrimSpace(clientID)
	if clientID == "" {
		return "", fmt.Errorf("client id required")
	}
	if err := m.ReloadIfChanged(); err != nil {
		return "", err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state == nil {
		return "", state.ErrNotBootstrapped
	}
	client, ok := m.state.Clients[clientID]
	if !ok || client.Revoked {
		return "", ErrClientInactive
	}
	return strings.TrimSpace(client.Namespace), nil
}

// EnsureClientActive validates that the client exists and is not revoked.
func (m *Manager) EnsureClientActive(clientID string) error {
	clientID = strings.TrimSpace(clientID)
	if clientID == "" {
		return fmt.Errorf("client id required")
	}
	if err := m.ReloadIfChanged(); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state == nil {
		return state.ErrNotBootstrapped
	}
	client, ok := m.state.Clients[clientID]
	if !ok || client.Revoked {
		return ErrClientInactive
	}
	return nil
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
		client, secret, err := d.AddClient(req.Name, req.Namespace, req.LockdPreset, req.Presets, req.Scopes, req.RedirectURIs, now)
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
	trimmed := strings.TrimSpace(clientID)
	if err := m.mutate(func(d *state.Data, now time.Time) error {
		return d.RemoveClient(trimmed, now)
	}); err != nil {
		return err
	}
	return m.purgeClientTokens(trimmed)
}

// RevokeClient sets client revocation status.
func (m *Manager) RevokeClient(clientID string, revoked bool) error {
	trimmed := strings.TrimSpace(clientID)
	if err := m.mutate(func(d *state.Data, now time.Time) error {
		return d.RevokeClient(trimmed, revoked, now)
	}); err != nil {
		return err
	}
	if revoked {
		return m.purgeClientTokens(trimmed)
	}
	return nil
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
		if req.RedirectURIs != nil {
			if err := d.UpdateClientRedirectURIs(req.ClientID, req.RedirectURIs, now); err != nil {
				return err
			}
		}
		if req.Namespace != nil {
			if err := d.UpdateClientNamespace(req.ClientID, *req.Namespace, now); err != nil {
				return err
			}
		}
		if req.LockdPreset != nil || req.Presets != nil {
			lockdPreset := false
			client, ok := d.Clients[strings.TrimSpace(req.ClientID)]
			if !ok {
				return state.ErrClientNotFound
			}
			lockdPreset = client.LockdPreset
			if req.LockdPreset != nil {
				lockdPreset = *req.LockdPreset
			}
			if err := d.UpdateClientPresets(req.ClientID, lockdPreset, req.Presets, now); err != nil {
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
		if err := m.saveTokenStoreLocked(); err != nil {
			m.logger.Warn("oauth token store persist failed", "path", m.tokenStore, "error", err)
		}
		return nil, fmt.Errorf("%w: client revoked or missing", mcpauth.ErrInvalidToken)
	}

	info := &mcpauth.TokenInfo{
		Scopes:     append([]string(nil), entry.Scopes...),
		Expiration: entry.ExpiresAt,
		UserID:     entry.ClientID,
	}
	if ns := strings.TrimSpace(client.Namespace); ns != "" {
		info.Extra = map[string]any{
			TokenInfoExtraDefaultNamespace: ns,
		}
	}
	return info, nil
}

// HandleToken serves OAuth token issuance.
func (m *Manager) HandleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := m.ReloadIfChanged(); err != nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "reload oauth state failed")
		return
	}
	if err := r.ParseForm(); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "invalid form")
		return
	}

	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupLocked(now)
	if m.state == nil {
		writeOAuthError(w, http.StatusUnauthorized, "invalid_client", "oauth state not initialized")
		return
	}
	grantType := strings.TrimSpace(r.Form.Get("grant_type"))
	scopes := splitScope(r.Form.Get("scope"))
	switch grantType {
	case "client_credentials":
		client, clientID, ok := m.verifyConfidentialClientLocked(r)
		if !ok {
			writeOAuthErrorWithAuthenticate(w, http.StatusUnauthorized, "invalid_client", "invalid client credentials")
			return
		}
		effectiveScopes, scopeOK := allowedScopes(client.Scopes, scopes)
		if !scopeOK {
			writeOAuthError(w, http.StatusBadRequest, "invalid_scope", "invalid scope")
			return
		}
		resource, err := parseResourceParam(r.Form.Get("resource"))
		if err != nil {
			writeOAuthError(w, http.StatusBadRequest, "invalid_target", err.Error())
			return
		}
		resp, err := m.issueTokenLocked(clientID, effectiveScopes, resource, false, now)
		if err != nil {
			writeOAuthError(w, http.StatusInternalServerError, "server_error", "failed to issue token")
			return
		}
		writeOAuthJSON(w, http.StatusOK, resp)
	case "authorization_code":
		client, clientID, ok := m.verifyConfidentialClientLocked(r)
		if !ok {
			writeOAuthErrorWithAuthenticate(w, http.StatusUnauthorized, "invalid_client", "invalid client credentials")
			return
		}
		code := strings.TrimSpace(r.Form.Get("code"))
		if code == "" {
			writeOAuthError(w, http.StatusBadRequest, "invalid_request", "code is required")
			return
		}
		redirectURI := strings.TrimSpace(r.Form.Get("redirect_uri"))
		if redirectURI == "" {
			writeOAuthError(w, http.StatusBadRequest, "invalid_request", "redirect_uri is required")
			return
		}
		codeVerifier := strings.TrimSpace(r.Form.Get("code_verifier"))
		if codeVerifier == "" {
			writeOAuthError(w, http.StatusBadRequest, "invalid_request", "code_verifier is required")
			return
		}
		entry, ok := m.authorizationCode[code]
		if !ok || !entry.ExpiresAt.After(now) {
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "invalid authorization code")
			return
		}
		delete(m.authorizationCode, code)
		if entry.ClientID != clientID {
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "authorization code does not belong to client")
			return
		}
		if entry.RedirectURI != redirectURI {
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "redirect_uri mismatch")
			return
		}
		if !redirectURIAllowed(client.RedirectURIs, redirectURI) {
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "redirect_uri is not allowed for client")
			return
		}
		if !verifyPKCE(codeVerifier, entry.PKCEChallenge, entry.PKCEChallengeMethod) {
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "PKCE verification failed")
			return
		}
		requestedResource, err := parseResourceParam(r.Form.Get("resource"))
		if err != nil {
			writeOAuthError(w, http.StatusBadRequest, "invalid_target", err.Error())
			return
		}
		resource := entry.Resource
		if requestedResource != "" {
			if resource != "" && requestedResource != resource {
				writeOAuthError(w, http.StatusBadRequest, "invalid_target", "resource does not match authorization request")
				return
			}
			resource = requestedResource
		}
		resp, err := m.issueTokenLocked(clientID, entry.Scopes, resource, true, now)
		if err != nil {
			writeOAuthError(w, http.StatusInternalServerError, "server_error", "failed to issue token")
			return
		}
		writeOAuthJSON(w, http.StatusOK, resp)
	case "refresh_token":
		_, clientID, ok := m.verifyConfidentialClientLocked(r)
		if !ok {
			writeOAuthErrorWithAuthenticate(w, http.StatusUnauthorized, "invalid_client", "invalid client credentials")
			return
		}
		refresh := strings.TrimSpace(r.Form.Get("refresh_token"))
		if refresh == "" {
			writeOAuthError(w, http.StatusBadRequest, "invalid_request", "refresh_token is required")
			return
		}
		entry, ok := m.refreshToken[refresh]
		if !ok || !entry.ExpiresAt.After(now) {
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "invalid refresh_token")
			return
		}
		if entry.ClientID != clientID {
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "refresh_token does not belong to client")
			return
		}
		delete(m.refreshToken, refresh)
		effectiveScopes := append([]string(nil), entry.Scopes...)
		if len(scopes) > 0 {
			var allowed bool
			effectiveScopes, allowed = allowedScopes(entry.Scopes, scopes)
			if !allowed {
				writeOAuthError(w, http.StatusBadRequest, "invalid_scope", "invalid scope")
				return
			}
		}
		requestedResource, err := parseResourceParam(r.Form.Get("resource"))
		if err != nil {
			writeOAuthError(w, http.StatusBadRequest, "invalid_target", err.Error())
			return
		}
		resource := entry.Resource
		if requestedResource != "" {
			if resource != "" && requestedResource != resource {
				writeOAuthError(w, http.StatusBadRequest, "invalid_target", "resource does not match refresh token")
				return
			}
			resource = requestedResource
		}
		resp, err := m.issueTokenLocked(clientID, effectiveScopes, resource, true, now)
		if err != nil {
			writeOAuthError(w, http.StatusInternalServerError, "server_error", "failed to issue token")
			return
		}
		writeOAuthJSON(w, http.StatusOK, resp)
	default:
		writeOAuthError(w, http.StatusBadRequest, "unsupported_grant_type", "unsupported grant_type")
	}
}

// HandleAuthorize serves the authorization-code endpoint with PKCE enforcement.
func (m *Manager) HandleAuthorize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := m.ReloadIfChanged(); err != nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "reload oauth state failed")
		return
	}
	responseType := strings.TrimSpace(r.URL.Query().Get("response_type"))
	if responseType != "code" {
		writeOAuthError(w, http.StatusBadRequest, "unsupported_response_type", "response_type must be code")
		return
	}
	clientID := strings.TrimSpace(r.URL.Query().Get("client_id"))
	if clientID == "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "client_id is required")
		return
	}
	redirectURI := strings.TrimSpace(r.URL.Query().Get("redirect_uri"))
	if redirectURI == "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "redirect_uri is required")
		return
	}
	parsedRedirectURI, err := url.Parse(redirectURI)
	if err != nil || parsedRedirectURI == nil || !parsedRedirectURI.IsAbs() {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "redirect_uri must be an absolute URL")
		return
	}
	codeChallenge := strings.TrimSpace(r.URL.Query().Get("code_challenge"))
	if codeChallenge == "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "code_challenge is required")
		return
	}
	codeChallengeMethod := strings.TrimSpace(r.URL.Query().Get("code_challenge_method"))
	if !strings.EqualFold(codeChallengeMethod, "S256") {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "code_challenge_method must be S256")
		return
	}
	resource, err := parseResourceParam(r.URL.Query().Get("resource"))
	if err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_target", err.Error())
		return
	}
	requestedScopes := splitScope(r.URL.Query().Get("scope"))
	stateParam := strings.TrimSpace(r.URL.Query().Get("state"))

	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cleanupLocked(now)
	if m.state == nil {
		writeOAuthError(w, http.StatusUnauthorized, "invalid_client", "oauth state not initialized")
		return
	}
	client, ok := m.state.Clients[clientID]
	if !ok || client.Revoked {
		writeOAuthError(w, http.StatusUnauthorized, "invalid_client", "invalid client")
		return
	}
	if !redirectURIAllowed(client.RedirectURIs, redirectURI) {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "redirect_uri is not allowed for client")
		return
	}
	effectiveScopes, scopeOK := allowedScopes(client.Scopes, requestedScopes)
	if !scopeOK {
		writeOAuthError(w, http.StatusBadRequest, "invalid_scope", "invalid scope")
		return
	}

	code, err := randomToken(32)
	if err != nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "failed to issue authorization code")
		return
	}
	m.authorizationCode[code] = authorizationCode{
		ClientID:            clientID,
		RedirectURI:         redirectURI,
		Scopes:              effectiveScopes,
		Resource:            resource,
		PKCEChallenge:       codeChallenge,
		PKCEChallengeMethod: "S256",
		ExpiresAt:           now.Add(m.authorizationCodeTTL),
	}

	q := parsedRedirectURI.Query()
	q.Set("code", code)
	if stateParam != "" {
		q.Set("state", stateParam)
	}
	parsedRedirectURI.RawQuery = q.Encode()
	http.Redirect(w, r, parsedRedirectURI.String(), http.StatusFound)
}

type dynamicClientRegistrationRequest struct {
	ClientName              string   `json:"client_name,omitempty"`
	RedirectURIs            []string `json:"redirect_uris,omitempty"`
	GrantTypes              []string `json:"grant_types,omitempty"`
	ResponseTypes           []string `json:"response_types,omitempty"`
	Scope                   string   `json:"scope,omitempty"`
	TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method,omitempty"`
}

type dynamicClientRegistrationResponse struct {
	ClientID                string   `json:"client_id"`
	ClientSecret            string   `json:"client_secret,omitempty"`
	ClientIDIssuedAt        int64    `json:"client_id_issued_at,omitempty"`
	ClientSecretExpiresAt   int64    `json:"client_secret_expires_at,omitempty"`
	ClientName              string   `json:"client_name,omitempty"`
	RedirectURIs            []string `json:"redirect_uris,omitempty"`
	GrantTypes              []string `json:"grant_types,omitempty"`
	ResponseTypes           []string `json:"response_types,omitempty"`
	Scope                   string   `json:"scope,omitempty"`
	TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method,omitempty"`
}

// HandleRegister serves OAuth dynamic client registration.
func (m *Manager) HandleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := m.ReloadIfChanged(); err != nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "reload oauth state failed")
		return
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req dynamicClientRegistrationRequest
	if err := dec.Decode(&req); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_client_metadata", "invalid registration payload")
		return
	}
	for _, responseType := range req.ResponseTypes {
		if strings.TrimSpace(responseType) != "code" {
			writeOAuthError(w, http.StatusBadRequest, "invalid_client_metadata", "unsupported response_types")
			return
		}
	}
	if strings.TrimSpace(req.TokenEndpointAuthMethod) != "" &&
		strings.TrimSpace(req.TokenEndpointAuthMethod) != "client_secret_basic" &&
		strings.TrimSpace(req.TokenEndpointAuthMethod) != "client_secret_post" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_client_metadata", "unsupported token_endpoint_auth_method")
		return
	}
	grantTypes := normalizeGrantTypes(req.GrantTypes)
	if len(grantTypes) == 0 {
		grantTypes = []string{"authorization_code", "refresh_token", "client_credentials"}
	}
	for _, grantType := range grantTypes {
		switch grantType {
		case "authorization_code", "refresh_token", "client_credentials":
		default:
			writeOAuthError(w, http.StatusBadRequest, "invalid_client_metadata", "unsupported grant_types")
			return
		}
	}
	if grantTypeContains(grantTypes, "authorization_code") && len(req.RedirectURIs) == 0 {
		writeOAuthError(w, http.StatusBadRequest, "invalid_client_metadata", "redirect_uris required for authorization_code")
		return
	}
	name := strings.TrimSpace(req.ClientName)
	if name == "" {
		name = "dynamic-client"
	}
	scopes := splitScope(req.Scope)
	resp, err := m.AddClient(AddClientRequest{
		Name:         name,
		Scopes:       scopes,
		RedirectURIs: req.RedirectURIs,
	})
	if err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_client_metadata", err.Error())
		return
	}
	registeredClient, err := m.clientByID(resp.ClientID)
	if err != nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "failed to load registered client")
		return
	}
	authMethod := strings.TrimSpace(req.TokenEndpointAuthMethod)
	if authMethod == "" {
		authMethod = "client_secret_basic"
	}
	out := dynamicClientRegistrationResponse{
		ClientID:                resp.ClientID,
		ClientSecret:            resp.ClientSecret,
		ClientIDIssuedAt:        time.Now().UTC().Unix(),
		ClientSecretExpiresAt:   0,
		ClientName:              registeredClient.Name,
		RedirectURIs:            append([]string(nil), registeredClient.RedirectURIs...),
		GrantTypes:              grantTypes,
		ResponseTypes:           []string{"code"},
		Scope:                   strings.Join(registeredClient.Scopes, " "),
		TokenEndpointAuthMethod: authMethod,
	}
	writeOAuthJSON(w, http.StatusCreated, out)
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
		RegistrationEndpoint:              joinURL(issuer, "/register"),
		ResponseTypesSupported:            []string{"code"},
		GrantTypesSupported:               []string{"authorization_code", "refresh_token", "client_credentials"},
		TokenEndpointAuthMethodsSupported: []string{"client_secret_basic", "client_secret_post"},
		CodeChallengeMethodsSupported:     []string{"S256"},
		ScopesSupported:                   m.scopesSupported(),
	}
	writeJSON(w, http.StatusOK, meta)
}

// HandleOpenIDConfiguration serves an OIDC discovery alias.
func (m *Manager) HandleOpenIDConfiguration(w http.ResponseWriter, r *http.Request) {
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
	meta := OpenIDConfigurationMetadata{
		Issuer:                            issuer,
		AuthorizationEndpoint:             joinURL(issuer, "/authorize"),
		TokenEndpoint:                     joinURL(issuer, "/token"),
		RegistrationEndpoint:              joinURL(issuer, "/register"),
		ResponseTypesSupported:            []string{"code"},
		GrantTypesSupported:               []string{"authorization_code", "refresh_token", "client_credentials"},
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

func (m *Manager) issueTokenLocked(clientID string, scopes []string, resource string, issueRefresh bool, now time.Time) (TokenResponse, error) {
	token, err := randomToken(32)
	if err != nil {
		return TokenResponse{}, err
	}
	expiresAt := now.Add(m.accessTokenTTL)
	m.accessToken[token] = accessToken{
		Token:     token,
		ClientID:  clientID,
		Scopes:    append([]string(nil), scopes...),
		Resource:  strings.TrimSpace(resource),
		ExpiresAt: expiresAt,
	}
	resp := TokenResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresIn:   int64(m.accessTokenTTL.Seconds()),
		Scope:       strings.Join(scopes, " "),
	}
	refreshTokenValue := ""
	if issueRefresh {
		refreshTokenValue, err = randomToken(32)
		if err != nil {
			delete(m.accessToken, token)
			return TokenResponse{}, err
		}
		m.refreshToken[refreshTokenValue] = refreshToken{
			Token:     refreshTokenValue,
			ClientID:  clientID,
			Scopes:    append([]string(nil), scopes...),
			Resource:  strings.TrimSpace(resource),
			ExpiresAt: now.Add(m.refreshTokenTTL),
		}
		resp.RefreshToken = refreshTokenValue
	}
	if err := m.saveTokenStoreLocked(); err != nil {
		delete(m.accessToken, token)
		if refreshTokenValue != "" {
			delete(m.refreshToken, refreshTokenValue)
		}
		return TokenResponse{}, err
	}
	return resp, nil
}

func (m *Manager) cleanupLocked(now time.Time) {
	storeChanged := false
	for token, info := range m.accessToken {
		if !info.ExpiresAt.After(now) {
			delete(m.accessToken, token)
			storeChanged = true
		}
	}
	for token, info := range m.refreshToken {
		if !info.ExpiresAt.After(now) {
			delete(m.refreshToken, token)
			storeChanged = true
		}
	}
	for code, info := range m.authorizationCode {
		if !info.ExpiresAt.After(now) {
			delete(m.authorizationCode, code)
		}
	}
	if storeChanged {
		if err := m.saveTokenStoreLocked(); err != nil {
			m.logger.Warn("oauth token store persist failed", "path", m.tokenStore, "error", err)
		}
	}
}

func (m *Manager) purgeClientTokens(clientID string) error {
	clientID = strings.TrimSpace(clientID)
	if clientID == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	changed := false
	for token, info := range m.accessToken {
		if info.ClientID == clientID {
			delete(m.accessToken, token)
			changed = true
		}
	}
	for token, info := range m.refreshToken {
		if info.ClientID == clientID {
			delete(m.refreshToken, token)
			changed = true
		}
	}
	for code, info := range m.authorizationCode {
		if info.ClientID == clientID {
			delete(m.authorizationCode, code)
		}
	}
	if !changed {
		return nil
	}
	return m.saveTokenStoreLocked()
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

func (m *Manager) loadTokenStore() error {
	raw, err := os.ReadFile(m.tokenStore)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read token store: %w", err)
	}
	var store tokenStoreFile
	if err := json.Unmarshal(raw, &store); err != nil {
		return fmt.Errorf("decode token store envelope: %w", err)
	}
	if strings.TrimSpace(store.Descriptor) == "" || strings.TrimSpace(store.Ciphertext) == "" {
		return fmt.Errorf("decode token store envelope: descriptor and ciphertext are required")
	}

	desc, err := keymgmt.DescriptorFromHex(strings.TrimSpace(store.Descriptor))
	if err != nil {
		return fmt.Errorf("decode token store descriptor: %w", err)
	}
	ciphertext, err := base64.StdEncoding.DecodeString(strings.TrimSpace(store.Ciphertext))
	if err != nil {
		return fmt.Errorf("decode token store ciphertext: %w", err)
	}
	material, err := m.reconstructTokenStoreMaterial(desc)
	if err != nil {
		return err
	}
	defer material.Zero()
	plaintext, err := decryptTokenStorePayload(ciphertext, material)
	if err != nil {
		return err
	}
	var payload tokenStorePayload
	if err := json.Unmarshal(plaintext, &payload); err != nil {
		return fmt.Errorf("decode token store payload: %w", err)
	}
	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	accessTokens := payload.AccessTokens
	if len(accessTokens) == 0 && len(payload.LegacyTokens) > 0 {
		accessTokens = payload.LegacyTokens
	}
	for token, entry := range accessTokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		if !entry.ExpiresAt.After(now) {
			continue
		}
		m.accessToken[token] = accessToken{
			Token:     token,
			ClientID:  strings.TrimSpace(entry.ClientID),
			Scopes:    append([]string(nil), entry.Scopes...),
			Resource:  strings.TrimSpace(entry.Resource),
			ExpiresAt: entry.ExpiresAt.UTC(),
		}
	}
	for token, entry := range payload.RefreshTokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		if !entry.ExpiresAt.After(now) {
			continue
		}
		m.refreshToken[token] = refreshToken{
			Token:     token,
			ClientID:  strings.TrimSpace(entry.ClientID),
			Scopes:    append([]string(nil), entry.Scopes...),
			Resource:  strings.TrimSpace(entry.Resource),
			ExpiresAt: entry.ExpiresAt.UTC(),
		}
	}
	return nil
}

func (m *Manager) saveTokenStoreLocked() error {
	payload := tokenStorePayload{
		Version:       defaultTokenStoreVersion,
		AccessTokens:  make(map[string]accessTokenRecord, len(m.accessToken)),
		RefreshTokens: make(map[string]refreshTokenRecord, len(m.refreshToken)),
	}
	for token, entry := range m.accessToken {
		if !entry.ExpiresAt.After(time.Now().UTC()) {
			continue
		}
		payload.AccessTokens[token] = accessTokenRecord{
			ClientID:  strings.TrimSpace(entry.ClientID),
			Scopes:    append([]string(nil), entry.Scopes...),
			Resource:  strings.TrimSpace(entry.Resource),
			ExpiresAt: entry.ExpiresAt.UTC(),
		}
	}
	for token, entry := range m.refreshToken {
		if !entry.ExpiresAt.After(time.Now().UTC()) {
			continue
		}
		payload.RefreshTokens[token] = refreshTokenRecord{
			ClientID:  strings.TrimSpace(entry.ClientID),
			Scopes:    append([]string(nil), entry.Scopes...),
			Resource:  strings.TrimSpace(entry.Resource),
			ExpiresAt: entry.ExpiresAt.UTC(),
		}
	}
	plaintext, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode token store payload: %w", err)
	}

	material, err := m.mintTokenStoreMaterial()
	if err != nil {
		return err
	}
	defer material.Zero()
	ciphertext, err := encryptTokenStorePayload(plaintext, material)
	if err != nil {
		return err
	}
	descHex, err := material.Descriptor.EncodeToHex()
	if err != nil {
		return fmt.Errorf("encode token store descriptor: %w", err)
	}
	envelope := tokenStoreFile{
		Version:    defaultTokenStoreVersion,
		Descriptor: descHex,
		Ciphertext: base64.StdEncoding.EncodeToString(ciphertext),
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode token store envelope: %w", err)
	}
	if err := writeFileAtomic(m.tokenStore, encoded, 0o600); err != nil {
		return fmt.Errorf("persist token store: %w", err)
	}
	return nil
}

func (m *Manager) mintTokenStoreMaterial() (kryptograf.Material, error) {
	root, err := loadOAuthRootKey(m.path)
	if err != nil {
		return kryptograf.Material{}, err
	}
	material, err := kryptograf.New(root).MintDEK([]byte(tokenStoreContext))
	if err != nil {
		return kryptograf.Material{}, fmt.Errorf("mint token-store DEK: %w", err)
	}
	return material, nil
}

func (m *Manager) reconstructTokenStoreMaterial(desc keymgmt.Descriptor) (kryptograf.Material, error) {
	root, err := loadOAuthRootKey(m.path)
	if err != nil {
		return kryptograf.Material{}, err
	}
	material, err := kryptograf.New(root).ReconstructDEK([]byte(tokenStoreContext), desc)
	if err != nil {
		return kryptograf.Material{}, fmt.Errorf("reconstruct token-store DEK: %w", err)
	}
	return material, nil
}

func loadOAuthRootKey(statePath string) (keymgmt.RootKey, error) {
	raw, err := os.ReadFile(strings.TrimSpace(statePath))
	if err != nil {
		return keymgmt.RootKey{}, fmt.Errorf("read oauth state for token-store keying: %w", err)
	}
	store, err := keymgmt.LoadPEM(raw)
	if err != nil {
		return keymgmt.RootKey{}, fmt.Errorf("load oauth key material: %w", err)
	}
	root, ok, err := store.RootKey()
	if err != nil {
		return keymgmt.RootKey{}, fmt.Errorf("read oauth root key: %w", err)
	}
	if !ok {
		return keymgmt.RootKey{}, fmt.Errorf("read oauth root key: missing")
	}
	return root, nil
}

func encryptTokenStorePayload(plaintext []byte, material kryptograf.Material) ([]byte, error) {
	var ciphertext bytes.Buffer
	writer, err := kryptograf.New(keymgmt.RootKey{}).EncryptWriter(&ciphertext, material)
	if err != nil {
		return nil, fmt.Errorf("encrypt token store payload: %w", err)
	}
	if _, err := writer.Write(plaintext); err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("encrypt token store payload write: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("encrypt token store payload close: %w", err)
	}
	return ciphertext.Bytes(), nil
}

func decryptTokenStorePayload(ciphertext []byte, material kryptograf.Material) ([]byte, error) {
	reader, err := kryptograf.New(keymgmt.RootKey{}).DecryptReader(bytes.NewReader(ciphertext), material)
	if err != nil {
		return nil, fmt.Errorf("decrypt token store payload: %w", err)
	}
	defer reader.Close()
	plaintext, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("decrypt token store payload read: %w", err)
	}
	return plaintext, nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

type oauthErrorResponse struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description,omitempty"`
}

func writeOAuthJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeOAuthError(w http.ResponseWriter, status int, errCode, description string) {
	writeOAuthJSON(w, status, oauthErrorResponse{
		Error:            strings.TrimSpace(errCode),
		ErrorDescription: strings.TrimSpace(description),
	})
}

func writeOAuthErrorWithAuthenticate(w http.ResponseWriter, status int, errCode, description string) {
	w.Header().Set("WWW-Authenticate", `Basic realm="lockd-mcp"`)
	writeOAuthError(w, status, errCode, description)
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

func (m *Manager) verifyConfidentialClientLocked(r *http.Request) (*state.Client, string, bool) {
	clientID, clientSecret := clientCredentials(r)
	if clientID == "" || clientSecret == "" {
		return nil, "", false
	}
	client, ok := m.state.VerifyClientSecret(clientID, clientSecret)
	if !ok {
		return nil, "", false
	}
	return client, clientID, true
}

func (m *Manager) clientByID(clientID string) (state.Client, error) {
	target := strings.TrimSpace(clientID)
	if target == "" {
		return state.Client{}, fmt.Errorf("client id required")
	}
	clients, err := m.ListClients()
	if err != nil {
		return state.Client{}, err
	}
	for _, client := range clients {
		if client.ID == target {
			return client, nil
		}
	}
	return state.Client{}, fmt.Errorf("oauth client %s not found", target)
}

func parseResourceParam(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", nil
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed == nil || !parsed.IsAbs() {
		return "", fmt.Errorf("resource must be an absolute URL")
	}
	if strings.TrimSpace(parsed.Scheme) == "" || strings.TrimSpace(parsed.Host) == "" {
		return "", fmt.Errorf("resource must include scheme and host")
	}
	return value, nil
}

func redirectURIAllowed(allowList []string, candidate string) bool {
	trimmedCandidate := strings.TrimSpace(candidate)
	if trimmedCandidate == "" {
		return false
	}
	for _, allowed := range allowList {
		if strings.TrimSpace(allowed) == trimmedCandidate {
			return true
		}
	}
	return false
}

func verifyPKCE(verifier, expectedChallenge, method string) bool {
	if !strings.EqualFold(strings.TrimSpace(method), "S256") {
		return false
	}
	value := strings.TrimSpace(verifier)
	if len(value) < 43 || len(value) > 128 {
		return false
	}
	sum := sha256.Sum256([]byte(value))
	computed := base64.RawURLEncoding.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(strings.TrimSpace(expectedChallenge)), []byte(computed)) == 1
}

func normalizeGrantTypes(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func grantTypeContains(grantTypes []string, target string) bool {
	for _, grantType := range grantTypes {
		if strings.TrimSpace(grantType) == strings.TrimSpace(target) {
			return true
		}
	}
	return false
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

func defaultTokenStorePath(statePath string) string {
	dir := filepath.Dir(statePath)
	return filepath.Join(dir, tokenStoreFileName)
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
