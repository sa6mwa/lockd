package oauth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	mcpstate "pkt.systems/lockd/mcp/state"
)

func TestUnsupportedGrantRejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")

	boot, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
		InitialScopes:     []string{"read"},
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	mgr, err := NewManager(ManagerConfig{StatePath: statePath, TokenStore: tokenStorePath})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	unsupported := issueTokenGrantType(t, mgr, boot.ClientID, boot.ClientSecret, "urn:example:custom_grant")
	if unsupported.statusCode != http.StatusBadRequest {
		t.Fatalf("unsupported grant status=%d body=%q", unsupported.statusCode, unsupported.body)
	}
	if unsupported.errorCode != "unsupported_grant_type" {
		t.Fatalf("unsupported grant error=%q want unsupported_grant_type body=%q", unsupported.errorCode, unsupported.body)
	}
}

func TestReloadIfChangedAppliesClientRevocationLive(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")

	boot, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	mgr, err := NewManager(ManagerConfig{StatePath: statePath, TokenStore: tokenStorePath})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	if status := issueClientCredentials(t, mgr, boot.ClientID, boot.ClientSecret).statusCode; status != http.StatusOK {
		t.Fatalf("expected first token request to succeed, got %d", status)
	}

	loaded, err := mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if err := loaded.RevokeClient(boot.ClientID, true, loaded.UpdatedAt); err != nil {
		t.Fatalf("revoke client: %v", err)
	}
	if err := mcpstate.Save(statePath, loaded); err != nil {
		t.Fatalf("save state: %v", err)
	}

	result := issueClientCredentials(t, mgr, boot.ClientID, boot.ClientSecret)
	if result.statusCode != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized after live reload, got %d body=%q", result.statusCode, result.body)
	}
}

func TestVerifyTokenIncludesClientNamespaceExtra(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")

	boot, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	mgr, err := NewManager(ManagerConfig{StatePath: statePath, TokenStore: tokenStorePath})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	namespace := "agents"
	if err := mgr.UpdateClient(UpdateClientRequest{
		ClientID:  boot.ClientID,
		Namespace: &namespace,
	}); err != nil {
		t.Fatalf("update client namespace: %v", err)
	}

	issued := issueClientCredentials(t, mgr, boot.ClientID, boot.ClientSecret)
	if issued.statusCode != http.StatusOK {
		t.Fatalf("token status=%d body=%q", issued.statusCode, issued.body)
	}
	info, err := mgr.VerifyToken(context.Background(), issued.token.AccessToken, nil)
	if err != nil {
		t.Fatalf("verify token: %v", err)
	}
	if got := anyString(info.Extra[TokenInfoExtraDefaultNamespace]); got != "agents" {
		t.Fatalf("token extra namespace=%q want %q", got, "agents")
	}
}

func TestUpdateClientNamespaceValidation(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")

	boot, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	mgr, err := NewManager(ManagerConfig{StatePath: statePath, TokenStore: tokenStorePath})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	invalid := "bad/namespace"
	if err := mgr.UpdateClient(UpdateClientRequest{
		ClientID:  boot.ClientID,
		Namespace: &invalid,
	}); err == nil {
		t.Fatalf("expected invalid namespace update to fail")
	}
	valid := "teamx"
	if err := mgr.UpdateClient(UpdateClientRequest{
		ClientID:  boot.ClientID,
		Namespace: &valid,
	}); err != nil {
		t.Fatalf("update valid namespace: %v", err)
	}
	loaded, err := mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if got := loaded.Clients[boot.ClientID].Namespace; got != "teamx" {
		t.Fatalf("namespace=%q want %q", got, "teamx")
	}
	clear := ""
	if err := mgr.UpdateClient(UpdateClientRequest{
		ClientID:  boot.ClientID,
		Namespace: &clear,
	}); err != nil {
		t.Fatalf("clear namespace: %v", err)
	}
	loaded, err = mcpstate.Load(statePath)
	if err != nil {
		t.Fatalf("load state after clear: %v", err)
	}
	if got := loaded.Clients[boot.ClientID].Namespace; got != "" {
		t.Fatalf("namespace=%q want empty", got)
	}
}

func TestClientNamespaceAndEnsureActive(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")

	boot, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	mgr, err := NewManager(ManagerConfig{StatePath: statePath, TokenStore: tokenStorePath})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	namespace := "agents"
	if err := mgr.UpdateClient(UpdateClientRequest{
		ClientID:  boot.ClientID,
		Namespace: &namespace,
	}); err != nil {
		t.Fatalf("update namespace: %v", err)
	}
	gotNamespace, err := mgr.ClientNamespace(boot.ClientID)
	if err != nil {
		t.Fatalf("client namespace: %v", err)
	}
	if gotNamespace != "agents" {
		t.Fatalf("namespace=%q want %q", gotNamespace, "agents")
	}
	if err := mgr.EnsureClientActive(boot.ClientID); err != nil {
		t.Fatalf("ensure active: %v", err)
	}

	if err := mgr.RevokeClient(boot.ClientID, true); err != nil {
		t.Fatalf("revoke client: %v", err)
	}
	if _, err := mgr.ClientNamespace(boot.ClientID); !errors.Is(err, ErrClientInactive) {
		t.Fatalf("expected ErrClientInactive from ClientNamespace after revoke, got %v", err)
	}
	if err := mgr.EnsureClientActive(boot.ClientID); !errors.Is(err, ErrClientInactive) {
		t.Fatalf("expected ErrClientInactive from EnsureClientActive after revoke, got %v", err)
	}
}

type tokenExchange struct {
	statusCode int
	body       string
	errorCode  string
	token      TokenResponse
}

func issueClientCredentials(t *testing.T, mgr *Manager, clientID, clientSecret string) tokenExchange {
	t.Helper()
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(clientID, clientSecret)
	rr := httptest.NewRecorder()
	mgr.HandleToken(rr, req)
	result := tokenExchange{statusCode: rr.Code, body: rr.Body.String()}
	if rr.Code == http.StatusOK {
		if err := json.Unmarshal(rr.Body.Bytes(), &result.token); err != nil {
			t.Fatalf("decode token response: %v", err)
		}
	} else {
		var payload map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &payload); err == nil {
			if value, ok := payload["error"].(string); ok {
				result.errorCode = strings.TrimSpace(value)
			}
		}
	}
	return result
}

func TestAuthorizationCodePKCEAndRefreshFlow(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	redirectURI := "https://chat.openai.com/aip/callback"

	boot, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
		InitialScopes:     []string{"read"},
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	mgr, err := NewManager(ManagerConfig{StatePath: statePath, TokenStore: tokenStorePath})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := mgr.UpdateClient(UpdateClientRequest{
		ClientID:     boot.ClientID,
		RedirectURIs: []string{redirectURI},
	}); err != nil {
		t.Fatalf("update client redirect uris: %v", err)
	}

	verifier := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~kPCEproof"
	challenge := pkceS256Challenge(verifier)
	authReq := httptest.NewRequest(http.MethodGet, "/authorize", nil)
	q := authReq.URL.Query()
	q.Set("response_type", "code")
	q.Set("client_id", boot.ClientID)
	q.Set("redirect_uri", redirectURI)
	q.Set("code_challenge", challenge)
	q.Set("code_challenge_method", "S256")
	q.Set("scope", "read")
	q.Set("state", "opaque-state")
	authReq.URL.RawQuery = q.Encode()

	authRec := httptest.NewRecorder()
	mgr.HandleAuthorize(authRec, authReq)
	if authRec.Code != http.StatusFound {
		t.Fatalf("authorize status=%d body=%q", authRec.Code, authRec.Body.String())
	}
	location := authRec.Header().Get("Location")
	if strings.TrimSpace(location) == "" {
		t.Fatalf("authorize response missing redirect location")
	}
	redirectParsed, err := url.Parse(location)
	if err != nil {
		t.Fatalf("parse authorize redirect location: %v", err)
	}
	code := strings.TrimSpace(redirectParsed.Query().Get("code"))
	if code == "" {
		t.Fatalf("authorize redirect missing code: %q", location)
	}
	if gotState := strings.TrimSpace(redirectParsed.Query().Get("state")); gotState != "opaque-state" {
		t.Fatalf("authorize redirect state=%q want opaque-state", gotState)
	}

	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", redirectURI)
	form.Set("code_verifier", verifier)
	tokenReq := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	tokenReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokenReq.SetBasicAuth(boot.ClientID, boot.ClientSecret)
	tokenRec := httptest.NewRecorder()
	mgr.HandleToken(tokenRec, tokenReq)
	if tokenRec.Code != http.StatusOK {
		t.Fatalf("authorization_code token status=%d body=%q", tokenRec.Code, tokenRec.Body.String())
	}
	var authCodeToken TokenResponse
	if err := json.Unmarshal(tokenRec.Body.Bytes(), &authCodeToken); err != nil {
		t.Fatalf("decode authorization_code token response: %v", err)
	}
	if strings.TrimSpace(authCodeToken.AccessToken) == "" {
		t.Fatalf("authorization_code token missing access_token")
	}
	if strings.TrimSpace(authCodeToken.RefreshToken) == "" {
		t.Fatalf("authorization_code token missing refresh_token")
	}

	refreshForm := url.Values{}
	refreshForm.Set("grant_type", "refresh_token")
	refreshForm.Set("refresh_token", authCodeToken.RefreshToken)
	refreshReq := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(refreshForm.Encode()))
	refreshReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	refreshReq.SetBasicAuth(boot.ClientID, boot.ClientSecret)
	refreshRec := httptest.NewRecorder()
	mgr.HandleToken(refreshRec, refreshReq)
	if refreshRec.Code != http.StatusOK {
		t.Fatalf("refresh_token status=%d body=%q", refreshRec.Code, refreshRec.Body.String())
	}
	var refreshed TokenResponse
	if err := json.Unmarshal(refreshRec.Body.Bytes(), &refreshed); err != nil {
		t.Fatalf("decode refresh token response: %v", err)
	}
	if strings.TrimSpace(refreshed.AccessToken) == "" {
		t.Fatalf("refresh_token response missing access_token")
	}
	if strings.TrimSpace(refreshed.RefreshToken) == "" {
		t.Fatalf("refresh_token response missing rotated refresh_token")
	}
}

func TestDynamicClientRegistrationCreatesConfidentialClient(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")

	if _, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	mgr, err := NewManager(ManagerConfig{StatePath: statePath, TokenStore: tokenStorePath})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	body := `{"client_name":"chatgpt","redirect_uris":["https://chat.openai.com/aip/callback"],"grant_types":["authorization_code","refresh_token","client_credentials"],"response_types":["code"],"scope":"read","token_endpoint_auth_method":"client_secret_basic"}`
	req := httptest.NewRequest(http.MethodPost, "/register", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mgr.HandleRegister(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("register status=%d body=%q", rec.Code, rec.Body.String())
	}

	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode register response: %v", err)
	}
	if strings.TrimSpace(anyString(out["client_id"])) == "" {
		t.Fatalf("register response missing client_id")
	}
	if strings.TrimSpace(anyString(out["client_secret"])) == "" {
		t.Fatalf("register response missing client_secret")
	}
}

func issueTokenGrantType(t *testing.T, mgr *Manager, clientID, clientSecret, grantType string) tokenExchange {
	t.Helper()
	form := url.Values{}
	form.Set("grant_type", grantType)
	if grantType == "refresh_token" {
		form.Set("refresh_token", "dummy")
	}
	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(clientID, clientSecret)
	rr := httptest.NewRecorder()
	mgr.HandleToken(rr, req)
	result := tokenExchange{statusCode: rr.Code, body: rr.Body.String()}
	var payload map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err == nil {
		if value, ok := payload["error"].(string); ok {
			result.errorCode = strings.TrimSpace(value)
		}
	}
	return result
}

func TestHandleAuthServerMetadataRFC8414(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	issuer := "https://public.example/mcp"

	_, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            issuer,
		InitialClientName: "default",
		InitialScopes:     []string{"read", "write"},
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	mgr, err := NewManager(ManagerConfig{StatePath: statePath, TokenStore: tokenStorePath})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/.well-known/oauth-authorization-server", nil)
	rr := httptest.NewRecorder()
	mgr.HandleAuthServerMetadata(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("metadata status=%d body=%q", rr.Code, rr.Body.String())
	}

	var meta AuthServerMetadata
	if err := json.Unmarshal(rr.Body.Bytes(), &meta); err != nil {
		t.Fatalf("decode metadata response: %v", err)
	}
	if meta.Issuer != issuer {
		t.Fatalf("issuer=%q want %q", meta.Issuer, issuer)
	}
	if meta.AuthorizationEndpoint != joinURL(issuer, "/authorize") {
		t.Fatalf("authorization_endpoint=%q want %q", meta.AuthorizationEndpoint, joinURL(issuer, "/authorize"))
	}
	if meta.TokenEndpoint != joinURL(issuer, "/token") {
		t.Fatalf("token_endpoint=%q want %q", meta.TokenEndpoint, joinURL(issuer, "/token"))
	}
	if meta.RegistrationEndpoint != joinURL(issuer, "/register") {
		t.Fatalf("registration_endpoint=%q want %q", meta.RegistrationEndpoint, joinURL(issuer, "/register"))
	}
	if !containsString(meta.ResponseTypesSupported, "code") {
		t.Fatalf("response_types_supported=%v missing code", meta.ResponseTypesSupported)
	}
	if !containsString(meta.GrantTypesSupported, "client_credentials") ||
		!containsString(meta.GrantTypesSupported, "authorization_code") ||
		!containsString(meta.GrantTypesSupported, "refresh_token") {
		t.Fatalf("grant_types_supported=%v missing expected grants", meta.GrantTypesSupported)
	}
	if !containsString(meta.TokenEndpointAuthMethodsSupported, "client_secret_basic") || !containsString(meta.TokenEndpointAuthMethodsSupported, "client_secret_post") {
		t.Fatalf("token_endpoint_auth_methods_supported missing required methods: %v", meta.TokenEndpointAuthMethodsSupported)
	}
	if !containsString(meta.CodeChallengeMethodsSupported, "S256") {
		t.Fatalf("code_challenge_methods_supported=%v missing S256", meta.CodeChallengeMethodsSupported)
	}
}

func TestProtectedResourceMetadataDefaultsAndOverride(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	issuer := "https://public.example/mcp"

	_, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            issuer,
		InitialClientName: "default",
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	mgr, err := NewManager(ManagerConfig{StatePath: statePath, TokenStore: tokenStorePath})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	meta, err := mgr.ProtectedResourceMetadata("")
	if err != nil {
		t.Fatalf("protected resource metadata default: %v", err)
	}
	if meta.Resource != joinURL(issuer, "/mcp") {
		t.Fatalf("resource=%q want %q", meta.Resource, joinURL(issuer, "/mcp"))
	}
	if len(meta.AuthorizationServers) != 1 || meta.AuthorizationServers[0] != issuer {
		t.Fatalf("authorization_servers=%v want [%q]", meta.AuthorizationServers, issuer)
	}
	if !containsString(meta.BearerMethodsSupported, "header") {
		t.Fatalf("bearer_methods_supported missing header: %v", meta.BearerMethodsSupported)
	}

	explicit := "https://api.example/ctx/mcp"
	meta, err = mgr.ProtectedResourceMetadata(explicit)
	if err != nil {
		t.Fatalf("protected resource metadata override: %v", err)
	}
	if meta.Resource != explicit {
		t.Fatalf("resource override=%q want %q", meta.Resource, explicit)
	}
}

func TestAccessTokenSurvivesManagerRestart(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")

	boot, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
		InitialScopes:     []string{"read"},
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	mgr, err := NewManager(ManagerConfig{StatePath: statePath, TokenStore: tokenStorePath})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	issued := issueClientCredentials(t, mgr, boot.ClientID, boot.ClientSecret)
	if issued.statusCode != http.StatusOK {
		t.Fatalf("issue token status=%d body=%q", issued.statusCode, issued.body)
	}

	restarted, err := NewManager(ManagerConfig{StatePath: statePath, TokenStore: tokenStorePath})
	if err != nil {
		t.Fatalf("new manager after restart: %v", err)
	}
	info, err := restarted.VerifyToken(context.Background(), issued.token.AccessToken, nil)
	if err != nil {
		t.Fatalf("verify token after restart: %v", err)
	}
	if info == nil {
		t.Fatalf("verify token after restart returned nil info")
	}
	if info.UserID != boot.ClientID {
		t.Fatalf("token user_id=%q want %q", info.UserID, boot.ClientID)
	}
}

func TestTokenStoreIsEncryptedEnvelope(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")

	boot, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
		InitialScopes:     []string{"read"},
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	mgr, err := NewManager(ManagerConfig{StatePath: statePath, TokenStore: tokenStorePath})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	issued := issueClientCredentials(t, mgr, boot.ClientID, boot.ClientSecret)
	if issued.statusCode != http.StatusOK {
		t.Fatalf("issue token status=%d body=%q", issued.statusCode, issued.body)
	}

	raw, err := os.ReadFile(tokenStorePath)
	if err != nil {
		t.Fatalf("read token store: %v", err)
	}
	storeText := string(raw)
	if strings.Contains(storeText, issued.token.AccessToken) {
		t.Fatalf("token store leaked access token plaintext")
	}
	if strings.Contains(storeText, boot.ClientID) {
		t.Fatalf("token store leaked client_id plaintext")
	}

	var envelope tokenStoreFile
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("decode token store envelope: %v", err)
	}
	if envelope.Version != defaultTokenStoreVersion {
		t.Fatalf("token store version=%d want %d", envelope.Version, defaultTokenStoreVersion)
	}
	if strings.TrimSpace(envelope.Descriptor) == "" {
		t.Fatalf("token store descriptor missing")
	}
	if strings.TrimSpace(envelope.Ciphertext) == "" {
		t.Fatalf("token store ciphertext missing")
	}
	if _, err := base64.StdEncoding.DecodeString(envelope.Ciphertext); err != nil {
		t.Fatalf("token store ciphertext is not base64: %v", err)
	}

	var envelopeMap map[string]any
	if err := json.Unmarshal(raw, &envelopeMap); err != nil {
		t.Fatalf("decode token store envelope map: %v", err)
	}
	if _, ok := envelopeMap["tokens"]; ok {
		t.Fatalf("token store must not expose plaintext token payload")
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func pkceS256Challenge(verifier string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(verifier)))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func anyString(value any) string {
	out, _ := value.(string)
	return strings.TrimSpace(out)
}
