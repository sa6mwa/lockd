package oauth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	mcpstate "pkt.systems/lockd/mcp/state"
)

func TestAuthorizationCodeRefreshTokenPersistsAcrossRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	refreshPath := filepath.Join(dir, "mcp-auth-store.json")

	boot, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
		InitialScopes:     []string{"read"},
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	mgr, err := NewManager(ManagerConfig{StatePath: statePath, RefreshStore: refreshPath})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	code := issueAuthCode(t, mgr, boot.ClientID, "https://client.example/callback", "read")
	first := exchangeAuthCode(t, mgr, boot.ClientID, boot.ClientSecret, code, "https://client.example/callback")
	if first.RefreshToken == "" {
		t.Fatalf("expected refresh token in authorization_code exchange")
	}

	mgr2, err := NewManager(ManagerConfig{StatePath: statePath, RefreshStore: refreshPath})
	if err != nil {
		t.Fatalf("new manager restart: %v", err)
	}
	refreshed := exchangeRefreshToken(t, mgr2, boot.ClientID, boot.ClientSecret, first.RefreshToken)
	if refreshed.AccessToken == "" {
		t.Fatalf("expected access token in refresh exchange")
	}
	if refreshed.RefreshToken == "" {
		t.Fatalf("expected rotated refresh token")
	}
}

func TestReloadIfChangedAppliesClientRevocationLive(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	refreshPath := filepath.Join(dir, "mcp-auth-store.json")

	boot, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	mgr, err := NewManager(ManagerConfig{StatePath: statePath, RefreshStore: refreshPath})
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

type tokenExchange struct {
	statusCode int
	body       string
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
	}
	return result
}

func issueAuthCode(t *testing.T, mgr *Manager, clientID, redirectURI, scope string) string {
	t.Helper()
	q := url.Values{}
	q.Set("response_type", "code")
	q.Set("client_id", clientID)
	q.Set("redirect_uri", redirectURI)
	q.Set("scope", scope)
	req := httptest.NewRequest(http.MethodGet, "/authorize?"+q.Encode(), nil)
	rr := httptest.NewRecorder()
	mgr.HandleAuthorize(rr, req)
	if rr.Code != http.StatusFound {
		t.Fatalf("authorize failed: status=%d body=%q", rr.Code, rr.Body.String())
	}
	loc := rr.Header().Get("Location")
	if loc == "" {
		t.Fatalf("missing redirect location")
	}
	u, err := url.Parse(loc)
	if err != nil {
		t.Fatalf("parse location: %v", err)
	}
	code := u.Query().Get("code")
	if code == "" {
		t.Fatalf("missing code in redirect: %s", loc)
	}
	return code
}

func exchangeAuthCode(t *testing.T, mgr *Manager, clientID, clientSecret, code, redirectURI string) TokenResponse {
	t.Helper()
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", redirectURI)
	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(clientID, clientSecret)
	rr := httptest.NewRecorder()
	mgr.HandleToken(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("token exchange failed: status=%d body=%q", rr.Code, rr.Body.String())
	}
	var out TokenResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode token response: %v", err)
	}
	return out
}

func exchangeRefreshToken(t *testing.T, mgr *Manager, clientID, clientSecret, refreshToken string) TokenResponse {
	t.Helper()
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refreshToken)
	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(clientID, clientSecret)
	rr := httptest.NewRecorder()
	mgr.HandleToken(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("refresh exchange failed: status=%d body=%q", rr.Code, rr.Body.String())
	}
	var out TokenResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode refresh response: %v", err)
	}
	return out
}
