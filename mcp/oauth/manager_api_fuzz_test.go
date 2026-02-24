package oauth

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	mcpstate "pkt.systems/lockd/mcp/state"
)

func FuzzHandleAuthorizeNoServerError(f *testing.F) {
	dir := f.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	refreshPath := filepath.Join(dir, "mcp-auth-store.json")
	boot, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
		InitialScopes:     []string{"read", "write"},
	})
	if err != nil {
		f.Fatalf("bootstrap: %v", err)
	}
	mgr, err := NewManager(ManagerConfig{StatePath: statePath, RefreshStore: refreshPath})
	if err != nil {
		f.Fatalf("new manager: %v", err)
	}

	f.Add(uint8(0), "code", boot.ClientID, "https://client.example/callback", "read", "state-1")
	f.Add(uint8(1), "token", boot.ClientID, "https://client.example/callback", "read write", "state-2")
	f.Add(uint8(2), "code", "missing-client", "not-a-uri", "unknown", "")

	f.Fuzz(func(t *testing.T, methodSel uint8, responseType, clientID, redirectURI, scope, state string) {
		methods := []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete}
		method := methods[int(methodSel)%len(methods)]

		q := url.Values{}
		q.Set("response_type", fuzzClampString(responseType, 256))
		q.Set("client_id", fuzzClampString(clientID, 256))
		q.Set("redirect_uri", fuzzClampString(redirectURI, 512))
		q.Set("scope", fuzzClampString(scope, 512))
		q.Set("state", fuzzClampString(state, 256))

		req := httptest.NewRequest(method, "/authorize?"+q.Encode(), nil)
		rr := httptest.NewRecorder()
		mgr.HandleAuthorize(rr, req)
		if rr.Code >= http.StatusInternalServerError {
			t.Fatalf("unexpected server error status=%d body=%q", rr.Code, rr.Body.String())
		}
	})
}

func FuzzHandleTokenNoServerError(f *testing.F) {
	dir := f.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	refreshPath := filepath.Join(dir, "mcp-auth-store.json")
	boot, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
		InitialScopes:     []string{"read", "write"},
	})
	if err != nil {
		f.Fatalf("bootstrap: %v", err)
	}
	mgr, err := NewManager(ManagerConfig{StatePath: statePath, RefreshStore: refreshPath})
	if err != nil {
		f.Fatalf("new manager: %v", err)
	}

	f.Add(uint8(0), "client_credentials", "", "https://client.example/callback", "", "read", boot.ClientID, boot.ClientSecret, "application/x-www-form-urlencoded")
	f.Add(uint8(1), "authorization_code", "bad-code", "https://client.example/callback", "", "read", boot.ClientID, boot.ClientSecret, "application/x-www-form-urlencoded")
	f.Add(uint8(2), "refresh_token", "", "", "bad-refresh", "", "bad-user", "bad-pass", "application/x-www-form-urlencoded")
	f.Add(uint8(3), "", "", "", "", "", "", "", "text/plain")

	f.Fuzz(func(t *testing.T, methodSel uint8, grantType, code, redirectURI, refreshToken, scope, user, pass, contentType string) {
		methods := []string{http.MethodPost, http.MethodGet, http.MethodPut, http.MethodDelete}
		method := methods[int(methodSel)%len(methods)]

		form := url.Values{}
		form.Set("grant_type", fuzzClampString(grantType, 128))
		form.Set("code", fuzzClampString(code, 512))
		form.Set("redirect_uri", fuzzClampString(redirectURI, 512))
		form.Set("refresh_token", fuzzClampString(refreshToken, 512))
		form.Set("scope", fuzzClampString(scope, 512))

		req := httptest.NewRequest(method, "/token", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", fuzzClampString(contentType, 128))
		user = fuzzClampString(user, 256)
		pass = fuzzClampString(pass, 256)
		if user != "" || pass != "" {
			req.SetBasicAuth(user, pass)
		}

		rr := httptest.NewRecorder()
		mgr.HandleToken(rr, req)
		if rr.Code >= http.StatusInternalServerError {
			t.Fatalf("unexpected server error status=%d body=%q", rr.Code, rr.Body.String())
		}
	})
}

func fuzzClampString(in string, max int) string {
	if max <= 0 || len(in) <= max {
		return in
	}
	return in[:max]
}
