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

func FuzzHandleTokenNoServerError(f *testing.F) {
	dir := f.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	boot, err := mcpstate.Bootstrap(mcpstate.BootstrapRequest{
		Path:              statePath,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
		InitialScopes:     []string{"read", "write"},
	})
	if err != nil {
		f.Fatalf("bootstrap: %v", err)
	}
	mgr, err := NewManager(ManagerConfig{StatePath: statePath, TokenStore: tokenStorePath})
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
