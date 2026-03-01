package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd"
	mcpadmin "pkt.systems/lockd/mcp/admin"
	"pkt.systems/pslog"
)

type m2mAuthServerMetadata struct {
	Issuer                string   `json:"issuer"`
	AuthorizationEndpoint string   `json:"authorization_endpoint,omitempty"`
	TokenEndpoint         string   `json:"token_endpoint"`
	GrantTypesSupported   []string `json:"grant_types_supported,omitempty"`
}

type m2mOpenIDMetadata struct {
	Issuer        string `json:"issuer"`
	TokenEndpoint string `json:"token_endpoint"`
}

type m2mTokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

type m2mTokenError struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description,omitempty"`
}

type m2mComplianceCase struct {
	name    string
	baseURL func(listen string) string
	mcpPath string
}

func defaultM2MComplianceCases() []m2mComplianceCase {
	return []m2mComplianceCase{
		{
			name: "base-root-mcp-root",
			baseURL: func(listen string) string {
				return "https://" + listen
			},
			mcpPath: "/",
		},
		{
			name: "base-subpath-mcp-root",
			baseURL: func(listen string) string {
				return "https://" + listen + "/edge/mcp"
			},
			mcpPath: "/",
		},
		{
			name: "base-root-mcp-subpath",
			baseURL: func(listen string) string {
				return "https://" + listen
			},
			mcpPath: "/mcp",
		},
	}
}

func TestM2MDiscoveryCompliance(t *testing.T) {
	t.Parallel()

	for _, tc := range defaultM2MComplianceCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			httpClient, baseURL, _, _ := startM2MComplianceServer(t, tc.baseURL, tc.mcpPath)

			authMetaURL := authServerWellKnownURL(baseURL)
			authMeta := fetchJSON[m2mAuthServerMetadata](t, httpClient, authMetaURL)
			if authMeta.Issuer != baseURL {
				t.Fatalf("issuer=%q want %q", authMeta.Issuer, baseURL)
			}
			if authMeta.AuthorizationEndpoint != joinURL(baseURL, "/authorize") {
				t.Fatalf("authorization_endpoint=%q want %q", authMeta.AuthorizationEndpoint, joinURL(baseURL, "/authorize"))
			}
			if authMeta.TokenEndpoint != joinURL(baseURL, "/token") {
				t.Fatalf("token_endpoint=%q want %q", authMeta.TokenEndpoint, joinURL(baseURL, "/token"))
			}
			if !sliceContains(authMeta.GrantTypesSupported, "client_credentials") ||
				!sliceContains(authMeta.GrantTypesSupported, "authorization_code") ||
				!sliceContains(authMeta.GrantTypesSupported, "refresh_token") {
				t.Fatalf("grant_types_supported missing expected grants: %v", authMeta.GrantTypesSupported)
			}

			openidMetaURL := openIDWellKnownURL(baseURL)
			openidMeta := fetchJSON[m2mOpenIDMetadata](t, httpClient, openidMetaURL)
			if openidMeta.Issuer != authMeta.Issuer {
				t.Fatalf("openid issuer=%q want %q", openidMeta.Issuer, authMeta.Issuer)
			}
			if openidMeta.TokenEndpoint != authMeta.TokenEndpoint {
				t.Fatalf("openid token_endpoint=%q want %q", openidMeta.TokenEndpoint, authMeta.TokenEndpoint)
			}
		})
	}
}

func TestM2MOpenIDDiscoveryAliasCompliance(t *testing.T) {
	t.Parallel()

	for _, tc := range defaultM2MComplianceCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			httpClient, baseURL, _, _ := startM2MComplianceServer(t, tc.baseURL, tc.mcpPath)
			openidMetaURL := openIDWellKnownURL(baseURL)
			openidMeta := fetchJSON[m2mOpenIDMetadata](t, httpClient, openidMetaURL)
			if strings.TrimSpace(openidMeta.Issuer) == "" {
				t.Fatalf("openid issuer must be set")
			}
			if strings.TrimSpace(openidMeta.TokenEndpoint) == "" {
				t.Fatalf("openid token_endpoint must be set")
			}
		})
	}
}

func TestM2MTokenEndpointSuccessCompliance(t *testing.T) {
	t.Parallel()

	for _, tc := range defaultM2MComplianceCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			httpClient, baseURL, _, boot := startM2MComplianceServer(t, tc.baseURL, tc.mcpPath)
			authMeta := fetchJSON[m2mAuthServerMetadata](t, httpClient, authServerWellKnownURL(baseURL))

			tokenBody := postTokenForm(t, httpClient, authMeta.TokenEndpoint, boot.ClientID, boot.ClientSecret, url.Values{
				"grant_type": []string{"client_credentials"},
				"scope":      []string{"read"},
			})
			expectHeaderContains(t, tokenBody.header, "Content-Type", "application/json")
			expectHeaderContains(t, tokenBody.header, "Cache-Control", "no-store")
			expectHeaderContains(t, tokenBody.header, "Pragma", "no-cache")
			if tokenBody.status != http.StatusOK {
				t.Fatalf("token status=%d body=%q", tokenBody.status, string(tokenBody.body))
			}
			var tokenOK m2mTokenResponse
			if err := json.Unmarshal(tokenBody.body, &tokenOK); err != nil {
				t.Fatalf("decode token success: %v body=%q", err, string(tokenBody.body))
			}
			if strings.TrimSpace(tokenOK.AccessToken) == "" {
				t.Fatalf("token response missing access_token")
			}
			if tokenOK.TokenType != "Bearer" {
				t.Fatalf("token_type=%q want Bearer", tokenOK.TokenType)
			}
			if tokenOK.ExpiresIn <= 0 {
				t.Fatalf("expires_in=%d want >0", tokenOK.ExpiresIn)
			}
			if strings.TrimSpace(tokenOK.RefreshToken) != "" {
				t.Fatalf("client_credentials token must not include refresh_token, got %q", tokenOK.RefreshToken)
			}
		})
	}
}

func TestM2MTokenEndpointErrorCompliance(t *testing.T) {
	t.Parallel()

	for _, tc := range defaultM2MComplianceCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			httpClient, baseURL, _, boot := startM2MComplianceServer(t, tc.baseURL, tc.mcpPath)
			authMeta := fetchJSON[m2mAuthServerMetadata](t, httpClient, authServerWellKnownURL(baseURL))

			unsupportedBody := postTokenForm(t, httpClient, authMeta.TokenEndpoint, boot.ClientID, boot.ClientSecret, url.Values{
				"grant_type": []string{"urn:example:custom_grant"},
			})
			if unsupportedBody.status != http.StatusBadRequest {
				t.Fatalf("unsupported grant status=%d body=%q", unsupportedBody.status, string(unsupportedBody.body))
			}
			expectHeaderContains(t, unsupportedBody.header, "Content-Type", "application/json")
			expectHeaderContains(t, unsupportedBody.header, "Cache-Control", "no-store")
			expectHeaderContains(t, unsupportedBody.header, "Pragma", "no-cache")
			assertOAuthError(t, unsupportedBody.body, "unsupported_grant_type")

			badClientBody := postTokenForm(t, httpClient, authMeta.TokenEndpoint, boot.ClientID, "bad-secret", url.Values{
				"grant_type": []string{"client_credentials"},
			})
			if badClientBody.status != http.StatusUnauthorized {
				t.Fatalf("invalid client status=%d body=%q", badClientBody.status, string(badClientBody.body))
			}
			expectHeaderContains(t, badClientBody.header, "Content-Type", "application/json")
			expectHeaderContains(t, badClientBody.header, "Cache-Control", "no-store")
			expectHeaderContains(t, badClientBody.header, "Pragma", "no-cache")
			assertOAuthError(t, badClientBody.body, "invalid_client")
		})
	}
}

func TestM2MUnauthorizedChallengeCompliance(t *testing.T) {
	t.Parallel()

	for _, tc := range defaultM2MComplianceCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			httpClient, baseURL, protectedURL, _ := startM2MComplianceServer(t, tc.baseURL, tc.mcpPath)
			authMeta := fetchJSON[m2mAuthServerMetadata](t, httpClient, authServerWellKnownURL(baseURL))
			challengeHeader := unauthenticatedChallenge(t, httpClient, protectedURL)
			resourceMetadataURL := parseResourceMetadataURL(t, challengeHeader)
			if !strings.HasPrefix(resourceMetadataURL, "https://") {
				t.Fatalf("resource_metadata must be absolute https URL, got %q", resourceMetadataURL)
			}
			protectedMeta := fetchJSON[oauthProtectedResourceMetadata](t, httpClient, resourceMetadataURL)
			if len(protectedMeta.AuthorizationServers) != 1 || protectedMeta.AuthorizationServers[0] != authMeta.Issuer {
				t.Fatalf("authorization_servers=%v want [%q]", protectedMeta.AuthorizationServers, authMeta.Issuer)
			}
		})
	}
}

type tokenHTTPResponse struct {
	status int
	body   []byte
	header http.Header
}

func postTokenForm(t testing.TB, httpClient *http.Client, tokenURL, clientID, clientSecret string, form url.Values) tokenHTTPResponse {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("build token request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if strings.TrimSpace(clientID) != "" || strings.TrimSpace(clientSecret) != "" {
		req.SetBasicAuth(clientID, clientSecret)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("call token endpoint: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read token response: %v", err)
	}
	return tokenHTTPResponse{
		status: resp.StatusCode,
		body:   body,
		header: resp.Header.Clone(),
	}
}

func startM2MComplianceServer(t testing.TB, baseURLFn func(string) string, mcpPath string) (*http.Client, string, string, mcpadmin.BootstrapResponse) {
	t.Helper()
	upstream := lockd.StartTestServer(t, lockd.WithoutTestMTLS())
	listen := reserveLoopbackAddr(t)
	baseURL := baseURLFn(listen)
	tmp := t.TempDir()

	caBundle, err := lockd.CreateCABundle(lockd.CreateCABundleRequest{})
	if err != nil {
		t.Fatalf("create ca bundle: %v", err)
	}
	serverBundle, err := lockd.CreateServerBundle(lockd.CreateServerBundleRequest{
		CABundlePEM: caBundle,
		Hosts:       []string{"127.0.0.1", "localhost"},
	})
	if err != nil {
		t.Fatalf("create server bundle: %v", err)
	}
	serverBundlePath := filepath.Join(tmp, "mcp-server.pem")
	if err := os.WriteFile(serverBundlePath, serverBundle, 0o600); err != nil {
		t.Fatalf("write mcp server bundle: %v", err)
	}
	httpClient := trustedHTTPClientFromServerBundle(t, serverBundlePath)

	statePath := filepath.Join(tmp, "mcp.pem")
	tokenStorePath := filepath.Join(tmp, "mcp-token-store.enc.json")
	admin := mcpadmin.New(mcpadmin.Config{
		StatePath:  statePath,
		TokenStore: tokenStorePath,
	})
	boot, err := admin.Bootstrap(mcpadmin.BootstrapRequest{
		Path:              statePath,
		Issuer:            baseURL,
		InitialClientName: "m2m-client",
		InitialScopes:     []string{"read"},
	})
	if err != nil {
		t.Fatalf("bootstrap oauth state: %v", err)
	}

	srv, err := NewServer(NewServerRequest{
		Config: Config{
			Listen:              listen,
			DisableTLS:          false,
			BaseURL:             baseURL,
			BundlePath:          serverBundlePath,
			UpstreamServer:      upstream.URL(),
			UpstreamDisableMTLS: true,
			OAuthStatePath:      statePath,
			OAuthTokenStorePath: tokenStorePath,
			MCPPath:             mcpPath,
		},
		Logger: pslog.NewStructured(context.Background(), io.Discard),
	})
	if err != nil {
		t.Fatalf("new mcp server: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- srv.Run(runCtx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !strings.Contains(err.Error(), "context canceled") {
				t.Fatalf("mcp run returned error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for mcp shutdown")
		}
	})

	waitForHTTPStatusOK(t, httpClient, authServerWellKnownURL(baseURL))

	protectedPath := scopedHTTPPath(urlPathPrefix(baseURL), mcpPath)
	protectedURL := "https://" + listen + protectedPath
	return httpClient, baseURL, protectedURL, boot
}

func fetchJSON[T any](t testing.TB, httpClient *http.Client, targetURL string) T {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, targetURL, http.NoBody)
	if err != nil {
		t.Fatalf("build request for %s: %v", targetURL, err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("call %s: %v", targetURL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s response body: %v", targetURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status from %s=%d body=%q", targetURL, resp.StatusCode, string(body))
	}
	var out T
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode %s response: %v body=%q", targetURL, err, string(body))
	}
	return out
}

func unauthenticatedChallenge(t testing.TB, httpClient *http.Client, targetURL string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, targetURL, http.NoBody)
	if err != nil {
		t.Fatalf("build unauthenticated request: %v", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("call protected endpoint unauthenticated: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d want %d", resp.StatusCode, http.StatusUnauthorized)
	}
	challenge := strings.TrimSpace(resp.Header.Get("WWW-Authenticate"))
	if !strings.HasPrefix(challenge, "Bearer ") {
		t.Fatalf("WWW-Authenticate=%q must start with Bearer", challenge)
	}
	return challenge
}

func assertOAuthError(t testing.TB, body []byte, wantError string) {
	t.Helper()
	var out m2mTokenError
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode oauth error: %v body=%q", err, string(body))
	}
	if out.Error != wantError {
		t.Fatalf("oauth error=%q want %q body=%q", out.Error, wantError, string(body))
	}
}

func parseResourceMetadataURL(t testing.TB, challenge string) string {
	t.Helper()
	re := regexp.MustCompile(`resource_metadata=(?:"([^"]+)"|([^,\s]+))`)
	match := re.FindStringSubmatch(challenge)
	if len(match) != 3 {
		t.Fatalf("resource_metadata missing from challenge=%q", challenge)
	}
	value := strings.TrimSpace(match[1])
	if value == "" {
		value = strings.TrimSpace(match[2])
	}
	return value
}

func expectHeaderContains(t testing.TB, h http.Header, key, wantPart string) {
	t.Helper()
	value := strings.TrimSpace(h.Get(key))
	if !strings.Contains(strings.ToLower(value), strings.ToLower(wantPart)) {
		t.Fatalf("header %s=%q must contain %q", key, value, wantPart)
	}
}

func authServerWellKnownURL(baseURL string) string {
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return ""
	}
	p := path.Join(cleanHTTPPath(base.Path), ".well-known", "oauth-authorization-server")
	return fmt.Sprintf("%s://%s%s", base.Scheme, base.Host, p)
}

func openIDWellKnownURL(baseURL string) string {
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return ""
	}
	p := path.Join(cleanHTTPPath(base.Path), ".well-known", "openid-configuration")
	return fmt.Sprintf("%s://%s%s", base.Scheme, base.Host, p)
}

func sliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
