package mcp

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"pkt.systems/lockd"
	mcpadmin "pkt.systems/lockd/mcp/admin"
	presetcfg "pkt.systems/lockd/mcp/preset"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"
)

type oauthAuthorizationServerMetadata struct {
	Issuer                string   `json:"issuer"`
	AuthorizationEndpoint string   `json:"authorization_endpoint"`
	TokenEndpoint         string   `json:"token_endpoint"`
	RegistrationEndpoint  string   `json:"registration_endpoint"`
	GrantTypesSupported   []string `json:"grant_types_supported"`
}

type oauthProtectedResourceMetadata struct {
	Resource             string   `json:"resource"`
	AuthorizationServers []string `json:"authorization_servers"`
}

func TestOAuthClientCredentialsFlowWithBaseURLVariants(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		path    string
		mcpPath string
	}{
		{
			name:    "root-base-url",
			path:    "",
			mcpPath: "/",
		},
		{
			name:    "base-path-scoped-everything",
			path:    "/edge/mcp",
			mcpPath: "/",
		},
		{
			name:    "base-root-with-mcp-subpath",
			path:    "",
			mcpPath: "/mcp",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			upstream := lockd.StartTestServer(t, lockd.WithoutTestMTLS())
			listener := reserveLoopbackListener(t)
			listen := listener.Addr().String()
			baseURL := "https://" + listen + tc.path
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
				InitialClientName: "e2e-client",
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
					MCPPath:             tc.mcpPath,
				},
				Logger:   pslog.NewStructured(context.Background(), io.Discard),
				Listener: listener,
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

			expectedResourceURL := joinURL(baseURL, tc.mcpPath)
			protectedMetadataURL := wellKnownURLForScopedResource(baseURL, "oauth-protected-resource", expectedResourceURL)
			waitForHTTPStatusOK(t, httpClient, protectedMetadataURL)

			protected := fetchProtectedResourceMetadata(t, httpClient, protectedMetadataURL)
			if protected.Resource != expectedResourceURL {
				t.Fatalf("protected resource=%q want %q", protected.Resource, expectedResourceURL)
			}
			if len(protected.AuthorizationServers) == 0 || strings.TrimSpace(protected.AuthorizationServers[0]) == "" {
				t.Fatalf("authorization_servers missing in protected metadata: %+v", protected)
			}
			if protected.AuthorizationServers[0] != baseURL {
				t.Fatalf("authorization_server=%q want %q", protected.AuthorizationServers[0], baseURL)
			}

			authMetadataURL := wellKnownURLForScopedResource(baseURL, "oauth-authorization-server", protected.AuthorizationServers[0])
			authMeta := fetchAuthServerMetadata(t, httpClient, authMetadataURL)
			if authMeta.Issuer != baseURL {
				t.Fatalf("issuer=%q want %q", authMeta.Issuer, baseURL)
			}
			if authMeta.AuthorizationEndpoint != joinURL(baseURL, "/authorize") {
				t.Fatalf("authorization_endpoint=%q want %q", authMeta.AuthorizationEndpoint, joinURL(baseURL, "/authorize"))
			}
			if authMeta.RegistrationEndpoint != joinURL(baseURL, "/register") {
				t.Fatalf("registration_endpoint=%q want %q", authMeta.RegistrationEndpoint, joinURL(baseURL, "/register"))
			}
			if authMeta.TokenEndpoint != joinURL(baseURL, "/token") {
				t.Fatalf("token_endpoint=%q want %q", authMeta.TokenEndpoint, joinURL(baseURL, "/token"))
			}

			tokenCfg := clientcredentials.Config{
				ClientID:     boot.ClientID,
				ClientSecret: boot.ClientSecret,
				TokenURL:     authMeta.TokenEndpoint,
				Scopes:       []string{"read"},
			}
			tokenCtx := context.WithValue(context.Background(), oauth2.HTTPClient, httpClient)
			token, err := tokenCfg.Token(tokenCtx)
			if err != nil {
				t.Fatalf("oauth2 client_credentials token request failed: %v", err)
			}
			if strings.TrimSpace(token.AccessToken) == "" {
				t.Fatalf("oauth2 token missing access token")
			}

			protectedURL := "https://" + listen + scopedHTTPPath(urlPathPrefix(baseURL), tc.mcpPath)
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, protectedURL, http.NoBody)
			if err != nil {
				t.Fatalf("build protected resource request: %v", err)
			}
			req.Header.Set("Authorization", "Bearer "+token.AccessToken)
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatalf("call protected mcp endpoint: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("expected non-auth failure with valid token; status=%d body=%q", resp.StatusCode, string(body))
			}
		})
	}
}

func TestOAuthToolsListUsesClientPresetSurface(t *testing.T) {
	t.Parallel()

	upstream := lockd.StartTestServer(t, lockd.WithoutTestMTLS())
	listener := reserveLoopbackListener(t)
	listen := listener.Addr().String()
	baseURL := "https://" + listen
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
		InitialClientName: "preset-client",
		InitialScopes:     []string{"read"},
	})
	if err != nil {
		t.Fatalf("bootstrap oauth state: %v", err)
	}
	lockdPreset := false
	if err := admin.UpdateClient(mcpadmin.UpdateClientRequest{
		ClientID:    boot.ClientID,
		LockdPreset: &lockdPreset,
		Presets: []presetcfg.Definition{{
			Name: "memory",
			Kinds: []presetcfg.Kind{{
				Name:      "note",
				Namespace: "agents",
				Schema: presetcfg.Schema{
					Type: "object",
					Properties: map[string]presetcfg.Schema{
						"text": {Type: "string"},
					},
				},
			}},
		}},
	}); err != nil {
		t.Fatalf("update client presets: %v", err)
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
			MCPPath:             "/",
		},
		Logger:   pslog.NewStructured(context.Background(), io.Discard),
		Listener: listener,
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

	waitForHTTPStatusOK(t, httpClient, wellKnownURLForScopedResource(baseURL, "oauth-protected-resource", baseURL))
	tokenBody := postTokenForm(t, httpClient, joinURL(baseURL, "/token"), boot.ClientID, boot.ClientSecret, url.Values{
		"grant_type": []string{"client_credentials"},
		"scope":      []string{"read"},
	})
	if tokenBody.status != http.StatusOK {
		t.Fatalf("token status=%d body=%q", tokenBody.status, string(tokenBody.body))
	}
	var issued oauth2.Token
	if err := json.Unmarshal(tokenBody.body, &issued); err != nil {
		t.Fatalf("decode token response: %v body=%q", err, string(tokenBody.body))
	}

	authClient := &http.Client{
		Transport: &bearerAuthTransport{
			base:  httpClient.Transport,
			token: issued.AccessToken,
		},
	}
	mcpClient := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "e2e-client", Version: "0.0.1"}, nil)
	session, err := mcpClient.Connect(context.Background(), &mcpsdk.StreamableClientTransport{
		Endpoint:   baseURL,
		HTTPClient: authClient,
	}, nil)
	if err != nil {
		t.Fatalf("connect mcp client: %v", err)
	}
	defer session.Close()

	tools, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	names := make([]string, 0, len(tools.Tools))
	for _, tool := range tools.Tools {
		names = append(names, tool.Name)
	}
	joined := strings.Join(names, ",")
	if strings.Contains(joined, "lockd.help") {
		t.Fatalf("unexpected lockd tool in preset-only oauth surface: %s", joined)
	}
	if !strings.Contains(joined, "memory.note.state.get") {
		t.Fatalf("missing preset state.get tool in %s", joined)
	}
}

func TestOAuthAccessTokenSurvivesMCPServerRestart(t *testing.T) {
	upstream := lockd.StartTestServer(t, lockd.WithoutTestMTLS())
	listener := reserveLoopbackListener(t)
	listen := listener.Addr().String()
	baseURL := "https://" + listen
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
		InitialClientName: "restart-client",
		InitialScopes:     []string{"read"},
	})
	if err != nil {
		t.Fatalf("bootstrap oauth state: %v", err)
	}

	startServer := func() (context.CancelFunc, chan error) {
		t.Helper()
		currentListener := listener
		listener = nil
		srv, err := NewServer(NewServerRequest{
			Config: Config{
				Listen:                    listen,
				DisableTLS:                false,
				BaseURL:                   baseURL,
				BundlePath:                serverBundlePath,
				UpstreamServer:            upstream.URL(),
				UpstreamDisableMTLS:       true,
				OAuthStatePath:            statePath,
				OAuthTokenStorePath:       tokenStorePath,
				MCPPath:                   "/",
				OAuthProtectedResourceURL: "",
			},
			Logger:   pslog.NewStructured(context.Background(), io.Discard),
			Listener: currentListener,
		})
		if err != nil {
			t.Fatalf("new mcp server: %v", err)
		}

		runCtx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- srv.Run(runCtx)
		}()
		waitForHTTPStatusOK(t, httpClient, authServerWellKnownURL(baseURL))
		return cancel, done
	}

	assertProtectedWithToken := func(accessToken string) {
		t.Helper()
		protectedURL := "https://" + listen + scopedHTTPPath(urlPathPrefix(baseURL), "/")
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, protectedURL, http.NoBody)
		if err != nil {
			t.Fatalf("build protected request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(accessToken))
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("call protected endpoint: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected non-auth failure with valid token; status=%d body=%q", resp.StatusCode, string(body))
		}
	}

	stopServer := func(cancel context.CancelFunc, done <-chan error) {
		t.Helper()
		cancel()
		select {
		case err := <-done:
			if err != nil && !strings.Contains(err.Error(), "context canceled") {
				t.Fatalf("mcp run returned error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for mcp shutdown")
		}
	}

	cancelFirst, doneFirst := startServer()
	tokenBody := postTokenForm(t, httpClient, joinURL(baseURL, "/token"), boot.ClientID, boot.ClientSecret, url.Values{
		"grant_type": []string{"client_credentials"},
		"scope":      []string{"read"},
	})
	if tokenBody.status != http.StatusOK {
		t.Fatalf("token status=%d body=%q", tokenBody.status, string(tokenBody.body))
	}
	var issued m2mTokenResponse
	if err := json.Unmarshal(tokenBody.body, &issued); err != nil {
		t.Fatalf("decode token response: %v body=%q", err, string(tokenBody.body))
	}
	if strings.TrimSpace(issued.AccessToken) == "" {
		t.Fatalf("missing access token in token response")
	}
	assertProtectedWithToken(issued.AccessToken)
	stopServer(cancelFirst, doneFirst)
	listener = reserveLoopbackListenerAt(t, listen)

	cancelSecond, doneSecond := startServer()
	defer stopServer(cancelSecond, doneSecond)
	assertProtectedWithToken(issued.AccessToken)
}

type bearerAuthTransport struct {
	base  http.RoundTripper
	token string
}

func (t *bearerAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	cloned := req.Clone(req.Context())
	cloned.Header = req.Header.Clone()
	cloned.Header.Set("Authorization", "Bearer "+t.token)
	return base.RoundTrip(cloned)
}

func TestOAuthDynamicRegistrationAndAuthorizationCodeFlow(t *testing.T) {
	upstream := lockd.StartTestServer(t, lockd.WithoutTestMTLS())
	listener := reserveLoopbackListener(t)
	listen := listener.Addr().String()
	baseURL := "https://" + listen
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
	if _, err := admin.Bootstrap(mcpadmin.BootstrapRequest{
		Path:              statePath,
		Issuer:            baseURL,
		InitialClientName: "default",
		InitialScopes:     []string{"read"},
	}); err != nil {
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
			MCPPath:             "/",
		},
		Logger:   pslog.NewStructured(context.Background(), io.Discard),
		Listener: listener,
	})
	if err != nil {
		t.Fatalf("new mcp server: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- srv.Run(runCtx)
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !strings.Contains(err.Error(), "context canceled") {
				t.Fatalf("mcp run returned error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for mcp shutdown")
		}
	}()

	waitForHTTPStatusOK(t, httpClient, authServerWellKnownURL(baseURL))
	authMeta := fetchAuthServerMetadata(t, httpClient, authServerWellKnownURL(baseURL))
	if strings.TrimSpace(authMeta.AuthorizationEndpoint) == "" {
		t.Fatalf("authorization_endpoint missing")
	}
	if strings.TrimSpace(authMeta.RegistrationEndpoint) == "" {
		t.Fatalf("registration_endpoint missing")
	}

	registerBody := `{"client_name":"chatgpt","redirect_uris":["https://chat.openai.com/aip/callback"],"grant_types":["authorization_code","refresh_token","client_credentials"],"response_types":["code"],"scope":"read","token_endpoint_auth_method":"client_secret_basic"}`
	registerReq, err := http.NewRequestWithContext(context.Background(), http.MethodPost, authMeta.RegistrationEndpoint, strings.NewReader(registerBody))
	if err != nil {
		t.Fatalf("build registration request: %v", err)
	}
	registerReq.Header.Set("Content-Type", "application/json")
	registerResp, err := httpClient.Do(registerReq)
	if err != nil {
		t.Fatalf("call registration endpoint: %v", err)
	}
	defer registerResp.Body.Close()
	if registerResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(registerResp.Body)
		t.Fatalf("registration status=%d body=%q", registerResp.StatusCode, string(body))
	}
	var registration struct {
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
	}
	if err := json.NewDecoder(registerResp.Body).Decode(&registration); err != nil {
		t.Fatalf("decode registration response: %v", err)
	}
	if strings.TrimSpace(registration.ClientID) == "" || strings.TrimSpace(registration.ClientSecret) == "" {
		t.Fatalf("registration missing client credentials: %+v", registration)
	}

	authorizeClient := *httpClient
	authorizeClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	verifier := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~pkce-check"
	sum := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	authorizeReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, authMeta.AuthorizationEndpoint, http.NoBody)
	if err != nil {
		t.Fatalf("build authorize request: %v", err)
	}
	q := authorizeReq.URL.Query()
	q.Set("response_type", "code")
	q.Set("client_id", registration.ClientID)
	q.Set("redirect_uri", "https://chat.openai.com/aip/callback")
	q.Set("scope", "read")
	q.Set("state", "state-check")
	q.Set("resource", joinURL(baseURL, "/"))
	q.Set("code_challenge", challenge)
	q.Set("code_challenge_method", "S256")
	authorizeReq.URL.RawQuery = q.Encode()
	authorizeResp, err := authorizeClient.Do(authorizeReq)
	if err != nil {
		t.Fatalf("call authorize endpoint: %v", err)
	}
	defer authorizeResp.Body.Close()
	if authorizeResp.StatusCode != http.StatusFound {
		body, _ := io.ReadAll(authorizeResp.Body)
		t.Fatalf("authorize status=%d body=%q", authorizeResp.StatusCode, string(body))
	}
	location := strings.TrimSpace(authorizeResp.Header.Get("Location"))
	if location == "" {
		t.Fatalf("authorize redirect missing location header")
	}
	redirectParsed, err := url.Parse(location)
	if err != nil {
		t.Fatalf("parse authorize redirect location: %v", err)
	}
	code := strings.TrimSpace(redirectParsed.Query().Get("code"))
	if code == "" {
		t.Fatalf("authorize redirect missing code: %q", location)
	}
	if got := strings.TrimSpace(redirectParsed.Query().Get("state")); got != "state-check" {
		t.Fatalf("authorize redirect state=%q want state-check", got)
	}

	tokenBody := postTokenForm(t, httpClient, authMeta.TokenEndpoint, registration.ClientID, registration.ClientSecret, url.Values{
		"grant_type":    []string{"authorization_code"},
		"code":          []string{code},
		"redirect_uri":  []string{"https://chat.openai.com/aip/callback"},
		"code_verifier": []string{verifier},
		"resource":      []string{joinURL(baseURL, "/")},
	})
	if tokenBody.status != http.StatusOK {
		t.Fatalf("authorization_code token status=%d body=%q", tokenBody.status, string(tokenBody.body))
	}
	var issued m2mTokenResponse
	if err := json.Unmarshal(tokenBody.body, &issued); err != nil {
		t.Fatalf("decode authorization_code token response: %v body=%q", err, string(tokenBody.body))
	}
	if strings.TrimSpace(issued.AccessToken) == "" {
		t.Fatalf("authorization_code token missing access token")
	}
	if strings.TrimSpace(issued.RefreshToken) == "" {
		t.Fatalf("authorization_code token missing refresh token")
	}

	protectedURL := "https://" + listen + scopedHTTPPath(urlPathPrefix(baseURL), "/")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, protectedURL, http.NoBody)
	if err != nil {
		t.Fatalf("build protected request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+issued.AccessToken)
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("call protected mcp endpoint: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected non-auth failure with valid token; status=%d body=%q", resp.StatusCode, string(body))
	}
}

func reserveLoopbackListener(t testing.TB) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback listener: %v", err)
	}
	t.Cleanup(func() {
		_ = ln.Close()
	})
	return ln
}

func reserveLoopbackListenerAt(t testing.TB, addr string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("reserve loopback listener at %s: %v", addr, err)
	}
	t.Cleanup(func() {
		_ = ln.Close()
	})
	return ln
}

func trustedHTTPClientFromServerBundle(t testing.TB, serverBundlePath string) *http.Client {
	t.Helper()
	bundle, err := tlsutil.LoadBundle(serverBundlePath, "")
	if err != nil {
		t.Fatalf("load server bundle: %v", err)
	}
	pool := x509.NewCertPool()
	if ok := pool.AppendCertsFromPEM(bundle.CACertPEM); !ok {
		t.Fatalf("append CA certs")
	}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    pool,
		},
	}
	return &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}
}

func waitForHTTPStatusOK(t testing.TB, httpClient *http.Client, targetURL string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, targetURL, http.NoBody)
		if err != nil {
			t.Fatalf("build readiness request: %v", err)
		}
		resp, err := httpClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for status=200 from %s", targetURL)
}

func fetchProtectedResourceMetadata(t testing.TB, httpClient *http.Client, targetURL string) oauthProtectedResourceMetadata {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, targetURL, http.NoBody)
	if err != nil {
		t.Fatalf("build protected metadata request: %v", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("call protected metadata endpoint: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("protected metadata status=%d body=%q", resp.StatusCode, string(body))
	}
	var out oauthProtectedResourceMetadata
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode protected metadata: %v", err)
	}
	return out
}

func fetchAuthServerMetadata(t testing.TB, httpClient *http.Client, targetURL string) oauthAuthorizationServerMetadata {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, targetURL, http.NoBody)
	if err != nil {
		t.Fatalf("build auth metadata request: %v", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("call auth metadata endpoint: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("auth metadata status=%d body=%q", resp.StatusCode, string(body))
	}
	var out oauthAuthorizationServerMetadata
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode auth metadata: %v", err)
	}
	return out
}

func wellKnownURLForScopedResource(baseURL, kind, targetURL string) string {
	base, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return ""
	}
	target, err := url.Parse(strings.TrimSpace(targetURL))
	if err != nil {
		return ""
	}
	root := cleanHTTPPath(base.Path)
	p := path.Join(root, ".well-known", strings.Trim(strings.TrimSpace(kind), "/"))
	relative := scopedRelativePath(root, cleanHTTPPath(target.Path))
	if relative != "/" {
		p = path.Join(p, strings.TrimPrefix(relative, "/"))
	}
	return fmt.Sprintf("%s://%s%s", base.Scheme, base.Host, p)
}
