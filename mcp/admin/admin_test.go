package admin

import (
	"path/filepath"
	"testing"

	"pkt.systems/lockd/mcp/preset"
)

func TestServiceBootstrapAndCredentials(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	tokenStorePath := filepath.Join(dir, "mcp-token-store.enc.json")
	svc := New(Config{
		StatePath:  statePath,
		TokenStore: tokenStorePath,
	})

	boot, err := svc.Bootstrap(BootstrapRequest{
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
		InitialScopes:     []string{"read"},
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if boot.ClientID == "" || boot.ClientSecret == "" {
		t.Fatalf("expected initial credentials, got %+v", boot)
	}

	creds, err := svc.Credentials(CredentialsRequest{
		ClientID: boot.ClientID,
		MCPPath:  "/",
	})
	if err != nil {
		t.Fatalf("credentials: %v", err)
	}
	if creds.ClientID != boot.ClientID {
		t.Fatalf("expected client id %q, got %q", boot.ClientID, creds.ClientID)
	}
	if creds.ClientSecret != "" {
		t.Fatalf("expected secret hidden without rotate, got %q", creds.ClientSecret)
	}
	if creds.TokenURL != "https://127.0.0.1:19341/token" {
		t.Fatalf("unexpected token url: %s", creds.TokenURL)
	}
	if creds.AuthorizationURL != "https://127.0.0.1:19341/authorize" {
		t.Fatalf("unexpected authorization url: %s", creds.AuthorizationURL)
	}
	if creds.RegistrationURL != "https://127.0.0.1:19341/register" {
		t.Fatalf("unexpected registration url: %s", creds.RegistrationURL)
	}
	if creds.ResourceURL != "https://127.0.0.1:19341/" {
		t.Fatalf("unexpected resource url: %s", creds.ResourceURL)
	}
}

func TestServiceClientLifecycle(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	svc := New(Config{
		StatePath:  filepath.Join(dir, "mcp.pem"),
		TokenStore: filepath.Join(dir, "mcp-token-store.enc.json"),
	})
	if _, err := svc.Bootstrap(BootstrapRequest{
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	added, err := svc.AddClient(AddClientRequest{
		Name:      "extra",
		Namespace: "teama",
		Scopes:    []string{"read", "write"},
	})
	if err != nil {
		t.Fatalf("add client: %v", err)
	}
	if added.ClientID == "" || added.ClientSecret == "" {
		t.Fatalf("expected added client credentials, got %+v", added)
	}

	if err := svc.UpdateClient(UpdateClientRequest{
		ClientID: added.ClientID,
		Name:     "extra-renamed",
		Namespace: func() *string {
			value := "teamb"
			return &value
		}(),
		Scopes: []string{"write"},
	}); err != nil {
		t.Fatalf("update client: %v", err)
	}

	client, err := svc.GetClient(added.ClientID)
	if err != nil {
		t.Fatalf("get client: %v", err)
	}
	if client.Name != "extra-renamed" {
		t.Fatalf("expected updated name, got %q", client.Name)
	}
	if len(client.Scopes) != 1 || client.Scopes[0] != "write" {
		t.Fatalf("expected updated scopes [write], got %#v", client.Scopes)
	}
	if client.Namespace != "teamb" {
		t.Fatalf("expected updated namespace teamb, got %q", client.Namespace)
	}

	if err := svc.SetClientRevoked(added.ClientID, true); err != nil {
		t.Fatalf("revoke client: %v", err)
	}
	client, err = svc.GetClient(added.ClientID)
	if err != nil {
		t.Fatalf("get revoked client: %v", err)
	}
	if !client.Revoked {
		t.Fatalf("expected client revoked=true")
	}

	if err := svc.RemoveClient(added.ClientID); err != nil {
		t.Fatalf("remove client: %v", err)
	}
	if _, err := svc.GetClient(added.ClientID); err == nil {
		t.Fatalf("expected error for removed client")
	}
}

func TestServiceAddClientPersistsPresetSurface(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	svc := New(Config{
		StatePath:  filepath.Join(dir, "mcp.pem"),
		TokenStore: filepath.Join(dir, "mcp-token-store.enc.json"),
	})
	if _, err := svc.Bootstrap(BootstrapRequest{
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	added, err := svc.AddClient(AddClientRequest{
		Name:        "memory-client",
		LockdPreset: false,
		Presets: []preset.Definition{{
			Name: "memory",
			Kinds: []preset.Kind{{
				Name:      "note",
				Namespace: "agents",
				Schema: preset.Schema{
					Type: "object",
					Properties: map[string]preset.Schema{
						"text": {Type: "string"},
					},
				},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("add client: %v", err)
	}

	client, err := svc.GetClient(added.ClientID)
	if err != nil {
		t.Fatalf("get client: %v", err)
	}
	if client.LockdPreset {
		t.Fatalf("expected lockd preset disabled")
	}
	if len(client.Presets) != 1 || client.Presets[0].Name != "memory" {
		t.Fatalf("unexpected presets: %#v", client.Presets)
	}
}

func TestServiceResolvesClientByName(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	svc := New(Config{
		StatePath:  filepath.Join(dir, "mcp.pem"),
		TokenStore: filepath.Join(dir, "mcp-token-store.enc.json"),
	})
	if _, err := svc.Bootstrap(BootstrapRequest{
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	added, err := svc.AddClient(AddClientRequest{
		Name:   "memory-client",
		Scopes: []string{"read"},
	})
	if err != nil {
		t.Fatalf("add client: %v", err)
	}

	client, err := svc.GetClient("MEMORY-client")
	if err != nil {
		t.Fatalf("get client by name: %v", err)
	}
	if client.ID != added.ClientID {
		t.Fatalf("resolved id=%q want %q", client.ID, added.ClientID)
	}

	creds, err := svc.Credentials(CredentialsRequest{
		ClientID: "memory-client",
		MCPPath:  "/",
	})
	if err != nil {
		t.Fatalf("credentials by name: %v", err)
	}
	if creds.ClientID != added.ClientID {
		t.Fatalf("credentials client id=%q want %q", creds.ClientID, added.ClientID)
	}
}
