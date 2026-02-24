package admin

import (
	"path/filepath"
	"testing"
)

func TestServiceBootstrapAndCredentials(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	statePath := filepath.Join(dir, "mcp.pem")
	refreshPath := filepath.Join(dir, "mcp-auth-store.json")
	svc := New(Config{
		StatePath:    statePath,
		RefreshStore: refreshPath,
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
	if creds.ResourceURL != "https://127.0.0.1:19341/" {
		t.Fatalf("unexpected resource url: %s", creds.ResourceURL)
	}
}

func TestServiceClientLifecycle(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	svc := New(Config{
		StatePath:    filepath.Join(dir, "mcp.pem"),
		RefreshStore: filepath.Join(dir, "mcp-auth-store.json"),
	})
	if _, err := svc.Bootstrap(BootstrapRequest{
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "default",
	}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	added, err := svc.AddClient(AddClientRequest{
		Name:   "extra",
		Scopes: []string{"read", "write"},
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
		Scopes:   []string{"write"},
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

