package state

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBootstrapLoadAndVerifySecret(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "mcp.pem")

	boot, err := Bootstrap(BootstrapRequest{
		Path:              path,
		Issuer:            "https://127.0.0.1:19341",
		InitialClientName: "cli",
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if boot.ClientID == "" || boot.ClientSecret == "" {
		t.Fatalf("expected bootstrap client credentials, got %#v", boot)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.Issuer != "https://127.0.0.1:19341" {
		t.Fatalf("issuer mismatch: got %q", loaded.Issuer)
	}
	client, ok := loaded.VerifyClientSecret(boot.ClientID, boot.ClientSecret)
	if !ok || client == nil {
		t.Fatalf("expected secret verification to succeed")
	}
	if client.Name != "cli" {
		t.Fatalf("client name mismatch: got %q", client.Name)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read state file: %v", err)
	}
	if strings.Contains(string(raw), boot.ClientSecret) {
		t.Fatalf("state file leaks client secret in plaintext")
	}
}

func TestLoadMissingReturnsNotBootstrapped(t *testing.T) {
	t.Parallel()
	_, err := Load(filepath.Join(t.TempDir(), "missing.pem"))
	if err == nil || !strings.Contains(err.Error(), ErrNotBootstrapped.Error()) {
		t.Fatalf("expected ErrNotBootstrapped, got %v", err)
	}
}

func TestSaveRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "mcp.pem")
	now := time.Now().UTC()

	base := NewData("https://localhost:19341", now)
	client, secret, err := base.AddClient("test", []string{"read", "write"}, now)
	if err != nil {
		t.Fatalf("add client: %v", err)
	}
	if secret == "" {
		t.Fatalf("expected generated secret")
	}
	if err := Save(path, base); err != nil {
		t.Fatalf("save: %v", err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got, ok := loaded.VerifyClientSecret(client.ID, secret); !ok || got == nil {
		t.Fatalf("expected verified client after reload")
	}
}
