package state

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/mcp/preset"
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
	client, secret, err := base.AddClient("test", "team_alpha", true, nil, []string{"read", "write"}, []string{"https://example.test/callback"}, now)
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
	if loaded.Clients[client.ID].Namespace != "team_alpha" {
		t.Fatalf("namespace=%q want %q", loaded.Clients[client.ID].Namespace, "team_alpha")
	}
	if !loaded.Clients[client.ID].LockdPreset {
		t.Fatalf("expected lockd preset enabled by default")
	}
}

func TestAddClientRejectsInvalidRedirectURI(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	data := NewData("https://issuer.example", now)
	if _, _, err := data.AddClient("invalid", "", true, nil, []string{"read"}, []string{"/relative/callback"}, now); err == nil {
		t.Fatalf("expected invalid redirect URI error")
	}
}

func TestUpdateClientRedirectURIs(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	data := NewData("https://issuer.example", now)
	client, _, err := data.AddClient("test", "", true, nil, []string{"read"}, nil, now)
	if err != nil {
		t.Fatalf("add client: %v", err)
	}
	if err := data.UpdateClientRedirectURIs(client.ID, []string{"https://example.test/callback", "https://example.test/callback2"}, now); err != nil {
		t.Fatalf("update redirect uris: %v", err)
	}
	got := data.Clients[client.ID]
	if len(got.RedirectURIs) != 2 {
		t.Fatalf("redirect uri count=%d want 2", len(got.RedirectURIs))
	}
}

func TestAddClientRejectsInvalidNamespace(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	data := NewData("https://issuer.example", now)
	if _, _, err := data.AddClient("invalid", "bad/namespace", true, nil, []string{"read"}, nil, now); err == nil {
		t.Fatalf("expected invalid namespace error")
	}
}

func TestUpdateClientNamespace(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	data := NewData("https://issuer.example", now)
	client, _, err := data.AddClient("test", "", true, nil, []string{"read"}, nil, now)
	if err != nil {
		t.Fatalf("add client: %v", err)
	}
	if err := data.UpdateClientNamespace(client.ID, "TeamB", now); err != nil {
		t.Fatalf("update namespace: %v", err)
	}
	if got := data.Clients[client.ID].Namespace; got != "teamb" {
		t.Fatalf("namespace=%q want %q", got, "teamb")
	}
	if err := data.UpdateClientNamespace(client.ID, "", now); err != nil {
		t.Fatalf("clear namespace: %v", err)
	}
	if got := data.Clients[client.ID].Namespace; got != "" {
		t.Fatalf("namespace=%q want empty", got)
	}
}

func TestAddClientPersistsNormalizedPresets(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	data := NewData("https://issuer.example", now)
	defs := []preset.Definition{{
		Name: "Memory Vault",
		Kinds: []preset.Kind{{
			Name:      "Note",
			Namespace: "TeamA",
			Schema: preset.Schema{
				Type: "object",
				Properties: map[string]preset.Schema{
					"text": {Type: "string"},
				},
				Required: []string{"text"},
			},
		}},
	}}
	client, _, err := data.AddClient("preset-client", "", false, defs, []string{"read"}, nil, now)
	if err != nil {
		t.Fatalf("add client: %v", err)
	}
	got := data.Clients[client.ID]
	if got.LockdPreset {
		t.Fatalf("expected lockd preset disabled for custom-only client")
	}
	if len(got.Presets) != 1 {
		t.Fatalf("len(got.Presets)=%d want 1", len(got.Presets))
	}
	if got.Presets[0].Name != "memory_vault" {
		t.Fatalf("preset name=%q want memory_vault", got.Presets[0].Name)
	}
	if got.Presets[0].Kinds[0].Namespace != "teama" {
		t.Fatalf("preset namespace=%q want teama", got.Presets[0].Kinds[0].Namespace)
	}
}

func TestUpdateClientPresetsReplacesEnabledSurface(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	data := NewData("https://issuer.example", now)
	client, _, err := data.AddClient("preset-client", "", true, nil, []string{"read"}, nil, now)
	if err != nil {
		t.Fatalf("add client: %v", err)
	}
	defs := []preset.Definition{{
		Name: "memory",
		Kinds: []preset.Kind{{
			Name:      "note",
			Namespace: "ops",
			Operations: []preset.Operation{
				preset.OperationQuery,
				preset.OperationStateGet,
			},
			Schema: preset.Schema{
				Type: "object",
				Properties: map[string]preset.Schema{
					"text": {Type: "string"},
				},
			},
		}},
	}}
	if err := data.UpdateClientPresets(client.ID, false, defs, now); err != nil {
		t.Fatalf("update presets: %v", err)
	}
	got := data.Clients[client.ID]
	if got.LockdPreset {
		t.Fatalf("expected lockd preset disabled")
	}
	if !reflect.DeepEqual(got.Presets[0].Kinds[0].Operations, []preset.Operation{
		preset.OperationQuery,
		preset.OperationStateGet,
	}) {
		t.Fatalf("operations=%v", got.Presets[0].Kinds[0].Operations)
	}
}

func TestUpdateClientPresetsRejectsEmptySurface(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	data := NewData("https://issuer.example", now)
	client, _, err := data.AddClient("preset-client", "", true, nil, []string{"read"}, nil, now)
	if err != nil {
		t.Fatalf("add client: %v", err)
	}
	if err := data.UpdateClientPresets(client.ID, false, nil, now); err == nil {
		t.Fatalf("expected empty surface update to fail")
	}
}
