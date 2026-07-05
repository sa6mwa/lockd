package state

import (
	"encoding/json"
	"errors"
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

func TestLoadDefaultsLegacyClientToLockdPreset(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	data := NewData("https://issuer.example", now)
	data.Clients["legacy-client"] = Client{
		ID:         "legacy-client",
		Name:       "legacy",
		SecretSalt: "salt",
		SecretHash: "hash",
		Scopes:     []string{"read"},
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal legacy state: %v", err)
	}
	material, basePEM, err := ensureMaterial(nil)
	if err != nil {
		t.Fatalf("ensure material: %v", err)
	}
	ciphertext, err := encryptPayload(payload, material)
	if err != nil {
		t.Fatalf("encrypt payload: %v", err)
	}
	raw, err := upsertStateBlock(basePEM, ciphertext)
	if err != nil {
		t.Fatalf("upsert state block: %v", err)
	}
	path := filepath.Join(t.TempDir(), "mcp.pem")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write state file: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("load legacy state: %v", err)
	}
	client := loaded.Clients["legacy-client"]
	if !client.LockdPreset {
		t.Fatalf("expected legacy client to default to lockd preset")
	}
	if len(client.Presets) != 0 {
		t.Fatalf("expected no custom presets, got %#v", client.Presets)
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

func TestNormalizeRejectsInvalidCustomPresetState(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	data := NewData("https://issuer.example", now)
	client, _, err := data.AddClient("preset-client", "", false, []preset.Definition{{
		Name: "memory",
		Kinds: []preset.Kind{{
			Name:      "note",
			Namespace: "ops",
			Schema: preset.Schema{
				Type: "object",
				Properties: map[string]preset.Schema{
					"text": {Type: "string"},
				},
			},
		}},
	}}, []string{"read"}, nil, now)
	if err != nil {
		t.Fatalf("add client: %v", err)
	}

	broken := data.Clients[client.ID]
	broken.Presets[0].Kinds[0].Namespace = "bad namespace"
	data.Clients[client.ID] = broken

	if err := data.Normalize(); err == nil {
		t.Fatalf("expected invalid preset state to fail normalization")
	} else if !strings.Contains(err.Error(), `client "`+client.ID+`" presets`) {
		t.Fatalf("unexpected normalize error: %v", err)
	}
}

func TestLoadRejectsInvalidCustomPresetState(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	data := NewData("https://issuer.example", now)
	client, _, err := data.AddClient("preset-client", "", false, []preset.Definition{{
		Name: "memory",
		Kinds: []preset.Kind{{
			Name:      "note",
			Namespace: "ops",
			Schema: preset.Schema{
				Type: "object",
				Properties: map[string]preset.Schema{
					"text": {Type: "string"},
				},
			},
		}},
	}}, []string{"read"}, nil, now)
	if err != nil {
		t.Fatalf("add client: %v", err)
	}

	broken := data.Clone()
	clientState := broken.Clients[client.ID]
	clientState.Presets[0].Kinds[0].Namespace = "bad namespace"
	broken.Clients[client.ID] = clientState

	payload, err := json.Marshal(broken)
	if err != nil {
		t.Fatalf("marshal broken state: %v", err)
	}
	material, basePEM, err := ensureMaterial(nil)
	if err != nil {
		t.Fatalf("ensure material: %v", err)
	}
	ciphertext, err := encryptPayload(payload, material)
	if err != nil {
		t.Fatalf("encrypt payload: %v", err)
	}
	raw, err := upsertStateBlock(basePEM, ciphertext)
	if err != nil {
		t.Fatalf("upsert state block: %v", err)
	}
	path := filepath.Join(t.TempDir(), "mcp.pem")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write state file: %v", err)
	}

	_, err = Load(path)
	if err == nil {
		t.Fatalf("expected load to reject invalid preset state")
	}
	if !strings.Contains(err.Error(), "normalize mcp state") {
		t.Fatalf("unexpected load error: %v", err)
	}
	if !strings.Contains(err.Error(), `client "`+client.ID+`" presets`) {
		t.Fatalf("unexpected load error detail: %v", err)
	}
}

func TestAddClientRejectsDuplicateNameCaseInsensitive(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	data := NewData("https://issuer.example", now)
	if _, _, err := data.AddClient("Memory", "", true, nil, []string{"read"}, nil, now); err != nil {
		t.Fatalf("add client: %v", err)
	}
	if _, _, err := data.AddClient("memory", "", true, nil, []string{"read"}, nil, now); err == nil {
		t.Fatalf("expected duplicate client name to fail")
	} else if !errors.Is(err, ErrClientNameConflict) {
		t.Fatalf("expected ErrClientNameConflict, got %v", err)
	}
}

func TestUpdateClientNameRejectsDuplicateRevokedName(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	data := NewData("https://issuer.example", now)
	first, _, err := data.AddClient("first", "", true, nil, []string{"read"}, nil, now)
	if err != nil {
		t.Fatalf("add first client: %v", err)
	}
	second, _, err := data.AddClient("second", "", true, nil, []string{"read"}, nil, now)
	if err != nil {
		t.Fatalf("add second client: %v", err)
	}
	if err := data.RevokeClient(first.ID, true, now); err != nil {
		t.Fatalf("revoke first client: %v", err)
	}
	if err := data.UpdateClientName(second.ID, "FIRST", now); err == nil {
		t.Fatalf("expected duplicate revoked client name to fail")
	} else if !errors.Is(err, ErrClientNameConflict) {
		t.Fatalf("expected ErrClientNameConflict, got %v", err)
	}
}
