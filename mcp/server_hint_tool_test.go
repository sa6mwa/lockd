package mcp

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/nsauth"
	"pkt.systems/lockd/tlsutil"
)

func TestExtractNamespaceHintsFromURIsStrongestWins(t *testing.T) {
	t.Parallel()

	ordersWrite, err := nsauth.ClaimURI("orders", nsauth.PermissionWrite)
	if err != nil {
		t.Fatalf("orders write claim: %v", err)
	}
	ordersReadWrite, err := nsauth.ClaimURI("orders", nsauth.PermissionReadWrite)
	if err != nil {
		t.Fatalf("orders readwrite claim: %v", err)
	}
	allRead, err := nsauth.ClaimURI("ALL", nsauth.PermissionRead)
	if err != nil {
		t.Fatalf("all read claim: %v", err)
	}

	snapshot := extractNamespaceHintsFromURIs([]*url.URL{ordersWrite, ordersReadWrite, allRead})
	if snapshot.Source != "client_bundle_claims" {
		t.Fatalf("expected source client_bundle_claims, got %q", snapshot.Source)
	}
	if snapshot.WildcardPermission != "read" {
		t.Fatalf("expected wildcard permission read, got %q", snapshot.WildcardPermission)
	}
	if len(snapshot.NamespaceHints) != 1 {
		t.Fatalf("expected one namespace hint, got %d", len(snapshot.NamespaceHints))
	}
	if snapshot.NamespaceHints[0].Namespace != "orders" {
		t.Fatalf("expected orders namespace, got %q", snapshot.NamespaceHints[0].Namespace)
	}
	if snapshot.NamespaceHints[0].Permission != "read_write" {
		t.Fatalf("expected read_write permission, got %q", snapshot.NamespaceHints[0].Permission)
	}
}

func TestHandleHintToolIncludesClientBundleClaimsAndTokenContext(t *testing.T) {
	t.Parallel()

	ca, err := tlsutil.GenerateCA("mcp-hint-ca", 24*time.Hour)
	if err != nil {
		t.Fatalf("generate ca: %v", err)
	}
	ordersWrite, err := nsauth.ClaimURI("orders", nsauth.PermissionWrite)
	if err != nil {
		t.Fatalf("orders claim: %v", err)
	}
	allRead, err := nsauth.ClaimURI("ALL", nsauth.PermissionRead)
	if err != nil {
		t.Fatalf("all claim: %v", err)
	}
	sdkID, err := lockd.SPIFFEURIForRole(lockd.ClientBundleRoleSDK, "mcp-hint-client")
	if err != nil {
		t.Fatalf("sdk spiffe uri: %v", err)
	}
	issued, err := ca.IssueClient(tlsutil.ClientCertRequest{
		CommonName: "mcp-hint-client",
		Validity:   24 * time.Hour,
		URIs:       []*url.URL{sdkID, ordersWrite, allRead},
	})
	if err != nil {
		t.Fatalf("issue client cert: %v", err)
	}
	bundlePEM, err := tlsutil.EncodeClientBundle(ca.CertPEM, issued.CertPEM, issued.KeyPEM)
	if err != nil {
		t.Fatalf("encode client bundle: %v", err)
	}
	bundlePath := filepath.Join(t.TempDir(), "client.pem")
	if err := os.WriteFile(bundlePath, bundlePEM, 0o600); err != nil {
		t.Fatalf("write client bundle: %v", err)
	}

	s := &server{cfg: Config{
		DefaultNamespace:         "mcp",
		UpstreamDisableMTLS:      false,
		UpstreamClientBundlePath: bundlePath,
	}}

	req := &mcpsdk.CallToolRequest{
		Extra: &mcpsdk.RequestExtra{TokenInfo: &mcpauth.TokenInfo{
			UserID: "agent-1",
			Scopes: []string{"scope:z", "scope:a"},
		}},
	}

	_, out, err := s.handleHintTool(context.Background(), req, hintToolInput{})
	if err != nil {
		t.Fatalf("hint tool: %v", err)
	}
	if out.ClientID != "agent-1" {
		t.Fatalf("expected client id agent-1, got %q", out.ClientID)
	}
	if out.DefaultNamespace != "mcp" {
		t.Fatalf("expected default namespace mcp, got %q", out.DefaultNamespace)
	}
	if out.Source != "client_bundle_claims" {
		t.Fatalf("expected source client_bundle_claims, got %q", out.Source)
	}
	if out.WildcardPermission != "read" {
		t.Fatalf("expected wildcard permission read, got %q", out.WildcardPermission)
	}
	if len(out.NamespaceHints) != 1 {
		t.Fatalf("expected one namespace hint, got %d", len(out.NamespaceHints))
	}
	if out.NamespaceHints[0].Namespace != "orders" || out.NamespaceHints[0].Permission != "write" {
		t.Fatalf("unexpected namespace hint: %#v", out.NamespaceHints[0])
	}
	if len(out.TokenScopes) != 2 || out.TokenScopes[0] != "scope:a" || out.TokenScopes[1] != "scope:z" {
		t.Fatalf("expected sorted token scopes, got %#v", out.TokenScopes)
	}
}

func TestHandleHintToolWithUpstreamMTLSDisabled(t *testing.T) {
	t.Parallel()

	s := &server{cfg: Config{
		DefaultNamespace:    "mcp",
		UpstreamDisableMTLS: true,
	}}

	_, out, err := s.handleHintTool(context.Background(), nil, hintToolInput{})
	if err != nil {
		t.Fatalf("hint tool: %v", err)
	}
	if out.Source != "upstream_mtls_disabled" {
		t.Fatalf("expected source upstream_mtls_disabled, got %q", out.Source)
	}
	if len(out.NamespaceHints) != 0 {
		t.Fatalf("expected no namespace hints, got %#v", out.NamespaceHints)
	}
}
