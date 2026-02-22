package lockd

import (
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/nsauth"
	"pkt.systems/lockd/tlsutil"
)

func TestCreateCABundleIncludesKryptografMaterial(t *testing.T) {
	caBundle, err := CreateCABundle(CreateCABundleRequest{
		CommonName: "lockd-ca-test",
	})
	if err != nil {
		t.Fatalf("create ca bundle: %v", err)
	}
	ca, err := tlsutil.LoadCAFromBytes(caBundle)
	if err != nil {
		t.Fatalf("load ca from bytes: %v", err)
	}
	if got := ca.Cert.Subject.CommonName; got != "lockd-ca-test" {
		t.Fatalf("ca common name mismatch: got %q", got)
	}
	material, err := cryptoutil.MetadataMaterialFromBytes(caBundle)
	if err != nil {
		t.Fatalf("metadata material from ca: %v", err)
	}
	if reflect.ValueOf(material.Root).IsZero() {
		t.Fatalf("expected non-zero kryptograf root key")
	}
	if reflect.ValueOf(material.Descriptor).IsZero() {
		t.Fatalf("expected non-zero kryptograf descriptor")
	}
}

func TestCreateServerBundleUsesCAMetadataAndServerClaims(t *testing.T) {
	caBundle, err := CreateCABundle(CreateCABundleRequest{
		CommonName: "lockd-ca-test",
	})
	if err != nil {
		t.Fatalf("create ca bundle: %v", err)
	}
	nodeID := "server-node-123"
	serverBundle, err := CreateServerBundle(CreateServerBundleRequest{
		CABundlePEM: caBundle,
		CommonName:  "lockd-server-test",
		Hosts:       []string{"127.0.0.1", "localhost"},
		NodeID:      nodeID,
	})
	if err != nil {
		t.Fatalf("create server bundle: %v", err)
	}
	parsed, err := tlsutil.LoadBundleFromBytes(serverBundle)
	if err != nil {
		t.Fatalf("load server bundle: %v", err)
	}
	if parsed.ServerCert == nil {
		t.Fatalf("expected server certificate")
	}
	expectedSPIFFE, err := SPIFFEURIForServer(nodeID)
	if err != nil {
		t.Fatalf("server spiffe: %v", err)
	}
	expectedAllClaim, err := nsauth.ClaimURI("ALL", nsauth.PermissionReadWrite)
	if err != nil {
		t.Fatalf("all claim: %v", err)
	}
	if !containsURI(parsed.ServerCert.URIs, expectedSPIFFE) {
		t.Fatalf("expected server spiffe uri %s", expectedSPIFFE.String())
	}
	if !containsURI(parsed.ServerCert.URIs, expectedAllClaim) {
		t.Fatalf("expected ALL claim uri %s", expectedAllClaim.String())
	}
	caMaterial, err := cryptoutil.MetadataMaterialFromBytes(caBundle)
	if err != nil {
		t.Fatalf("metadata material from ca: %v", err)
	}
	serverMaterial, err := cryptoutil.MetadataMaterialFromBytes(serverBundle)
	if err != nil {
		t.Fatalf("metadata material from server: %v", err)
	}
	if !reflect.DeepEqual(serverMaterial.Root, caMaterial.Root) {
		t.Fatalf("server bundle root key should match ca bundle root key")
	}
	if !reflect.DeepEqual(serverMaterial.Descriptor, caMaterial.Descriptor) {
		t.Fatalf("server bundle descriptor should match ca bundle descriptor")
	}
}

func TestCreateClientBundleDefaultClaims(t *testing.T) {
	caBundle, err := CreateCABundle(CreateCABundleRequest{})
	if err != nil {
		t.Fatalf("create ca bundle: %v", err)
	}
	clientBundle, err := CreateClientBundle(CreateClientBundleRequest{
		CABundlePEM: caBundle,
		CommonName:  "lockd-client-test",
	})
	if err != nil {
		t.Fatalf("create client bundle: %v", err)
	}
	parsed, err := tlsutil.LoadClientBundleFromBytes(clientBundle)
	if err != nil {
		t.Fatalf("load client bundle: %v", err)
	}
	expectedSPIFFE, err := SPIFFEURIForRole(ClientBundleRoleSDK, "lockd-client-test")
	if err != nil {
		t.Fatalf("sdk spiffe uri: %v", err)
	}
	expectedDefaultClaim, err := nsauth.ClaimURI(DefaultNamespace, nsauth.PermissionReadWrite)
	if err != nil {
		t.Fatalf("default claim: %v", err)
	}
	if !containsURI(parsed.ClientCert.URIs, expectedSPIFFE) {
		t.Fatalf("expected sdk spiffe uri %s", expectedSPIFFE.String())
	}
	if !containsURI(parsed.ClientCert.URIs, expectedDefaultClaim) {
		t.Fatalf("expected default claim uri %s", expectedDefaultClaim.String())
	}
}

func TestCreateClientBundleExplicitClaims(t *testing.T) {
	caBundle, err := CreateCABundle(CreateCABundleRequest{})
	if err != nil {
		t.Fatalf("create ca bundle: %v", err)
	}
	clientBundle, err := CreateClientBundle(CreateClientBundleRequest{
		CABundlePEM:     caBundle,
		CommonName:      "lockd-client-explicit",
		NamespaceClaims: []string{"orders=w,stash", "orders=r"},
		ReadAll:         true,
	})
	if err != nil {
		t.Fatalf("create client bundle: %v", err)
	}
	parsed, err := tlsutil.LoadClientBundleFromBytes(clientBundle)
	if err != nil {
		t.Fatalf("load client bundle: %v", err)
	}
	expectedAllClaim, err := nsauth.ClaimURI("ALL", nsauth.PermissionRead)
	if err != nil {
		t.Fatalf("all claim: %v", err)
	}
	expectedOrdersClaim, err := nsauth.ClaimURI("orders", nsauth.PermissionWrite)
	if err != nil {
		t.Fatalf("orders claim: %v", err)
	}
	expectedStashClaim, err := nsauth.ClaimURI("stash", nsauth.PermissionReadWrite)
	if err != nil {
		t.Fatalf("stash claim: %v", err)
	}
	defaultClaim, err := nsauth.ClaimURI(DefaultNamespace, nsauth.PermissionReadWrite)
	if err != nil {
		t.Fatalf("default claim: %v", err)
	}
	if !containsURI(parsed.ClientCert.URIs, expectedAllClaim) {
		t.Fatalf("expected ALL claim uri %s", expectedAllClaim.String())
	}
	if !containsURI(parsed.ClientCert.URIs, expectedOrdersClaim) {
		t.Fatalf("expected orders claim uri %s", expectedOrdersClaim.String())
	}
	if !containsURI(parsed.ClientCert.URIs, expectedStashClaim) {
		t.Fatalf("expected stash claim uri %s", expectedStashClaim.String())
	}
	if containsURI(parsed.ClientCert.URIs, defaultClaim) {
		t.Fatalf("did not expect default namespace claim when explicit claims are set")
	}
}

func TestCreateTCClientBundleClaims(t *testing.T) {
	caBundle, err := CreateCABundle(CreateCABundleRequest{})
	if err != nil {
		t.Fatalf("create ca bundle: %v", err)
	}
	clientBundle, err := CreateTCClientBundle(CreateTCClientBundleRequest{
		CABundlePEM: caBundle,
		CommonName:  "lockd-tc-client-test",
	})
	if err != nil {
		t.Fatalf("create tc client bundle: %v", err)
	}
	parsed, err := tlsutil.LoadClientBundleFromBytes(clientBundle)
	if err != nil {
		t.Fatalf("load tc client bundle: %v", err)
	}
	expectedSPIFFE, err := SPIFFEURIForRole(ClientBundleRoleTC, "lockd-tc-client-test")
	if err != nil {
		t.Fatalf("tc spiffe uri: %v", err)
	}
	expectedAllClaim, err := nsauth.ClaimURI("ALL", nsauth.PermissionReadWrite)
	if err != nil {
		t.Fatalf("all claim: %v", err)
	}
	if !containsURI(parsed.ClientCert.URIs, expectedSPIFFE) {
		t.Fatalf("expected tc spiffe uri %s", expectedSPIFFE.String())
	}
	if !containsURI(parsed.ClientCert.URIs, expectedAllClaim) {
		t.Fatalf("expected ALL claim uri %s", expectedAllClaim.String())
	}
}

func TestCreateBundleFileWrappers(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	serverPath := filepath.Join(dir, "server.pem")
	clientPath := filepath.Join(dir, "client.pem")
	tcPath := filepath.Join(dir, "tc-client.pem")

	if err := CreateCABundleFile(CreateCABundleFileRequest{
		Path: caPath,
		CreateCABundleRequest: CreateCABundleRequest{
			CommonName: "lockd-ca-file",
		},
	}); err != nil {
		t.Fatalf("create ca file: %v", err)
	}
	if err := CreateCABundleFile(CreateCABundleFileRequest{
		Path: caPath,
	}); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected already exists error, got %v", err)
	}
	if err := CreateCABundleFile(CreateCABundleFileRequest{
		Path:  caPath,
		Force: true,
	}); err != nil {
		t.Fatalf("force overwrite ca file: %v", err)
	}

	caBundle, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatalf("read ca file: %v", err)
	}
	if err := CreateServerBundleFile(CreateServerBundleFileRequest{
		Path: serverPath,
		CreateServerBundleRequest: CreateServerBundleRequest{
			CABundlePEM: caBundle,
		},
	}); err != nil {
		t.Fatalf("create server file: %v", err)
	}
	if err := CreateClientBundleFile(CreateClientBundleFileRequest{
		Path: clientPath,
		CreateClientBundleRequest: CreateClientBundleRequest{
			CABundlePEM: caBundle,
		},
	}); err != nil {
		t.Fatalf("create client file: %v", err)
	}
	if err := CreateTCClientBundleFile(CreateTCClientBundleFileRequest{
		Path: tcPath,
		CreateTCClientBundleRequest: CreateTCClientBundleRequest{
			CABundlePEM: caBundle,
		},
	}); err != nil {
		t.Fatalf("create tc client file: %v", err)
	}

	serverBytes, err := os.ReadFile(serverPath)
	if err != nil {
		t.Fatalf("read server file: %v", err)
	}
	if _, err := tlsutil.LoadBundleFromBytes(serverBytes); err != nil {
		t.Fatalf("parse server file: %v", err)
	}
	clientBytes, err := os.ReadFile(clientPath)
	if err != nil {
		t.Fatalf("read client file: %v", err)
	}
	if _, err := tlsutil.LoadClientBundleFromBytes(clientBytes); err != nil {
		t.Fatalf("parse client file: %v", err)
	}
	tcBytes, err := os.ReadFile(tcPath)
	if err != nil {
		t.Fatalf("read tc file: %v", err)
	}
	if _, err := tlsutil.LoadClientBundleFromBytes(tcBytes); err != nil {
		t.Fatalf("parse tc file: %v", err)
	}
}

func TestCreateClientBundleInvalidNamespaceClaim(t *testing.T) {
	caBundle, err := CreateCABundle(CreateCABundleRequest{})
	if err != nil {
		t.Fatalf("create ca bundle: %v", err)
	}
	if _, err := CreateClientBundle(CreateClientBundleRequest{
		CABundlePEM:     caBundle,
		NamespaceClaims: []string{"default=x"},
	}); err == nil {
		t.Fatalf("expected invalid namespace claim error")
	}
}

func containsURI(uris []*url.URL, expected *url.URL) bool {
	if expected == nil {
		return false
	}
	want := expected.String()
	for _, uri := range uris {
		if uri == nil {
			continue
		}
		if uri.String() == want {
			return true
		}
	}
	return false
}
