package main

import (
	"bytes"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pkt.systems/kryptograf"
	"pkt.systems/kryptograf/keymgmt"
	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/tlsutil"
)

func runAuthCommand(t *testing.T, args ...string) string {
	t.Helper()
	cmd := newAuthCommand()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs(args)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("auth command %q failed: %v\noutput: %s", strings.Join(args, " "), err, buf.String())
	}
	return buf.String()
}

func runAuthCommandExpectError(t *testing.T, args ...string) string {
	t.Helper()
	cmd := newAuthCommand()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs(args)
	if err := cmd.Execute(); err == nil {
		t.Fatalf("expected auth command %q to fail\noutput: %s", strings.Join(args, " "), buf.String())
	}
	return buf.String()
}

func TestAuthWorkflowSplitCA(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	runAuthCommand(t, "new", "ca", "--cn", "lockd-ca-test")

	caPath, err := lockd.DefaultCAPath()
	if err != nil {
		t.Fatalf("default ca path: %v", err)
	}
	if _, err := os.Stat(caPath); err != nil {
		t.Fatalf("expected ca bundle at %s: %v", caPath, err)
	}
	ca, err := tlsutil.LoadCA(caPath)
	if err != nil {
		t.Fatalf("load ca: %v", err)
	}
	if len(ca.KeyPEM) == 0 {
		t.Fatalf("expected ca private key in %s", caPath)
	}
	store, err := keymgmt.LoadPEM(caPath)
	if err != nil {
		t.Fatalf("load kryptograf ca bundle: %v", err)
	}
	caRoot, ok, err := store.RootKey()
	if err != nil {
		t.Fatalf("read ca root key: %v", err)
	}
	if !ok {
		t.Fatalf("expected ca bundle to store kryptograf root key")
	}
	caDesc, ok, err := store.Descriptor(cryptoutil.MetadataDescriptorName)
	if err != nil {
		t.Fatalf("read ca descriptor: %v", err)
	}
	if !ok {
		t.Fatalf("expected ca bundle to include descriptor %q", cryptoutil.MetadataDescriptorName)
	}
	caID, err := cryptoutil.CACertificateID(ca.CertPEM)
	if err != nil {
		t.Fatalf("derive ca id: %v", err)
	}
	if _, err := kryptograf.ReconstructMaterial(caRoot, []byte(caID), caDesc); err != nil {
		t.Fatalf("reconstruct material from ca bundle: %v", err)
	}

	runAuthCommand(t, "new", "server", "--cn", "lockd-server-test", "--hosts", "127.0.0.1")

	serverPath, err := lockd.DefaultBundlePath()
	if err != nil {
		t.Fatalf("default bundle path: %v", err)
	}
	bundle, err := tlsutil.LoadBundle(serverPath, "")
	if err != nil {
		t.Fatalf("load server bundle: %v", err)
	}
	serverMaterial, err := cryptoutil.MetadataMaterialFromBundle(serverPath)
	if err != nil {
		t.Fatalf("load server kryptograf material: %v", err)
	}
	if serverMaterial.Root != caRoot {
		t.Fatalf("expected server bundle root key to match ca root")
	}
	if serverMaterial.Descriptor != caDesc {
		t.Fatalf("expected server bundle descriptor to match ca descriptor")
	}
	if _, err := kryptograf.ReconstructMaterial(serverMaterial.Root, []byte(caID), serverMaterial.Descriptor); err != nil {
		t.Fatalf("reconstruct material from server bundle: %v", err)
	}
	if bundle.CACertificate == nil {
		t.Fatalf("expected ca certificate embedded in server bundle")
	}
	if bundle.CAPrivateKey != nil || len(bundle.CAPrivateKeyPEM) != 0 {
		t.Fatalf("server bundle should not contain ca private key")
	}
	if bundle.ServerCert == nil || bundle.ServerCertificate.PrivateKey == nil {
		t.Fatalf("expected server certificate and key in bundle")
	}
	expectedServerAllClaim, err := url.Parse("lockd://ns/ALL?perm=rw")
	if err != nil {
		t.Fatalf("parse server all claim uri: %v", err)
	}
	if !containsURI(bundle.ServerCert.URIs, expectedServerAllClaim) {
		t.Fatalf("expected ALL namespace claim %s in server bundle", expectedServerAllClaim.String())
	}

	nextOutput := runAuthCommand(t, "new", "server", "--cn", "lockd-server-test", "--hosts", "127.0.0.1")
	nextPath := filepath.Join(filepath.Dir(serverPath), "server02.pem")
	if _, err := os.Stat(nextPath); err != nil {
		t.Fatalf("expected deduped server bundle at %s: %v", nextPath, err)
	}
	if !strings.Contains(nextOutput, nextPath) {
		t.Fatalf("expected command output to reference %s, got %q", nextPath, nextOutput)
	}

	configDir, err := lockd.DefaultConfigDir()
	if err != nil {
		t.Fatalf("default config dir: %v", err)
	}

	clientOut := runAuthCommand(t, "new", "client", "--cn", "lockd-client-test")

	clientPath := filepath.Join(configDir, "client.pem")
	if _, err := os.Stat(clientPath); err != nil {
		t.Fatalf("expected client bundle at %s: %v", clientPath, err)
	}
	if !strings.Contains(clientOut, clientPath) {
		t.Fatalf("expected command output to reference %s, got %q", clientPath, clientOut)
	}
	clientBundle, err := tlsutil.LoadClientBundle(clientPath)
	if err != nil {
		t.Fatalf("load client bundle: %v", err)
	}
	if len(clientBundle.CACerts) == 0 {
		t.Fatalf("expected ca certificate embedded in client bundle")
	}
	expectedSDKURI, err := lockd.SPIFFEURIForRole(lockd.ClientBundleRoleSDK, "lockd-client-test")
	if err != nil {
		t.Fatalf("spiffe uri: %v", err)
	}
	if !containsURI(clientBundle.ClientCert.URIs, expectedSDKURI) {
		t.Fatalf("expected sdk spiffe uri %s in client bundle", expectedSDKURI.String())
	}
	expectedDefaultClaim, err := url.Parse("lockd://ns/default?perm=rw")
	if err != nil {
		t.Fatalf("parse default claim uri: %v", err)
	}
	if !containsURI(clientBundle.ClientCert.URIs, expectedDefaultClaim) {
		t.Fatalf("expected default namespace claim %s in client bundle", expectedDefaultClaim.String())
	}
	clientSerial := strings.ToLower(clientBundle.ClientCert.SerialNumber.Text(16))

	tcOut := runAuthCommand(t, "new", "tcclient", "--cn", "lockd-tc-client-test")
	tcPath := filepath.Join(configDir, "tc-client.pem")
	if _, err := os.Stat(tcPath); err != nil {
		t.Fatalf("expected tc client bundle at %s: %v", tcPath, err)
	}
	if !strings.Contains(tcOut, tcPath) {
		t.Fatalf("expected command output to reference %s, got %q", tcPath, tcOut)
	}
	tcBundle, err := tlsutil.LoadClientBundle(tcPath)
	if err != nil {
		t.Fatalf("load tc client bundle: %v", err)
	}
	expectedTCURI, err := lockd.SPIFFEURIForRole(lockd.ClientBundleRoleTC, "lockd-tc-client-test")
	if err != nil {
		t.Fatalf("spiffe uri: %v", err)
	}
	if !containsURI(tcBundle.ClientCert.URIs, expectedTCURI) {
		t.Fatalf("expected tc spiffe uri %s in client bundle", expectedTCURI.String())
	}
	expectedAllClaim, err := url.Parse("lockd://ns/ALL?perm=rw")
	if err != nil {
		t.Fatalf("parse all claim uri: %v", err)
	}
	if !containsURI(tcBundle.ClientCert.URIs, expectedAllClaim) {
		t.Fatalf("expected ALL namespace claim %s in tc client bundle", expectedAllClaim.String())
	}

	scopedClientPath := filepath.Join(configDir, "scoped-client.pem")
	runAuthCommand(t, "new", "client", "--out", scopedClientPath, "--cn", "lockd-client-scoped", "-n", "orders=w,stash")
	scopedClientBundle, err := tlsutil.LoadClientBundle(scopedClientPath)
	if err != nil {
		t.Fatalf("load scoped client bundle: %v", err)
	}
	expectedOrdersW, err := url.Parse("lockd://ns/orders?perm=w")
	if err != nil {
		t.Fatalf("parse orders claim uri: %v", err)
	}
	if !containsURI(scopedClientBundle.ClientCert.URIs, expectedOrdersW) {
		t.Fatalf("expected orders claim %s in scoped client bundle", expectedOrdersW.String())
	}
	expectedStashRW, err := url.Parse("lockd://ns/stash?perm=rw")
	if err != nil {
		t.Fatalf("parse stash claim uri: %v", err)
	}
	if !containsURI(scopedClientBundle.ClientCert.URIs, expectedStashRW) {
		t.Fatalf("expected stash claim %s in scoped client bundle", expectedStashRW.String())
	}
	if containsURI(scopedClientBundle.ClientCert.URIs, expectedDefaultClaim) {
		t.Fatalf("expected scoped client bundle to omit default claim %s when explicit namespace flags are provided", expectedDefaultClaim.String())
	}

	resolvedSDK, err := lockd.ResolveClientBundlePath(lockd.ClientBundleRoleSDK, "")
	if err != nil {
		t.Fatalf("resolve sdk bundle: %v", err)
	}
	if resolvedSDK != clientPath {
		t.Fatalf("expected sdk bundle %s, got %s", clientPath, resolvedSDK)
	}
	resolvedTC, err := lockd.ResolveClientBundlePath(lockd.ClientBundleRoleTC, "")
	if err != nil {
		t.Fatalf("resolve tc bundle: %v", err)
	}
	if resolvedTC != tcPath {
		t.Fatalf("expected tc bundle %s, got %s", tcPath, resolvedTC)
	}

	nextClientOut := runAuthCommand(t, "new", "client", "--cn", "lockd-client-test")
	nextClientPath := filepath.Join(configDir, "client02.pem")
	if _, err := os.Stat(nextClientPath); err != nil {
		t.Fatalf("expected deduped client bundle at %s: %v", nextClientPath, err)
	}
	if !strings.Contains(nextClientOut, nextClientPath) {
		t.Fatalf("expected command output to reference %s, got %q", nextClientPath, nextClientOut)
	}

	verifyServers := runAuthCommand(t, "verify", "server")
	if !strings.Contains(verifyServers, "Server verification succeeded for 2 bundle(s).") {
		t.Fatalf("expected verify server to report two bundles, got %q", verifyServers)
	}
	clientVerify := runAuthCommand(t, "verify", "client", "--server-in", serverPath, "--in", clientPath)
	if !strings.Contains(clientVerify, "CA serial") {
		t.Fatalf("expected verify client output to include CA serial, got %q", clientVerify)
	}
	runAuthCommand(t, "verify", "client", "--server-in", serverPath, "--in", nextClientPath)

	inspectServer := runAuthCommand(t, "inspect", "server", "--in", serverPath)
	if !strings.Contains(inspectServer, "Bundle: "+serverPath) {
		t.Fatalf("expected inspect server output to reference %s, got %q", serverPath, inspectServer)
	}
	inspectAllServers := runAuthCommand(t, "inspect", "server")
	if !strings.Contains(inspectAllServers, "Bundle: "+serverPath) || !strings.Contains(inspectAllServers, "Bundle: "+nextPath) {
		t.Fatalf("expected inspect server default output to include both bundles, got %q", inspectAllServers)
	}
	inspectClient := runAuthCommand(t, "inspect", "client", "--in", clientPath)
	if !strings.Contains(inspectClient, "Client bundle: "+clientPath) {
		t.Fatalf("expected inspect client output to reference %s, got %q", clientPath, inspectClient)
	}

	revokeOut := runAuthCommand(t, "revoke", "client", clientSerial)
	if !strings.Contains(revokeOut, "denylist propagated") {
		t.Fatalf("expected revoke output to mention propagation, got %q", revokeOut)
	}
	bundle, err = tlsutil.LoadBundle(serverPath, "")
	if err != nil {
		t.Fatalf("reload server bundle: %v", err)
	}
	if _, ok := bundle.Denylist[clientSerial]; !ok {
		t.Fatalf("expected server.pem denylist to contain %s", clientSerial)
	}
	otherBundle, err := tlsutil.LoadBundle(nextPath, "")
	if err != nil {
		t.Fatalf("reload server02 bundle: %v", err)
	}
	if _, ok := otherBundle.Denylist[clientSerial]; !ok {
		t.Fatalf("expected server02.pem denylist to contain %s", clientSerial)
	}

	verifyErr := runAuthCommandExpectError(t, "verify", "client", "--server-in", serverPath, "--in", clientPath)
	if !strings.Contains(strings.ToLower(verifyErr), "revoked") {
		t.Fatalf("expected verify client error to mention revocation, got %q", verifyErr)
	}

	thirdOut := runAuthCommand(t, "new", "server", "--cn", "lockd-server-test", "--hosts", "127.0.0.1")
	thirdPath := filepath.Join(configDir, "server03.pem")
	if _, err := os.Stat(thirdPath); err != nil {
		t.Fatalf("expected third server bundle at %s: %v", thirdPath, err)
	}
	if !strings.Contains(thirdOut, thirdPath) {
		t.Fatalf("expected command output to reference %s, got %q", thirdPath, thirdOut)
	}
	verifyThird := runAuthCommand(t, "verify", "server", "--in", thirdPath)
	if !strings.Contains(verifyThird, "Server verification succeeded for 1 bundle(s).") {
		t.Fatalf("expected verify server to succeed for server03, got %q", verifyThird)
	}

	noPropOut := runAuthCommand(t, "revoke", "client", "--server-in", thirdPath, "--propagate=false", "beadface")
	if strings.Contains(noPropOut, "denylist propagated") {
		t.Fatalf("expected no propagation message when --propagate=false, got %q", noPropOut)
	}
	thirdBundle, err := tlsutil.LoadBundle(thirdPath, "")
	if err != nil {
		t.Fatalf("reload server03 bundle: %v", err)
	}
	if _, ok := thirdBundle.Denylist["beadface"]; !ok {
		t.Fatalf("expected server03.pem denylist to contain beadface")
	}
	otherBundle, err = tlsutil.LoadBundle(nextPath, "")
	if err != nil {
		t.Fatalf("reload server02 bundle second time: %v", err)
	}
	if _, ok := otherBundle.Denylist["beadface"]; ok {
		t.Fatalf("expected server02.pem denylist to omit beadface when propagate disabled")
	}

	thirdClientVerify := runAuthCommand(t, "verify", "client", "--server-in", thirdPath, "--in", nextClientPath)
	if !strings.Contains(thirdClientVerify, "CA serial") {
		t.Fatalf("expected verify client output to include CA serial for third server, got %q", thirdClientVerify)
	}
}

func TestAuthNewServerMissingCA(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	cmd := newAuthCommand()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"new", "server", "--cn", "missing-ca-test"})
	err := cmd.Execute()
	if err == nil {
		t.Fatalf("expected failure when issuing server certificate without ca")
	}
	if !strings.Contains(buf.String(), "load ca") {
		t.Fatalf("expected error output to mention load ca, got %q", buf.String())
	}
	if !strings.Contains(buf.String(), "lockd auth new ca") {
		t.Fatalf("expected error output to hint at creating a CA, got %q", buf.String())
	}
}

func TestAuthNewClientNamespaceAliases(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	runAuthCommand(t, "new", "ca", "--cn", "lockd-ca-test")

	cfgDir, err := lockd.DefaultConfigDir()
	if err != nil {
		t.Fatalf("default config dir: %v", err)
	}

	aliasPath := filepath.Join(cfgDir, "client-aliases.pem")
	runAuthCommand(t, "new", "client", "--out", aliasPath, "--read-all", "--write-all")
	aliasBundle, err := tlsutil.LoadClientBundle(aliasPath)
	if err != nil {
		t.Fatalf("load alias bundle: %v", err)
	}
	expectedAllW, err := url.Parse("lockd://ns/ALL?perm=w")
	if err != nil {
		t.Fatalf("parse ALL=w uri: %v", err)
	}
	if !containsURI(aliasBundle.ClientCert.URIs, expectedAllW) {
		t.Fatalf("expected ALL write claim %s in alias bundle", expectedAllW.String())
	}
	expectedDefault, err := url.Parse("lockd://ns/default?perm=rw")
	if err != nil {
		t.Fatalf("parse default uri: %v", err)
	}
	if containsURI(aliasBundle.ClientCert.URIs, expectedDefault) {
		t.Fatalf("expected explicit alias claims to suppress implicit default claim %s", expectedDefault.String())
	}

	rwAllPath := filepath.Join(cfgDir, "client-rw-all.pem")
	runAuthCommand(t, "new", "client", "--out", rwAllPath, "--rw-all")
	rwAllBundle, err := tlsutil.LoadClientBundle(rwAllPath)
	if err != nil {
		t.Fatalf("load rw-all bundle: %v", err)
	}
	expectedAllRW, err := url.Parse("lockd://ns/ALL?perm=rw")
	if err != nil {
		t.Fatalf("parse ALL=rw uri: %v", err)
	}
	if !containsURI(rwAllBundle.ClientCert.URIs, expectedAllRW) {
		t.Fatalf("expected ALL rw claim %s in rw-all bundle", expectedAllRW.String())
	}
}

func TestAuthNewClientNamespaceRepeatImplicitRW(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	runAuthCommand(t, "new", "ca", "--cn", "lockd-ca-test")

	cfgDir, err := lockd.DefaultConfigDir()
	if err != nil {
		t.Fatalf("default config dir: %v", err)
	}
	path := filepath.Join(cfgDir, "client-repeat.pem")
	runAuthCommand(t, "new", "client", "--out", path, "-n", "default,orders", "-n", "stash")
	bundle, err := tlsutil.LoadClientBundle(path)
	if err != nil {
		t.Fatalf("load repeat bundle: %v", err)
	}

	expected := []string{
		"lockd://ns/default?perm=rw",
		"lockd://ns/orders?perm=rw",
		"lockd://ns/stash?perm=rw",
	}
	for _, raw := range expected {
		claim, parseErr := url.Parse(raw)
		if parseErr != nil {
			t.Fatalf("parse expected claim %q: %v", raw, parseErr)
		}
		if !containsURI(bundle.ClientCert.URIs, claim) {
			t.Fatalf("expected claim %s in repeat bundle", claim.String())
		}
	}
}

func TestAuthNewClientNamespaceDuplicateStrongestWins(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	runAuthCommand(t, "new", "ca", "--cn", "lockd-ca-test")

	cfgDir, err := lockd.DefaultConfigDir()
	if err != nil {
		t.Fatalf("default config dir: %v", err)
	}
	path := filepath.Join(cfgDir, "client-strongest.pem")
	runAuthCommand(t, "new", "client", "--out", path, "-n", "orders=r", "-n", "orders=w", "-n", "orders=rw", "-n", "orders=w")
	bundle, err := tlsutil.LoadClientBundle(path)
	if err != nil {
		t.Fatalf("load strongest bundle: %v", err)
	}
	expectedRW, err := url.Parse("lockd://ns/orders?perm=rw")
	if err != nil {
		t.Fatalf("parse expected claim: %v", err)
	}
	if !containsURI(bundle.ClientCert.URIs, expectedRW) {
		t.Fatalf("expected strongest claim %s in bundle", expectedRW.String())
	}
	if got := countClaimURIsWithPrefix(bundle.ClientCert.URIs, "lockd://ns/orders?perm="); got != 1 {
		t.Fatalf("expected one orders claim after merge, got %d", got)
	}
}

func TestAuthNewClientNamespaceInvalidInput(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	runAuthCommand(t, "new", "ca", "--cn", "lockd-ca-test")

	out := runAuthCommandExpectError(t, "new", "client", "-n", "default==rw")
	if !strings.Contains(out, "invalid namespace claim") {
		t.Fatalf("expected invalid namespace claim error, got %q", out)
	}
	out = runAuthCommandExpectError(t, "new", "client", "-n", "default=x")
	if !strings.Contains(out, "invalid namespace claim") {
		t.Fatalf("expected invalid namespace permission error, got %q", out)
	}
}

func containsURI(uris []*url.URL, expected *url.URL) bool {
	if expected == nil {
		return false
	}
	expectedStr := expected.String()
	for _, uri := range uris {
		if uri == nil {
			continue
		}
		if uri.String() == expectedStr {
			return true
		}
	}
	return false
}

func countClaimURIsWithPrefix(uris []*url.URL, prefix string) int {
	count := 0
	for _, uri := range uris {
		if uri == nil {
			continue
		}
		if strings.HasPrefix(uri.String(), prefix) {
			count++
		}
	}
	return count
}
