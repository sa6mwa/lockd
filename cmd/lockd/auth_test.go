package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pkt.systems/kryptograf"
	"pkt.systems/kryptograf/keymgmt"
	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/tlsutil"
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
	clientSerial := strings.ToLower(clientBundle.ClientCert.SerialNumber.Text(16))

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
