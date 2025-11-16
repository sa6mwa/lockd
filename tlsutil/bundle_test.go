package tlsutil

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/internal/cryptoutil"
)

func TestLoadBundle(t *testing.T) {
	dir := t.TempDir()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate ca key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "Lockd CA",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}

	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: "Lockd Server",
		},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}

	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(caKey)})
	caBundle, err := EncodeCABundle(caCertPEM, caKeyPEM)
	if err != nil {
		t.Fatalf("encode ca bundle: %v", err)
	}
	material, err := cryptoutil.MetadataMaterialFromBytes(caBundle)
	if err != nil {
		t.Fatalf("extract ca material: %v", err)
	}
	serverCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER})
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})
	serverBundle, err := EncodeServerBundle(caCertPEM, nil, serverCertPEM, serverKeyPEM, nil)
	if err != nil {
		t.Fatalf("encode server bundle: %v", err)
	}
	serverBundle, err = cryptoutil.ApplyMetadataMaterial(serverBundle, material)
	if err != nil {
		t.Fatalf("apply material: %v", err)
	}

	bundlePath := filepath.Join(dir, "bundle.pem")
	if err := os.WriteFile(bundlePath, serverBundle, 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}

	denyPath := filepath.Join(dir, "deny.txt")
	denyContent := serverTemplate.SerialNumber.Text(16)
	if err := os.WriteFile(denyPath, []byte(denyContent+"\n#comment\n"), 0o600); err != nil {
		t.Fatalf("write denylist: %v", err)
	}

	loaded, err := LoadBundle(bundlePath, denyPath)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	if loaded.CAPool == nil || len(loaded.CAPool.Subjects()) == 0 {
		t.Fatal("expected CA pool populated")
	}
	if len(loaded.ServerCertificate.Certificate) == 0 {
		t.Fatal("expected server certificate")
	}
	if _, ok := loaded.Denylist[strings.ToLower(denyContent)]; !ok {
		t.Fatalf("expected denylist entry for %s", denyContent)
	}
}
