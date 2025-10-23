package tlsutil

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"pkt.systems/kryptograf/keymgmt"

	"pkt.systems/lockd/internal/cryptoutil"
)

// Bundle represents the parsed contents of a combined PEM bundle containing
// CA certificates, a server certificate, and the associated private key.
type Bundle struct {
	ServerCertificate  tls.Certificate
	ServerCert         *x509.Certificate
	ServerCertPEM      []byte
	ServerKeyPEM       []byte
	CACertificate      *x509.Certificate
	CACertPEM          []byte
	CAPrivateKey       crypto.Signer
	CAPrivateKeyPEM    []byte
	CAPool             *x509.CertPool
	Denylist           map[string]struct{}
	DenylistEntries    []string
	MetadataRootKey    keymgmt.RootKey
	MetadataDescriptor keymgmt.Descriptor
}

// LoadBundle parses a lockd server bundle from path, optionally applying a
// denylist of revoked serial numbers.
func LoadBundle(bundlePath, denylistPath string) (*Bundle, error) {
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		return nil, fmt.Errorf("read bundle: %w", err)
	}
	parsed, err := parseBundle(data)
	if err != nil {
		return nil, err
	}
	if parsed.ServerCertPEM == nil || parsed.ServerKeyPEM == nil {
		return nil, errors.New("bundle: missing server certificate or key")
	}
	tlsCert, err := tls.X509KeyPair(parsed.ServerCertPEM, parsed.ServerKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("bundle: build key pair: %w", err)
	}
	caPool := x509.NewCertPool()
	for _, ca := range parsed.CACerts {
		caPool.AddCert(ca)
	}
	entries := append([]string(nil), parsed.DenylistEntries...)
	external := map[string]struct{}{}
	if denylistPath != "" {
		deny, err := loadDenylist(denylistPath)
		if err != nil {
			return nil, err
		}
		for serial := range deny {
			entries = append(entries, serial)
			external[serial] = struct{}{}
		}
	}
	entries = NormalizeSerials(entries)
	denyMap := make(map[string]struct{}, len(entries))
	for _, serial := range entries {
		denyMap[serial] = struct{}{}
	}
	serverCert := parsed.ServerCert
	if serverCert == nil && len(parsed.ServerCertPEM) > 0 {
		if cert, err := FirstCertificateFromPEM(parsed.ServerCertPEM); err == nil {
			serverCert = cert
		}
	}
	material, err := cryptoutil.MetadataMaterialFromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("bundle: kryptograf material: %w", err)
	}
	return &Bundle{
		ServerCertificate:  tlsCert,
		ServerCert:         serverCert,
		ServerCertPEM:      parsed.ServerCertPEM,
		ServerKeyPEM:       parsed.ServerKeyPEM,
		CACertificate:      parsed.CACert,
		CACertPEM:          parsed.CACertPEM,
		CAPrivateKey:       parsed.CAPrivateKey,
		CAPrivateKeyPEM:    parsed.CAPrivateKeyPEM,
		CAPool:             caPool,
		Denylist:           denyMap,
		DenylistEntries:    entries,
		MetadataRootKey:    material.Root,
		MetadataDescriptor: material.Descriptor,
	}, nil
}

func loadDenylist(path string) (map[string]struct{}, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open denylist: %w", err)
	}
	defer f.Close()
	buf, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read denylist: %w", err)
	}
	lines := strings.Split(string(buf), "\n")
	out := make(map[string]struct{}, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out[strings.ToLower(line)] = struct{}{}
	}
	return out, nil
}

type parsedBundle struct {
	CACerts         []*x509.Certificate
	CACert          *x509.Certificate
	CACertPEM       []byte
	CAPrivateKey    crypto.Signer
	CAPrivateKeyPEM []byte
	ServerCert      *x509.Certificate
	ServerCertPEM   []byte
	ServerKeyPEM    []byte
	DenylistEntries []string
}

func parseBundle(data []byte) (*parsedBundle, error) {
	result := &parsedBundle{}
	var privKeys []struct {
		pem    []byte
		signer crypto.Signer
	}
	var leafCerts []*x509.Certificate

	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		switch block.Type {
		case "CERTIFICATE":
			pemBytes := pem.EncodeToMemory(block)
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("bundle: parse certificate: %w", err)
			}
			if cert.IsCA {
				result.CACerts = append(result.CACerts, cert)
				if result.CACert == nil {
					result.CACert = cert
					result.CACertPEM = pemBytes
				}
			} else {
				leafCerts = append(leafCerts, cert)
				if result.ServerCertPEM == nil {
					result.ServerCertPEM = pemBytes
				} else {
					result.ServerCertPEM = append(result.ServerCertPEM, pemBytes...)
				}
			}
		case "PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY":
			signer, err := parsePrivateKey(block)
			if err != nil {
				return nil, fmt.Errorf("bundle: parse private key: %w", err)
			}
			privKeys = append(privKeys, struct {
				pem    []byte
				signer crypto.Signer
			}{pem: pem.EncodeToMemory(block), signer: signer})
		case "LOCKD DENYLIST":
			lines := strings.Split(string(block.Bytes), "\n")
			for _, line := range lines {
				serial := strings.TrimSpace(strings.ToLower(line))
				if serial == "" {
					continue
				}
				result.DenylistEntries = append(result.DenylistEntries, serial)
			}
		default:
			// ignore
		}
	}

	if len(leafCerts) == 0 {
		return nil, errors.New("bundle: no server certificate found")
	}
	leaf := leafCerts[0]
	result.ServerCert = leaf

	for _, key := range privKeys {
		if publicKeysEqual(leaf.PublicKey, key.signer.Public()) {
			result.ServerKeyPEM = key.pem
			break
		}
	}
	if result.ServerKeyPEM == nil {
		return nil, errors.New("bundle: unable to match server key")
	}

	if result.CACert != nil {
		for _, key := range privKeys {
			if publicKeysEqual(result.CACert.PublicKey, key.signer.Public()) {
				result.CAPrivateKey = key.signer
				result.CAPrivateKeyPEM = key.pem
				break
			}
		}
	}

	return result, nil
}

func parsePrivateKey(block *pem.Block) (crypto.Signer, error) {
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
			return k, nil
		}
		if k, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
			return k, nil
		}
		return nil, err
	}
	switch k := key.(type) {
	case ed25519.PrivateKey:
		return k, nil
	case *rsa.PrivateKey:
		return k, nil
	case *ecdsa.PrivateKey:
		return k, nil
	default:
		return nil, fmt.Errorf("unsupported private key type %T", key)
	}
}

func publicKeysEqual(a, b crypto.PublicKey) bool {
	switch ak := a.(type) {
	case ed25519.PublicKey:
		bk, ok := b.(ed25519.PublicKey)
		return ok && bytes.Equal(ak, bk)
	case *rsa.PublicKey:
		bk, ok := b.(*rsa.PublicKey)
		if !ok {
			return false
		}
		return ak.E == bk.E && ak.N.Cmp(bk.N) == 0
	case *ecdsa.PublicKey:
		bk, ok := b.(*ecdsa.PublicKey)
		if !ok {
			return false
		}
		return ak.Curve == bk.Curve && ak.X.Cmp(bk.X) == 0 && ak.Y.Cmp(bk.Y) == 0
	default:
		return false
	}
}

// NormalizeSerials lowercases, trims, de-duplicates, and sorts serials.
func NormalizeSerials(serials []string) []string {
	set := make(map[string]struct{}, len(serials))
	for _, s := range serials {
		s = strings.TrimSpace(strings.ToLower(s))
		if s == "" {
			continue
		}
		set[s] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// EncodeServerBundle encodes the server bundle components into PEM.
func EncodeServerBundle(caCertPEM, caKeyPEM, serverCertPEM, serverKeyPEM []byte, denylist []string) ([]byte, error) {
	if len(caCertPEM) == 0 || len(serverCertPEM) == 0 || len(serverKeyPEM) == 0 {
		return nil, errors.New("encode bundle: missing components")
	}
	denylist = NormalizeSerials(denylist)
	var buf bytes.Buffer
	buf.Write(caCertPEM)
	if len(caKeyPEM) > 0 {
		buf.Write(caKeyPEM)
	}
	buf.Write(serverCertPEM)
	buf.Write(serverKeyPEM)
	if len(denylist) > 0 {
		block := &pem.Block{Type: "LOCKD DENYLIST", Bytes: []byte(strings.Join(denylist, "\n"))}
		buf.Write(pem.EncodeToMemory(block))
	}
	return buf.Bytes(), nil
}

// EncodeClientBundle encodes a client PEM (CA cert + client cert + key).
func EncodeClientBundle(caCertPEM, clientCertPEM, clientKeyPEM []byte) ([]byte, error) {
	if len(clientCertPEM) == 0 || len(clientKeyPEM) == 0 {
		return nil, errors.New("encode client bundle: missing components")
	}
	var buf bytes.Buffer
	if len(caCertPEM) > 0 {
		buf.Write(caCertPEM)
	}
	buf.Write(clientCertPEM)
	buf.Write(clientKeyPEM)
	return buf.Bytes(), nil
}

// EncodeCABundle concatenates the CA certificate and private key into a PEM file.
func EncodeCABundle(caCertPEM, caKeyPEM []byte) ([]byte, error) {
	if len(caCertPEM) == 0 || len(caKeyPEM) == 0 {
		return nil, errors.New("encode ca bundle: missing components")
	}
	var base bytes.Buffer
	base.Write(caCertPEM)
	base.Write(caKeyPEM)

	var out []byte
	store, err := keymgmt.LoadPEMInto(base.Bytes(), &out)
	if err != nil {
		return nil, fmt.Errorf("encode ca bundle: load kryptograf store: %w", err)
	}

	root, err := store.EnsureRootKey()
	if err != nil {
		return nil, fmt.Errorf("encode ca bundle: ensure root key: %w", err)
	}

	caID, err := cryptoutil.CACertificateID(caCertPEM)
	if err != nil {
		return nil, fmt.Errorf("encode ca bundle: derive certificate id: %w", err)
	}

	if _, err := store.EnsureDescriptor(cryptoutil.MetadataDescriptorName, root, []byte(caID)); err != nil {
		return nil, fmt.Errorf("encode ca bundle: ensure descriptor: %w", err)
	}

	if err := store.Commit(); err != nil {
		return nil, fmt.Errorf("encode ca bundle: commit kryptograf store: %w", err)
	}

	if len(out) == 0 {
		return base.Bytes(), nil
	}
	return out, nil
}

// FirstCertificateFromPEM returns the first certificate contained in pemBytes.
func FirstCertificateFromPEM(pemBytes []byte) (*x509.Certificate, error) {
	rest := pemBytes
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			return x509.ParseCertificate(block.Bytes)
		}
	}
	return nil, errors.New("no certificate found")
}
