package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// Bundle represents the parsed contents of a combined PEM bundle containing
// CA certificates, a server certificate, and the associated private key.
type Bundle struct {
	ServerCertificate tls.Certificate
	CAPool            *x509.CertPool
	Denylist          map[string]struct{}
}

// LoadBundle parses a lockd server bundle from path, optionally applying a
// denylist of revoked serial numbers.
func LoadBundle(bundlePath, denylistPath string) (*Bundle, error) {
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		return nil, fmt.Errorf("read bundle: %w", err)
	}
	cas, serverCert, serverKey, err := parseBundle(data)
	if err != nil {
		return nil, err
	}
	if serverCert == nil || serverKey == nil {
		return nil, errors.New("bundle: missing server certificate or key")
	}
	tlsCert, err := tls.X509KeyPair(serverCert, serverKey)
	if err != nil {
		return nil, fmt.Errorf("bundle: build key pair: %w", err)
	}
	caPool := x509.NewCertPool()
	for _, ca := range cas {
		caPool.AddCert(ca)
	}
	var deny map[string]struct{}
	if denylistPath != "" {
		deny, err = loadDenylist(denylistPath)
		if err != nil {
			return nil, err
		}
	}
	return &Bundle{
		ServerCertificate: tlsCert,
		CAPool:            caPool,
		Denylist:          deny,
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

func parseBundle(data []byte) ([]*x509.Certificate, []byte, []byte, error) {
	var (
		cas         []*x509.Certificate
		serverCert  []byte
		serverKey   []byte
		leafCert    *x509.Certificate
		privateKeys = make([][]byte, 0, 2)
	)

	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		switch block.Type {
		case "CERTIFICATE":
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("bundle: parse certificate: %w", err)
			}
			if cert.IsCA {
				cas = append(cas, cert)
			} else if leafCert == nil {
				serverCert = pem.EncodeToMemory(block)
				leafCert = cert
			} else {
				// Additional intermediates appended.
				serverCert = append(serverCert, pem.EncodeToMemory(block)...)
			}
		case "PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY":
			privateKeys = append(privateKeys, pem.EncodeToMemory(block))
		default:
			// Ignore unknown blocks.
		}
	}

	if leafCert == nil {
		return cas, nil, nil, errors.New("bundle: no server certificate found")
	}

	// Attempt to match private key by trying all available ones.
	for _, keyPEM := range privateKeys {
		if keyPEM == nil {
			continue
		}
		if _, err := tls.X509KeyPair(serverCert, keyPEM); err == nil {
			serverKey = keyPEM
			break
		}
	}
	if serverKey == nil {
		return cas, nil, nil, errors.New("bundle: unable to match server key")
	}
	return cas, serverCert, serverKey, nil
}
