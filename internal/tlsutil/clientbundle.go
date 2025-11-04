package tlsutil

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
)

// ClientBundle represents a parsed client PEM bundle containing CA certs and
// a client certificate+key pair suitable for building an mTLS HTTP client.
type ClientBundle struct {
	Certificate   tls.Certificate
	ClientCert    *x509.Certificate
	ClientCertPEM []byte
	ClientKeyPEM  []byte
	CACerts       []*x509.Certificate
	CAPool        *x509.CertPool
}

// LoadClientBundle parses a client bundle from path.
func LoadClientBundle(path string) (*ClientBundle, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read client bundle: %w", err)
	}
	return LoadClientBundleFromBytes(data)
}

// LoadClientBundleFromBytes parses a client bundle from the provided byte slice.
func LoadClientBundleFromBytes(data []byte) (*ClientBundle, error) {
	var (
		caCerts       []*x509.Certificate
		caPool        = x509.NewCertPool()
		clientCert    *x509.Certificate
		clientCertPEM []byte
		clientKeyPEM  []byte
		privKeys      []struct {
			signer crypto.Signer
			pem    []byte
		}
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
			pemBytes := pem.EncodeToMemory(block)
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("client bundle: parse certificate: %w", err)
			}
			if cert.IsCA {
				caCerts = append(caCerts, cert)
				caPool.AddCert(cert)
			} else if clientCert == nil {
				clientCert = cert
				clientCertPEM = pemBytes
			} else {
				// append any intermediates to the existing client certificate PEM
				clientCertPEM = append(clientCertPEM, pemBytes...)
			}
		case "PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY":
			key, err := parsePrivateKey(block)
			if err != nil {
				return nil, fmt.Errorf("client bundle: parse private key: %w", err)
			}
			privKeys = append(privKeys, struct {
				signer crypto.Signer
				pem    []byte
			}{signer: key, pem: pem.EncodeToMemory(block)})
		default:
			// ignore additional blocks
		}
	}

	if clientCert == nil {
		return nil, errors.New("client bundle: client certificate not found")
	}
	for _, key := range privKeys {
		if publicKeysEqual(clientCert.PublicKey, key.signer.Public()) {
			clientKeyPEM = key.pem
			break
		}
	}
	if len(clientKeyPEM) == 0 {
		return nil, errors.New("client bundle: matching private key not found")
	}
	if len(caCerts) == 0 {
		return nil, errors.New("client bundle: CA certificate required")
	}

	tlsCert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("client bundle: build key pair: %w", err)
	}
	return &ClientBundle{
		Certificate:   tlsCert,
		ClientCert:    clientCert,
		ClientCertPEM: clientCertPEM,
		ClientKeyPEM:  clientKeyPEM,
		CACerts:       caCerts,
		CAPool:        caPool,
	}, nil
}
