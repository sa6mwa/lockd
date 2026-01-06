package tcclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"time"

	"pkt.systems/lockd/tlsutil"
)

// Config configures a TC-aware HTTP client.
type Config struct {
	DisableMTLS      bool
	BundlePath       string
	ServerBundlePath string
	ServerBundle     *tlsutil.Bundle
	Timeout          time.Duration
	TrustPEM         [][]byte
}

// NewHTTPClient builds an HTTP client with optional mTLS and custom trust roots.
func NewHTTPClient(cfg Config) (*http.Client, error) {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("tcclient: http transport unexpected type")
	}
	tr := transport.Clone()
	if !cfg.DisableMTLS {
		if cfg.ServerBundle != nil || cfg.ServerBundlePath != "" {
			serverBundle := cfg.ServerBundle
			if serverBundle == nil {
				var err error
				serverBundle, err = tlsutil.LoadBundle(cfg.ServerBundlePath, "")
				if err != nil {
					return nil, fmt.Errorf("tcclient: load server bundle: %w", err)
				}
			}
			roots := x509.NewCertPool()
			if serverBundle.CAPool != nil {
				roots = serverBundle.CAPool.Clone()
			}
			for _, blob := range cfg.TrustPEM {
				if len(blob) == 0 {
					continue
				}
				roots.AppendCertsFromPEM(blob)
			}
			tr.TLSClientConfig = buildServerTLS(serverBundle, roots)
		} else if cfg.BundlePath != "" {
			clientBundle, err := tlsutil.LoadClientBundle(cfg.BundlePath)
			if err != nil {
				return nil, fmt.Errorf("tcclient: load tc client bundle: %w", err)
			}
			roots := x509.NewCertPool()
			for _, cert := range clientBundle.CACerts {
				roots.AddCert(cert)
			}
			for _, blob := range cfg.TrustPEM {
				if len(blob) == 0 {
					continue
				}
				roots.AppendCertsFromPEM(blob)
			}
			tr.TLSClientConfig = buildClientTLS(clientBundle, roots)
		} else {
			return nil, errors.New("tcclient: client or server bundle required for mTLS")
		}
	} else {
		tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return &http.Client{
		Timeout:   cfg.Timeout,
		Transport: tr,
	}, nil
}

func buildClientTLS(bundle *tlsutil.ClientBundle, roots *x509.CertPool) *tls.Config {
	if roots == nil {
		roots = bundle.CAPool
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		Certificates:       []tls.Certificate{bundle.Certificate},
		RootCAs:            roots,
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return verifyServerCertificate(rawCerts, roots)
		},
	}
}

func buildServerTLS(bundle *tlsutil.Bundle, roots *x509.CertPool) *tls.Config {
	if roots == nil {
		roots = bundle.CAPool
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		Certificates:       []tls.Certificate{bundle.ServerCertificate},
		RootCAs:            roots,
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return verifyServerCertificate(rawCerts, roots)
		},
	}
}

func verifyServerCertificate(rawCerts [][]byte, roots *x509.CertPool) error {
	if len(rawCerts) == 0 {
		return errors.New("mtls: missing server certificate")
	}
	certs := make([]*x509.Certificate, 0, len(rawCerts))
	for _, raw := range rawCerts {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return fmt.Errorf("mtls: parse server certificate: %w", err)
		}
		certs = append(certs, cert)
	}
	leaf := certs[0]
	opts := x509.VerifyOptions{
		Roots:         roots,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		Intermediates: x509.NewCertPool(),
		CurrentTime:   time.Now(),
	}
	for _, cert := range certs[1:] {
		opts.Intermediates.AddCert(cert)
	}
	if _, err := leaf.Verify(opts); err != nil {
		return fmt.Errorf("mtls: verify server certificate: %w", err)
	}
	return nil
}
