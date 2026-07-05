package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"time"
)

const lockdServerSPIFFEPrefix = "spiffe://lockd/server/"

// VerifyLockdServerConnection verifies a lockd server certificate against the
// configured trust roots and accepted lockd server identity forms.
func VerifyLockdServerConnection(state tls.ConnectionState, roots *x509.CertPool) error {
	if len(state.PeerCertificates) == 0 {
		return errors.New("mtls: missing server certificate")
	}
	leaf := state.PeerCertificates[0]
	opts := lockdServerVerifyOptions(roots, state.PeerCertificates[1:], "")
	if _, err := leaf.Verify(opts); err != nil {
		return fmt.Errorf("mtls: verify server certificate: %w", err)
	}
	if hasLockdServerSPIFFE(leaf) || hasLegacyWildcardSAN(leaf) {
		return nil
	}
	return errors.New("mtls: server certificate missing lockd server SPIFFE identity")
}

func lockdServerVerifyOptions(roots *x509.CertPool, intermediates []*x509.Certificate, dnsName string) x509.VerifyOptions {
	opts := x509.VerifyOptions{
		Roots:         roots,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		Intermediates: x509.NewCertPool(),
		CurrentTime:   time.Now(),
		DNSName:       dnsName,
	}
	for _, cert := range intermediates {
		opts.Intermediates.AddCert(cert)
	}
	return opts
}

func hasLockdServerSPIFFE(cert *x509.Certificate) bool {
	if cert == nil {
		return false
	}
	for _, uri := range cert.URIs {
		if uri == nil {
			continue
		}
		if strings.HasPrefix(uri.String(), lockdServerSPIFFEPrefix) {
			return true
		}
	}
	return false
}

func hasLegacyWildcardSAN(cert *x509.Certificate) bool {
	if cert == nil {
		return false
	}
	for _, name := range cert.DNSNames {
		if strings.TrimSpace(name) == "*" {
			return true
		}
	}
	return false
}
