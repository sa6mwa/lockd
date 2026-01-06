package tccluster

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"strings"
)

// IdentityFromCertificate derives a stable identity string from a certificate.
func IdentityFromCertificate(cert *x509.Certificate) string {
	if cert == nil {
		return ""
	}
	for _, uri := range cert.URIs {
		if uri == nil {
			continue
		}
		if uri.Scheme != "spiffe" || !strings.EqualFold(uri.Host, "lockd") {
			continue
		}
		if !strings.HasPrefix(uri.Path, "/server/") {
			continue
		}
		nodeID := strings.Trim(strings.TrimPrefix(uri.Path, "/server/"), "/")
		if nodeID != "" {
			return nodeID
		}
	}
	return ""
}

// IdentityFromEndpoint derives a stable identity string from an endpoint.
func IdentityFromEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(endpoint))
	return strings.ToLower(hex.EncodeToString(sum[:]))
}
