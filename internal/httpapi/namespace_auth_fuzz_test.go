package httpapi

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http/httptest"
	"net/url"
	"testing"

	"pkt.systems/lockd/internal/nsauth"
)

func FuzzAuthorizeNamespaceFromSAN(f *testing.F) {
	f.Add("spiffe", "lockd", "/sdk/client-a", "", "lockd", "ns", "/default", "perm=rw", "default", uint8(3), true)
	f.Add("spiffe", "lockd", "/sdk/client-a", "", "lockd", "ns", "/default", "perm=r&perm=rw", "default", uint8(2), true)
	f.Add("lockd", "ns", "/default", "perm=rw", "", "", "", "", "default", uint8(1), true)
	f.Add("", "", "", "", "", "", "", "", "default", uint8(0), false)

	f.Fuzz(func(t *testing.T,
		scheme1 string,
		host1 string,
		path1 string,
		query1 string,
		scheme2 string,
		host2 string,
		path2 string,
		query2 string,
		namespace string,
		accessRaw uint8,
		withCert bool,
	) {
		h := &Handler{enforceClientIdentity: true}
		access := nsauth.Access(accessRaw % 4)

		var cert *x509.Certificate
		if withCert {
			cert = &x509.Certificate{
				URIs: []*url.URL{
					{Scheme: scheme1, Host: host1, Path: path1, RawQuery: query1},
					{Scheme: scheme2, Host: host2, Path: path2, RawQuery: query2},
				},
			}
		}

		req := httptest.NewRequest("POST", "/v1/acquire", nil)
		if cert != nil {
			req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
		}

		err := h.authorizeNamespace(req, namespace, access)
		gotCode := httpErrorCode(err)
		wantCode := expectedNamespaceAuthCode(cert, namespace, access)
		if gotCode != wantCode {
			t.Fatalf("authorizeNamespace mismatch: got=%q want=%q access=%v namespace=%q cert=%#v err=%v", gotCode, wantCode, access, namespace, cert, err)
		}
	})
}

func FuzzAuthorizeAllNamespacesFromSAN(f *testing.F) {
	f.Add("spiffe", "lockd", "/tc/client-a", "", "lockd", "ns", "/ALL", "perm=rw", uint8(3), true)
	f.Add("spiffe", "lockd", "/tc/client-a", "", "lockd", "ns", "/default", "perm=rw", uint8(3), true)
	f.Add("spiffe", "lockd", "/tc/client-a", "", "lockd", "ns", "/ALL", "perm=w", uint8(1), true)
	f.Add("", "", "", "", "", "", "", "", uint8(0), false)

	f.Fuzz(func(t *testing.T,
		scheme1 string,
		host1 string,
		path1 string,
		query1 string,
		scheme2 string,
		host2 string,
		path2 string,
		query2 string,
		accessRaw uint8,
		withCert bool,
	) {
		h := &Handler{enforceClientIdentity: true}
		access := nsauth.Access(accessRaw % 4)

		var cert *x509.Certificate
		if withCert {
			cert = &x509.Certificate{
				URIs: []*url.URL{
					{Scheme: scheme1, Host: host1, Path: path1, RawQuery: query1},
					{Scheme: scheme2, Host: host2, Path: path2, RawQuery: query2},
				},
			}
		}

		req := httptest.NewRequest("POST", "/v1/txn/replay", nil)
		if cert != nil {
			req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
		}

		err := h.authorizeAllNamespaces(req, access)
		gotCode := httpErrorCode(err)
		wantCode := expectedAllNamespacesAuthCode(cert, access)
		if gotCode != wantCode {
			t.Fatalf("authorizeAllNamespaces mismatch: got=%q want=%q access=%v cert=%#v err=%v", gotCode, wantCode, access, cert, err)
		}
	})
}

func expectedNamespaceAuthCode(cert *x509.Certificate, namespace string, access nsauth.Access) string {
	if cert == nil {
		return "namespace_forbidden"
	}
	claims, err := nsauth.ParseCertificate(cert)
	if err != nil {
		return "invalid_namespace_claims"
	}
	if !claims.HasSPIFFEID || !claims.HasNamespaceClaims() {
		return "namespace_forbidden"
	}
	if !claims.Allows(namespace, access) {
		return "namespace_forbidden"
	}
	return ""
}

func expectedAllNamespacesAuthCode(cert *x509.Certificate, access nsauth.Access) string {
	if cert == nil {
		return "namespace_forbidden"
	}
	claims, err := nsauth.ParseCertificate(cert)
	if err != nil {
		return "invalid_namespace_claims"
	}
	if !claims.HasSPIFFEID || !claims.HasNamespaceClaims() {
		return "namespace_forbidden"
	}
	if !claims.AllowsWildcard(access) {
		return "namespace_forbidden"
	}
	return ""
}

func httpErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var httpErr httpError
	if errors.As(err, &httpErr) {
		return httpErr.Code
	}
	return "non_http_error"
}
