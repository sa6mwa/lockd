package nsauth

import (
	"crypto/x509"
	"net/url"
	"strings"
	"testing"

	"pkt.systems/lockd/namespaces"
)

type oracleClaims struct {
	hasSPIFFE bool
	hasClaims bool
	exact     map[string]Permission
	wildcard  Permission
}

func FuzzParseCertificateAuthorization(f *testing.F) {
	f.Add("spiffe", "lockd", "/sdk/client-a", "", "lockd", "ns", "/default", "perm=rw", "default", uint8(3))
	f.Add("spiffe", "lockd", "/tc/client-a", "", "lockd", "ns", "/ALL", "perm=w", "orders", uint8(2))
	f.Add("spiffe", "lockd", "/sdk/client-a", "", "lockd", "ns", "/default/nested", "perm=rw", "default", uint8(1))

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
	) {
		cert := &x509.Certificate{
			URIs: []*url.URL{
				{Scheme: scheme1, Host: host1, Path: path1, RawQuery: query1},
				{Scheme: scheme2, Host: host2, Path: path2, RawQuery: query2},
			},
		}
		claims, err := ParseCertificate(cert)
		oracle, oracleErr := oracleParseCertificate(cert)
		if oracleErr != nil {
			if err == nil {
				t.Fatalf("expected parse error, got nil for cert URIs %#v", cert.URIs)
			}
			return
		}
		if err != nil {
			t.Fatalf("unexpected parse error: %v (cert URIs %#v)", err, cert.URIs)
		}

		if claims.HasSPIFFEID != oracle.hasSPIFFE {
			t.Fatalf("spiffe mismatch: got=%v want=%v", claims.HasSPIFFEID, oracle.hasSPIFFE)
		}
		if claims.HasNamespaceClaims() != oracle.hasClaims {
			t.Fatalf("claim presence mismatch: got=%v want=%v", claims.HasNamespaceClaims(), oracle.hasClaims)
		}

		access := Access(accessRaw % 4)
		wantAllow := oracleAllows(oracle, namespace, access)
		gotAllow := claims.Allows(namespace, access)
		if gotAllow != wantAllow {
			t.Fatalf("allow mismatch namespace=%q access=%v got=%v want=%v", namespace, access, gotAllow, wantAllow)
		}

		if claims.AllowsWildcard(access) != oracle.wildcard.Allows(access) {
			t.Fatalf("wildcard allow mismatch access=%v", access)
		}
	})
}

func oracleParseCertificate(cert *x509.Certificate) (oracleClaims, error) {
	out := oracleClaims{
		exact: make(map[string]Permission),
	}
	if cert == nil {
		return out, nil
	}
	for _, uri := range cert.URIs {
		if uri == nil {
			continue
		}
		scheme := strings.ToLower(strings.TrimSpace(uri.Scheme))
		switch scheme {
		case "spiffe":
			out.hasSPIFFE = true
		case lockdScheme:
			if !strings.EqualFold(strings.TrimSpace(uri.Host), lockdClaimHost) {
				continue
			}
			namespace := strings.TrimSpace(strings.Trim(uri.Path, "/"))
			if namespace == "" {
				return oracleClaims{}, errFuzzMalformedClaim
			}
			if strings.Contains(namespace, "/") {
				return oracleClaims{}, errFuzzMalformedClaim
			}
			perm, err := oracleParsePermission(uri)
			if err != nil {
				return oracleClaims{}, err
			}
			if namespace == wildcardNamespace {
				if stronger(perm, out.wildcard) {
					out.wildcard = perm
				}
				out.hasClaims = true
				continue
			}
			normalized, err := namespaces.Normalize(namespace, "")
			if err != nil {
				return oracleClaims{}, err
			}
			if stronger(perm, out.exact[normalized]) {
				out.exact[normalized] = perm
			}
			out.hasClaims = true
		}
	}
	return out, nil
}

func oracleParsePermission(uri *url.URL) (Permission, error) {
	permValues := uri.Query()["perm"]
	perm := PermissionReadWrite
	switch len(permValues) {
	case 0:
		return perm, nil
	case 1:
		raw := strings.ToLower(strings.TrimSpace(permValues[0]))
		switch raw {
		case "r":
			return PermissionRead, nil
		case "w":
			return PermissionWrite, nil
		case "rw":
			return PermissionReadWrite, nil
		default:
			return PermissionNone, errFuzzMalformedClaim
		}
	default:
		return PermissionNone, errFuzzMalformedClaim
	}
}

func oracleAllows(claims oracleClaims, namespace string, access Access) bool {
	normalized, err := namespaces.Normalize(namespace, "")
	if err != nil {
		return false
	}
	perm := claims.wildcard
	if exact, ok := claims.exact[normalized]; ok {
		perm = exact
	}
	return perm.Allows(access)
}

var errFuzzMalformedClaim = &url.Error{Op: "parse", URL: "lockd://ns", Err: errMalformedClaim{}}

type errMalformedClaim struct{}

func (errMalformedClaim) Error() string { return "malformed claim" }
