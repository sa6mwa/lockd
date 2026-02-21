package nsauth

import (
	"crypto/x509"
	"net/url"
	"testing"
)

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	out, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url %q: %v", raw, err)
	}
	return out
}

func TestParseCertificateEntitlements(t *testing.T) {
	cert := &x509.Certificate{
		URIs: []*url.URL{
			mustURL(t, "spiffe://lockd/sdk/client-a"),
			mustURL(t, "lockd://ns/default?perm=rw"),
			mustURL(t, "lockd://ns/orders?perm=r"),
			mustURL(t, "lockd://ns/ALL?perm=w"),
		},
	}
	claims, err := ParseCertificate(cert)
	if err != nil {
		t.Fatalf("parse claims: %v", err)
	}
	if !claims.HasSPIFFEID {
		t.Fatal("expected spiffe identity")
	}
	if !claims.HasNamespaceClaims() {
		t.Fatal("expected namespace claims")
	}
	if got := claims.EffectivePermission("orders"); got != PermissionRead {
		t.Fatalf("expected exact permission read, got %v", got)
	}
	if got := claims.EffectivePermission("payments"); got != PermissionWrite {
		t.Fatalf("expected wildcard write, got %v", got)
	}
	if !claims.Allows("orders", AccessRead) {
		t.Fatal("expected read access on orders")
	}
	if claims.Allows("orders", AccessWrite) {
		t.Fatal("did not expect write access on orders")
	}
	if !claims.Allows("payments", AccessWrite) {
		t.Fatal("expected write access on wildcard namespace")
	}
}

func TestParseCertificateDuplicateStrongestWins(t *testing.T) {
	cert := &x509.Certificate{
		URIs: []*url.URL{
			mustURL(t, "lockd://ns/alpha?perm=r"),
			mustURL(t, "lockd://ns/alpha?perm=w"),
			mustURL(t, "lockd://ns/alpha?perm=rw"),
			mustURL(t, "lockd://ns/ALL?perm=r"),
			mustURL(t, "lockd://ns/ALL?perm=w"),
		},
	}
	claims, err := ParseCertificate(cert)
	if err != nil {
		t.Fatalf("parse claims: %v", err)
	}
	if got := claims.EffectivePermission("alpha"); got != PermissionReadWrite {
		t.Fatalf("expected rw for alpha, got %v", got)
	}
	if got := claims.EffectivePermission("beta"); got != PermissionWrite {
		t.Fatalf("expected w wildcard, got %v", got)
	}
}

func TestParseCertificateDefaultsPermToRW(t *testing.T) {
	cert := &x509.Certificate{
		URIs: []*url.URL{
			mustURL(t, "spiffe://lockd/sdk/client-a"),
			mustURL(t, "lockd://ns/default"),
		},
	}
	claims, err := ParseCertificate(cert)
	if err != nil {
		t.Fatalf("parse claims: %v", err)
	}
	if got := claims.EffectivePermission("default"); got != PermissionReadWrite {
		t.Fatalf("expected default rw, got %v", got)
	}
}

func TestParseCertificateTreatsLowercaseAllAsNamespace(t *testing.T) {
	cert := &x509.Certificate{
		URIs: []*url.URL{
			mustURL(t, "lockd://ns/all?perm=r"),
			mustURL(t, "lockd://ns/ALL?perm=w"),
		},
	}
	claims, err := ParseCertificate(cert)
	if err != nil {
		t.Fatalf("parse claims: %v", err)
	}
	if got := claims.EffectivePermission("all"); got != PermissionRead {
		t.Fatalf("expected exact all namespace read, got %v", got)
	}
	if got := claims.EffectivePermission("other"); got != PermissionWrite {
		t.Fatalf("expected wildcard write, got %v", got)
	}
}

func TestParseCertificateRejectsMalformedClaim(t *testing.T) {
	cert := &x509.Certificate{
		URIs: []*url.URL{
			mustURL(t, "lockd://ns/default?perm=x"),
		},
	}
	if _, err := ParseCertificate(cert); err == nil {
		t.Fatal("expected malformed claim error")
	}
}

func TestParseCertificateRejectsNestedNamespaceClaim(t *testing.T) {
	cert := &x509.Certificate{
		URIs: []*url.URL{
			mustURL(t, "lockd://ns/default/nested?perm=rw"),
		},
	}
	if _, err := ParseCertificate(cert); err == nil {
		t.Fatal("expected nested namespace claim to fail")
	}
}

func TestParseCertificateRejectsDuplicatePermQuery(t *testing.T) {
	cert := &x509.Certificate{
		URIs: []*url.URL{
			mustURL(t, "lockd://ns/default?perm=r&perm=rw"),
		},
	}
	if _, err := ParseCertificate(cert); err == nil {
		t.Fatal("expected duplicate perm query to fail")
	}
}

func TestParseCertificateExactClaimOverridesWildcard(t *testing.T) {
	cert := &x509.Certificate{
		URIs: []*url.URL{
			mustURL(t, "spiffe://lockd/sdk/client-a"),
			mustURL(t, "lockd://ns/ALL?perm=rw"),
			mustURL(t, "lockd://ns/default?perm=r"),
		},
	}
	claims, err := ParseCertificate(cert)
	if err != nil {
		t.Fatalf("parse claims: %v", err)
	}
	if claims.Allows("default", AccessWrite) {
		t.Fatal("expected exact read claim to block write despite wildcard rw")
	}
	if !claims.Allows("orders", AccessWrite) {
		t.Fatal("expected wildcard rw to allow write outside exact override")
	}
}

func TestParseCertificateAcceptsCaseInsensitiveSchemeAndHost(t *testing.T) {
	cert := &x509.Certificate{
		URIs: []*url.URL{
			mustURL(t, "SpIfFe://lockd/sdk/client-a"),
			mustURL(t, "LOCKD://NS/default?perm=rw"),
		},
	}
	claims, err := ParseCertificate(cert)
	if err != nil {
		t.Fatalf("parse claims: %v", err)
	}
	if !claims.HasSPIFFEID {
		t.Fatal("expected SPIFFE claim to be recognized case-insensitively")
	}
	if !claims.Allows("default", AccessReadWrite) {
		t.Fatal("expected default rw claim to be recognized case-insensitively")
	}
}

func TestClaimURI(t *testing.T) {
	claim, err := ClaimURI("Default", PermissionReadWrite)
	if err != nil {
		t.Fatalf("claim uri: %v", err)
	}
	if got := claim.String(); got != "lockd://ns/default?perm=rw" {
		t.Fatalf("unexpected claim: %s", got)
	}
	all, err := ClaimURI("ALL", PermissionWrite)
	if err != nil {
		t.Fatalf("wildcard claim uri: %v", err)
	}
	if got := all.String(); got != "lockd://ns/ALL?perm=w" {
		t.Fatalf("unexpected wildcard claim: %s", got)
	}
}
