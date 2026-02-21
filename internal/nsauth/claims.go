package nsauth

import (
	"crypto/x509"
	"fmt"
	"net/url"
	"strings"

	"pkt.systems/lockd/namespaces"
)

const (
	lockdScheme       = "lockd"
	lockdClaimHost    = "ns"
	wildcardNamespace = "ALL"
)

// Permission defines the namespace-level entitlement encoded in certificate claims.
type Permission uint8

const (
	// PermissionNone indicates no namespace access.
	PermissionNone Permission = iota
	// PermissionRead allows read-class namespace operations.
	PermissionRead
	// PermissionWrite allows write-class namespace operations.
	PermissionWrite
	// PermissionReadWrite allows all namespace operations.
	PermissionReadWrite
)

// Access defines the operation category required for an endpoint.
type Access uint8

const (
	// AccessAny requires any explicit namespace claim.
	AccessAny Access = iota
	// AccessRead requires read-class namespace permission.
	AccessRead
	// AccessWrite requires write-class namespace permission.
	AccessWrite
	// AccessReadWrite requires full namespace permission.
	AccessReadWrite
)

// Claims captures parsed namespace claims from a certificate.
type Claims struct {
	HasSPIFFEID bool

	exact    map[string]Permission
	wildcard Permission
}

// ParseCertificate extracts SPIFFE identity and lockd namespace claims from cert.
func ParseCertificate(cert *x509.Certificate) (Claims, error) {
	if cert == nil {
		return Claims{}, fmt.Errorf("certificate required")
	}
	out := Claims{
		exact: make(map[string]Permission),
	}
	for _, uri := range cert.URIs {
		if uri == nil {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(uri.Scheme)) {
		case "spiffe":
			out.HasSPIFFEID = true
		case lockdScheme:
			if !strings.EqualFold(strings.TrimSpace(uri.Host), lockdClaimHost) {
				continue
			}
			ns, perm, err := parseNamespaceClaim(uri)
			if err != nil {
				return Claims{}, err
			}
			if ns == wildcardNamespace {
				if stronger(perm, out.wildcard) {
					out.wildcard = perm
				}
				continue
			}
			if stronger(perm, out.exact[ns]) {
				out.exact[ns] = perm
			}
		}
	}
	return out, nil
}

// HasNamespaceClaims reports whether at least one lockd namespace claim was found.
func (c Claims) HasNamespaceClaims() bool {
	return c.wildcard != PermissionNone || len(c.exact) > 0
}

// EffectivePermission returns the resolved permission for namespace.
func (c Claims) EffectivePermission(namespace string) Permission {
	normalized, err := namespaces.Normalize(namespace, "")
	if err != nil {
		return PermissionNone
	}
	if perm, ok := c.exact[normalized]; ok {
		return perm
	}
	return c.wildcard
}

// Allows reports whether access is permitted for namespace.
func (c Claims) Allows(namespace string, access Access) bool {
	return c.EffectivePermission(namespace).Allows(access)
}

// AllowsWildcard reports whether wildcard claims allow access.
func (c Claims) AllowsWildcard(access Access) bool {
	return c.wildcard.Allows(access)
}

// Allows reports whether p satisfies access.
func (p Permission) Allows(access Access) bool {
	switch access {
	case AccessAny:
		return p != PermissionNone
	case AccessRead:
		return p == PermissionRead || p == PermissionReadWrite
	case AccessWrite:
		return p == PermissionWrite || p == PermissionReadWrite
	case AccessReadWrite:
		return p == PermissionReadWrite
	default:
		return false
	}
}

// ParsePermission decodes a claim permission value.
func ParsePermission(raw string) (Permission, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "r":
		return PermissionRead, nil
	case "w":
		return PermissionWrite, nil
	case "rw":
		return PermissionReadWrite, nil
	default:
		return PermissionNone, fmt.Errorf("invalid permission %q", raw)
	}
}

// ClaimURI builds a lockd namespace claim URI.
func ClaimURI(namespace string, permission Permission) (*url.URL, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if namespace != wildcardNamespace {
		normalized, err := namespaces.Normalize(namespace, "")
		if err != nil {
			return nil, fmt.Errorf("normalize namespace %q: %w", namespace, err)
		}
		namespace = normalized
	}
	if permission == PermissionNone {
		return nil, fmt.Errorf("permission required")
	}
	claim := &url.URL{
		Scheme: lockdScheme,
		Host:   lockdClaimHost,
		Path:   "/" + namespace,
	}
	query := claim.Query()
	switch permission {
	case PermissionRead:
		query.Set("perm", "r")
	case PermissionWrite:
		query.Set("perm", "w")
	case PermissionReadWrite:
		query.Set("perm", "rw")
	default:
		return nil, fmt.Errorf("invalid permission %d", permission)
	}
	claim.RawQuery = query.Encode()
	return claim, nil
}

func parseNamespaceClaim(uri *url.URL) (string, Permission, error) {
	namespace := strings.TrimSpace(strings.Trim(uri.Path, "/"))
	if namespace == "" {
		return "", PermissionNone, fmt.Errorf("invalid namespace claim %q: namespace required", uri.String())
	}
	if strings.Contains(namespace, "/") {
		return "", PermissionNone, fmt.Errorf("invalid namespace claim %q: namespace must be a single component", uri.String())
	}
	permValues := uri.Query()["perm"]
	perm := PermissionReadWrite
	switch len(permValues) {
	case 0:
		// default rw
	case 1:
		var err error
		perm, err = ParsePermission(permValues[0])
		if err != nil {
			return "", PermissionNone, fmt.Errorf("invalid namespace claim %q: %w", uri.String(), err)
		}
	default:
		return "", PermissionNone, fmt.Errorf("invalid namespace claim %q: perm may appear once", uri.String())
	}
	if namespace == wildcardNamespace {
		return wildcardNamespace, perm, nil
	}
	normalized, err := namespaces.Normalize(namespace, "")
	if err != nil {
		return "", PermissionNone, fmt.Errorf("invalid namespace claim %q: %w", uri.String(), err)
	}
	return normalized, perm, nil
}

func stronger(a, b Permission) bool {
	return rank(a) > rank(b)
}

func rank(permission Permission) int {
	switch permission {
	case PermissionRead:
		return 1
	case PermissionWrite:
		return 2
	case PermissionReadWrite:
		return 3
	default:
		return 0
	}
}
