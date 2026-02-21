package httpapi

import (
	"fmt"
	"net/http"

	"pkt.systems/lockd/internal/nsauth"
)

func (h *Handler) authorizeNamespace(r *http.Request, namespace string, access nsauth.Access) error {
	if h == nil || !h.enforceClientIdentity {
		return nil
	}
	claims, err := h.requestNamespaceClaims(r)
	if err != nil {
		return err
	}
	if !claims.Allows(namespace, access) {
		return httpError{
			Status: http.StatusForbidden,
			Code:   "namespace_forbidden",
			Detail: fmt.Sprintf("namespace %q requires %s permission", namespace, accessLabel(access)),
		}
	}
	return nil
}

func (h *Handler) authorizeAllNamespaces(r *http.Request, access nsauth.Access) error {
	if h == nil || !h.enforceClientIdentity {
		return nil
	}
	claims, err := h.requestNamespaceClaims(r)
	if err != nil {
		return err
	}
	if !claims.AllowsWildcard(access) {
		return httpError{
			Status: http.StatusForbidden,
			Code:   "namespace_forbidden",
			Detail: fmt.Sprintf("operation requires ALL namespace claim with %s permission", accessLabel(access)),
		}
	}
	return nil
}

func (h *Handler) requestNamespaceClaims(r *http.Request) (nsauth.Claims, error) {
	cert := peerCertificate(r)
	if cert == nil {
		return nsauth.Claims{}, httpError{
			Status: http.StatusForbidden,
			Code:   "namespace_forbidden",
			Detail: "client certificate required for namespace authorization",
		}
	}
	claims, err := nsauth.ParseCertificate(cert)
	if err != nil {
		return nsauth.Claims{}, httpError{
			Status: http.StatusForbidden,
			Code:   "invalid_namespace_claims",
			Detail: err.Error(),
		}
	}
	if !claims.HasSPIFFEID {
		return nsauth.Claims{}, httpError{
			Status: http.StatusForbidden,
			Code:   "namespace_forbidden",
			Detail: "certificate missing SPIFFE URI SAN",
		}
	}
	if !claims.HasNamespaceClaims() {
		return nsauth.Claims{}, httpError{
			Status: http.StatusForbidden,
			Code:   "namespace_forbidden",
			Detail: "certificate has no namespace claims",
		}
	}
	return claims, nil
}

func accessLabel(access nsauth.Access) string {
	switch access {
	case nsauth.AccessRead:
		return "read"
	case nsauth.AccessWrite:
		return "write"
	case nsauth.AccessReadWrite:
		return "rw"
	default:
		return "namespace access"
	}
}
