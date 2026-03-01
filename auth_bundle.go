package lockd

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/nsauth"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lockd/tlsutil"
)

const authAllNamespace = "ALL"

// CreateCABundleRequest controls CA bundle generation.
type CreateCABundleRequest struct {
	// CommonName sets the CA certificate subject CN.
	// When empty, the default is "lockd-ca".
	CommonName string
	// ValidFor sets CA certificate validity duration.
	// When <= 0, the default is 10 years (10 * 365 * 24h).
	ValidFor time.Duration
}

// CreateServerBundleRequest controls server bundle generation from an existing CA bundle.
type CreateServerBundleRequest struct {
	// CABundlePEM is the CA bundle content (CA cert + CA key + kryptograf metadata).
	// This field is required.
	CABundlePEM []byte
	// CommonName sets the server certificate subject CN.
	// When empty, the default is "lockd-server".
	CommonName string
	// ValidFor sets server certificate validity duration.
	// When <= 0, the default is 1 year (365 * 24h).
	ValidFor time.Duration
	// Hosts lists DNS names/IPs for SANs. Values are trimmed.
	// When empty, a wildcard DNS SAN "*" is used.
	Hosts []string
	// NodeID controls the server SPIFFE URI identity (spiffe://lockd/server/<NodeID>).
	// When empty, a new UUIDv7 is generated.
	NodeID string
	// Denylist optionally seeds revoked client serials embedded in the server bundle.
	// Nil/empty means no revoked serials.
	Denylist []string
}

// CreateClientBundleRequest controls SDK client bundle generation from an existing CA bundle.
type CreateClientBundleRequest struct {
	// CABundlePEM is the CA bundle content (CA cert + CA key + kryptograf metadata).
	// This field is required.
	CABundlePEM []byte
	// CommonName sets the client certificate subject CN.
	// When empty, the default is "lockd-client".
	CommonName string
	// ValidFor sets client certificate validity duration.
	// When <= 0, the default is 1 year (365 * 24h).
	ValidFor time.Duration
	// NamespaceClaims defines namespace ACL claims in CLI-compatible format.
	// Accepted formats are "namespace" (defaults to rw) or "namespace=perm",
	// and each entry may contain comma-separated values.
	// Perm values are r, w, rw.
	// If no explicit claims/flags are provided, default namespace rw is added.
	NamespaceClaims []string
	// ReadAll adds the ALL=r claim (alias of CLI --read-all).
	ReadAll bool
	// WriteAll adds the ALL=w claim (alias of CLI --write-all).
	WriteAll bool
	// ReadWriteAll adds the ALL=rw claim (alias of CLI --rw-all).
	ReadWriteAll bool
}

// CreateTCClientBundleRequest controls TC client bundle generation from an existing CA bundle.
type CreateTCClientBundleRequest struct {
	// CABundlePEM is the CA bundle content (CA cert + CA key + kryptograf metadata).
	// This field is required.
	CABundlePEM []byte
	// CommonName sets the TC client certificate subject CN.
	// When empty, the default is "lockd-tc-client".
	CommonName string
	// ValidFor sets client certificate validity duration.
	// When <= 0, the default is 1 year (365 * 24h).
	ValidFor time.Duration
}

// CreateCABundleFileRequest controls CA bundle generation + file write.
type CreateCABundleFileRequest struct {
	// Path is the destination PEM file path. This field is required.
	Path string
	// Force controls overwrite behavior.
	// When false, writing fails if Path already exists.
	Force bool
	// CreateCABundleRequest configures CA generation.
	CreateCABundleRequest
}

// CreateServerBundleFileRequest controls server bundle generation + file write.
type CreateServerBundleFileRequest struct {
	// Path is the destination PEM file path. This field is required.
	Path string
	// Force controls overwrite behavior.
	// When false, writing fails if Path already exists.
	Force bool
	// CreateServerBundleRequest configures server bundle generation.
	CreateServerBundleRequest
}

// CreateClientBundleFileRequest controls SDK client bundle generation + file write.
type CreateClientBundleFileRequest struct {
	// Path is the destination PEM file path. This field is required.
	Path string
	// Force controls overwrite behavior.
	// When false, writing fails if Path already exists.
	Force bool
	// CreateClientBundleRequest configures client bundle generation.
	CreateClientBundleRequest
}

// CreateTCClientBundleFileRequest controls TC client bundle generation + file write.
type CreateTCClientBundleFileRequest struct {
	// Path is the destination PEM file path. This field is required.
	Path string
	// Force controls overwrite behavior.
	// When false, writing fails if Path already exists.
	Force bool
	// CreateTCClientBundleRequest configures TC client bundle generation.
	CreateTCClientBundleRequest
}

// CreateCABundle generates a CA PEM bundle containing CA cert+key and kryptograf metadata material.
func CreateCABundle(req CreateCABundleRequest) ([]byte, error) {
	ca, err := tlsutil.GenerateCA(req.CommonName, req.ValidFor)
	if err != nil {
		return nil, fmt.Errorf("create ca bundle: generate ca: %w", err)
	}
	bundle, err := tlsutil.EncodeCABundle(ca.CertPEM, ca.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("create ca bundle: encode ca bundle: %w", err)
	}
	return bundle, nil
}

// CreateServerBundle generates a server PEM bundle signed by the supplied CA bundle.
func CreateServerBundle(req CreateServerBundleRequest) ([]byte, error) {
	ca, err := loadCAFromBundlePEM(req.CABundlePEM)
	if err != nil {
		return nil, fmt.Errorf("create server bundle: %w", err)
	}
	material, err := cryptoutil.MetadataMaterialFromBytes(req.CABundlePEM)
	if err != nil {
		return nil, fmt.Errorf("create server bundle: metadata material: %w", err)
	}
	nodeID := strings.TrimSpace(req.NodeID)
	if nodeID == "" {
		nodeID = uuidv7.NewString()
	}
	spiffeURI, err := SPIFFEURIForServer(nodeID)
	if err != nil {
		return nil, fmt.Errorf("create server bundle: spiffe uri: %w", err)
	}
	allClaim, err := nsauth.ClaimURI(authAllNamespace, nsauth.PermissionReadWrite)
	if err != nil {
		return nil, fmt.Errorf("create server bundle: all namespace claim: %w", err)
	}
	issued, err := ca.IssueServerWithRequest(tlsutil.ServerCertRequest{
		CommonName: req.CommonName,
		Validity:   req.ValidFor,
		Hosts:      append([]string(nil), req.Hosts...),
		URIs:       []*url.URL{spiffeURI, allClaim},
	})
	if err != nil {
		return nil, fmt.Errorf("create server bundle: issue server cert: %w", err)
	}
	bundle, err := tlsutil.EncodeServerBundle(ca.CertPEM, nil, issued.CertPEM, issued.KeyPEM, req.Denylist)
	if err != nil {
		return nil, fmt.Errorf("create server bundle: encode server bundle: %w", err)
	}
	augmented, err := cryptoutil.ApplyMetadataMaterial(bundle, material)
	if err != nil {
		return nil, fmt.Errorf("create server bundle: apply metadata material: %w", err)
	}
	return augmented, nil
}

// CreateClientBundle generates an SDK client PEM bundle signed by the supplied CA bundle.
func CreateClientBundle(req CreateClientBundleRequest) ([]byte, error) {
	ca, err := loadCAFromBundlePEM(req.CABundlePEM)
	if err != nil {
		return nil, fmt.Errorf("create client bundle: %w", err)
	}
	effectiveCN := strings.TrimSpace(req.CommonName)
	if effectiveCN == "" {
		effectiveCN = "lockd-client"
	}
	spiffeURI, err := SPIFFEURIForRole(ClientBundleRoleSDK, effectiveCN)
	if err != nil {
		return nil, fmt.Errorf("create client bundle: spiffe uri: %w", err)
	}
	namespacePerms, hasExplicit, err := parseNamespacePermissions(req.NamespaceClaims, req.ReadAll, req.WriteAll, req.ReadWriteAll)
	if err != nil {
		return nil, fmt.Errorf("create client bundle: namespace claims: %w", err)
	}
	if !hasExplicit {
		mergeNamespacePermission(namespacePerms, DefaultNamespace, nsauth.PermissionReadWrite)
	}
	namespaceClaims, err := namespacePermissionsToURIs(namespacePerms)
	if err != nil {
		return nil, fmt.Errorf("create client bundle: namespace claim uris: %w", err)
	}
	uris := []*url.URL{spiffeURI}
	uris = append(uris, namespaceClaims...)
	issued, err := ca.IssueClient(tlsutil.ClientCertRequest{
		CommonName: effectiveCN,
		Validity:   req.ValidFor,
		URIs:       uris,
	})
	if err != nil {
		return nil, fmt.Errorf("create client bundle: issue client cert: %w", err)
	}
	clientPEM, err := tlsutil.EncodeClientBundle(ca.CertPEM, issued.CertPEM, issued.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("create client bundle: encode client bundle: %w", err)
	}
	return clientPEM, nil
}

// CreateTCClientBundle generates a TC client PEM bundle signed by the supplied CA bundle.
func CreateTCClientBundle(req CreateTCClientBundleRequest) ([]byte, error) {
	ca, err := loadCAFromBundlePEM(req.CABundlePEM)
	if err != nil {
		return nil, fmt.Errorf("create tc client bundle: %w", err)
	}
	effectiveCN := strings.TrimSpace(req.CommonName)
	if effectiveCN == "" {
		effectiveCN = "lockd-tc-client"
	}
	spiffeURI, err := SPIFFEURIForRole(ClientBundleRoleTC, effectiveCN)
	if err != nil {
		return nil, fmt.Errorf("create tc client bundle: spiffe uri: %w", err)
	}
	allClaim, err := nsauth.ClaimURI(authAllNamespace, nsauth.PermissionReadWrite)
	if err != nil {
		return nil, fmt.Errorf("create tc client bundle: all namespace claim: %w", err)
	}
	issued, err := ca.IssueClient(tlsutil.ClientCertRequest{
		CommonName: effectiveCN,
		Validity:   req.ValidFor,
		URIs:       []*url.URL{spiffeURI, allClaim},
	})
	if err != nil {
		return nil, fmt.Errorf("create tc client bundle: issue tc client cert: %w", err)
	}
	clientPEM, err := tlsutil.EncodeClientBundle(ca.CertPEM, issued.CertPEM, issued.KeyPEM)
	if err != nil {
		return nil, fmt.Errorf("create tc client bundle: encode tc client bundle: %w", err)
	}
	return clientPEM, nil
}

// CreateCABundleFile writes a generated CA bundle to path.
// Parent directories are created with mode 0700 and the output file uses mode 0600.
func CreateCABundleFile(req CreateCABundleFileRequest) error {
	pemBytes, err := CreateCABundle(req.CreateCABundleRequest)
	if err != nil {
		return err
	}
	return writePEMFile(req.Path, pemBytes, req.Force)
}

// CreateServerBundleFile writes a generated server bundle to path.
// Parent directories are created with mode 0700 and the output file uses mode 0600.
func CreateServerBundleFile(req CreateServerBundleFileRequest) error {
	pemBytes, err := CreateServerBundle(req.CreateServerBundleRequest)
	if err != nil {
		return err
	}
	return writePEMFile(req.Path, pemBytes, req.Force)
}

// CreateClientBundleFile writes a generated SDK client bundle to path.
// Parent directories are created with mode 0700 and the output file uses mode 0600.
func CreateClientBundleFile(req CreateClientBundleFileRequest) error {
	pemBytes, err := CreateClientBundle(req.CreateClientBundleRequest)
	if err != nil {
		return err
	}
	return writePEMFile(req.Path, pemBytes, req.Force)
}

// CreateTCClientBundleFile writes a generated TC client bundle to path.
// Parent directories are created with mode 0700 and the output file uses mode 0600.
func CreateTCClientBundleFile(req CreateTCClientBundleFileRequest) error {
	pemBytes, err := CreateTCClientBundle(req.CreateTCClientBundleRequest)
	if err != nil {
		return err
	}
	return writePEMFile(req.Path, pemBytes, req.Force)
}

func loadCAFromBundlePEM(caBundlePEM []byte) (*tlsutil.CA, error) {
	if len(caBundlePEM) == 0 {
		return nil, fmt.Errorf("ca bundle required")
	}
	ca, err := tlsutil.LoadCAFromBytes(caBundlePEM)
	if err != nil {
		return nil, fmt.Errorf("load ca bundle: %w", err)
	}
	return ca, nil
}

func writePEMFile(path string, data []byte, force bool) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("path required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create dir %s: %w", filepath.Dir(path), err)
	}
	if !force {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("%s already exists (use force to overwrite)", path)
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat %s: %w", path, err)
		}
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write file %s: %w", path, err)
	}
	return nil
}

func parseNamespacePermissions(namespaceInputs []string, readAll, writeAll, rwAll bool) (map[string]nsauth.Permission, bool, error) {
	perms := make(map[string]nsauth.Permission)
	hasExplicit := len(namespaceInputs) > 0 || readAll || writeAll || rwAll

	if readAll {
		mergeNamespacePermission(perms, authAllNamespace, nsauth.PermissionRead)
	}
	if writeAll {
		mergeNamespacePermission(perms, authAllNamespace, nsauth.PermissionWrite)
	}
	if rwAll {
		mergeNamespacePermission(perms, authAllNamespace, nsauth.PermissionReadWrite)
	}

	for _, rawInput := range namespaceInputs {
		rawInput = strings.TrimSpace(rawInput)
		if rawInput == "" {
			continue
		}
		parts := strings.Split(rawInput, ",")
		for _, rawPart := range parts {
			rawPart = strings.TrimSpace(rawPart)
			if rawPart == "" {
				continue
			}
			namespace, permission, err := parseNamespacePermissionPart(rawPart)
			if err != nil {
				return nil, false, err
			}
			mergeNamespacePermission(perms, namespace, permission)
		}
	}
	return perms, hasExplicit, nil
}

func namespacePermissionsToURIs(perms map[string]nsauth.Permission) ([]*url.URL, error) {
	keys := make([]string, 0, len(perms))
	for namespace := range perms {
		keys = append(keys, namespace)
	}
	slices.Sort(keys)
	if idx := slices.Index(keys, authAllNamespace); idx > 0 {
		keys[0], keys[idx] = keys[idx], keys[0]
	}

	out := make([]*url.URL, 0, len(keys))
	for _, namespace := range keys {
		claim, err := nsauth.ClaimURI(namespace, perms[namespace])
		if err != nil {
			return nil, err
		}
		out = append(out, claim)
	}
	return out, nil
}

func parseNamespacePermissionPart(raw string) (string, nsauth.Permission, error) {
	if strings.Count(raw, "=") > 1 {
		return "", nsauth.PermissionNone, fmt.Errorf("invalid namespace claim %q: expected namespace or namespace=perm", raw)
	}
	namespace := raw
	permission := nsauth.PermissionReadWrite
	if strings.Contains(raw, "=") {
		var permRaw string
		namespace, permRaw, _ = strings.Cut(raw, "=")
		namespace = strings.TrimSpace(namespace)
		permRaw = strings.TrimSpace(permRaw)
		if permRaw == "" {
			return "", nsauth.PermissionNone, fmt.Errorf("invalid namespace claim %q: permission required after '='", raw)
		}
		parsed, err := nsauth.ParsePermission(permRaw)
		if err != nil {
			return "", nsauth.PermissionNone, fmt.Errorf("invalid namespace claim %q: %w", raw, err)
		}
		permission = parsed
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return "", nsauth.PermissionNone, fmt.Errorf("invalid namespace claim %q: namespace required", raw)
	}
	if strings.EqualFold(namespace, authAllNamespace) {
		return authAllNamespace, permission, nil
	}
	normalized, err := namespaces.Normalize(namespace, "")
	if err != nil {
		return "", nsauth.PermissionNone, fmt.Errorf("invalid namespace claim %q: %w", raw, err)
	}
	return normalized, permission, nil
}

func mergeNamespacePermission(perms map[string]nsauth.Permission, namespace string, permission nsauth.Permission) {
	if perms == nil {
		return
	}
	existing := perms[namespace]
	if permissionRank(permission) > permissionRank(existing) {
		perms[namespace] = permission
	}
}

func permissionRank(permission nsauth.Permission) int {
	switch permission {
	case nsauth.PermissionReadWrite:
		return 3
	case nsauth.PermissionWrite:
		return 2
	case nsauth.PermissionRead:
		return 1
	default:
		return 0
	}
}
