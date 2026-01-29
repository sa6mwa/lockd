package lockd

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"pkt.systems/lockd/tlsutil"
)

// ClientBundleRole identifies which client bundle role to resolve.
type ClientBundleRole int

const (
	// ClientBundleRoleSDK resolves SDK client bundles.
	ClientBundleRoleSDK ClientBundleRole = iota
	// ClientBundleRoleTC resolves transaction coordinator client bundles.
	ClientBundleRoleTC
)

const (
	spiffeHost = "lockd"
	// SPIFFESDKPrefix is the SPIFFE URI prefix for SDK client identities.
	SPIFFESDKPrefix = "spiffe://lockd/sdk/"
	// SPIFFETCPrefix is the SPIFFE URI prefix for transaction coordinator client identities.
	SPIFFETCPrefix = "spiffe://lockd/tc/"
	// SPIFFEServerPrefix is the SPIFFE URI prefix for server identities.
	SPIFFEServerPrefix  = "spiffe://lockd/server/"
	defaultClientPrefix = "client"
	defaultTCClientName = "tc-client"
)

func (r ClientBundleRole) String() string {
	switch r {
	case ClientBundleRoleSDK:
		return "client"
	case ClientBundleRoleTC:
		return "tc client"
	default:
		return "client"
	}
}

// SPIFFEURIForRole builds the default SPIFFE URI for a role and common name.
func SPIFFEURIForRole(role ClientBundleRole, name string) (*url.URL, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, errors.New("spiffe: name required")
	}
	path := "/sdk/" + strings.TrimPrefix(name, "/")
	switch role {
	case ClientBundleRoleTC:
		path = "/tc/" + strings.TrimPrefix(name, "/")
	}
	return &url.URL{
		Scheme: "spiffe",
		Host:   spiffeHost,
		Path:   path,
	}, nil
}

// SPIFFEURIForServer builds the SPIFFE URI for a server node identity.
func SPIFFEURIForServer(nodeID string) (*url.URL, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return nil, errors.New("spiffe: node id required")
	}
	return &url.URL{
		Scheme: "spiffe",
		Host:   spiffeHost,
		Path:   "/server/" + strings.TrimPrefix(nodeID, "/"),
	}, nil
}

// ResolveClientBundlePath resolves or validates the client bundle path for a role.
// When explicitPath is empty, it auto-discovers bundle files under the default config dir.
func ResolveClientBundlePath(role ClientBundleRole, explicitPath string) (string, error) {
	if strings.TrimSpace(explicitPath) != "" {
		return validateClientBundlePath(role, explicitPath)
	}
	dir, err := DefaultConfigDir()
	if err != nil {
		return "", err
	}
	candidates, err := listClientBundleCandidates(dir, role)
	if err != nil {
		return "", err
	}
	return selectClientBundleCandidate(role, candidates)
}

func listClientBundleCandidates(dir string, role ClientBundleRole) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read config dir %s: %w", dir, err)
	}
	var targets []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".pem") {
			continue
		}
		switch role {
		case ClientBundleRoleTC:
			if strings.HasPrefix(name, defaultTCClientName) {
				targets = append(targets, filepath.Join(dir, name))
			}
		default:
			if strings.HasPrefix(name, defaultClientPrefix) {
				targets = append(targets, filepath.Join(dir, name))
			}
		}
	}
	slices.Sort(targets)
	return targets, nil
}

func selectClientBundleCandidate(role ClientBundleRole, candidates []string) (string, error) {
	if len(candidates) == 0 {
		return "", missingClientBundleError(role, nil)
	}
	var (
		matches   []string
		legacy    []string
		parseErrs []string
	)
	for _, path := range candidates {
		bundle, err := tlsutil.LoadClientBundle(path)
		if err != nil {
			parseErrs = append(parseErrs, fmt.Sprintf("%s: %v", path, err))
			continue
		}
		match, hasURIs := clientBundleMatchesRole(bundle.ClientCert, role)
		if match {
			matches = append(matches, path)
			continue
		}
		if role == ClientBundleRoleSDK && !hasURIs {
			legacy = append(legacy, path)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	if len(matches) > 1 {
		return "", multipleClientBundleError(role, matches)
	}
	if role == ClientBundleRoleSDK {
		if len(legacy) == 1 {
			return legacy[0], nil
		}
		if len(legacy) > 1 {
			return "", multipleClientBundleError(role, legacy)
		}
	}
	return "", missingClientBundleError(role, parseErrs)
}

func validateClientBundlePath(role ClientBundleRole, path string) (string, error) {
	bundle, err := tlsutil.LoadClientBundle(path)
	if err != nil {
		return "", fmt.Errorf("load client bundle %s: %w", path, err)
	}
	match, hasURIs := clientBundleMatchesRole(bundle.ClientCert, role)
	if match || (role == ClientBundleRoleSDK && !hasURIs) {
		return path, nil
	}
	return "", fmt.Errorf("client bundle %s does not match %s SPIFFE identity (expected %s)", path, role, spiffePrefix(role))
}

func clientBundleMatchesRole(cert *x509.Certificate, role ClientBundleRole) (bool, bool) {
	if cert == nil {
		return false, false
	}
	if len(cert.URIs) == 0 {
		return false, false
	}
	for _, uri := range cert.URIs {
		if uri == nil {
			continue
		}
		if uri.Scheme != "spiffe" || !strings.EqualFold(uri.Host, spiffeHost) {
			continue
		}
		switch role {
		case ClientBundleRoleTC:
			if strings.HasPrefix(uri.Path, "/tc/") {
				return true, true
			}
		default:
			if strings.HasPrefix(uri.Path, "/sdk/") {
				return true, true
			}
		}
	}
	return false, true
}

func spiffePrefix(role ClientBundleRole) string {
	if role == ClientBundleRoleTC {
		return SPIFFETCPrefix
	}
	return SPIFFESDKPrefix
}

func missingClientBundleError(role ClientBundleRole, parseErrs []string) error {
	hint := "--bundle"
	if role == ClientBundleRoleTC {
		hint = "--tc-client-bundle"
	}
	if len(parseErrs) == 0 {
		return fmt.Errorf("no %s bundle found; provide %s", role, hint)
	}
	return fmt.Errorf("no %s bundle found; provide %s (parse errors: %s)", role, hint, strings.Join(parseErrs, "; "))
}

func multipleClientBundleError(role ClientBundleRole, paths []string) error {
	hint := "--bundle"
	if role == ClientBundleRoleTC {
		hint = "--tc-client-bundle"
	}
	return fmt.Errorf("multiple %s bundles found (%s); specify %s", role, strings.Join(paths, ", "), hint)
}
