package lockd

import (
	"crypto/x509"
	"net/url"
	"strings"
	"testing"
)

func FuzzClientBundleMatchesRoleSAN(f *testing.F) {
	f.Add("spiffe", "lockd", "/sdk/client-a", true, "spiffe", "lockd", "/tc/client-a", true, uint8(0))
	f.Add("spiffe", "lockd", "/tc/client-a", true, "spiffe", "lockd", "/sdk/client-a", true, uint8(1))
	f.Add("spiffe", "evil", "/sdk/client-a", true, "", "", "", false, uint8(0))

	f.Fuzz(func(t *testing.T,
		scheme1 string,
		host1 string,
		path1 string,
		include1 bool,
		scheme2 string,
		host2 string,
		path2 string,
		include2 bool,
		roleRaw uint8,
	) {
		role := ClientBundleRole(roleRaw % 2)
		var uris []*url.URL
		if include1 {
			uris = append(uris, &url.URL{Scheme: scheme1, Host: host1, Path: path1})
		}
		if include2 {
			uris = append(uris, &url.URL{Scheme: scheme2, Host: host2, Path: path2})
		}
		cert := &x509.Certificate{URIs: uris}

		gotMatch, gotHasURIs := clientBundleMatchesRole(cert, role)
		wantMatch, wantHasURIs := oracleRoleMatch(cert, role)
		if gotHasURIs != wantHasURIs {
			t.Fatalf("hasURIs mismatch: got=%v want=%v cert=%#v", gotHasURIs, wantHasURIs, cert.URIs)
		}
		if gotMatch != wantMatch {
			t.Fatalf("match mismatch: got=%v want=%v role=%v cert=%#v", gotMatch, wantMatch, role, cert.URIs)
		}
	})
}

func FuzzSPIFFEURIBuilders(f *testing.F) {
	f.Add("client-a", uint8(0))
	f.Add("client-b", uint8(1))
	f.Add("  ", uint8(0))

	f.Fuzz(func(t *testing.T, name string, roleRaw uint8) {
		role := ClientBundleRole(roleRaw % 2)
		uri, err := SPIFFEURIForRole(role, name)
		if strings.TrimSpace(name) == "" {
			if err == nil {
				t.Fatal("expected error for empty SPIFFE role name")
			}
		} else {
			if err != nil {
				t.Fatalf("unexpected SPIFFE role error: %v", err)
			}
			if uri.Scheme != "spiffe" || uri.Host != spiffeHost {
				t.Fatalf("unexpected role SPIFFE URI %q", uri.String())
			}
			switch role {
			case ClientBundleRoleTC:
				if !strings.HasPrefix(uri.Path, "/tc/") {
					t.Fatalf("expected tc SPIFFE path, got %q", uri.Path)
				}
			default:
				if !strings.HasPrefix(uri.Path, "/sdk/") {
					t.Fatalf("expected sdk SPIFFE path, got %q", uri.Path)
				}
			}
		}

		serverURI, serverErr := SPIFFEURIForServer(name)
		if strings.TrimSpace(name) == "" {
			if serverErr == nil {
				t.Fatal("expected error for empty server node id")
			}
			return
		}
		if serverErr != nil {
			t.Fatalf("unexpected server SPIFFE error: %v", serverErr)
		}
		if serverURI.Scheme != "spiffe" || serverURI.Host != spiffeHost || !strings.HasPrefix(serverURI.Path, "/server/") {
			t.Fatalf("unexpected server SPIFFE URI %q", serverURI.String())
		}
	})
}

func oracleRoleMatch(cert *x509.Certificate, role ClientBundleRole) (bool, bool) {
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
		if !strings.EqualFold(uri.Scheme, "spiffe") || !strings.EqualFold(uri.Host, spiffeHost) {
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
