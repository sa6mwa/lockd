package mcp

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"pkt.systems/lockd/internal/nsauth"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lockd/tlsutil"
)

type hintToolInput struct{}

type namespaceHint struct {
	Namespace  string `json:"namespace"`
	Permission string `json:"permission"`
}

type hintToolOutput struct {
	ClientID           string          `json:"client_id,omitempty"`
	DefaultNamespace   string          `json:"default_namespace"`
	InlineMaxBytes     int64           `json:"inline_max_payload_bytes"`
	NamespaceHints     []namespaceHint `json:"namespace_hints,omitempty"`
	WildcardPermission string          `json:"wildcard_permission,omitempty"`
	ClaimURIs          []string        `json:"claim_uris,omitempty"`
	TokenScopes        []string        `json:"token_scopes,omitempty"`
	Source             string          `json:"source"`
	Notes              []string        `json:"notes,omitempty"`
}

type namespaceHintSnapshot struct {
	NamespaceHints     []namespaceHint
	WildcardPermission string
	ClaimURIs          []string
	Source             string
	Notes              []string
}

func (s *server) handleHintTool(_ context.Context, req *mcpsdk.CallToolRequest, _ hintToolInput) (*mcpsdk.CallToolResult, hintToolOutput, error) {
	clientID := ""
	scopes := []string(nil)
	if req != nil && req.Extra != nil && req.Extra.TokenInfo != nil {
		clientID = strings.TrimSpace(req.Extra.TokenInfo.UserID)
		if len(req.Extra.TokenInfo.Scopes) > 0 {
			scopes = append([]string(nil), req.Extra.TokenInfo.Scopes...)
			sort.Strings(scopes)
		}
	}
	hints := namespaceHintsFromConfig(s.cfg)
	return nil, hintToolOutput{
		ClientID:           clientID,
		DefaultNamespace:   s.cfg.DefaultNamespace,
		InlineMaxBytes:     normalizedInlineMaxBytes(s.cfg.InlineMaxBytes),
		NamespaceHints:     hints.NamespaceHints,
		WildcardPermission: hints.WildcardPermission,
		ClaimURIs:          hints.ClaimURIs,
		TokenScopes:        scopes,
		Source:             hints.Source,
		Notes:              hints.Notes,
	}, nil
}

func namespaceHintsFromConfig(cfg Config) namespaceHintSnapshot {
	if cfg.UpstreamDisableMTLS {
		return namespaceHintSnapshot{
			Source: "upstream_mtls_disabled",
			Notes: []string{
				"upstream mTLS is disabled; no client-bundle namespace claims are available",
			},
		}
	}

	bundlePath, err := resolveUpstreamClientBundlePath(cfg)
	if err != nil {
		return namespaceHintSnapshot{
			Source: "client_bundle_unavailable",
			Notes:  []string{err.Error()},
		}
	}

	bundle, err := tlsutil.LoadClientBundle(bundlePath)
	if err != nil {
		return namespaceHintSnapshot{
			Source: "client_bundle_unavailable",
			Notes:  []string{fmt.Sprintf("load client bundle %q: %v", bundlePath, err)},
		}
	}

	snapshot := extractNamespaceHintsFromURIs(bundle.ClientCert.URIs)
	if snapshot.Source == "" {
		snapshot.Source = "client_bundle_claims"
	}
	if len(snapshot.NamespaceHints) == 0 && snapshot.WildcardPermission == "" {
		snapshot.Notes = append(snapshot.Notes, "no lockd namespace claims found in client certificate")
	}
	return snapshot
}

func extractNamespaceHintsFromURIs(uris []*url.URL) namespaceHintSnapshot {
	out := namespaceHintSnapshot{
		Source: "client_bundle_claims",
	}
	if len(uris) == 0 {
		return out
	}

	exact := make(map[string]nsauth.Permission)
	wildcard := nsauth.PermissionNone
	claimSet := make(map[string]struct{})

	for _, uri := range uris {
		if uri == nil {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(uri.Scheme), "lockd") {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(uri.Host), "ns") {
			continue
		}
		raw := uri.String()
		if _, seen := claimSet[raw]; !seen {
			claimSet[raw] = struct{}{}
			out.ClaimURIs = append(out.ClaimURIs, raw)
		}

		namespace := strings.TrimSpace(strings.Trim(uri.Path, "/"))
		if namespace == "" {
			out.Notes = append(out.Notes, fmt.Sprintf("ignored invalid claim %q: namespace required", raw))
			continue
		}
		if strings.Contains(namespace, "/") {
			out.Notes = append(out.Notes, fmt.Sprintf("ignored invalid claim %q: namespace must be single-component", raw))
			continue
		}

		perm := nsauth.PermissionReadWrite
		permValues := uri.Query()["perm"]
		switch len(permValues) {
		case 0:
			// implicit rw
		case 1:
			parsed, err := nsauth.ParsePermission(permValues[0])
			if err != nil {
				out.Notes = append(out.Notes, fmt.Sprintf("ignored invalid claim %q: %v", raw, err))
				continue
			}
			perm = parsed
		default:
			out.Notes = append(out.Notes, fmt.Sprintf("ignored invalid claim %q: perm may appear once", raw))
			continue
		}

		if strings.EqualFold(namespace, "ALL") {
			if strongerPermissionHint(perm, wildcard) {
				wildcard = perm
			}
			continue
		}
		normalized, err := namespaces.Normalize(namespace, "")
		if err != nil {
			out.Notes = append(out.Notes, fmt.Sprintf("ignored invalid claim %q: %v", raw, err))
			continue
		}
		if strongerPermissionHint(perm, exact[normalized]) {
			exact[normalized] = perm
		}
	}

	if wildcard != nsauth.PermissionNone {
		out.WildcardPermission = permissionHintLabel(wildcard)
	}
	if len(exact) > 0 {
		namespacesList := make([]string, 0, len(exact))
		for namespace := range exact {
			namespacesList = append(namespacesList, namespace)
		}
		sort.Strings(namespacesList)
		out.NamespaceHints = make([]namespaceHint, 0, len(namespacesList))
		for _, namespace := range namespacesList {
			out.NamespaceHints = append(out.NamespaceHints, namespaceHint{
				Namespace:  namespace,
				Permission: permissionHintLabel(exact[namespace]),
			})
		}
	}
	sort.Strings(out.ClaimURIs)
	sort.Strings(out.Notes)
	return out
}

func strongerPermissionHint(candidate, current nsauth.Permission) bool {
	return permissionHintRank(candidate) > permissionHintRank(current)
}

func permissionHintRank(permission nsauth.Permission) int {
	switch permission {
	case nsauth.PermissionRead:
		return 1
	case nsauth.PermissionWrite:
		return 2
	case nsauth.PermissionReadWrite:
		return 3
	default:
		return 0
	}
}

func permissionHintLabel(permission nsauth.Permission) string {
	switch permission {
	case nsauth.PermissionRead:
		return "read"
	case nsauth.PermissionWrite:
		return "write"
	case nsauth.PermissionReadWrite:
		return "read_write"
	default:
		return "none"
	}
}
