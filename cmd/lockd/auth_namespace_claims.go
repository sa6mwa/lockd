package main

import (
	"fmt"
	"net/url"
	"slices"
	"strings"

	"pkt.systems/lockd/internal/nsauth"
	"pkt.systems/lockd/namespaces"
)

const allNamespace = "ALL"

func parseNamespacePermissions(namespaceInputs []string, readAll, writeAll, rwAll bool) (map[string]nsauth.Permission, bool, error) {
	perms := make(map[string]nsauth.Permission)
	hasExplicit := len(namespaceInputs) > 0 || readAll || writeAll || rwAll

	if readAll {
		mergeNamespacePermission(perms, allNamespace, nsauth.PermissionRead)
	}
	if writeAll {
		mergeNamespacePermission(perms, allNamespace, nsauth.PermissionWrite)
	}
	if rwAll {
		mergeNamespacePermission(perms, allNamespace, nsauth.PermissionReadWrite)
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
	if idx := slices.Index(keys, allNamespace); idx > 0 {
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
	if strings.EqualFold(namespace, allNamespace) {
		return allNamespace, permission, nil
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
