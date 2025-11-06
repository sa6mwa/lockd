package storage

import (
	"fmt"
	"path"
	"strings"
)

// validateNamespace ensures namespace is non-empty and normalized.
func validateNamespace(namespace string) (string, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return "", fmt.Errorf("storage: namespace required")
	}
	if strings.Contains(namespace, "/") {
		return "", fmt.Errorf("storage: namespace %q must not contain '/'", namespace)
	}
	return namespace, nil
}

// splitKeySegments normalizes key into clean path segments.
func splitKeySegments(key string) ([]string, error) {
	clean := strings.TrimSpace(key)
	if clean == "" {
		return nil, fmt.Errorf("storage: key required")
	}
	clean = path.Clean("/" + clean)
	if clean == "/" {
		return nil, fmt.Errorf("storage: invalid key %q", key)
	}
	clean = strings.TrimPrefix(clean, "/")
	if strings.HasPrefix(clean, "../") {
		return nil, fmt.Errorf("storage: invalid key %q", key)
	}
	parts := strings.Split(clean, "/")
	for _, part := range parts {
		if part == "" {
			return nil, fmt.Errorf("storage: invalid key %q", key)
		}
	}
	return parts, nil
}

// SplitNamespacedKey separates a logical storage key into namespace and remaining path segments.
// Keys are expected to be formatted as "<namespace>/<segment>/...".
func SplitNamespacedKey(key string) (string, []string, error) {
	clean := strings.TrimSpace(key)
	clean = strings.TrimPrefix(clean, "/")
	if clean == "" {
		return "", nil, fmt.Errorf("storage: key %q missing namespace", key)
	}
	parts := strings.Split(clean, "/")
	if len(parts) < 2 {
		return "", nil, fmt.Errorf("storage: key %q missing resource segments", key)
	}
	return parts[0], parts[1:], nil
}

// NamespacedMetaObject returns the object key for metadata within a namespace.
func NamespacedMetaObject(namespace, key string) (string, error) {
	ns, err := validateNamespace(namespace)
	if err != nil {
		return "", err
	}
	segments, err := splitKeySegments(key)
	if err != nil {
		return "", err
	}
	return path.Join(append([]string{ns, "meta"}, segments...)...) + ".pb", nil
}

// NamespacedStateObject returns the object key for JSON state within a namespace.
func NamespacedStateObject(namespace, key string) (string, error) {
	ns, err := validateNamespace(namespace)
	if err != nil {
		return "", err
	}
	segments, err := splitKeySegments(key)
	if err != nil {
		return "", err
	}
	return path.Join(append([]string{ns, "state"}, segments...)...) + ".json", nil
}

// NamespacedMetaTemp returns the temp metadata object key for the namespace.
func NamespacedMetaTemp(namespace, token string) (string, error) {
	ns, err := validateNamespace(namespace)
	if err != nil {
		return "", err
	}
	if token == "" {
		return "", fmt.Errorf("storage: temp token required")
	}
	return path.Join(ns, "meta", "tmp", token+".pb"), nil
}

// NamespacedStateTemp returns the temp state object key for the namespace.
func NamespacedStateTemp(namespace, token string) (string, error) {
	ns, err := validateNamespace(namespace)
	if err != nil {
		return "", err
	}
	if token == "" {
		return "", fmt.Errorf("storage: temp token required")
	}
	return path.Join(ns, "state", "tmp", token+".json"), nil
}
