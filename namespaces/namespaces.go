package namespaces

import (
	"fmt"
	"strings"
)

const (
	// Default applies when callers omit a namespace.
	Default = "default"

	// MaxLength caps the length of namespaces, queue names, and key segments.
	MaxLength = 128
)

// Normalize lowercases and validates ns, applying fallback when ns is empty.
func Normalize(ns, fallback string) (string, error) {
	ns = strings.TrimSpace(ns)
	if ns == "" {
		ns = strings.TrimSpace(fallback)
	}
	if ns == "" {
		return "", fmt.Errorf("namespace required")
	}
	normalized, err := NormalizeComponent(ns)
	if err != nil {
		return "", err
	}
	return normalized, nil
}

// Validate reports whether ns satisfies namespace constraints.
func Validate(ns string) error {
	_, err := Normalize(ns, Default)
	return err
}

// NormalizeComponent lowercases and validates a single name component.
func NormalizeComponent(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("value required")
	}
	if len(name) > MaxLength {
		return "", fmt.Errorf("value too long (max %d characters)", MaxLength)
	}
	name = strings.ToLower(name)
	if !isValidComponent(name) {
		return "", fmt.Errorf("invalid value %q (allowed: lowercase letters, digits, '.', '_', '-')", name)
	}
	return name, nil
}

func isValidComponent(name string) bool {
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z':
			continue
		case c >= '0' && c <= '9':
			continue
		case c == '.' || c == '_' || c == '-':
			continue
		default:
			return false
		}
	}
	return true
}

// NormalizeKey lowercases and validates a lock key. Keys may contain '/' to
// denote hierarchical segments; each segment must satisfy NormalizeComponent.
func NormalizeKey(key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", fmt.Errorf("key required")
	}
	if len(key) > MaxLength {
		return "", fmt.Errorf("key too long (max %d characters)", MaxLength)
	}
	segments := strings.Split(key, "/")
	for i, segment := range segments {
		if segment == "" {
			return "", fmt.Errorf("key contains empty segment")
		}
		normalized, err := NormalizeComponent(segment)
		if err != nil {
			return "", fmt.Errorf("invalid key segment %q: %w", segment, err)
		}
		segments[i] = normalized
	}
	return strings.Join(segments, "/"), nil
}
