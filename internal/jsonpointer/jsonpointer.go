package jsonpointer

import (
	"fmt"
	"strings"
)

var (
	encoder = strings.NewReplacer("~", "~0", "/", "~1")
	decoder = strings.NewReplacer("~1", "/", "~0", "~")
)

// EncodeSegment escapes a JSON pointer segment per RFC 6901.
func EncodeSegment(segment string) string {
	if segment == "" {
		return segment
	}
	return encoder.Replace(segment)
}

// DecodeSegment unescapes a JSON pointer segment.
func DecodeSegment(segment string) string {
	if segment == "" {
		return segment
	}
	return decoder.Replace(segment)
}

// Normalize ensures the path is expressed as an absolute JSON pointer (RFC 6901).
func Normalize(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" || path == "/" {
		return "", nil
	}
	if !strings.HasPrefix(path, "/") {
		return "", fmt.Errorf("json pointer %q must start with '/'", path)
	}
	return path, nil
}

// Join appends child to parent using pointer semantics.
func Join(parent, child string) string {
	child = EncodeSegment(child)
	if parent == "" {
		if child == "" {
			return ""
		}
		return "/" + child
	}
	if child == "" {
		return parent
	}
	return parent + "/" + child
}

// Split decomposes a pointer into decoded segments. An empty or root pointer yields no segments.
func Split(path string) ([]string, error) {
	path = strings.TrimSpace(path)
	if path == "" || path == "/" {
		return nil, nil
	}
	if !strings.HasPrefix(path, "/") {
		return nil, fmt.Errorf("invalid json pointer %q", path)
	}
	raw := path[1:]
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, "/")
	for i, part := range parts {
		parts[i] = DecodeSegment(part)
	}
	return parts, nil
}
