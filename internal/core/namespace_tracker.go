package core

import (
	"sort"
	"strings"
	"sync"
)

// NamespaceTracker records namespaces observed by the core so other components
// (sweepers, diagnostics) can iterate active namespaces without transport ties.
type NamespaceTracker struct {
	mu         sync.RWMutex
	namespaces map[string]struct{}
}

// NewNamespaceTracker initializes a tracker seeded with the provided default
// namespace (if non-empty).
func NewNamespaceTracker(defaultNamespace string) *NamespaceTracker {
	t := &NamespaceTracker{
		namespaces: make(map[string]struct{}),
	}
	if defaultNamespace != "" {
		t.namespaces[defaultNamespace] = struct{}{}
	}
	return t
}

// Observe records namespace as active.
func (t *NamespaceTracker) Observe(namespace string) {
	if t == nil {
		return
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return
	}
	t.mu.Lock()
	t.namespaces[namespace] = struct{}{}
	t.mu.Unlock()
}

// All returns a sorted slice of observed namespaces.
func (t *NamespaceTracker) All() []string {
	if t == nil {
		return nil
	}
	t.mu.RLock()
	names := make([]string, 0, len(t.namespaces))
	for ns := range t.namespaces {
		names = append(names, ns)
	}
	t.mu.RUnlock()
	sort.Strings(names)
	return names
}
