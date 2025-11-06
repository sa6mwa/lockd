package lockd

import "pkt.systems/lockd/namespaces"

const (
	// DefaultNamespace applies when callers omit a namespace.
	DefaultNamespace = namespaces.Default
)

// NormalizeNamespace delegates to namespaces.Normalize for config-level usage.
func NormalizeNamespace(ns, fallback string) (string, error) {
	return namespaces.Normalize(ns, fallback)
}
