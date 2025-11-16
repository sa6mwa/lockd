package namespaces

// ConfigProvider exposes the default namespace configuration for backends.
type ConfigProvider interface {
	DefaultNamespaceConfig() Config
}
