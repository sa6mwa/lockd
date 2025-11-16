package namespaces

import (
	"fmt"
	"strings"

	lockdproto "pkt.systems/lockd/internal/proto"
	"pkt.systems/lockd/internal/search"
)

// FallbackMode determines how queries behave when the preferred engine is
// unavailable.
type FallbackMode string

const (
	// FallbackScan allows queries to fall back to the scan engine.
	FallbackScan FallbackMode = "scan"
	// FallbackNone disables fallback; queries fail if the preferred engine is unavailable.
	FallbackNone FallbackMode = "none"
)

// Config captures namespace-level configuration.
type Config struct {
	Query QueryEngines `json:"query"`
}

// QueryEngines describes the preferred and fallback engines for /v1/query.
type QueryEngines struct {
	Preferred search.EngineHint `json:"preferred_engine"`
	Fallback  FallbackMode      `json:"fallback_engine"`
}

// DefaultConfig returns the default namespace configuration.
func DefaultConfig() Config {
	return Config{
		Query: QueryEngines{
			Preferred: search.EngineIndex,
			Fallback:  FallbackNone,
		},
	}
}

// Normalize applies default values for unset fields.
func (c *Config) Normalize() {
	if c.Query.Preferred == "" {
		c.Query.Preferred = search.EngineIndex
	}
	if c.Query.Fallback == "" {
		c.Query.Fallback = FallbackNone
	}
}

// Validate ensures the configuration satisfies namespace constraints.
func (c Config) Validate() error {
	switch c.Query.Preferred {
	case search.EngineIndex, search.EngineScan:
	default:
		return fmt.Errorf("invalid preferred engine %q (expected index or scan)", c.Query.Preferred)
	}
	switch strings.ToLower(string(c.Query.Fallback)) {
	case string(FallbackScan):
		c.Query.Fallback = FallbackScan
	case string(FallbackNone):
		c.Query.Fallback = FallbackNone
	default:
		return fmt.Errorf("invalid fallback engine %q (expected scan or none)", c.Query.Fallback)
	}
	return nil
}

// ToProto converts the config into the protobuf representation.
func (c Config) ToProto() *lockdproto.NamespaceConfig {
	return &lockdproto.NamespaceConfig{
		Query: &lockdproto.NamespaceQueryConfig{
			PreferredEngine: string(c.Query.Preferred),
			FallbackEngine:  string(c.Query.Fallback),
		},
	}
}

// ConfigFromProto converts the protobuf namespace config into the Go struct.
func ConfigFromProto(msg *lockdproto.NamespaceConfig) Config {
	cfg := DefaultConfig()
	if msg == nil {
		return cfg
	}
	if q := msg.GetQuery(); q != nil {
		cfg.Query.Preferred = search.EngineHint(q.GetPreferredEngine())
		cfg.Query.Fallback = FallbackMode(q.GetFallbackEngine())
	}
	cfg.Normalize()
	return cfg
}
