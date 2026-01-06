package namespaces

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	lockdproto "pkt.systems/lockd/internal/proto"
	"pkt.systems/lockd/internal/search"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

const (
	namespaceConfigKey        = "config/namespace.pb"
	namespaceConfigContent    = storage.ContentTypeProtobuf
	namespaceConfigContentEnc = storage.ContentTypeProtobufEncrypted
)

// ConfigStore persists namespace configuration documents inside the storage backend.
type ConfigStore struct {
	backend    storage.Backend
	crypto     *storage.Crypto
	logger     pslog.Logger
	defaultCfg Config
}

// ConfigLoadResult captures a namespace config and its ETag.
type ConfigLoadResult struct {
	Config Config
	ETag   string
}

// NewConfigStore builds a ConfigStore backed by backend. When backend is nil a nil
// store is returned.
func NewConfigStore(backend storage.Backend, crypto *storage.Crypto, logger pslog.Logger, defaultCfg Config) *ConfigStore {
	if backend == nil {
		return nil
	}
	defaultCfg.Normalize()
	if err := defaultCfg.Validate(); err != nil {
		defaultCfg = DefaultConfig()
	}
	return &ConfigStore{
		backend:    backend,
		crypto:     crypto,
		logger:     logger,
		defaultCfg: defaultCfg,
	}
}

// Load retrieves the namespace config and its ETag. When no config exists the
// default configuration is returned with an empty ETag.
func (s *ConfigStore) Load(ctx context.Context, namespace string) (ConfigLoadResult, error) {
	var empty Config
	if s == nil {
		return ConfigLoadResult{Config: cloneConfig(DefaultConfig())}, nil
	}
	if s.backend == nil {
		return ConfigLoadResult{Config: cloneConfig(s.defaultCfg)}, nil
	}
	obj, err := s.backend.GetObject(ctx, namespace, namespaceConfigKey)
	if err != nil {
		if err == storage.ErrNotFound {
			return ConfigLoadResult{Config: cloneConfig(s.defaultCfg)}, nil
		}
		return ConfigLoadResult{Config: empty}, err
	}
	defer obj.Reader.Close()
	payload, err := io.ReadAll(obj.Reader)
	if err != nil {
		return ConfigLoadResult{Config: empty}, fmt.Errorf("read namespace config: %w", err)
	}
	if s.crypto != nil && s.crypto.Enabled() {
		payload, err = s.crypto.DecryptMetadata(payload)
		if err != nil {
			return ConfigLoadResult{Config: empty}, fmt.Errorf("decrypt namespace config: %w", err)
		}
	}
	var msg lockdproto.NamespaceConfig
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return ConfigLoadResult{Config: empty}, fmt.Errorf("decode namespace config: %w", err)
	}
	cfg := ConfigFromProto(&msg)
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return ConfigLoadResult{Config: empty}, err
	}
	return ConfigLoadResult{Config: cfg, ETag: obj.Info.ETag}, nil
}

// Save persists the namespace configuration enforcing the provided ETag when non-empty.
func (s *ConfigStore) Save(ctx context.Context, namespace string, cfg Config, expectedETag string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("namespace config store unavailable")
	}
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return "", err
	}
	payload, err := proto.Marshal(cfg.ToProto())
	if err != nil {
		return "", fmt.Errorf("encode namespace config: %w", err)
	}
	if s.crypto != nil && s.crypto.Enabled() {
		payload, err = s.crypto.EncryptMetadata(payload)
		if err != nil {
			return "", fmt.Errorf("encrypt namespace config: %w", err)
		}
	}
	reader := bytes.NewReader(payload)
	contentType := namespaceConfigContent
	if s.crypto != nil && s.crypto.Enabled() {
		contentType = namespaceConfigContentEnc
	}
	info, err := s.backend.PutObject(ctx, namespace, namespaceConfigKey, reader, storage.PutObjectOptions{
		ExpectedETag: expectedETag,
		ContentType:  contentType,
	})
	if err != nil {
		return "", err
	}
	if s.logger != nil {
		s.logger.Debug("namespace.config.saved",
			"namespace", namespace,
			"etag", info.ETag,
			"preferred_engine", cfg.Query.Preferred,
			"fallback_engine", cfg.Query.Fallback,
			"updated_at", time.Now().UTC().Format(time.RFC3339))
	}
	return info.ETag, nil
}

// SelectEngine determines the engine to use for the namespace based on the
// provided hint and actual adapter capabilities.
func (c Config) SelectEngine(hint search.EngineHint, caps search.Capabilities) (search.EngineHint, error) {
	c.Normalize()
	if err := c.Validate(); err != nil {
		return "", err
	}
	requested := strings.ToLower(string(hint))
	if requested == "" || hint == search.EngineAuto {
		return c.pickPreferred(caps)
	}
	switch hint {
	case search.EngineIndex:
		if !caps.Index {
			return "", fmt.Errorf("index engine unavailable for namespace")
		}
		if c.Query.Preferred != search.EngineIndex {
			return "", fmt.Errorf("index engine disabled for namespace")
		}
		return search.EngineIndex, nil
	case search.EngineScan:
		if !caps.Scan {
			return "", fmt.Errorf("scan engine unavailable for namespace")
		}
		if c.Query.Preferred == search.EngineScan || c.Query.Fallback == FallbackScan {
			return search.EngineScan, nil
		}
		return "", fmt.Errorf("scan engine disabled for namespace")
	default:
		return "", fmt.Errorf("unsupported engine hint %q", hint)
	}
}

func (c Config) pickPreferred(caps search.Capabilities) (search.EngineHint, error) {
	if c.Query.Preferred == search.EngineIndex && caps.Index {
		return search.EngineIndex, nil
	}
	if c.Query.Preferred == search.EngineScan && caps.Scan {
		return search.EngineScan, nil
	}
	if c.Query.Fallback == FallbackScan && caps.Scan {
		return search.EngineScan, nil
	}
	return "", fmt.Errorf("no query engine available for namespace")
}

func cloneConfig(cfg Config) Config {
	out := cfg
	out.Normalize()
	return out
}

// Default returns the store's default namespace configuration.
func (s *ConfigStore) Default() Config {
	if s == nil {
		return DefaultConfig()
	}
	return cloneConfig(s.defaultCfg)
}
