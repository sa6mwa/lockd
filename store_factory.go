package lockd

import (
	"fmt"
	"net/url"
	"strings"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/memory"
	pebblestore "pkt.systems/lockd/internal/storage/pebble"
	"pkt.systems/lockd/internal/storage/s3"
)

func openBackend(cfg Config) (storage.Backend, error) {
	u, err := url.Parse(cfg.Store)
	if err != nil {
		return nil, fmt.Errorf("parse store URL: %w", err)
	}
	switch u.Scheme {
	case "memory", "mem", "":
		return memory.New(), nil
	case "s3":
		s3cfg, _, _, err := BuildS3Config(cfg)
		if err != nil {
			return nil, err
		}
		backend, err := s3.New(s3cfg)
		if err != nil {
			return nil, err
		}
		return backend, nil
	case "pebble":
		path := strings.TrimPrefix(u.Path, "/")
		if path == "" {
			return nil, fmt.Errorf("pebble store path required (e.g. pebble:///var/lib/lockd)")
		}
		return pebblestore.Open(path)
	default:
		return nil, fmt.Errorf("store scheme %q not supported yet", u.Scheme)
	}
}

// BuildS3Config derives the S3 backend configuration along with bucket and prefix.
func BuildS3Config(cfg Config) (s3.Config, string, string, error) {
	u, err := url.Parse(cfg.Store)
	if err != nil {
		return s3.Config{}, "", "", fmt.Errorf("parse store URL: %w", err)
	}
	if u.Scheme != "s3" {
		return s3.Config{}, "", "", fmt.Errorf("store scheme %q not supported", u.Scheme)
	}
	bucket := u.Host
	if bucket == "" {
		return s3.Config{}, "", "", fmt.Errorf("store url missing bucket")
	}
	prefix := strings.TrimPrefix(u.Path, "/")
	secure := !cfg.S3DisableTLS
	endpoint := cfg.S3Endpoint
	forcePath := cfg.S3ForcePath
	if endpoint == "" && cfg.S3Region == "" {
		return s3.Config{}, "", "", fmt.Errorf("s3 region or endpoint required for bucket %s", bucket)
	}
	if endpoint != "" {
		eu, err := url.Parse(endpoint)
		if err != nil {
			return s3.Config{}, "", "", fmt.Errorf("parse s3 endpoint: %w", err)
		}
		if eu.Scheme != "" {
			secure = eu.Scheme != "http"
		}
		if eu.Host != "" {
			endpoint = eu.Host
		} else {
			endpoint = eu.Path
		}
		if eu.RawQuery != "" {
			endpoint = eu.Host
		}
		forcePath = true
	} else if cfg.S3Region != "" {
		endpoint = fmt.Sprintf("s3.%s.amazonaws.com", cfg.S3Region)
	}
	return s3.Config{
		Endpoint:       endpoint,
		Region:         cfg.S3Region,
		Bucket:         bucket,
		Prefix:         prefix,
		Secure:         secure,
		ForcePathStyle: forcePath,
		PartSize:       cfg.S3MaxPartSize,
		ServerSideEnc:  cfg.S3SSE,
		KMSKeyID:       cfg.S3KMSKeyID,
	}, bucket, prefix, nil
}
