package lockd

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/disk"
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
	case "minio":
		minioCfg, err := BuildMinioConfig(cfg)
		if err != nil {
			return nil, err
		}
		backend, err := s3.New(minioCfg)
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
	case "disk":
		path := strings.TrimSpace(u.Path)
		host := strings.TrimSpace(u.Host)
		if host != "" {
			if path == "" || path == "/" {
				path = "/" + host
			} else {
				path = "/" + host + "/" + strings.TrimPrefix(path, "/")
			}
		}
		if path == "" || path == "/" {
			return nil, fmt.Errorf("disk store path required (e.g. disk:///var/lib/lockd-data)")
		}
		return disk.New(disk.Config{
			Root:            filepath.Clean(path),
			Retention:       cfg.DiskRetention,
			JanitorInterval: cfg.DiskJanitorInterval,
		})
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
		Insecure:       !secure,
		ForcePathStyle: forcePath,
		PartSize:       cfg.S3MaxPartSize,
		ServerSideEnc:  cfg.S3SSE,
		KMSKeyID:       cfg.S3KMSKeyID,
	}, bucket, prefix, nil
}

// BuildMinioConfig adapts a minio:// store URL into an S3 configuration.
func BuildMinioConfig(cfg Config) (s3.Config, error) {
	u, err := url.Parse(cfg.Store)
	if err != nil {
		return s3.Config{}, fmt.Errorf("parse store URL: %w", err)
	}
	if u.Scheme != "minio" {
		return s3.Config{}, fmt.Errorf("store scheme %q not supported", u.Scheme)
	}
	endpoint := u.Host
	if endpoint == "" {
		return s3.Config{}, fmt.Errorf("minio store missing host (expected minio://host:port/bucket[/prefix])")
	}
	path := strings.Trim(strings.TrimPrefix(u.Path, "/"), "/")
	if path == "" {
		return s3.Config{}, fmt.Errorf("minio store missing bucket (expected minio://host:port/bucket[/prefix])")
	}
	parts := strings.SplitN(path, "/", 2)
	bucket := parts[0]
	if bucket == "" {
		return s3.Config{}, fmt.Errorf("minio store missing bucket name")
	}
	var prefix string
	if len(parts) == 2 {
		prefix = parts[1]
	}
	query := u.Query()
	secure := !cfg.S3DisableTLS
	if v := query.Get("secure"); v != "" {
		if v == "0" || strings.EqualFold(v, "false") || strings.EqualFold(v, "no") {
			secure = false
		} else if v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes") {
			secure = true
		}
	}
	if v := query.Get("tls"); v != "" {
		if v == "0" || strings.EqualFold(v, "false") || strings.EqualFold(v, "no") {
			secure = false
		} else if v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes") {
			secure = true
		}
	}
	if v := query.Get("scheme"); v != "" {
		if strings.EqualFold(v, "http") {
			secure = false
		} else if strings.EqualFold(v, "https") {
			secure = true
		}
	}
	if v := query.Get("insecure"); v != "" {
		if v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes") {
			secure = false
		}
	}

	return s3.Config{
		Endpoint:       endpoint,
		Region:         cfg.S3Region,
		Bucket:         bucket,
		Prefix:         prefix,
		Insecure:       !secure,
		ForcePathStyle: true,
		PartSize:       cfg.S3MaxPartSize,
		ServerSideEnc:  cfg.S3SSE,
		KMSKeyID:       cfg.S3KMSKeyID,
	}, nil
}
