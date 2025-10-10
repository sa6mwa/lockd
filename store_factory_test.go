package lockd

import (
	"testing"

	"pkt.systems/lockd/internal/storage/memory"
)

func TestOpenBackendMemory(t *testing.T) {
	cfg := Config{Store: "mem://"}
	backend, err := openBackend(cfg)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()
	if _, ok := backend.(*memory.Store); !ok {
		t.Fatalf("expected memory backend, got %T", backend)
	}
}

func TestBuildS3Config(t *testing.T) {
	cfg := Config{
		Store:         "s3://bucket/prefix",
		S3Region:      "us-west-2",
		S3MaxPartSize: 8 << 20,
		S3ForcePath:   true,
		S3DisableTLS:  true,
		S3SSE:         "AES256",
		S3Endpoint:    "https://s3.us-west-2.amazonaws.com",
	}
	s3cfg, bucket, prefix, err := BuildS3Config(cfg)
	if err != nil {
		t.Fatalf("BuildS3Config: %v", err)
	}
	if bucket != "bucket" {
		t.Fatalf("bucket mismatch: %s", bucket)
	}
	if prefix != "prefix" {
		t.Fatalf("prefix mismatch: %s", prefix)
	}
	if s3cfg.Region != "us-west-2" || !s3cfg.ForcePathStyle || !s3cfg.Secure {
		t.Fatalf("unexpected cfg: %+v", s3cfg)
	}

	if _, _, _, err := BuildS3Config(Config{Store: "mem://"}); err == nil {
		t.Fatalf("expected error for non-s3 store")
	}
}

func TestOpenBackendPebble(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Store: "pebble:///" + dir}
	backend, err := openBackend(cfg)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()
}
