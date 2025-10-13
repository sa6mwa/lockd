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
	if s3cfg.Region != "us-west-2" || !s3cfg.ForcePathStyle || s3cfg.Insecure {
		t.Fatalf("unexpected cfg: %+v", s3cfg)
	}

	if _, _, _, err := BuildS3Config(Config{Store: "mem://"}); err == nil {
		t.Fatalf("expected error for non-s3 store")
	}
}

func TestBuildMinioConfig(t *testing.T) {
	cfg := Config{
		Store:         "minio://localhost:9000/test-bucket/prefix/path",
		S3MaxPartSize: 4 << 20,
	}
	minioCfg, err := BuildMinioConfig(cfg)
	if err != nil {
		t.Fatalf("BuildMinioConfig: %v", err)
	}
	if minioCfg.Endpoint != "localhost:9000" {
		t.Fatalf("unexpected endpoint: %s", minioCfg.Endpoint)
	}
	if minioCfg.Bucket != "test-bucket" {
		t.Fatalf("unexpected bucket: %s", minioCfg.Bucket)
	}
	if minioCfg.Prefix != "prefix/path" {
		t.Fatalf("unexpected prefix: %s", minioCfg.Prefix)
	}
	if minioCfg.Insecure {
		t.Fatalf("expected TLS enabled by default for minio backend")
	}
	if _, err := BuildMinioConfig(Config{Store: "minio://localhost:9000"}); err == nil {
		t.Fatalf("expected error for missing bucket")
	}
	if _, err := BuildMinioConfig(Config{Store: "s3://bucket"}); err == nil {
		t.Fatalf("expected error for non-minio store")
	}
}

func TestBuildAzureConfig(t *testing.T) {
	t.Setenv("AZURE_STORAGE_ACCOUNT_KEY", "secret")
	cfg := Config{Store: "azure://myaccount/container/prefix/path"}
	azureCfg, err := BuildAzureConfig(cfg)
	if err != nil {
		t.Fatalf("BuildAzureConfig: %v", err)
	}
	if azureCfg.Account != "myaccount" {
		t.Fatalf("unexpected account: %s", azureCfg.Account)
	}
	if azureCfg.Container != "container" {
		t.Fatalf("unexpected container: %s", azureCfg.Container)
	}
	if azureCfg.Prefix != "prefix/path" {
		t.Fatalf("unexpected prefix: %s", azureCfg.Prefix)
	}
	if azureCfg.AccountKey != "secret" {
		t.Fatalf("expected account key from env")
	}

	cfgMissing := Config{Store: "azure:///container"}
	if _, err := BuildAzureConfig(cfgMissing); err == nil {
		t.Fatalf("expected error for missing account")
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
