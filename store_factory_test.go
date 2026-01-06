package lockd

import (
	"testing"

	"pkt.systems/lockd/internal/storage/memory"
)

func TestOpenBackendMemory(t *testing.T) {
	cfg := Config{
		Store:                    "mem://",
		DisableStorageEncryption: true, // memory backend has no bundle, so opt out of crypto
	}
	backend, err := openBackend(cfg, nil)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	defer backend.Close()
	if _, ok := backend.(*memory.Store); !ok {
		t.Fatalf("expected memory backend, got %T", backend)
	}
}

func TestBuildGenericS3Config(t *testing.T) {
	cfg := Config{
		Store:             "s3://localhost:9000/test-bucket/prefix/path?insecure=1&path-style=1&kms-key-id=k1",
		S3MaxPartSize:     8 << 20,
		S3SSE:             "AES256",
		S3AccessKeyID:     "minio",
		S3SecretAccessKey: "minio123",
		S3SessionToken:    "session",
	}
	s3Result, err := BuildGenericS3Config(cfg)
	if err != nil {
		t.Fatalf("BuildGenericS3Config: %v", err)
	}
	s3cfg := s3Result.Config
	summary := s3Result.Credentials
	if s3cfg.Endpoint != "localhost:9000" {
		t.Fatalf("unexpected endpoint: %s", s3cfg.Endpoint)
	}
	if s3cfg.Bucket != "test-bucket" {
		t.Fatalf("unexpected bucket: %s", s3cfg.Bucket)
	}
	if s3cfg.Prefix != "prefix/path" {
		t.Fatalf("unexpected prefix: %s", s3cfg.Prefix)
	}
	if !s3cfg.Insecure {
		t.Fatalf("expected insecure flag from query")
	}
	if !s3cfg.ForcePathStyle {
		t.Fatalf("expected force path style")
	}
	if s3cfg.KMSKeyID != "k1" {
		t.Fatalf("unexpected kms key: %s", s3cfg.KMSKeyID)
	}
	if summary.AccessKey != "minio" || !summary.HasSecret || summary.Source != "config" {
		t.Fatalf("unexpected credential summary: %+v", summary)
	}
	if _, err := BuildGenericS3Config(Config{Store: "s3://"}); err == nil {
		t.Fatalf("expected error for missing bucket")
	}
	if _, err := BuildGenericS3Config(Config{Store: "mem://"}); err == nil {
		t.Fatalf("expected error for non-s3 store")
	}
}

func TestBuildAWSConfig(t *testing.T) {
	cfg := Config{
		Store:         "aws://my-bucket/prefix",
		AWSRegion:     "us-west-2",
		AWSKMSKeyID:   "aws-kms",
		S3MaxPartSize: 4 << 20,
	}
	awsResult, err := BuildAWSConfig(cfg)
	if err != nil {
		t.Fatalf("BuildAWSConfig: %v", err)
	}
	awsCfg := awsResult.Config
	summary := awsResult.Credentials
	if awsCfg.Endpoint != "s3.us-west-2.amazonaws.com" {
		t.Fatalf("unexpected endpoint: %s", awsCfg.Endpoint)
	}
	if awsCfg.Bucket != "my-bucket" {
		t.Fatalf("unexpected bucket: %s", awsCfg.Bucket)
	}
	if awsCfg.Prefix != "prefix" {
		t.Fatalf("unexpected prefix: %s", awsCfg.Prefix)
	}
	if awsCfg.Region != "us-west-2" {
		t.Fatalf("unexpected region: %s", awsCfg.Region)
	}
	if awsCfg.KMSKeyID != "aws-kms" {
		t.Fatalf("unexpected kms key: %s", awsCfg.KMSKeyID)
	}
	if summary.Source == "" {
		t.Fatalf("expected credential summary source")
	}
	if _, err := BuildAWSConfig(Config{Store: "aws://"}); err == nil {
		t.Fatalf("expected error for missing bucket")
	}
	if _, err := BuildAWSConfig(Config{Store: "aws://bucket"}); err == nil {
		t.Fatalf("expected error for missing region")
	}
}

func TestBuildAzureConfig(t *testing.T) {
	cfg := Config{
		Store:           "azure://myaccount/container/prefix/path",
		AzureAccountKey: "secret",
	}
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
		t.Fatalf("expected account key from config")
	}

	cfgMissing := Config{Store: "azure:///container"}
	if _, err := BuildAzureConfig(cfgMissing); err == nil {
		t.Fatalf("expected error for missing account")
	}
}
