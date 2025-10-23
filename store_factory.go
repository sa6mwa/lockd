package lockd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	minioCredentials "github.com/minio/minio-go/v7/pkg/credentials"

	"pkt.systems/lockd/internal/storage"
	azurestore "pkt.systems/lockd/internal/storage/azure"
	"pkt.systems/lockd/internal/storage/disk"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/lockd/internal/storage/s3"
)

// CredentialSummary describes which credentials were selected for object storage.
type CredentialSummary struct {
	AccessKey string
	HasSecret bool
	Source    string
}

func openBackend(cfg Config, crypto *storage.Crypto) (storage.Backend, error) {
	if cfg.StorageEncryptionEnabled && (crypto == nil || !crypto.Enabled()) {
		return nil, fmt.Errorf("config: storage encryption enabled but crypto material missing")
	}
	u, err := url.Parse(cfg.Store)
	if err != nil {
		return nil, fmt.Errorf("parse store URL: %w", err)
	}
	switch u.Scheme {
	case "memory", "mem", "":
		return memory.NewWithConfig(memory.Config{QueueWatch: cfg.MemQueueWatch, Crypto: crypto}), nil
	case "s3":
		s3cfg, _, err := BuildGenericS3Config(cfg)
		if err != nil {
			return nil, err
		}
		s3cfg.Crypto = crypto
		backend, err := s3.New(s3cfg)
		if err != nil {
			return nil, err
		}
		if err := ensureObjectStoreReady(context.Background(), backend); err != nil {
			_ = backend.Close()
			return nil, err
		}
		return backend, nil
	case "aws":
		awscfg, _, err := BuildAWSConfig(cfg)
		if err != nil {
			return nil, err
		}
		awscfg.Crypto = crypto
		backend, err := s3.New(awscfg)
		if err != nil {
			return nil, err
		}
		if err := ensureObjectStoreReady(context.Background(), backend); err != nil {
			_ = backend.Close()
			return nil, err
		}
		return backend, nil
	case "disk":
		diskCfg, _, err := BuildDiskConfig(cfg)
		if err != nil {
			return nil, err
		}
		diskCfg.Crypto = crypto
		checks := disk.Verify(context.Background(), diskCfg)
		for _, check := range checks {
			if check.Err != nil {
				return nil, fmt.Errorf("disk store verification failed: %s: %v", check.Name, check.Err)
			}
		}
		return disk.New(diskCfg)
	case "azure":
		azureCfg, err := BuildAzureConfig(cfg)
		if err != nil {
			return nil, err
		}
		azureCfg.Crypto = crypto
		return azurestore.New(azureCfg)
	default:
		return nil, fmt.Errorf("store scheme %q not supported yet", u.Scheme)
	}
}

// BuildGenericS3Config parses s3:// URLs that target generic S3-compatible services (MinIO, etc.).
func BuildGenericS3Config(cfg Config) (s3.Config, CredentialSummary, error) {
	u, err := url.Parse(cfg.Store)
	if err != nil {
		return s3.Config{}, CredentialSummary{}, fmt.Errorf("parse store URL: %w", err)
	}
	if u.Scheme != "s3" {
		return s3.Config{}, CredentialSummary{}, fmt.Errorf("store scheme %q not supported", u.Scheme)
	}
	endpoint := strings.TrimSpace(u.Host)
	if endpoint == "" {
		return s3.Config{}, CredentialSummary{}, fmt.Errorf("s3 store missing host (expected s3://host[:port]/bucket[/prefix])")
	}
	path := strings.Trim(strings.TrimPrefix(u.Path, "/"), "/")
	if path == "" {
		return s3.Config{}, CredentialSummary{}, fmt.Errorf("s3 store missing bucket (expected s3://host[:port]/bucket[/prefix])")
	}
	parts := strings.SplitN(path, "/", 2)
	bucket := strings.TrimSpace(parts[0])
	if bucket == "" {
		return s3.Config{}, CredentialSummary{}, fmt.Errorf("s3 store missing bucket name")
	}
	var prefix string
	if len(parts) == 2 {
		prefix = strings.Trim(parts[1], "/")
	}
	query := u.Query()
	secure := true
	if v := query.Get("scheme"); strings.EqualFold(v, "http") {
		secure = false
	}
	if v := query.Get("tls"); v != "" {
		if ok, err := strconv.ParseBool(v); err == nil {
			secure = ok
		}
	}
	if v := query.Get("secure"); v != "" {
		if ok, err := strconv.ParseBool(v); err == nil {
			secure = ok
		}
	}
	if v := query.Get("insecure"); v != "" {
		if ok, err := strconv.ParseBool(v); err == nil && ok {
			secure = false
		}
	}
	forcePath := false
	if v := query.Get("path-style"); v != "" {
		if ok, err := strconv.ParseBool(v); err == nil {
			forcePath = ok
		}
	}
	kmsKey := cfg.S3KMSKeyID
	if v := query.Get("kms-key-id"); v != "" {
		kmsKey = v
	}
	cred, summary, err := resolveGenericS3Credentials(cfg)
	if err != nil {
		return s3.Config{}, summary, err
	}
	return s3.Config{
		Endpoint:       endpoint,
		Bucket:         bucket,
		Prefix:         prefix,
		Insecure:       !secure,
		ForcePathStyle: forcePath,
		PartSize:       cfg.S3MaxPartSize,
		ServerSideEnc:  cfg.S3SSE,
		KMSKeyID:       kmsKey,
		CustomCreds:    cred,
	}, summary, nil
}

// BuildAWSConfig parses aws:// URLs that target AWS S3 with regional configuration.
func BuildAWSConfig(cfg Config) (s3.Config, CredentialSummary, error) {
	u, err := url.Parse(cfg.Store)
	if err != nil {
		return s3.Config{}, CredentialSummary{}, fmt.Errorf("parse store URL: %w", err)
	}
	if u.Scheme != "aws" {
		return s3.Config{}, CredentialSummary{}, fmt.Errorf("store scheme %q not supported", u.Scheme)
	}
	bucket := strings.TrimSpace(u.Host)
	if bucket == "" {
		return s3.Config{}, CredentialSummary{}, fmt.Errorf("aws store missing bucket (expected aws://bucket[/prefix])")
	}
	prefix := strings.Trim(strings.TrimPrefix(u.Path, "/"), "/")
	region := strings.TrimSpace(cfg.AWSRegion)
	query := u.Query()
	if v := strings.TrimSpace(query.Get("region")); v != "" {
		region = v
	}
	if region == "" {
		return s3.Config{}, CredentialSummary{}, fmt.Errorf("aws store requires region (set --aws-region or LOCKD_AWS_REGION)")
	}
	secure := true
	if v := query.Get("insecure"); v != "" {
		if ok, err := strconv.ParseBool(v); err == nil && ok {
			secure = false
		}
	}
	kmsKey := cfg.AWSKMSKeyID
	if kmsKey == "" {
		kmsKey = cfg.S3KMSKeyID
	}
	if v := query.Get("kms-key-id"); v != "" {
		kmsKey = v
	}
	endpoint := query.Get("endpoint")
	if endpoint == "" {
		endpoint = fmt.Sprintf("s3.%s.amazonaws.com", region)
	}
	cred, summary := resolveAWSCredentials()
	return s3.Config{
		Endpoint:       endpoint,
		Region:         region,
		Bucket:         bucket,
		Prefix:         prefix,
		Insecure:       !secure,
		ForcePathStyle: false,
		PartSize:       cfg.S3MaxPartSize,
		ServerSideEnc:  cfg.S3SSE,
		KMSKeyID:       kmsKey,
		CustomCreds:    cred,
	}, summary, nil
}

func resolveGenericS3Credentials(cfg Config) (*minioCredentials.Credentials, CredentialSummary, error) {
	accessKey := strings.TrimSpace(cfg.S3AccessKeyID)
	secretKey := cfg.S3SecretAccessKey
	sessionToken := cfg.S3SessionToken
	source := "config"
	if accessKey == "" && secretKey == "" && sessionToken == "" {
		accessKey = strings.TrimSpace(os.Getenv("LOCKD_S3_ACCESS_KEY_ID"))
		secretKey = os.Getenv("LOCKD_S3_SECRET_ACCESS_KEY")
		sessionToken = os.Getenv("LOCKD_S3_SESSION_TOKEN")
		source = "env:LOCKD_S3_ACCESS_KEY_ID"
	}
	if accessKey == "" && secretKey == "" && sessionToken == "" {
		accessKey = strings.TrimSpace(os.Getenv("LOCKD_S3_ROOT_USER"))
		secretKey = os.Getenv("LOCKD_S3_ROOT_PASSWORD")
		source = "env:LOCKD_S3_ROOT_USER"
	}
	summary := CredentialSummary{}
	if accessKey == "" && secretKey == "" && sessionToken == "" {
		summary.Source = "anonymous"
		summary.HasSecret = false
		return minioCredentials.NewStaticV4("", "", ""), summary, nil
	}
	if accessKey == "" || secretKey == "" {
		summary.AccessKey = accessKey
		summary.HasSecret = secretKey != ""
		summary.Source = source
		return nil, summary, fmt.Errorf("s3 credentials incomplete (need access key and secret key)")
	}
	summary.AccessKey = accessKey
	summary.HasSecret = true
	summary.Source = source
	return minioCredentials.NewStaticV4(accessKey, secretKey, sessionToken), summary, nil
}

func resolveAWSCredentials() (*minioCredentials.Credentials, CredentialSummary) {
	summary := CredentialSummary{}
	if access := strings.TrimSpace(os.Getenv("AWS_ACCESS_KEY_ID")); access != "" {
		summary.AccessKey = access
		summary.HasSecret = strings.TrimSpace(os.Getenv("AWS_SECRET_ACCESS_KEY")) != ""
		summary.Source = "env:AWS_ACCESS_KEY_ID"
	} else if profile := strings.TrimSpace(os.Getenv("AWS_PROFILE")); profile != "" {
		summary.Source = "profile:" + profile
	} else {
		summary.Source = "auto"
	}
	return nil, summary
}

func ensureObjectStoreReady(ctx context.Context, backend storage.Backend) error {
	s3store, ok := backend.(*s3.Store)
	if !ok {
		return nil
	}
	cfg := s3store.Config()
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	exists, err := s3store.Client().BucketExists(timeoutCtx, cfg.Bucket)
	if err != nil {
		return fmt.Errorf("object store connectivity check failed: %w", err)
	}
	if !exists {
		return fmt.Errorf("object store bucket %s does not exist", cfg.Bucket)
	}
	return nil
}

// BuildAzureConfig derives the Azure backend configuration.
func BuildAzureConfig(cfg Config) (azurestore.Config, error) {
	u, err := url.Parse(cfg.Store)
	if err != nil {
		return azurestore.Config{}, fmt.Errorf("parse store URL: %w", err)
	}
	if u.Scheme != "azure" {
		return azurestore.Config{}, fmt.Errorf("store scheme %q not supported", u.Scheme)
	}
	account := strings.TrimSpace(u.Host)
	if cfg.AzureAccount != "" {
		account = cfg.AzureAccount
	}
	if account == "" {
		account = firstEnv("AZURE_STORAGE_ACCOUNT", "AZURE_STORAGE_ACCOUNT_NAME", "AZURE_ACCOUNT_NAME")
	}
	path := strings.Trim(strings.TrimPrefix(u.Path, "/"), "/")
	if path == "" {
		return azurestore.Config{}, fmt.Errorf("azure store missing container (expected azure://account/container[/prefix])")
	}
	parts := strings.SplitN(path, "/", 2)
	container := parts[0]
	if container == "" {
		return azurestore.Config{}, fmt.Errorf("azure store missing container name")
	}
	prefix := ""
	if len(parts) == 2 {
		prefix = parts[1]
	}
	query := u.Query()
	endpoint := strings.TrimSpace(cfg.AzureEndpoint)
	if v := strings.TrimSpace(query.Get("endpoint")); v != "" {
		endpoint = v
	}
	accountKey := strings.TrimSpace(cfg.AzureAccountKey)
	if accountKey == "" {
		accountKey = firstEnv("LOCKD_AZURE_ACCOUNT_KEY", "AZURE_STORAGE_ACCOUNT_KEY", "AZURE_ACCOUNT_KEY", "AZURE_STORAGE_KEY")
	}
	sas := strings.TrimSpace(cfg.AzureSASToken)
	if v := strings.TrimSpace(query.Get("sas")); v != "" {
		sas = v
	}
	if sas == "" {
		sas = firstEnv("LOCKD_AZURE_SAS_TOKEN", "AZURE_STORAGE_SAS_TOKEN", "AZURE_SAS_TOKEN")
	}
	if account == "" {
		return azurestore.Config{}, fmt.Errorf("azure: account name required (set azure://account/... or AZURE_STORAGE_ACCOUNT)")
	}
	return azurestore.Config{
		Account:    account,
		AccountKey: accountKey,
		Endpoint:   endpoint,
		SASToken:   sas,
		Container:  container,
		Prefix:     prefix,
	}, nil
}

func firstEnv(names ...string) string {
	for _, name := range names {
		if name == "" {
			continue
		}
		if val := strings.TrimSpace(os.Getenv(name)); val != "" {
			return val
		}
	}
	return ""
}

// BuildDiskConfig parses disk:// URLs into a disk.Config.
func BuildDiskConfig(cfg Config) (disk.Config, string, error) {
	u, err := url.Parse(cfg.Store)
	if err != nil {
		return disk.Config{}, "", fmt.Errorf("parse store URL: %w", err)
	}
	if u.Scheme != "disk" {
		return disk.Config{}, "", fmt.Errorf("store scheme %q not supported", u.Scheme)
	}
	pathPart := strings.TrimSpace(u.Path)
	host := strings.TrimSpace(u.Host)
	if host != "" {
		if pathPart == "" || pathPart == "/" {
			pathPart = "/" + host
		} else {
			pathPart = "/" + host + "/" + strings.TrimPrefix(pathPart, "/")
		}
	}
	if pathPart == "" || pathPart == "/" {
		return disk.Config{}, "", fmt.Errorf("disk store path required (e.g. disk:///var/lib/lockd-data)")
	}
	root := filepath.Clean(pathPart)
	cfgDisk := disk.Config{
		Root:            root,
		Retention:       cfg.DiskRetention,
		JanitorInterval: cfg.DiskJanitorInterval,
		QueueWatch:      cfg.DiskQueueWatch,
	}
	return cfgDisk, root, nil
}
