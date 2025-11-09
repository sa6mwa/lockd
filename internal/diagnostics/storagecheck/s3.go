package storagecheck

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/namespaces"
)

// Result captures the outcome of store verification checks.
type Result struct {
	Provider          string
	Bucket            string
	Prefix            string
	Path              string
	Endpoint          string
	Insecure          bool
	Credentials       lockd.CredentialSummary
	Checks            []CheckResult
	RecommendedPolicy string
	AdditionalMessage string
}

// Passed reports whether all checks succeeded.
func (r Result) Passed() bool {
	for _, check := range r.Checks {
		if check.Err != nil {
			return false
		}
	}
	return true
}

// CheckResult is the outcome of a single verification step.
type CheckResult struct {
	Name string
	Err  error
}

// VerifyStore runs provider-specific diagnostics for the configured backend.
func VerifyStore(ctx context.Context, cfg lockd.Config) (Result, error) {
	crypto, cfgWithCrypto, err := prepareCryptoForVerify(cfg)
	if err != nil {
		return Result{}, err
	}
	cfg = cfgWithCrypto
	if strings.HasPrefix(cfg.Store, "aws://") {
		awsCfg, creds, err := lockd.BuildAWSConfig(cfg)
		if err != nil {
			return Result{}, err
		}
		awsCfg.Crypto = crypto
		return verifyObjectStore(ctx, "aws", awsCfg, true, creds)
	}
	if strings.HasPrefix(cfg.Store, "s3://") {
		s3cfg, creds, err := lockd.BuildGenericS3Config(cfg)
		if err != nil {
			return Result{}, err
		}
		s3cfg.Crypto = crypto
		return verifyObjectStore(ctx, "s3-compatible", s3cfg, false, creds)
	}
	if strings.HasPrefix(cfg.Store, "azure://") {
		azureCfg, err := lockd.BuildAzureConfig(cfg)
		if err != nil {
			return Result{}, err
		}
		azureCfg.Crypto = crypto
		return verifyAzure(ctx, azureCfg, crypto)
	}
	if strings.HasPrefix(cfg.Store, "disk://") {
		diskCfg, root, err := lockd.BuildDiskConfig(cfg)
		if err != nil {
			return Result{}, err
		}
		diskCfg.Crypto = crypto
		return verifyDisk(ctx, diskCfg, root, crypto)
	}
	return Result{}, storage.ErrNotImplemented
}

func verifyObjectStore(ctx context.Context, provider string, s3cfg s3.Config, includePolicy bool, summary lockd.CredentialSummary) (Result, error) {
	store, err := s3.New(s3cfg)
	if err != nil {
		return Result{}, fmt.Errorf("init %s store: %w", provider, err)
	}
	defer store.Close()
	client := store.Client()
	result := Result{
		Provider:    provider,
		Bucket:      s3cfg.Bucket,
		Prefix:      s3cfg.Prefix,
		Path:        s3cfg.Prefix,
		Endpoint:    s3cfg.Endpoint,
		Insecure:    s3cfg.Insecure,
		Credentials: summary,
	}

	if provider == "aws" {
		if loc, err := client.GetBucketLocation(ctx, s3cfg.Bucket); err == nil && loc != "" && !strings.EqualFold(loc, s3cfg.Region) {
			result.AdditionalMessage = fmt.Sprintf("Bucket region is %s; set --aws-region, LOCKD_AWS_REGION, or AWS_REGION to match.", loc)
		}
	}

	run := func(name string, fn func(context.Context) error) {
		err := fn(ctx)
		result.Checks = append(result.Checks, CheckResult{Name: name, Err: err})
	}

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	cryptoEnabled := s3cfg.Crypto != nil && s3cfg.Crypto.Enabled()

	if cryptoEnabled {
		run("CleanupDiagnostics", func(ctx context.Context) error {
			return cleanupSyntheticDiagnostics(ctx, store)
		})
	}

	verifyPrefix := path.Join(strings.Trim(s3cfg.Prefix, "/"), namespaces.Default, "lockd-diagnostics")
	keyID := uuidv7.NewString()
	metaObject := path.Join(verifyPrefix, "meta", keyID+".json")
	stateObject := path.Join(verifyPrefix, "state", keyID+".json")
	queuePrefix := path.Join(verifyPrefix, "q", keyID, "msg")
	queueBinaryObject := path.Join(queuePrefix, keyID+".bin")
	queueIndexObject := path.Join(queuePrefix, keyID+".json")

	run("BucketExists", func(ctx context.Context) error {
		exists, err := client.BucketExists(ctx, s3cfg.Bucket)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("bucket %s does not exist", s3cfg.Bucket)
		}
		return nil
	})

	run("ListObjects", func(ctx context.Context) error {
		prefix := path.Join(strings.Trim(s3cfg.Prefix, "/"), "meta/")
		opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true, MaxKeys: 1}
		for obj := range client.ListObjects(ctx, s3cfg.Bucket, opts) {
			return obj.Err
		}
		return nil
	})

	run("PutMeta", func(ctx context.Context) error {
		if cryptoEnabled {
			return nil
		}
		_, err := client.PutObject(ctx, s3cfg.Bucket, metaObject, strings.NewReader(`{"diagnostic":true}`), -1, minio.PutObjectOptions{ContentType: "application/json"})
		return err
	})

	run("GetMeta", func(ctx context.Context) error {
		if cryptoEnabled {
			return nil
		}
		_, err := client.StatObject(ctx, s3cfg.Bucket, metaObject, minio.StatObjectOptions{})
		return err
	})

	run("PutState", func(ctx context.Context) error {
		if cryptoEnabled {
			return nil
		}
		_, err := client.PutObject(ctx, s3cfg.Bucket, stateObject, strings.NewReader("{}"), -1, minio.PutObjectOptions{ContentType: "application/json"})
		return err
	})

	run("PutQueueBinary", func(ctx context.Context) error {
		if cryptoEnabled {
			return nil
		}
		_, err := client.PutObject(ctx, s3cfg.Bucket, queueBinaryObject, strings.NewReader(""), 0, minio.PutObjectOptions{ContentType: "application/octet-stream"})
		return err
	})

	run("PutQueueIndex", func(ctx context.Context) error {
		if cryptoEnabled {
			return nil
		}
		_, err := client.PutObject(ctx, s3cfg.Bucket, queueIndexObject, strings.NewReader(`{"diagnostic":true}`), -1, minio.PutObjectOptions{ContentType: "application/json"})
		return err
	})

	if s3cfg.Crypto != nil && s3cfg.Crypto.Enabled() {
		run("CryptoMetaStateRoundTrip", func(ctx context.Context) error {
			return verifyMetaStateDecryption(ctx, store, s3cfg.Crypto)
		})
		run("CryptoQueueRoundTrip", func(ctx context.Context) error {
			return verifyQueueEncryption(ctx, store, s3cfg.Crypto)
		})

		run("CleanupDiagnosticsFinal", func(ctx context.Context) error {
			return cleanupSyntheticDiagnostics(ctx, store)
		})
	}

	run("DeleteObjects", func(ctx context.Context) error {
		if cryptoEnabled {
			return nil
		}
		if err := client.RemoveObject(ctx, s3cfg.Bucket, metaObject, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
		if err := client.RemoveObject(ctx, s3cfg.Bucket, stateObject, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
		if err := client.RemoveObject(ctx, s3cfg.Bucket, queueBinaryObject, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
		if err := client.RemoveObject(ctx, s3cfg.Bucket, queueIndexObject, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
		return nil
	})

	if includePolicy && !result.Passed() {
		result.RecommendedPolicy = buildAWSPolicy(s3cfg.Bucket, s3cfg.Prefix)
	}
	return result, nil
}

func buildAWSPolicy(bucket, prefix string) string {
	bucketARN := fmt.Sprintf("arn:aws:s3:::%s", bucket)
	trim := strings.Trim(prefix, "/")
	basePrefix := fmt.Sprintf("arn:aws:s3:::%s/*", bucket)
	if trim != "" {
		basePrefix = fmt.Sprintf("arn:aws:s3:::%s/%s/*", bucket, trim)
	}
	objectResources := []string{basePrefix}
	policy := map[string]any{
		"Version": "2012-10-17",
		"Statement": []any{
			map[string]any{
				"Effect": "Allow",
				"Action": []string{
					"s3:ListBucket",
					"s3:GetBucketLocation",
				},
				"Resource": []string{bucketARN},
			},
			map[string]any{
				"Effect": "Allow",
				"Action": []string{
					"s3:GetObject",
					"s3:PutObject",
					"s3:DeleteObject",
					"s3:ListBucket",
					"s3:AbortMultipartUpload",
				},
				"Resource": objectResources,
			},
		},
	}
	enc, _ := json.MarshalIndent(policy, "", "  ")
	return string(enc)
}
