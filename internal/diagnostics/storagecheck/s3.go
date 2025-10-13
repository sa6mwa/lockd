package storagecheck

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	minio "github.com/minio/minio-go/v7"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/storage/s3"
)

// Result captures the outcome of store verification checks.
type Result struct {
	Provider          string
	Bucket            string
	Prefix            string
	Path              string
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
	if strings.HasPrefix(cfg.Store, "s3://") {
		s3cfg, bucket, prefix, err := lockd.BuildS3Config(cfg)
		if err != nil {
			return Result{}, err
		}
		return verifyObjectStore(ctx, "aws-s3", s3cfg, bucket, prefix, true)
	}
	if strings.HasPrefix(cfg.Store, "minio://") {
		minioCfg, err := lockd.BuildMinioConfig(cfg)
		if err != nil {
			return Result{}, err
		}
		return verifyObjectStore(ctx, "minio", minioCfg, minioCfg.Bucket, minioCfg.Prefix, false)
	}
	if strings.HasPrefix(cfg.Store, "disk://") {
		return verifyDisk(ctx, cfg)
	}
	return Result{}, storage.ErrNotImplemented
}

func verifyObjectStore(ctx context.Context, provider string, s3cfg s3.Config, bucket, prefix string, includePolicy bool) (Result, error) {
	store, err := s3.New(s3cfg)
	if err != nil {
		return Result{}, fmt.Errorf("init %s store: %w", provider, err)
	}
	client := store.Client()
	result := Result{Provider: provider, Bucket: bucket, Prefix: prefix}

	if provider == "aws-s3" {
		if loc, err := client.GetBucketLocation(ctx, bucket); err == nil && loc != "" && !strings.EqualFold(loc, s3cfg.Region) {
			result.AdditionalMessage = fmt.Sprintf("Bucket region is %s; set LOCKD_S3_REGION or LOCKD_S3_ENDPOINT to match.", loc)
		}
	}

	run := func(name string, fn func(context.Context) error) {
		err := fn(ctx)
		result.Checks = append(result.Checks, CheckResult{Name: name, Err: err})
	}

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	verifyPrefix := strings.Trim(prefix, "/")
	if verifyPrefix != "" {
		verifyPrefix += "/"
	}
	verifyPrefix = path.Join(verifyPrefix, "lockd-diagnostics")
	keyID := uuid.NewString()
	metaObject := path.Join(verifyPrefix, "meta", keyID+".json")
	stateObject := path.Join(verifyPrefix, "state", keyID+".json")

	run("BucketExists", func(ctx context.Context) error {
		exists, err := client.BucketExists(ctx, bucket)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("bucket %s does not exist", bucket)
		}
		return nil
	})

	run("ListObjects", func(ctx context.Context) error {
		opts := minio.ListObjectsOptions{Prefix: path.Join(strings.Trim(prefix, "/"), "meta/"), Recursive: true, MaxKeys: 1}
		for obj := range client.ListObjects(ctx, bucket, opts) {
			return obj.Err
		}
		return nil
	})

	run("PutMeta", func(ctx context.Context) error {
		_, err := client.PutObject(ctx, bucket, metaObject, strings.NewReader(`{"diagnostic":true}`), -1, minio.PutObjectOptions{ContentType: "application/json"})
		return err
	})

	run("GetMeta", func(ctx context.Context) error {
		_, err := client.StatObject(ctx, bucket, metaObject, minio.StatObjectOptions{})
		return err
	})

	run("PutState", func(ctx context.Context) error {
		_, err := client.PutObject(ctx, bucket, stateObject, strings.NewReader("{}"), -1, minio.PutObjectOptions{ContentType: "application/json"})
		return err
	})

	run("DeleteObjects", func(ctx context.Context) error {
		if err := client.RemoveObject(ctx, bucket, metaObject, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
		if err := client.RemoveObject(ctx, bucket, stateObject, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
		return nil
	})

	if includePolicy && !result.Passed() {
		result.RecommendedPolicy = buildAWSPolicy(bucket, prefix)
	}
	return result, nil
}

func buildAWSPolicy(bucket, prefix string) string {
	resources := []string{fmt.Sprintf("arn:aws:s3:::%s", bucket)}
	trim := strings.Trim(prefix, "/")
	metaRes := fmt.Sprintf("arn:aws:s3:::%s/meta/*", bucket)
	stateRes := fmt.Sprintf("arn:aws:s3:::%s/state/*", bucket)
	diagMeta := fmt.Sprintf("arn:aws:s3:::%s/lockd-diagnostics/meta/*", bucket)
	diagState := fmt.Sprintf("arn:aws:s3:::%s/lockd-diagnostics/state/*", bucket)
	if trim != "" {
		metaRes = fmt.Sprintf("arn:aws:s3:::%s/%s/meta/*", bucket, trim)
		stateRes = fmt.Sprintf("arn:aws:s3:::%s/%s/state/*", bucket, trim)
		diagMeta = fmt.Sprintf("arn:aws:s3:::%s/%s/lockd-diagnostics/meta/*", bucket, trim)
		diagState = fmt.Sprintf("arn:aws:s3:::%s/%s/lockd-diagnostics/state/*", bucket, trim)
		resources = append(resources, fmt.Sprintf("arn:aws:s3:::%s/%s", bucket, trim))
	}
	resources = append(resources, metaRes, stateRes, diagMeta, diagState)
	policy := map[string]any{
		"Version": "2012-10-17",
		"Statement": []any{
			map[string]any{
				"Effect": "Allow",
				"Action": []string{
					"s3:GetObject",
					"s3:PutObject",
					"s3:DeleteObject",
					"s3:ListBucket",
					"s3:AbortMultipartUpload",
				},
				"Resource": resources,
			},
		},
	}
	enc, _ := json.MarshalIndent(policy, "", "  ")
	return string(enc)
}
