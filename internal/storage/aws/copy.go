package aws

import (
	"context"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"pkt.systems/lockd/internal/storage"
)

// CopyObject performs a server-side copy within the bucket.
func (s *Store) CopyObject(ctx context.Context, namespace, srcKey, dstKey string, opts storage.CopyObjectOptions) (*storage.ObjectInfo, error) {
	logger, verbose := s.loggers(ctx)
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	srcObject := s.objectKey(namespace, srcKey)
	dstObject := s.objectKey(namespace, dstKey)

	if opts.ExpectedETag != "" || opts.IfNotExists {
		stat, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(dstObject)})
		if err == nil {
			if opts.IfNotExists {
				return nil, storage.ErrCASMismatch
			}
			if opts.ExpectedETag != "" && stripETag(aws.ToString(stat.ETag)) != opts.ExpectedETag {
				return nil, storage.ErrCASMismatch
			}
		} else if !isNotFound(err) {
			return nil, s.wrapError(err, "aws: copy object stat dest")
		} else if opts.ExpectedETag != "" {
			return nil, storage.ErrNotFound
		}
	}

	verbose.Trace("aws.copy_object.begin",
		"namespace", namespace,
		"src_key", srcKey,
		"dst_key", dstKey,
		"src_object", srcObject,
		"dst_object", dstObject,
	)
	copySource := url.PathEscape(s.cfg.Bucket + "/" + srcObject)
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(s.cfg.Bucket),
		Key:        aws.String(dstObject),
		CopySource: aws.String(copySource),
	}
	if opts.ExpectedETag != "" {
		input.IfMatch = aws.String(opts.ExpectedETag)
	} else if opts.IfNotExists {
		input.IfNoneMatch = aws.String("*")
	}
	applySSEToCopy(input, s.cfg.ServerSideEnc, s.cfg.KMSKeyID)

	out, err := s.client.CopyObject(ctx, input)
	if err != nil {
		logger.Debug("aws.copy_object.copy_error", "namespace", namespace, "src_key", srcKey, "dst_key", dstKey, "error", err)
		return nil, s.wrapError(err, "aws: copy object")
	}
	etag := ""
	if out.CopyObjectResult != nil {
		etag = stripETag(aws.ToString(out.CopyObjectResult.ETag))
	}
	stat, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(dstObject)})
	if err != nil {
		return &storage.ObjectInfo{
			Key:          dstKey,
			ETag:         etag,
			LastModified: time.Now().UTC(),
		}, nil
	}
	outInfo := &storage.ObjectInfo{
		Key:          dstKey,
		ETag:         stripETag(aws.ToString(stat.ETag)),
		Size:         aws.ToInt64(stat.ContentLength),
		LastModified: aws.ToTime(stat.LastModified),
		ContentType:  aws.ToString(stat.ContentType),
	}
	if desc, derr := decodeDescriptor(stat.Metadata); derr == nil && len(desc) > 0 {
		outInfo.Descriptor = append([]byte(nil), desc...)
	}
	return outInfo, nil
}
