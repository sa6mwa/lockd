package s3

import (
	"context"
	"time"

	"github.com/minio/minio-go/v7"

	"pkt.systems/lockd/internal/storage"
)

// CopyObject performs a server-side copy within the bucket.
func (s *Store) CopyObject(ctx context.Context, namespace, srcKey, dstKey string, opts storage.CopyObjectOptions) (*storage.ObjectInfo, error) {
	logger, verbose := s.loggers(ctx)
	srcObject := s.objectKey(namespace, srcKey)
	dstObject := s.objectKey(namespace, dstKey)

	if opts.ExpectedETag != "" || opts.IfNotExists {
		info, err := s.client.StatObject(ctx, s.cfg.Bucket, dstObject, minio.StatObjectOptions{})
		if err == nil {
			if opts.IfNotExists {
				return nil, storage.ErrCASMismatch
			}
			if opts.ExpectedETag != "" && stripETag(info.ETag) != opts.ExpectedETag {
				return nil, storage.ErrCASMismatch
			}
		} else if !isNotFound(err) {
			return nil, s.wrapError(err, "s3: copy object stat dest")
		} else if opts.ExpectedETag != "" {
			return nil, storage.ErrNotFound
		}
	}

	verbose.Trace("s3.copy_object.begin",
		"namespace", namespace,
		"src_key", srcKey,
		"dst_key", dstKey,
		"src_object", srcObject,
		"dst_object", dstObject,
	)
	srcOpts := minio.CopySrcOptions{Bucket: s.cfg.Bucket, Object: srcObject}
	dstOpts := minio.CopyDestOptions{Bucket: s.cfg.Bucket, Object: dstObject}
	info, err := s.client.CopyObject(ctx, dstOpts, srcOpts)
	if err != nil {
		logger.Debug("s3.copy_object.copy_error", "namespace", namespace, "src_key", srcKey, "dst_key", dstKey, "error", err)
		return nil, s.wrapError(err, "s3: copy object")
	}
	meta, err := s.client.StatObject(ctx, s.cfg.Bucket, dstObject, minio.StatObjectOptions{})
	if err != nil {
		return &storage.ObjectInfo{
			Key:          dstKey,
			ETag:         stripETag(info.ETag),
			LastModified: time.Now().UTC(),
		}, nil
	}
	out := &storage.ObjectInfo{
		Key:          dstKey,
		ETag:         stripETag(meta.ETag),
		Size:         meta.Size,
		LastModified: meta.LastModified,
		ContentType:  meta.ContentType,
	}
	if desc, derr := decodeDescriptor(meta.UserMetadata); derr == nil && len(desc) > 0 {
		out.Descriptor = append([]byte(nil), desc...)
	}
	return out, nil
}
