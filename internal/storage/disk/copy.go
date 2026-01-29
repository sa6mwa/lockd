package disk

import (
	"context"

	"pkt.systems/lockd/internal/storage"
)

// CopyObject copies an object within the disk-backed store.
func (s *Store) CopyObject(ctx context.Context, namespace, srcKey, dstKey string, opts storage.CopyObjectOptions) (*storage.ObjectInfo, error) {
	src, err := s.GetObject(ctx, namespace, srcKey)
	if err != nil {
		return nil, err
	}
	defer src.Reader.Close()
	putOpts := storage.PutObjectOptions{
		ExpectedETag: opts.ExpectedETag,
		IfNotExists:  opts.IfNotExists,
		ContentType:  "",
		Descriptor:   nil,
	}
	if src.Info != nil {
		putOpts.ContentType = src.Info.ContentType
		putOpts.Descriptor = append([]byte(nil), src.Info.Descriptor...)
	}
	return s.PutObject(ctx, namespace, dstKey, src.Reader, putOpts)
}
