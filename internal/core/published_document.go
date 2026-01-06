package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"pkt.systems/lockd/internal/storage"
)

type PublishedDocumentResult struct {
	Reader    io.ReadCloser
	Version   int64
	NoContent bool
}

func (s *Service) openPublishedDocument(ctx context.Context, namespace, key string) (PublishedDocumentResult, error) {
	storageKey, err := s.namespacedKey(namespace, key)
	if err != nil {
		return PublishedDocumentResult{}, Failure{Code: "invalid_key", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	meta, _, err := s.ensureMeta(ctx, namespace, storageKey)
	if err != nil {
		return PublishedDocumentResult{}, err
	}
	if meta == nil || meta.QueryExcluded() || meta.StateETag == "" {
		return PublishedDocumentResult{NoContent: true}, nil
	}
	publishedVersion := meta.PublishedVersion
	if publishedVersion == 0 {
		publishedVersion = meta.Version
	}
	if publishedVersion == 0 {
		return PublishedDocumentResult{NoContent: true}, nil
	}
	if publishedVersion < meta.Version {
		return PublishedDocumentResult{}, Failure{
			Code:       "state_not_published",
			Detail:     "state update not published yet",
			HTTPStatus: http.StatusServiceUnavailable,
		}
	}
	stateCtx := ctx
	if len(meta.StateDescriptor) > 0 {
		stateCtx = storage.ContextWithStateDescriptor(stateCtx, meta.StateDescriptor)
	}
	if meta.StatePlaintextBytes > 0 {
		stateCtx = storage.ContextWithStatePlaintextSize(stateCtx, meta.StatePlaintextBytes)
	}
	reader, info, err := s.readStateWithWarmup(stateCtx, namespace, storageKey, true)
	if errors.Is(err, storage.ErrNotFound) {
		return PublishedDocumentResult{NoContent: true}, nil
	}
	if err != nil {
		return PublishedDocumentResult{}, err
	}
	size := meta.StatePlaintextBytes
	if size == 0 && info != nil {
		size = info.Size
	}
	if s.jsonMaxBytes > 0 && size > s.jsonMaxBytes {
		reader.Close()
		return PublishedDocumentResult{}, Failure{
			Code:       "document_too_large",
			Detail:     fmt.Sprintf("state exceeds %d bytes", s.jsonMaxBytes),
			HTTPStatus: http.StatusRequestEntityTooLarge,
		}
	}
	return PublishedDocumentResult{Reader: reader, Version: publishedVersion}, nil
}

// OpenPublishedDocument is exported for adapters to stream documents.
func (s *Service) OpenPublishedDocument(ctx context.Context, namespace, key string) (PublishedDocumentResult, error) {
	return s.openPublishedDocument(ctx, namespace, key)
}
