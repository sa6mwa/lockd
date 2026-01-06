package core

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"pkt.systems/lockd/internal/search"
)

// Query executes a selector and returns keys/cursor. Document streaming is handled by callers using OpenPublishedDocument.
func (s *Service) Query(ctx context.Context, cmd QueryCommand) (*QueryResult, error) {
	if s.searchAdapter == nil {
		return nil, Failure{Code: "query_disabled", Detail: "query service not configured", HTTPStatus: http.StatusNotImplemented}
	}
	if err := s.applyShutdownGuard("query"); err != nil {
		return nil, err
	}
	namespace, err := s.resolveNamespace(cmd.Namespace)
	if err != nil {
		return nil, Failure{Code: "invalid_namespace", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	s.observeNamespace(namespace)

	limit := cmd.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	engine := cmd.Engine
	if engine == "" || engine == search.EngineAuto {
		engine, err = s.selectQueryEngine(ctx, namespace, cmd.Engine)
		if err != nil {
			return nil, err
		}
	}
	if engine == search.EngineIndex && cmd.Refresh == RefreshWaitFor && s.indexManager != nil {
		if err := s.indexManager.WaitForReadable(ctx, namespace); err != nil {
			return nil, err
		}
	}

	req := search.Request{
		Namespace: namespace,
		Selector:  cmd.Selector,
		Limit:     limit,
		Cursor:    cmd.Cursor,
		Fields:    cmd.Fields,
		Engine:    engine,
	}
	result, err := s.searchAdapter.Query(ctx, req)
	if err != nil {
		if errors.Is(err, search.ErrInvalidCursor) {
			return nil, Failure{Code: "invalid_cursor", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
		}
		return nil, fmt.Errorf("query: %w", err)
	}
	if engine == search.EngineIndex && s.indexManager != nil {
		if result.Metadata == nil {
			result.Metadata = make(map[string]string)
		}
		result.Metadata["index_pending"] = fmt.Sprintf("%v", s.indexManager.Pending(namespace))
	}

	return &QueryResult{
		Namespace: namespace,
		Keys:      result.Keys,
		Cursor:    result.Cursor,
		IndexSeq:  result.IndexSeq,
		Metadata:  result.Metadata,
	}, nil
}

// QueryDocuments streams published documents for the selector to the provided sink.
func (s *Service) QueryDocuments(ctx context.Context, cmd QueryCommand, sink DocumentSink) (*QueryResult, error) {
	cmd.Return = apiQueryReturnDocuments
	result, err := s.Query(ctx, cmd)
	if err != nil {
		return nil, err
	}
	if sink == nil {
		return result, nil
	}
	keys := result.Keys
	if len(cmd.Keys) > 0 {
		keys = cmd.Keys
	}
	for _, key := range keys {
		if err := s.streamPublishedDocument(ctx, result.Namespace, key, sink); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// StreamPublishedDocuments streams already-selected keys to the sink.
func (s *Service) StreamPublishedDocuments(ctx context.Context, namespace string, keys []string, sink DocumentSink) error {
	for _, key := range keys {
		if err := s.streamPublishedDocument(ctx, namespace, key, sink); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) streamPublishedDocument(ctx context.Context, namespace, key string, sink DocumentSink) error {
	if sink == nil {
		return Failure{Code: "invalid_sink", Detail: "document sink required", HTTPStatus: http.StatusBadRequest}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	doc, err := s.OpenPublishedDocument(ctx, namespace, key)
	if err != nil {
		return err
	}
	if doc.NoContent {
		if doc.Reader != nil {
			doc.Reader.Close()
		}
		return nil
	}
	if doc.Reader == nil {
		return nil
	}
	if err := sink.OnDocument(ctx, namespace, key, doc.Version, doc.Reader); err != nil {
		doc.Reader.Close()
		return err
	}
	doc.Reader.Close()
	return nil
}

func (s *Service) selectQueryEngine(ctx context.Context, namespace string, hint search.EngineHint) (search.EngineHint, error) {
	caps, err := s.searchAdapter.Capabilities(ctx, namespace)
	if err != nil {
		return "", err
	}
	cfg := s.defaultNamespaceConfig
	if s.namespaceConfigs != nil {
		if loaded, loadErr := s.namespaceConfigs.Load(ctx, namespace); loadErr == nil {
			cfg = loaded.Config
			cfg.Normalize()
		}
	}
	engine, err := cfg.SelectEngine(hint, caps)
	if err != nil {
		return "", Failure{
			Code:       "query_engine_unavailable",
			Detail:     err.Error(),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return engine, nil
}
