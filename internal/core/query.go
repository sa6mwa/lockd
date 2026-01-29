package core

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"pkt.systems/lockd/internal/search"
)

// Query executes a selector and returns keys/cursor. Document streaming is handled by callers using OpenPublishedDocument.
func (s *Service) Query(ctx context.Context, cmd QueryCommand) (*QueryResult, error) {
	if s.searchAdapter == nil {
		return nil, Failure{Code: "query_disabled", Detail: "query service not configured", HTTPStatus: http.StatusNotImplemented}
	}
	if err := s.maybeThrottleQuery(ctx); err != nil {
		return nil, err
	}
	if err := s.applyShutdownGuard("query"); err != nil {
		return nil, err
	}
	finish := s.beginQueryOp()
	defer finish()
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
		_ = s.indexManager.WarmNamespace(ctx, namespace)
	}

	req := search.Request{
		Namespace: namespace,
		Selector:  cmd.Selector,
		Limit:     limit,
		Cursor:    cmd.Cursor,
		Fields:    cmd.Fields,
		Engine:    engine,
	}
	if cmd.Return == apiQueryReturnDocuments {
		req.IncludeDocMeta = true
	}
	result, err := s.searchAdapter.Query(ctx, req)
	if err != nil {
		if errors.Is(err, search.ErrInvalidCursor) {
			return nil, Failure{Code: "invalid_cursor", Detail: err.Error(), HTTPStatus: http.StatusBadRequest}
		}
		return nil, fmt.Errorf("query: %w", err)
	}
	if engine == search.EngineIndex && s.indexManager != nil {
		if rebuilt, err := s.maybeScheduleIndexRebuild(ctx, namespace, result.Format); err != nil {
			if s.logger != nil {
				s.logger.Warn("index.rebuild.on_read.failed", "namespace", namespace, "error", err)
			}
		} else if rebuilt {
			rebuiltResult, rebuildErr := s.searchAdapter.Query(ctx, req)
			if rebuildErr != nil {
				if s.logger != nil {
					s.logger.Warn("index.rebuild.on_read.query_failed", "namespace", namespace, "error", rebuildErr)
				}
			} else {
				result = rebuiltResult
			}
		}
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
		DocMeta:   result.DocMeta,
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
	if err := s.StreamPublishedDocuments(ctx, result.Namespace, keys, result.DocMeta, sink); err != nil {
		return nil, err
	}
	return result, nil
}

// StreamPublishedDocuments streams already-selected keys to the sink.
func (s *Service) StreamPublishedDocuments(ctx context.Context, namespace string, keys []string, docMeta map[string]search.DocMetadata, sink DocumentSink) error {
	if len(keys) == 0 {
		return nil
	}
	prefetch := s.queryDocPrefetch
	if prefetch <= 1 || len(keys) == 1 {
		return s.streamPublishedDocumentsSequential(ctx, namespace, keys, docMeta, sink)
	}
	return s.streamPublishedDocumentsParallel(ctx, namespace, keys, docMeta, sink, prefetch)
}

func (s *Service) streamPublishedDocument(ctx context.Context, namespace, key string, meta *search.DocMetadata, sink DocumentSink) error {
	if sink == nil {
		return Failure{Code: "invalid_sink", Detail: "document sink required", HTTPStatus: http.StatusBadRequest}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	doc, err := s.openPublishedDocumentWithMeta(ctx, namespace, key, meta)
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

type docFetch struct {
	idx int
	key string
	doc PublishedDocumentResult
	err error
}

func (s *Service) streamPublishedDocumentsSequential(ctx context.Context, namespace string, keys []string, docMeta map[string]search.DocMetadata, sink DocumentSink) error {
	for _, key := range keys {
		if err := s.streamPublishedDocument(ctx, namespace, key, docMetaLookup(docMeta, key), sink); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) streamPublishedDocumentsParallel(ctx context.Context, namespace string, keys []string, docMeta map[string]search.DocMetadata, sink DocumentSink, prefetch int) error {
	if sink == nil {
		return Failure{Code: "invalid_sink", Detail: "document sink required", HTTPStatus: http.StatusBadRequest}
	}
	if len(keys) == 0 {
		return nil
	}
	if prefetch > len(keys) {
		prefetch = len(keys)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int)
	results := make(chan docFetch, prefetch)

	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for idx := range jobs {
			if ctx.Err() != nil {
				return
			}
			key := keys[idx]
			doc, err := s.openPublishedDocumentWithMeta(ctx, namespace, key, docMetaLookup(docMeta, key))
			results <- docFetch{idx: idx, key: key, doc: doc, err: err}
		}
	}
	for i := 0; i < prefetch; i++ {
		wg.Add(1)
		go worker()
	}
	go func() {
		for i := range keys {
			select {
			case <-ctx.Done():
				close(jobs)
				wg.Wait()
				close(results)
				return
			case jobs <- i:
			}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	pending := make([]docFetch, len(keys))
	ready := make([]bool, len(keys))
	next := 0
	var outErr error
	closePending := func() {
		for i, ok := range ready {
			if !ok {
				continue
			}
			if r := pending[i]; r.doc.Reader != nil {
				r.doc.Reader.Close()
			}
			ready[i] = false
		}
	}
	for res := range results {
		if outErr != nil {
			if res.doc.Reader != nil {
				res.doc.Reader.Close()
			}
			continue
		}
		if res.err != nil {
			outErr = res.err
			cancel()
			closePending()
			if res.doc.Reader != nil {
				res.doc.Reader.Close()
			}
			continue
		}
		pending[res.idx] = res
		ready[res.idx] = true
		for next < len(keys) && ready[next] && outErr == nil {
			curr := pending[next]
			if curr.doc.NoContent {
				if curr.doc.Reader != nil {
					_ = curr.doc.Reader.Close()
				}
			} else if curr.doc.Reader != nil {
				if err := sink.OnDocument(ctx, namespace, curr.key, curr.doc.Version, curr.doc.Reader); err != nil {
					outErr = err
					cancel()
					_ = curr.doc.Reader.Close()
					closePending()
					break
				}
				_ = curr.doc.Reader.Close()
			}
			ready[next] = false
			next++
		}
	}
	if outErr != nil {
		return outErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func docMetaLookup(meta map[string]search.DocMetadata, key string) *search.DocMetadata {
	if meta == nil || key == "" {
		return nil
	}
	if entry, ok := meta[key]; ok {
		copyEntry := entry
		return &copyEntry
	}
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
