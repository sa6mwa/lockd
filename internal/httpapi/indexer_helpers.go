package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"pkt.systems/lockd/internal/jsonpointer"
	indexer "pkt.systems/lockd/internal/search/index"
)

func (h *Handler) indexStatePayload(ctx context.Context, namespace, key string, spool *payloadSpool) {
	if h.indexManager == nil || spool == nil {
		return
	}
	reader, err := spool.Reader()
	if err != nil {
		if h.logger != nil {
			h.logger.Warn("index.payload.reader_error", "namespace", namespace, "key", key, "error", err)
		}
		return
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		if h.logger != nil {
			h.logger.Warn("index.payload.read_error", "namespace", namespace, "key", key, "error", err)
		}
		return
	}
	doc, err := buildDocumentFromJSON(key, data)
	if err != nil {
		if h.logger != nil {
			h.logger.Debug("index.document.build_error", "namespace", namespace, "key", key, "error", err)
		}
		return
	}
	if err := h.indexManager.Insert(namespace, doc); err != nil && h.logger != nil {
		h.logger.Warn("index.document.insert_error", "namespace", namespace, "key", key, "error", err)
	}
}

func buildDocumentFromJSON(key string, payload []byte) (indexer.Document, error) {
	doc := indexer.Document{Key: key}
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return doc, fmt.Errorf("decode json: %w", err)
	}
	recursiveIndex("", raw, &doc)
	return doc, nil
}

func recursiveIndex(path string, value any, doc *indexer.Document) {
	switch v := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			child := v[k]
			childPath := joinPath(path, k)
			recursiveIndex(childPath, child, doc)
		}
	case []any:
		for _, elem := range v {
			recursiveIndex(path, elem, doc)
		}
	case string:
		doc.AddTerm(path, strings.ToLower(v))
	case json.Number:
		doc.AddTerm(path, v.String())
	case bool:
		doc.AddTerm(path, strconv.FormatBool(v))
	}
}

func joinPath(parent, child string) string {
	if parent == "" {
		return jsonpointer.Join("", child)
	}
	return jsonpointer.Join(parent, child)
}
