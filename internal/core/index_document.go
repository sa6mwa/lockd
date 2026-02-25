package core

import (
	"encoding/json"
	"io"
	"sort"
	"strconv"

	"pkt.systems/lockd/internal/jsonpointer"
	indexer "pkt.systems/lockd/internal/search/index"
	"pkt.systems/lockd/internal/storage"
)

func buildDocumentFromJSON(key string, reader io.Reader) (indexer.Document, error) {
	doc := indexer.Document{Key: key}
	dec := json.NewDecoder(reader)
	dec.UseNumber()
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return doc, err
	}
	recursiveIndex("", raw, &doc)
	return doc, nil
}

func buildIndexDocumentMeta(meta *storage.Meta) *indexer.DocumentMetadata {
	if meta == nil {
		return nil
	}
	publishedVersion := meta.PublishedVersion
	if publishedVersion == 0 {
		publishedVersion = meta.Version
	}
	out := &indexer.DocumentMetadata{
		StateETag:           meta.StateETag,
		StatePlaintextBytes: meta.StatePlaintextBytes,
		PublishedVersion:    publishedVersion,
		QueryExcluded:       meta.QueryExcluded(),
	}
	if len(meta.StateDescriptor) > 0 {
		out.StateDescriptor = append([]byte(nil), meta.StateDescriptor...)
	}
	return out
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
		doc.AddString(path, v)
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
