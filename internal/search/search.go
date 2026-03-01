package search

import (
	"context"
	"errors"
	"io"

	"pkt.systems/lockd/api"
)

// ErrInvalidCursor indicates that the supplied cursor could not be decoded.
var ErrInvalidCursor = errors.New("search: invalid cursor")

// ErrDocumentStreamingUnsupported indicates the selected adapter cannot stream
// query documents inline.
var ErrDocumentStreamingUnsupported = errors.New("search: document streaming unsupported")

// Request captures a namespace-scoped selector query.
type Request struct {
	Namespace string
	Selector  api.Selector
	Limit     int
	Cursor    string
	Fields    map[string]any
	Engine    EngineHint
	// DocPrefetch caps parallel document reads for return=documents flows.
	DocPrefetch int
	// IncludeDocMeta asks adapters to return document metadata for matched keys.
	IncludeDocMeta bool
}

// Result surfaces the adapter response.
type Result struct {
	Keys     []string
	Cursor   string
	IndexSeq uint64
	Metadata map[string]string
	DocMeta  map[string]DocMetadata
	Format   uint32
}

// DocMetadata carries index-provided document metadata for streaming reads.
type DocMetadata struct {
	StateETag           string
	StatePlaintextBytes int64
	StateDescriptor     []byte
	PublishedVersion    int64
}

// Adapter executes namespace-scoped queries.
type Adapter interface {
	Capabilities(ctx context.Context, namespace string) (Capabilities, error)
	Query(ctx context.Context, req Request) (Result, error)
}

// DocumentSink receives matched documents from a streaming query.
type DocumentSink interface {
	OnDocument(ctx context.Context, namespace, key string, version int64, reader io.Reader) error
}

// DocumentStreamer executes query return=documents as an inline stream.
type DocumentStreamer interface {
	QueryDocuments(ctx context.Context, req Request, sink DocumentSink) (Result, error)
}

// EngineHint guides the dispatcher when multiple engines (index vs scan) are available.
type EngineHint string

const (
	// EngineAuto lets the dispatcher pick between index and scan engines.
	EngineAuto EngineHint = "auto"
	// EngineIndex forces indexed query execution.
	EngineIndex EngineHint = "index"
	// EngineScan forces a storage scan for selectors.
	EngineScan EngineHint = "scan"
)

// Capabilities describes which engines a namespace/adapter supports.
type Capabilities struct {
	Index bool
	Scan  bool
}
