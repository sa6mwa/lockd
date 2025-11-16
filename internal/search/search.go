package search

import (
	"context"
	"errors"

	"pkt.systems/lockd/api"
)

// ErrInvalidCursor indicates that the supplied cursor could not be decoded.
var ErrInvalidCursor = errors.New("search: invalid cursor")

// Request captures a namespace-scoped selector query.
type Request struct {
	Namespace string
	Selector  api.Selector
	Limit     int
	Cursor    string
	Fields    map[string]any
	Engine    EngineHint
}

// Result surfaces the adapter response.
type Result struct {
	Keys     []string
	Cursor   string
	IndexSeq uint64
	Metadata map[string]string
}

// Adapter executes namespace-scoped queries.
type Adapter interface {
	Capabilities(ctx context.Context, namespace string) (Capabilities, error)
	Query(ctx context.Context, req Request) (Result, error)
}

// EngineHint guides the dispatcher when multiple engines (index vs scan) are available.
type EngineHint string

const (
	EngineAuto  EngineHint = "auto"
	EngineIndex EngineHint = "index"
	EngineScan  EngineHint = "scan"
)

// Capability flags describing which engines a namespace/adapter supports.
type Capabilities struct {
	Index bool
	Scan  bool
}
