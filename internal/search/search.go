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
	Query(ctx context.Context, req Request) (Result, error)
}
