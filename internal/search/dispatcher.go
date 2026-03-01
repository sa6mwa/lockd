package search

import (
	"context"
	"errors"
	"fmt"
)

// Dispatcher routes requests to the desired query engine.
type Dispatcher struct {
	index Adapter
	scan  Adapter
}

// DispatcherConfig configures a Dispatcher.
type DispatcherConfig struct {
	Index Adapter
	Scan  Adapter
}

// NewDispatcher builds a multi-engine adapter.
func NewDispatcher(cfg DispatcherConfig) *Dispatcher {
	return &Dispatcher{
		index: cfg.Index,
		scan:  cfg.Scan,
	}
}

// Capabilities merges engine support from all configured adapters. It degrades
// gracefully when one of the engines fails to report capabilities, returning
// the aggregate when at least one succeeds.
func (d *Dispatcher) Capabilities(ctx context.Context, namespace string) (Capabilities, error) {
	var combined Capabilities
	var lastErr error
	if d == nil {
		return combined, fmt.Errorf("search dispatcher unavailable")
	}
	if d.index != nil {
		if caps, err := d.index.Capabilities(ctx, namespace); err == nil {
			combined.Index = combined.Index || caps.Index
			combined.Scan = combined.Scan || caps.Scan
		} else {
			lastErr = err
		}
	}
	if d.scan != nil {
		if caps, err := d.scan.Capabilities(ctx, namespace); err == nil {
			combined.Index = combined.Index || caps.Index
			combined.Scan = combined.Scan || caps.Scan
		} else {
			lastErr = err
		}
	}
	if combined.Index || combined.Scan {
		return combined, nil
	}
	if lastErr != nil {
		return combined, lastErr
	}
	return combined, fmt.Errorf("no search engines available")
}

// Query executes the request using the selected engine (or auto-detects when
// the hint is missing).
func (d *Dispatcher) Query(ctx context.Context, req Request) (Result, error) {
	if d == nil {
		return Result{}, fmt.Errorf("search dispatcher unavailable")
	}
	switch req.Engine {
	case EngineIndex:
		if d.index == nil {
			return Result{}, fmt.Errorf("index engine unavailable")
		}
		return d.index.Query(ctx, req)
	case EngineScan:
		if d.scan == nil {
			return Result{}, fmt.Errorf("scan engine unavailable")
		}
		return d.scan.Query(ctx, req)
	default:
		if d.index != nil {
			if res, err := d.index.Query(ctx, req); err == nil {
				return res, nil
			}
		}
		if d.scan != nil {
			return d.scan.Query(ctx, req)
		}
		return Result{}, fmt.Errorf("no query engine available")
	}
}

// QueryDocuments streams matched documents in a single pass when supported by
// the selected engine, with fallback to scan streaming when index streaming is
// not available yet.
func (d *Dispatcher) QueryDocuments(ctx context.Context, req Request, sink DocumentSink) (Result, error) {
	if d == nil {
		return Result{}, fmt.Errorf("search dispatcher unavailable")
	}
	try := func(adapter Adapter, request Request) (Result, error) {
		if adapter == nil {
			return Result{}, ErrDocumentStreamingUnsupported
		}
		streamer, ok := adapter.(DocumentStreamer)
		if !ok {
			return Result{}, ErrDocumentStreamingUnsupported
		}
		return streamer.QueryDocuments(ctx, request, sink)
	}
	fallbackScan := func(request Request) (Result, error) {
		request.Engine = EngineScan
		res, err := try(d.scan, request)
		if err != nil {
			return Result{}, err
		}
		if res.Metadata == nil {
			res.Metadata = make(map[string]string)
		}
		res.Metadata["engine"] = string(EngineScan)
		return res, nil
	}

	switch req.Engine {
	case EngineIndex:
		res, err := try(d.index, req)
		if err == nil {
			return res, nil
		}
		if !errors.Is(err, ErrDocumentStreamingUnsupported) {
			return Result{}, err
		}
		return fallbackScan(req)
	case EngineScan:
		return try(d.scan, req)
	default:
		res, err := try(d.index, req)
		if err == nil {
			return res, nil
		}
		if !errors.Is(err, ErrDocumentStreamingUnsupported) {
			return Result{}, err
		}
		return fallbackScan(req)
	}
}
