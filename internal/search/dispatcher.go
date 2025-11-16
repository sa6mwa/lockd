package search

import (
	"context"
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
