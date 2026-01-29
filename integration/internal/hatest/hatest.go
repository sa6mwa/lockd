package hatest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/uuidv7"
)

const (
	probeOwner   = "ha-probe"
	probeTTL     = int64(5)
	probeTimeout = 2 * time.Second
	probeDelay   = 50 * time.Millisecond
)

// IsNodePassive reports whether err indicates a passive node.
func IsNodePassive(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *lockdclient.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.Response.ErrorCode == "node_passive"
}

// IsTransportError reports whether err is a non-API transport failure.
func IsTransportError(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *lockdclient.APIError
	if errors.As(err, &apiErr) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	return true
}

// FindActiveServer probes each server until it finds the active HA endpoint.
// It returns the active server and a client connected to it.
func FindActiveServer(ctx context.Context, servers ...*lockd.TestServer) (*lockd.TestServer, *lockdclient.Client, error) {
	if len(servers) == 0 {
		return nil, nil, fmt.Errorf("no servers provided")
	}
	key := "ha-probe-" + uuidv7.NewString()
	var lastErr error
	for {
		if ctx.Err() != nil {
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			return nil, nil, lastErr
		}
		for _, ts := range servers {
			if ts == nil {
				continue
			}
			cli, err := ts.NewClient(lockdclient.WithEndpointShuffle(false))
			if err != nil {
				return nil, nil, err
			}
			active, err := probeActive(ctx, cli, key)
			if err == nil && active {
				return ts, cli, nil
			}
			_ = cli.Close()
			if err != nil && !IsNodePassive(err) && !isRetryableProbeError(err) && !IsTransportError(err) {
				return nil, nil, err
			}
			if err != nil {
				lastErr = err
			}
		}
		time.Sleep(probeDelay)
	}
}

func isRetryableProbeError(err error) bool {
	var apiErr *lockdclient.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.Response.ErrorCode {
	case "waiting":
		return true
	default:
		return false
	}
}

func probeActive(ctx context.Context, cli *lockdclient.Client, key string) (bool, error) {
	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	lease, err := cli.Acquire(probeCtx, api.AcquireRequest{
		Key:        key,
		Owner:      probeOwner,
		TTLSeconds: probeTTL,
		BlockSecs:  lockdclient.BlockNoWait,
	})
	if err != nil {
		return false, err
	}
	releaseCtx, releaseCancel := context.WithTimeout(ctx, probeTimeout)
	_ = lease.Release(releaseCtx)
	releaseCancel()
	return true, nil
}

// WaitForActive blocks until the provided server is active or ctx expires.
func WaitForActive(ctx context.Context, server *lockd.TestServer) error {
	if server == nil {
		return fmt.Errorf("nil server")
	}
	active, cli, err := FindActiveServer(ctx, server)
	if cli != nil {
		_ = cli.Close()
	}
	if err != nil {
		return err
	}
	if active != server {
		return fmt.Errorf("unexpected active server")
	}
	return nil
}
