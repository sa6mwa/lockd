package inprocess

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
)

// Client provides the lockd client API backed by an in-process server instance.
type Client struct {
	inner     *lockdclient.Client
	stop      func(context.Context, ...lockd.CloseOption) error
	cleanup   func()
	closeOnce sync.Once
	closeErr  error
}

// New starts an in-process lockd server (MTLS is disabled automatically) and
// returns a client connected to it. The returned client should be closed when
// no longer needed to release resources.
// Example:
//
//	ctx := context.Background()
//	cfg := lockd.Config{Store: "mem://"}
//	inproc, err := inprocess.New(ctx, cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer inproc.Close(ctx)
func New(ctx context.Context, cfg lockd.Config, opts ...lockd.Option) (*Client, error) {
	if cfg.ListenProto == "" {
		cfg.ListenProto = "unix"
	}
	if cfg.ListenProto != "unix" {
		return nil, fmt.Errorf("inprocess: only unix sockets are supported; set ListenProto to 'unix'")
	}
	if !cfg.DisableMTLS {
		cfg.DisableMTLS = true
	}
	if ctx == nil {
		ctx = context.Background()
	}

	socketDir, err := os.MkdirTemp("", "lockd-inproc-")
	if err != nil {
		return nil, err
	}
	cleanup := func() { _ = os.RemoveAll(socketDir) }

	if cfg.Listen == "" {
		cfg.Listen = filepath.Join(socketDir, "lockd.sock")
	}

	_, stop, err := lockd.StartServer(ctx, cfg, opts...)
	if err != nil {
		cleanup()
		return nil, err
	}

	cli, err := lockdclient.New("unix://" + cfg.Listen)
	if err != nil {
		_ = stop(context.Background())
		cleanup()
		return nil, err
	}

	c := &Client{
		inner:   cli,
		stop:    stop,
		cleanup: cleanup,
	}
	return c, nil
}

// Close shuts down the embedded server and releases resources.
func (c *Client) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		if c.stop != nil {
			if err := c.stop(ctx); err != nil {
				c.closeErr = err
			}
		}
		if c.cleanup != nil {
			c.cleanup()
		}
	})
	return c.closeErr
}

// Acquire proxies to the embedded client Acquire call.
func (c *Client) Acquire(ctx context.Context, req api.AcquireRequest, opts ...lockdclient.AcquireOption) (*lockdclient.LeaseSession, error) {
	return c.inner.Acquire(ctx, req, opts...)
}

// KeepAlive forwards keepalive requests to the embedded client.
func (c *Client) KeepAlive(ctx context.Context, req api.KeepAliveRequest) (*api.KeepAliveResponse, error) {
	return c.inner.KeepAlive(ctx, req)
}

// Release sends the release request through the embedded client.
func (c *Client) Release(ctx context.Context, req api.ReleaseRequest) (*api.ReleaseResponse, error) {
	return c.inner.Release(ctx, req)
}

// Describe returns metadata for key via the embedded client.
func (c *Client) Describe(ctx context.Context, key string) (*api.DescribeResponse, error) {
	return c.inner.Describe(ctx, key)
}

// Get streams the current JSON state for key.
func (c *Client) Get(ctx context.Context, key string, optFns ...lockdclient.GetOption) (*lockdclient.GetResponse, error) {
	return c.inner.Get(ctx, key, optFns...)
}

// Update uploads a new JSON state body for key.
func (c *Client) Update(ctx context.Context, key, leaseID string, body io.Reader, opts lockdclient.UpdateOptions) (*lockdclient.UpdateResult, error) {
	return c.inner.Update(ctx, key, leaseID, body, opts)
}

// UpdateBytes uploads a new JSON state body from an in-memory buffer.
func (c *Client) UpdateBytes(ctx context.Context, key, leaseID string, body []byte, opts lockdclient.UpdateOptions) (*lockdclient.UpdateResult, error) {
	return c.inner.UpdateBytes(ctx, key, leaseID, body, opts)
}
