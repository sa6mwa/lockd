package inprocess

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
)

// Client provides the lockd client API backed by an in-process server instance.
type Client struct {
	inner     *lockdclient.Client
	stop      func(context.Context) error
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
	if cfg.MTLS {
		cfg.MTLS = false
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

func (c *Client) Acquire(ctx context.Context, req lockdclient.AcquireRequest, opts ...lockdclient.AcquireOption) (*lockdclient.AcquireResponse, error) {
	return c.inner.Acquire(ctx, req, opts...)
}

func (c *Client) KeepAlive(ctx context.Context, req lockdclient.KeepAliveRequest) (*lockdclient.KeepAliveResponse, error) {
	return c.inner.KeepAlive(ctx, req)
}

func (c *Client) Release(ctx context.Context, req lockdclient.ReleaseRequest) (*lockdclient.ReleaseResponse, error) {
	return c.inner.Release(ctx, req)
}

func (c *Client) Describe(ctx context.Context, key string) (*lockdclient.DescribeResponse, error) {
	return c.inner.Describe(ctx, key)
}

func (c *Client) GetState(ctx context.Context, key, leaseID string) (io.ReadCloser, string, string, error) {
	return c.inner.GetState(ctx, key, leaseID)
}

func (c *Client) GetStateBytes(ctx context.Context, key, leaseID string) ([]byte, string, string, error) {
	return c.inner.GetStateBytes(ctx, key, leaseID)
}

func (c *Client) UpdateState(ctx context.Context, key, leaseID string, body io.Reader, opts lockdclient.UpdateStateOptions) (*lockdclient.UpdateStateResult, error) {
	return c.inner.UpdateState(ctx, key, leaseID, body, opts)
}

func (c *Client) UpdateStateBytes(ctx context.Context, key, leaseID string, body []byte, opts lockdclient.UpdateStateOptions) (*lockdclient.UpdateStateResult, error) {
	return c.inner.UpdateStateBytes(ctx, key, leaseID, body, opts)
}
