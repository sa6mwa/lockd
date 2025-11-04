package lockd

import (
	"context"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/tlsutil"
)

func TestNewTestServerDefault(t *testing.T) {
	ts := StartTestServer(t)
	if ts.Client == nil {
		t.Fatal("expected auto client")
	}
	if ts.Config.MTLSEnabled() {
		if len(ts.Config.BundlePEM) == 0 {
			t.Fatalf("expected in-memory bundle for default test server")
		}
		bundle, err := tlsutil.LoadBundleFromBytes(ts.Config.BundlePEM)
		if err != nil {
			t.Fatalf("load bundle: %v", err)
		}
		foundIP := false
		for _, ip := range bundle.ServerCert.IPAddresses {
			if ip.String() == "127.0.0.1" {
				foundIP = true
				break
			}
		}
		if !foundIP {
			t.Fatalf("expected 127.0.0.1 SAN, got %v", bundle.ServerCert.IPAddresses)
		}
	} else if len(ts.Config.BundlePEM) != 0 {
		t.Fatalf("expected bundle omitted when MTLS disabled, got %d bytes", len(ts.Config.BundlePEM))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	lease, err := ts.Client.Acquire(ctx, api.AcquireRequest{
		Key:        "default-key",
		Owner:      "tester",
		TTLSeconds: 10,
		BlockSecs:  client.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestNewTestServerUnixSocket(t *testing.T) {
	socket := t.TempDir() + "/lockd.sock"
	ts := StartTestServer(t, WithTestUnixSocket(socket))
	if !strings.HasPrefix(ts.URL(), "unix://") {
		t.Fatalf("expected unix URL, got %s", ts.URL())
	}
	cli := ts.Client
	if cli == nil {
		var err error
		cli, err = ts.NewClient()
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        "unix-key",
		Owner:      "tester",
		TTLSeconds: 5,
		BlockSecs:  client.BlockNoWait,
	})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
}

func TestNewTestServerWithChaos(t *testing.T) {
	chaos := &ChaosConfig{
		Seed:            123,
		MinDelay:        time.Millisecond,
		MaxDelay:        2 * time.Millisecond,
		DropProbability: 0.0,
	}
	ts := StartTestServer(t, WithTestChaos(chaos))
	serverAddr := ts.Server.ListenerAddr().String()
	proxyAddr := ts.Addr().String()
	if serverAddr == proxyAddr {
		t.Fatalf("expected proxy address to differ (%s)", serverAddr)
	}
}

func TestNewTestServerWithoutClient(t *testing.T) {
	ts := StartTestServer(t, WithoutTestClient())
	if ts.Client != nil {
		t.Fatalf("expected client to be nil")
	}
	cli, err := ts.NewClient()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := cli.Acquire(ctx, api.AcquireRequest{
		Key:        "manual-client",
		Owner:      "tester",
		TTLSeconds: 5,
		BlockSecs:  client.BlockNoWait,
	}); err != nil {
		t.Fatalf("acquire: %v", err)
	}
}

func TestNewTestMTLSMaterialHosts(t *testing.T) {
	material, err := newTestMTLSMaterial([]string{"127.0.0.1", "localhost"})
	if err != nil {
		t.Fatalf("newTestMTLSMaterial: %v", err)
	}
	bundle, err := tlsutil.LoadBundleFromBytes(material.serverBundle)
	if err != nil {
		t.Fatalf("load bundle: %v", err)
	}
	foundIP := false
	for _, ip := range bundle.ServerCert.IPAddresses {
		if ip.String() == "127.0.0.1" {
			foundIP = true
			break
		}
	}
	if !foundIP {
		t.Fatalf("expected 127.0.0.1 in server certificate IP SANs, got %v", bundle.ServerCert.IPAddresses)
	}
}
