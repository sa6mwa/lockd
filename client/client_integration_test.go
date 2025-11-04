//go:build integration

package client_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	"pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/storage/memory"
	"pkt.systems/pslog"
)

func TestAcquireForUpdateCallbackSingleServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	watchdog := time.AfterFunc(10*time.Second, func() {
		buf := make([]byte, 1<<18)
		n := runtime.Stack(buf, true)
		panic("TestAcquireForUpdateCallbackSingleServer timeout after 10s:\n" + string(buf[:n]))
	})
	defer watchdog.Stop()

	chaos := &lockd.ChaosConfig{
		Seed:            123,
		DisconnectAfter: 150 * time.Millisecond,
		MinDelay:        5 * time.Millisecond,
		MaxDelay:        10 * time.Millisecond,
		MaxDisconnects:  1,
	}

	ts := lockd.StartTestServer(t,
		lockd.WithTestChaos(chaos),
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			client.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
			client.WithHTTPTimeout(300*time.Millisecond),
		),
	)

	proxiedClient := ts.Client
	if proxiedClient == nil {
		t.Fatalf("test server did not provide client")
	}
	t.Cleanup(func() { _ = proxiedClient.Close() })

	// Seed state directly against the server (bypass chaos) so the callback has something to read.
	directAddr := ts.Server.ListenerAddr()
	if directAddr == nil {
		t.Fatalf("server missing listener address")
	}
	scheme := "http"
	seedOptions := []client.Option{
		client.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
	}
	if ts.Config.MTLSEnabled() {
		scheme = "https"
		creds := ts.TestMTLSCredentials()
		if !creds.Valid() {
			t.Fatalf("test server missing MTLS credentials")
		}
		httpClient, err := creds.NewHTTPClient()
		if err != nil {
			t.Fatalf("seed http client: %v", err)
		}
		seedOptions = append(seedOptions, client.WithHTTPClient(httpClient))
	} else {
		seedOptions = append(seedOptions, client.WithDisableMTLS(true))
	}
	directURL := scheme + "://" + directAddr.String()
	seedClient, err := client.New(directURL, seedOptions...)
	if err != nil {
		t.Fatalf("seed client: %v", err)
	}
	t.Cleanup(func() { _ = seedClient.Close() })

	seedPayload := map[string]any{"value": 42}
	seedCtx, seedCancel := context.WithTimeout(ctx, time.Second)
	seedSession, err := seedClient.Acquire(seedCtx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "seed",
		TTLSeconds: 20,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		seedCancel()
		t.Fatalf("seed acquire: %v", err)
	}
	if err := seedSession.Save(seedCtx, seedPayload); err != nil {
		seedSession.Release(seedCtx)
		seedCancel()
		t.Fatalf("seed save: %v", err)
	}
	if err := seedSession.Release(seedCtx); err != nil {
		seedCancel()
		t.Fatalf("seed release: %v", err)
	}
	seedCancel()

	handlerCalled := false
	err = proxiedClient.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "reader",
		TTLSeconds: 20,
		BlockSecs:  client.BlockWaitForever,
	}, func(handlerCtx context.Context, af *client.AcquireForUpdateContext) error {
		handlerCalled = true
		var snapshot map[string]int
		if err := af.State.Decode(&snapshot); err != nil {
			return err
		}
		if snapshot["value"] != 42 {
			return fmt.Errorf("unexpected snapshot value %d", snapshot["value"])
		}
		return af.Save(handlerCtx, map[string]int{"value": 99})
	}, client.WithAcquireFailureRetries(5))
	watchdog.Stop()
	if err != nil {
		t.Fatalf("acquire-for-update callback: %v", err)
	}
	if !handlerCalled {
		t.Fatalf("callback was not invoked")
	}

	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), time.Second)
	defer verifyCancel()
	verifySession, err := seedClient.Acquire(verifyCtx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "verifier",
		TTLSeconds: 5,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("verify acquire: %v", err)
	}
	var out map[string]int
	if err := verifySession.Load(verifyCtx, &out); err != nil {
		t.Fatalf("verify load: %v", err)
	}
	if out["value"] != 99 {
		t.Fatalf("expected updated value 99, got %+v", out)
	}
	if err := verifySession.Release(verifyCtx); err != nil {
		t.Fatalf("verify release: %v", err)
	}
}

func TestAcquireForUpdateCallbackFailoverMultiServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	watchdog := time.AfterFunc(10*time.Second, func() {
		buf := make([]byte, 1<<18)
		n := runtime.Stack(buf, true)
		panic("TestAcquireForUpdateCallbackFailoverMultiServer timeout after 10s:\n" + string(buf[:n]))
	})
	t.Cleanup(func() { watchdog.Stop() })

	store := memory.New()

	primaryChaos := &lockd.ChaosConfig{
		Seed:            99,
		DisconnectAfter: 120 * time.Millisecond,
		MinDelay:        5 * time.Millisecond,
		MaxDelay:        15 * time.Millisecond,
		MaxDisconnects:  1,
	}

	primary := lockd.StartTestServer(t,
		lockd.WithTestBackend(store),
		lockd.WithTestChaos(primaryChaos),
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			client.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
			client.WithHTTPTimeout(300*time.Millisecond),
		),
	)

	primaryCreds := primary.TestMTLSCredentials()
	backupOpts := []lockd.TestServerOption{
		lockd.WithTestBackend(store),
		lockd.WithTestLoggerFromTB(t, pslog.TraceLevel),
		lockd.WithTestClientOptions(
			client.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
			client.WithHTTPTimeout(300*time.Millisecond),
		),
	}
	if primaryCreds.Valid() {
		backupOpts = append(backupOpts, lockd.WithTestMTLSCredentials(primaryCreds))
	}
	backup := lockd.StartTestServer(t, backupOpts...)

	seedCli := backup.Client
	if seedCli == nil {
		var err error
		seedCli, err = backup.NewClient()
		if err != nil {
			t.Fatalf("seed client: %v", err)
		}
	}
	t.Cleanup(func() { _ = seedCli.Close() })

	seedCtx, seedCancel := context.WithTimeout(ctx, time.Second)
	seedSession, err := seedCli.Acquire(seedCtx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "seed",
		TTLSeconds: 20,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		seedCancel()
		t.Fatalf("seed acquire: %v", err)
	}
	if err := seedSession.Save(seedCtx, map[string]int{"value": 7}); err != nil {
		seedSession.Release(seedCtx)
		seedCancel()
		t.Fatalf("seed save: %v", err)
	}
	if err := seedSession.Release(seedCtx); err != nil {
		seedCancel()
		t.Fatalf("seed release: %v", err)
	}
	seedCancel()

	endpoints := []string{primary.URL(), backup.URL()}
	failoverOpts := []client.Option{
		client.WithLogger(lockd.NewTestingLogger(t, pslog.TraceLevel)),
		client.WithHTTPTimeout(300 * time.Millisecond),
	}
	if primary.Config.MTLSEnabled() {
		if !primaryCreds.Valid() {
			t.Fatalf("primary missing MTLS credentials")
		}
		httpClient, err := primaryCreds.NewHTTPClient()
		if err != nil {
			t.Fatalf("failover http client: %v", err)
		}
		failoverOpts = append(failoverOpts, client.WithHTTPClient(httpClient))
	} else {
		failoverOpts = append(failoverOpts, client.WithDisableMTLS(true))
	}
	failoverClient, err := client.NewWithEndpoints(endpoints, failoverOpts...)
	if err != nil {
		t.Fatalf("failover client: %v", err)
	}
	t.Cleanup(func() { _ = failoverClient.Close() })

	handlerCalled := false
	err = failoverClient.AcquireForUpdate(ctx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "reader",
		TTLSeconds: 20,
		BlockSecs:  client.BlockWaitForever,
	}, func(handlerCtx context.Context, af *client.AcquireForUpdateContext) error {
		handlerCalled = true
		var snapshot map[string]int
		if err := af.State.Decode(&snapshot); err != nil {
			return err
		}
		if snapshot["value"] != 7 {
			return fmt.Errorf("unexpected snapshot: %+v", snapshot)
		}
		snapshot["value"] = 8
		return af.Save(handlerCtx, snapshot)
	}, client.WithAcquireFailureRetries(5))
	watchdog.Stop()
	if err != nil {
		t.Fatalf("acquire-for-update callback: %v", err)
	}
	if !handlerCalled {
		t.Fatalf("callback was not invoked")
	}

	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), time.Second)
	defer verifyCancel()
	verifySession, err := seedCli.Acquire(verifyCtx, api.AcquireRequest{
		Key:        "orders",
		Owner:      "verifier",
		TTLSeconds: 5,
		BlockSecs:  client.BlockWaitForever,
	})
	if err != nil {
		t.Fatalf("verify acquire: %v", err)
	}
	var snapshot map[string]int
	if err := verifySession.Load(verifyCtx, &snapshot); err != nil {
		t.Fatalf("verify load: %v", err)
	}
	if snapshot["value"] != 8 {
		t.Fatalf("expected updated value 8, got %+v", snapshot)
	}
	if err := verifySession.Release(verifyCtx); err != nil {
		t.Fatalf("verify release: %v", err)
	}
}
