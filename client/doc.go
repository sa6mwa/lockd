// Package client provides the Go SDK for talking to a lockd cluster over HTTP.
// It mirrors CLI behaviour while exposing a typed API that is easy to embed in
// workers, controllers, and administrative tools.
//
// Copyright (C) 2025 Michel Blomgren <https://pkt.systems>
//
// # Quick start
//
// Construct a client with either client.New (single endpoint) or
// client.NewWithEndpoints (ordered failover endpoints). Supported endpoint
// schemes are https:// for mTLS, http:// for plaintext trusted networks, and
// unix:///path/to/socket for Unix-domain sockets.
//
//	ctx := context.Background()
//	cli, err := client.New("https://lockd.example.com")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer cli.Close()
//
//	req := api.AcquireRequest{
//	    Namespace:  "workflows",
//	    Key:        "orders",
//	    Owner:      "worker-1",
//	    TTLSeconds: 30,
//	    BlockSecs:  client.BlockWaitForever,
//	}
//	lease, err := cli.Acquire(ctx, req)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer lease.Close()
//
//	var checkpoint map[string]any
//	if err := lease.Load(ctx, &checkpoint); err != nil && !errors.Is(err, client.ErrStateNotFound) {
//	    log.Fatal(err)
//	}
//	checkpoint["progress"] = "running"
//	if err := lease.Save(ctx, checkpoint); err != nil {
//	    log.Fatal(err)
//	}
//
// Acquire mints a transaction id and fencing token. Lease-bound mutations such
// as Update, Remove, UpdateMetadata, Release, and attachment changes are issued
// with the correct transaction and fencing headers automatically when using a
// LeaseSession.
//
// When most operations use one namespace, use client.WithDefaultNamespace so
// per-call Namespace values can be omitted intentionally. Namespaces that start
// with a dot are reserved for lockd internals and rejected for user workloads.
//
// # Acquire for update
//
// AcquireForUpdate wraps the common acquire, load, mutate, save, release flow.
// It keeps the lease alive while the callback runs and always attempts release.
//
//	err := cli.AcquireForUpdate(ctx, api.AcquireRequest{
//	    Namespace:  "workflows",
//	    Key:        "orders",
//	    Owner:      "worker-1",
//	    TTLSeconds: 45,
//	}, func(ctx context.Context, af *client.AcquireForUpdateContext) error {
//	    var state map[string]any
//	    if err := af.Load(ctx, &state); err != nil && !errors.Is(err, client.ErrStateNotFound) {
//	        return err
//	    }
//	    state["progress"] = "done"
//	    return af.Save(ctx, state)
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # Attachments
//
// Leases can stage binary attachments alongside JSON state. Staged writes and
// staged deletes are committed on Release and discarded on Rollback.
//
//	file, _ := os.Open("invoice.pdf")
//	defer file.Close()
//	if _, err := lease.Attach(ctx, client.AttachRequest{
//	    Name:        "invoice.pdf",
//	    ContentType: "application/pdf",
//	    Body:        file,
//	}); err != nil {
//	    log.Fatal(err)
//	}
//	if err := lease.Release(ctx); err != nil {
//	    log.Fatal(err)
//	}
//
// # Perimeter defence interoperability
//
// Servers can throttle requests with HTTP 429 while the perimeter defence is
// active. Responses can include Retry-After and X-Lockd-QRF-State headers.
// The SDK consumes those hints in retry loops and surfaces parsed values on
// APIError for custom handling.
//
// # Queue API
//
// Queue helpers implement at-least-once delivery. Enqueue accepts any io.Reader
// and streams payload bytes directly to lockd.
//
//	reader := strings.NewReader("{\"op\":\"ship\"}")
//	qres, err := cli.Enqueue(ctx, "orders", reader, client.EnqueueOptions{
//	    ContentType: "application/json",
//	    Delay:       2 * time.Second,
//	    Visibility:  30 * time.Second,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	log.Printf("queued message %s", qres.MessageID)
//
// Dequeue returns QueueMessage. DequeueWithState returns QueueMessage plus an
// attached QueueStateHandle in the message. QueueMessage implements io.ReadCloser
// and supports Ack, Nack, Defer, Extend, WritePayloadTo, and DecodePayloadJSON.
// QueueStateHandle mirrors lease-state helpers with Get, GetBytes, Load, Update,
// UpdateBytes, Save, UpdateMetadata, and Remove.
//
//	msg, err := cli.Dequeue(ctx, "orders", client.DequeueOptions{
//	    Namespace: "workflows",
//	    Owner:     "worker-1",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer msg.Close()
//
//	buf, err := io.ReadAll(msg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	log.Printf("attempt %d payload=%s", msg.Attempts(), string(buf))
//	if err := msg.Ack(ctx); err != nil {
//	    log.Fatal(err)
//	}
//
// If a handler returns without explicit Ack/Nack/Defer, Close performs an automatic
// Nack so another worker can continue. Tune that with DequeueOptions.OnCloseDelay
// or QueueMessage.SetOnCloseDelay. DequeueOptions.BlockSeconds controls waiting:
// BlockNoWait for immediate return, positive values for bounded wait, and zero
// for wait-forever.
//
// Subscribe and SubscribeWithState keep one streaming request open and invoke a
// user handler per delivery.
//
// # StartConsumer worker runner
//
// StartConsumer starts one or more managed consumer loops and blocks until they
// terminate. It is intended for long-running worker processes.
//
// Each ConsumerConfig runs in its own goroutine. WithState selects
// SubscribeWithState; false selects Subscribe. Options.Owner can be left empty;
// StartConsumer generates a unique owner token based on consumer name, host,
// process id, and sequence.
//
// The MessageHandler receives ConsumerMessage containing:
//
//   - Client: the active SDK client to reuse inside handlers
//   - Logger: the configured client logger; always non-nil
//   - Queue and Name(): queue identity and resolved consumer name
//   - Message: the leased QueueMessage
//   - State: QueueStateHandle for stateful consumers, nil for stateless
//
// Restart behaviour is controlled by ConsumerRestartPolicy. Defaults are three
// immediate retries, then exponential backoff starting at 250ms with multiplier
// 2.0 and max delay 5 minutes. Jitter defaults to zero and can be enabled.
//
// ErrorHandler receives ConsumerError before each restart. Returning nil keeps
// the loop running. Returning an error stops StartConsumer and returns that
// error. OnStart and OnStop lifecycle hooks can be used for observability.
// Panics in MessageHandler, ErrorHandler, OnStart, and OnStop are recovered and
// routed through the same failure path.
//
// Cancel the StartConsumer context to stop all loops. Expected context
// cancellation returns nil.
//
//	handler := func(ctx context.Context, cm client.ConsumerMessage) error {
//	    defer cm.Message.Close()
//
//	    if cm.State != nil {
//	        var state map[string]any
//	        if err := cm.State.Load(ctx, &state); err != nil && !errors.Is(err, client.ErrStateNotFound) {
//	            return err
//	        }
//	        state["last_message_id"] = cm.Message.MessageID()
//	        if err := cm.State.Save(ctx, state); err != nil {
//	            return err
//	        }
//	    }
//
//	    return cm.Message.Ack(ctx)
//	}
//
//	err := cli.StartConsumer(ctx,
//	    client.ConsumerConfig{
//	        Name:  "orders-fastlane",
//	        Queue: "orders",
//	        Options: client.SubscribeOptions{
//	            Namespace: "workflows",
//	            Prefetch:  16,
//	        },
//	        MessageHandler: handler,
//	    },
//	    client.ConsumerConfig{
//	        Queue:     "orders-stateful",
//	        WithState: true,
//	        Options: client.SubscribeOptions{
//	            Namespace: "workflows",
//	        },
//	        RestartPolicy: client.ConsumerRestartPolicy{
//	            MaxDelay: time.Minute,
//	        },
//	        MessageHandler: handler,
//	    },
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// Direct helpers Client.QueueAck, Client.QueueNack, and Client.QueueExtend are
// also available when metadata is managed outside QueueMessage helpers.
//
// # State removal
//
// Lease holders can delete state explicitly with LeaseSession.Remove or
// Client.Remove. CAS guards such as If-ETag and If-Version are supported by the
// API for concurrency-safe deletes.
//
// # Metadata attributes and query visibility
//
// Metadata holds lease internals plus user attributes. The SDK provides
// LeaseSession.UpdateMetadata and Client.UpdateMetadata, and metadata-aware state
// writes with WithMetadata, WithQueryHidden, and WithQueryVisible.
//
//	lease, err := cli.Acquire(ctx, api.AcquireRequest{Key: "orders", TTLSeconds: 30})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	if _, err := lease.UpdateMetadata(ctx, client.MetadataOptions{QueryHidden: client.Bool(true)}); err != nil {
//	    log.Fatal(err)
//	}
//
// # Multi-endpoint failover
//
// NewWithEndpoints accepts multiple endpoints. The SDK rotates through them on
// transport errors while preserving bounded retry semantics.
//
//	cli, err := client.NewWithEndpoints([]string{
//	    "https://lockd-primary.example.com",
//	    "https://lockd-backup.example.com",
//	}, client.WithDisableMTLS(false))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// # Correlation IDs and logging
//
// Use client.WithCorrelationID or client.GenerateCorrelationID to tie requests,
// queue deliveries, and retries together. QueueMessage carries correlation data
// and queue follow-up helpers forward it automatically.
//
// The client logger is configured with client.WithLogger. Nil logger input is
// normalized to pslog.NoopLogger so SDK logging calls remain safe.
//
// # In-process testing
//
// client/inprocess starts a lockd server in-process and returns a ready client.
// It is useful for tests and local development.
//
//	ctx := context.Background()
//	inproc, err := inprocess.New(ctx, lockd.Config{Store: "mem://"})
//	if err != nil {
//	    t.Fatal(err)
//	}
//	defer inproc.Close(ctx)
//
//	lease, err := inproc.Acquire(ctx, api.AcquireRequest{Owner: "test", TTLSeconds: 10})
//	if err != nil {
//	    t.Fatal(err)
//	}
//	_ = lease
//
// # Authentication and mTLS
//
// mTLS is enabled by default for https endpoints. Configure bundle PEM data with
// client.WithBundlePEM or file paths with client.WithBundlePath. Bundle paths
// expand shell-style home and environment variables by default; use
// client.WithBundlePathDisableExpansion to treat a path literally.
//
// To connect over plaintext http endpoints, use client.WithDisableMTLS(true)
// or provide an http URL. For mTLS, client certificates must include ClientAuth
// EKU and chain to a CA trusted by the server.
package client
