// Command startconsumer is an example where a consumer worker rolls over a
// single stateful message three times incrementing a counter on the stateful
// object. If the condition (3) is not met, the MessageHandler returns Defer
// (intentional requeue), handing the message back onto the queue. Next
// dequeue loads the stateful object and increments the counter. Only when the
// condition is met (3) does the MessageHandler return ACK (nil error) and the
// workflow (if you will) is complete. This example starts an in-process lockd
// using lockd.StartServer and a worker using client.StartConsumer.
package main

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/client"
	"pkt.systems/prettyx"
	"pkt.systems/pslog"
	"pkt.systems/pslog/ansi"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	l := pslog.LoggerFromEnv(context.Background(), pslog.WithEnvOptions(pslog.Options{
		Mode:       pslog.ModeConsole,
		TimeFormat: time.RFC3339,
		MinLevel:   pslog.InfoLevel,
	}))

	cfg := lockd.Config{
		Store:            "mem://",
		Listen:           "127.0.0.1:13371",
		BundlePath:       "$HOME/.lockd/server.pem",
		DefaultNamespace: "stash",
		DrainGrace:       3 * time.Second,
	}

	serverLogger := l.With("actor", "server")
	//serverLogger := pslog.NoopLogger()

	s, err := lockd.StartServer(ctx, cfg, lockd.WithLogger(serverLogger))
	if err != nil {
		l.With(err).Fatal("lockd.start_server")
	}

	type countType struct {
		Counter int `json:"counter"`
	}
	var counts []countType

	defer func() {
		s.Stop(ctx)
		b, err := json.Marshal(&counts)
		if err != nil {
			l.With(err).Fatal("json.marshal")
		}
		prettyx.PrettyTo(os.Stdout, b, nil)
	}()

	clientLogger := pslog.LoggerFromEnv(context.Background(), pslog.WithEnvPrefix("CLIENT_LOG_"), pslog.WithEnvOptions(pslog.Options{
		Mode:       pslog.ModeConsole,
		TimeFormat: time.RFC3339,
		MinLevel:   pslog.InfoLevel,
		Palette:    &ansi.PaletteEverforest,
	})).With("actor", "client")

	cli, err := client.New("localhost:13371", client.WithBundlePath("$HOME/.lockd/client.pem"), client.WithLogger(clientLogger))
	if err != nil {
		l.With(err).Fatal("client.new")
	}
	defer cli.Close()

	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()

	_, err = cli.EnqueueBytes(ctx, "testing", []byte(`{"hello":"world"}`), client.EnqueueOptions{
		Delay: 1 * time.Second,
	})
	if err != nil {
		l.With(err).Fatal("client.enqueue.bytes")
	}

	consumerCtx, consumerCancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(1)

	var acked bool
	go func() {
		defer wg.Done()
		for {
			if acked {
				time.Sleep(2 * time.Second)
				consumerCancel()
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	l.Info("client.start_consumer")

	if err := cli.StartConsumer(consumerCtx, client.ConsumerConfig{
		Name:      "ackOnThree",
		Queue:     "testing",
		WithState: true,
		MessageHandler: func(mctx context.Context, c client.ConsumerMessage) error {
			c.Logger.Info("consumer", "name", c.Name(), "with_state", c.WithState, "msg", c.Message.MessageID(), "payload_size", c.Message.PayloadSize(), "attempts", c.Message.Attempts(), "failure_attempts", c.Message.FailureAttempts())
			var v struct {
				Hello string `json:"hello"`
			}
			if err := c.Message.DecodePayloadJSON(&v); err != nil {
				return err
			}
			c.Logger.Info("consumer", "hello", v.Hello)

			if c.WithState {
				var count countType
				if err := c.State.Load(mctx, &count); err != nil {
					return err
				}
				// if counter is 3 or more, we
				if count.Counter > 2 {
					c.Logger.Info("consumer", "counter", count.Counter, "status", "done")
					acked = true
					return nil
				}
				// counter is not yet 3 or more, increment int and save it
				count.Counter++
				if err := c.State.Save(mctx, &count); err != nil {
					return err
				}
				counts = append(counts, count)
				return c.Message.Defer(mctx, 1*time.Second)
			}
			return nil
		},
	}); err != nil {
		l.With(err).Fatal("client.start_consumer")
	}

	wg.Wait()
}
