package oteltest

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	collectorMetric "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectorTrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Collector is a lightweight in-process OTLP collector used by integration tests.
type Collector struct {
	mu        sync.Mutex
	spanCount int
	signal    chan struct{}
	requests  []*collectorTrace.ExportTraceServiceRequest

	grpcServer *grpc.Server
	listener   net.Listener
}

// Start spins up a new Collector listening on 127.0.0.1 with an ephemeral pslog backend.
func Start() (*Collector, string, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", fmt.Errorf("listen otlp collector: %w", err)
	}
	srv := grpc.NewServer()
	c := &Collector{
		signal:     make(chan struct{}, 1),
		grpcServer: srv,
		listener:   lis,
	}
	collectorTrace.RegisterTraceServiceServer(srv, &traceHandler{collector: c})
	collectorMetric.RegisterMetricsServiceServer(srv, &metricHandler{})

	go func() {
		if serveErr := srv.Serve(lis); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			log.Printf("oteltest collector stopped unexpectedly: %v", serveErr)
		}
	}()

	readyCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(readyCtx, "tcp", lis.Addr().String())
	if err != nil {
		srv.Stop()
		_ = lis.Close()
		return nil, "", fmt.Errorf("wait for otlp collector readiness: %w", err)
	}
	_ = conn.Close()

	return c, lis.Addr().String(), nil
}

// Stop stops the collector server.
func (c *Collector) Stop() {
	if c.grpcServer != nil {
		c.grpcServer.Stop()
	}
	if c.listener != nil {
		_ = c.listener.Close()
	}
}

// WaitForSpans blocks until at least want spans have been exported or ctx expires.
func (c *Collector) WaitForSpans(ctx context.Context, want int) error {
	for {
		c.mu.Lock()
		if c.spanCount >= want {
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.signal:
		}
	}
}

// Requests returns a snapshot of exported trace requests.
func (c *Collector) Requests() []*collectorTrace.ExportTraceServiceRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*collectorTrace.ExportTraceServiceRequest, len(c.requests))
	copy(out, c.requests)
	return out
}

type traceHandler struct {
	collectorTrace.UnimplementedTraceServiceServer
	collector *Collector
}

func (h *traceHandler) Export(_ context.Context, req *collectorTrace.ExportTraceServiceRequest) (*collectorTrace.ExportTraceServiceResponse, error) {
	if req == nil {
		return &collectorTrace.ExportTraceServiceResponse{}, nil
	}
	count := 0
	for _, rs := range req.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			count += len(ss.Spans)
		}
	}
	h.collector.mu.Lock()
	h.collector.spanCount += count
	if count > 0 {
		cloned := proto.Clone(req).(*collectorTrace.ExportTraceServiceRequest)
		h.collector.requests = append(h.collector.requests, cloned)
		select {
		case h.collector.signal <- struct{}{}:
		default:
		}
	}
	h.collector.mu.Unlock()
	return &collectorTrace.ExportTraceServiceResponse{}, nil
}

type metricHandler struct {
	collectorMetric.UnimplementedMetricsServiceServer
}

func (metricHandler) Export(context.Context, *collectorMetric.ExportMetricsServiceRequest) (*collectorMetric.ExportMetricsServiceResponse, error) {
	return &collectorMetric.ExportMetricsServiceResponse{}, nil
}

// HasCorrelationAttribute reports whether any exported span carries the lockd correlation attribute.
func HasCorrelationAttribute(requests []*collectorTrace.ExportTraceServiceRequest) bool {
	for _, req := range requests {
		for _, rs := range req.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				for _, span := range ss.Spans {
					if span.GetName() != "lockd.tx.acquire" {
						continue
					}
					if hasCorrelation(span.Attributes) {
						return true
					}
				}
			}
		}
	}
	return false
}

func hasCorrelation(attrs []*commonpb.KeyValue) bool {
	for _, attr := range attrs {
		if attr.GetKey() == "lockd.correlation_id" {
			if val := attr.GetValue().GetStringValue(); val != "" {
				return true
			}
		}
	}
	return false
}
