package lockd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelruntime "go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelprometheus "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"pkt.systems/pslog"
)

type telemetryBundle struct {
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	metricsServer  *http.Server
	metricsLn      net.Listener
	pprofServer    *http.Server
	pprofLn        net.Listener
	logger         pslog.Logger
}

type otelErrorHandler struct {
	logger pslog.Logger
}

func (h otelErrorHandler) Handle(err error) {
	if err == nil {
		return
	}
	if strings.Contains(err.Error(), "waiting for connections to become ready") {
		if h.logger != nil {
			h.logger.Debug("telemetry.exporter.retry", "error", err)
		}
		return
	}
	if h.logger != nil {
		h.logger.Warn("telemetry.exporter.error", "error", err)
	}
}

func (t *telemetryBundle) Shutdown(ctx context.Context) error {
	var errs []error
	if t.meterProvider != nil {
		if err := t.meterProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("metric shutdown: %w", err))
			if t.logger != nil {
				t.logger.Warn("telemetry.shutdown.metric_failure", "error", err)
			}
		}
	}
	if t.metricsServer != nil {
		if err := t.metricsServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, fmt.Errorf("metrics server shutdown: %w", err))
			if t.logger != nil {
				t.logger.Warn("telemetry.shutdown.metrics_server_failure", "error", err)
			}
		}
	}
	if t.metricsLn != nil {
		_ = t.metricsLn.Close()
	}
	if t.pprofServer != nil {
		if err := t.pprofServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, fmt.Errorf("pprof server shutdown: %w", err))
			if t.logger != nil {
				t.logger.Warn("telemetry.shutdown.pprof_server_failure", "error", err)
			}
		}
	}
	if t.pprofLn != nil {
		_ = t.pprofLn.Close()
	}
	if t.tracerProvider != nil {
		if err := t.tracerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("trace shutdown: %w", err))
			if t.logger != nil {
				t.logger.Warn("telemetry.shutdown.trace_failure", "error", err)
			}
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if t.logger != nil {
		t.logger.Info("telemetry.shutdown.complete")
	}
	return nil
}

type otlpTarget struct {
	protocol string // "grpc" or "http"
	endpoint string // host:port
	path     string
	insecure bool
}

var runtimeMetricsOnce sync.Once
var runtimeMetricsErr error

func setupTelemetry(ctx context.Context, endpoint, metricsListen, pprofListen string, enableProfilingMetrics bool, logger pslog.Logger) (*telemetryBundle, error) {
	if strings.TrimSpace(endpoint) == "" && strings.TrimSpace(metricsListen) == "" && strings.TrimSpace(pprofListen) == "" && !enableProfilingMetrics {
		return nil, nil
	}
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	res, err := resource.New(ctx,
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithAttributes(
			semconv.ServiceName("lockd"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: build resource: %w", err)
	}

	var (
		traceProvider *sdktrace.TracerProvider
		meterProvider *sdkmetric.MeterProvider
		metricsServer *http.Server
		metricsLn     net.Listener
		pprofServer   *http.Server
		pprofLn       net.Listener
		target        otlpTarget
	)

	if strings.TrimSpace(endpoint) != "" {
		target, err = resolveOTLPTarget(endpoint)
		if err != nil {
			return nil, err
		}
		switch target.protocol {
		case "grpc":
			traceProvider, err = setupGRPCTracing(ctx, target, res)
		case "http":
			traceProvider, err = setupHTTPTracing(ctx, target, res)
		default:
			return nil, fmt.Errorf("telemetry: unsupported protocol %q", target.protocol)
		}
		if err != nil {
			return nil, err
		}
		otel.SetTracerProvider(traceProvider)
		logger.Info("telemetry.tracing.enabled",
			"protocol", target.protocol,
			"endpoint", target.endpoint,
			"path", target.path,
			"insecure", target.insecure,
		)
	}

	metricsListen = strings.TrimSpace(metricsListen)
	if metricsListen != "" {
		registry := prometheus.NewRegistry()
		exporterOpts := []otelprometheus.Option{otelprometheus.WithRegisterer(registry)}
		if enableProfilingMetrics {
			exporterOpts = append(exporterOpts, otelprometheus.WithProducer(otelruntime.NewProducer()))
		}
		exporter, err := otelprometheus.New(exporterOpts...)
		if err != nil {
			if traceProvider != nil {
				_ = traceProvider.Shutdown(ctx)
			}
			return nil, fmt.Errorf("telemetry: start prometheus exporter: %w", err)
		}
		meterProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(exporter),
		)
		otel.SetMeterProvider(meterProvider)
		if enableProfilingMetrics {
			if err := startRuntimeMetricsWithProvider(meterProvider); err != nil {
				if traceProvider != nil {
					_ = traceProvider.Shutdown(ctx)
				}
				_ = meterProvider.Shutdown(ctx)
				return nil, err
			}
			logger.Info("profiling.metrics.enabled")
		}
		metricsHandler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
		metricsServer, metricsLn, err = startMetricsServer(metricsListen, metricsHandler, logger)
		if err != nil {
			if traceProvider != nil {
				_ = traceProvider.Shutdown(ctx)
			}
			_ = meterProvider.Shutdown(ctx)
			return nil, err
		}
		logger.Info("telemetry.metrics.enabled", "listen", metricsListen)
	} else if enableProfilingMetrics {
		return nil, fmt.Errorf("telemetry: profiling metrics require metrics listen address")
	}

	pprofListen = strings.TrimSpace(pprofListen)
	if pprofListen != "" {
		pprofServer, pprofLn, err = startPprofServer(pprofListen, logger)
		if err != nil {
			if traceProvider != nil {
				_ = traceProvider.Shutdown(ctx)
			}
			if meterProvider != nil {
				_ = meterProvider.Shutdown(ctx)
			}
			return nil, err
		}
		logger.Info("profiling.pprof.enabled", "listen", pprofListen)
	}

	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)
	otel.SetErrorHandler(otelErrorHandler{logger: logger})

	return &telemetryBundle{
		tracerProvider: traceProvider,
		meterProvider:  meterProvider,
		metricsServer:  metricsServer,
		metricsLn:      metricsLn,
		pprofServer:    pprofServer,
		pprofLn:        pprofLn,
		logger:         logger,
	}, nil
}

func setupGRPCTracing(ctx context.Context, target otlpTarget, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	traceOpts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(target.endpoint),
		otlptracegrpc.WithTimeout(10 * time.Second),
	}
	if target.insecure {
		traceOpts = append(traceOpts, otlptracegrpc.WithInsecure())
		traceOpts = append(traceOpts, otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	} else {
		tlsConfig := credentials.NewClientTLSFromCert(nil, "")
		traceOpts = append(traceOpts, otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(tlsConfig)))
	}
	traceExporter, err := otlptracegrpc.New(ctx, traceOpts...)
	if err != nil {
		return nil, fmt.Errorf("telemetry: start trace exporter (grpc): %w", err)
	}

	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1.0))),
		sdktrace.WithBatcher(traceExporter),
	)
	return traceProvider, nil
}

func setupHTTPTracing(ctx context.Context, target otlpTarget, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	traceOpts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(target.endpoint),
		otlptracehttp.WithTimeout(10 * time.Second),
	}
	if target.insecure {
		traceOpts = append(traceOpts, otlptracehttp.WithInsecure())
	}
	if target.path != "" && target.path != "/" {
		traceOpts = append(traceOpts, otlptracehttp.WithURLPath(target.path))
	}

	traceExporter, err := otlptracehttp.New(ctx, traceOpts...)
	if err != nil {
		return nil, fmt.Errorf("telemetry: start trace exporter (http): %w", err)
	}

	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1.0))),
		sdktrace.WithBatcher(traceExporter),
	)
	return traceProvider, nil
}

func startMetricsServer(addr string, handler http.Handler, logger pslog.Logger) (*http.Server, net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry: metrics listen: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)
	srv := &http.Server{
		Handler: mux,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			if logger != nil {
				logger.Warn("telemetry.metrics.serve_error", "error", err)
			}
		}
	}()
	return srv, ln, nil
}

func startPprofServer(addr string, logger pslog.Logger) (*http.Server, net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("profiling: pprof listen: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	srv := &http.Server{
		Handler: mux,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			if logger != nil {
				logger.Warn("profiling.pprof.serve_error", "error", err)
			}
		}
	}()
	return srv, ln, nil
}

func startRuntimeMetricsWithProvider(provider metric.MeterProvider) error {
	if provider == nil {
		return fmt.Errorf("profiling: meter provider unavailable")
	}
	runtimeMetricsOnce.Do(func() {
		runtimeMetricsErr = otelruntime.Start(otelruntime.WithMeterProvider(provider))
	})
	return runtimeMetricsErr
}

func resolveOTLPTarget(raw string) (otlpTarget, error) {
	if raw == "" {
		return otlpTarget{}, fmt.Errorf("telemetry: empty endpoint")
	}
	if !strings.Contains(raw, "://") {
		endpoint := raw
		if !strings.Contains(endpoint, ":") {
			endpoint = net.JoinHostPort(endpoint, "4317")
		}
		return otlpTarget{
			protocol: "grpc",
			endpoint: endpoint,
			insecure: true,
		}, nil
	}

	u, err := url.Parse(raw)
	if err != nil {
		return otlpTarget{}, fmt.Errorf("telemetry: parse endpoint: %w", err)
	}
	host := u.Host
	if host == "" {
		host = u.Path
		u.Path = ""
	}
	target := otlpTarget{
		endpoint: host,
		path:     strings.TrimSuffix(u.Path, "/"),
	}
	switch strings.ToLower(u.Scheme) {
	case "grpc":
		target.protocol = "grpc"
		target.insecure = true
	case "grpcs":
		target.protocol = "grpc"
		target.insecure = false
	case "http":
		target.protocol = "http"
		target.insecure = true
		if !strings.Contains(target.endpoint, ":") {
			target.endpoint = net.JoinHostPort(target.endpoint, "4318")
		}
	case "https":
		target.protocol = "http"
		target.insecure = false
		if !strings.Contains(target.endpoint, ":") {
			target.endpoint = net.JoinHostPort(target.endpoint, "4318")
		}
	default:
		return otlpTarget{}, fmt.Errorf("telemetry: unknown scheme %q", u.Scheme)
	}
	if target.endpoint == "" {
		return otlpTarget{}, fmt.Errorf("telemetry: missing endpoint host")
	}
	if target.protocol == "grpc" && !strings.Contains(target.endpoint, ":") {
		target.endpoint = net.JoinHostPort(target.endpoint, "4317")
	}
	return target, nil
}
