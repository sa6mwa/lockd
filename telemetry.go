package lockd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
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

func setupTelemetry(ctx context.Context, endpoint string, logger pslog.Logger) (*telemetryBundle, error) {
	target, err := resolveOTLPTarget(endpoint)
	if err != nil {
		return nil, err
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
	)

	switch target.protocol {
	case "grpc":
		traceProvider, meterProvider, err = setupGRPCTelemetry(ctx, target, res)
	case "http":
		traceProvider, meterProvider, err = setupHTTPTelemetry(ctx, target, res)
	default:
		return nil, fmt.Errorf("telemetry: unsupported protocol %q", target.protocol)
	}
	if err != nil {
		return nil, err
	}

	otel.SetTracerProvider(traceProvider)
	otel.SetMeterProvider(meterProvider)
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)
	otel.SetErrorHandler(otelErrorHandler{logger: logger})

	logger.Info("telemetry.enabled",
		"protocol", target.protocol,
		"endpoint", target.endpoint,
		"path", target.path,
		"insecure", target.insecure,
	)

	return &telemetryBundle{
		tracerProvider: traceProvider,
		meterProvider:  meterProvider,
		logger:         logger,
	}, nil
}

func setupGRPCTelemetry(ctx context.Context, target otlpTarget, res *resource.Resource) (*sdktrace.TracerProvider, *sdkmetric.MeterProvider, error) {
	traceOpts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(target.endpoint),
		otlptracegrpc.WithTimeout(10 * time.Second),
	}
	metricOpts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpoint(target.endpoint),
		otlpmetricgrpc.WithTimeout(10 * time.Second),
	}
	if target.insecure {
		traceOpts = append(traceOpts, otlptracegrpc.WithInsecure())
		traceOpts = append(traceOpts, otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
		metricOpts = append(metricOpts, otlpmetricgrpc.WithInsecure())
		metricOpts = append(metricOpts, otlpmetricgrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	} else {
		tlsConfig := credentials.NewClientTLSFromCert(nil, "")
		traceOpts = append(traceOpts, otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(tlsConfig)))
		metricOpts = append(metricOpts, otlpmetricgrpc.WithDialOption(grpc.WithTransportCredentials(tlsConfig)))
	}
	traceExporter, err := otlptracegrpc.New(ctx, traceOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry: start trace exporter (grpc): %w", err)
	}
	metricExporter, err := otlpmetricgrpc.New(ctx, metricOpts...)
	if err != nil {
		_ = traceExporter.Shutdown(ctx)
		return nil, nil, fmt.Errorf("telemetry: start metric exporter (grpc): %w", err)
	}

	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1.0))),
		sdktrace.WithBatcher(traceExporter),
	)
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(15*time.Second))),
	)
	return traceProvider, meterProvider, nil
}

func setupHTTPTelemetry(ctx context.Context, target otlpTarget, res *resource.Resource) (*sdktrace.TracerProvider, *sdkmetric.MeterProvider, error) {
	traceOpts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(target.endpoint),
		otlptracehttp.WithTimeout(10 * time.Second),
	}
	metricOpts := []otlpmetrichttp.Option{
		otlpmetrichttp.WithEndpoint(target.endpoint),
		otlpmetrichttp.WithTimeout(10 * time.Second),
	}
	if target.insecure {
		traceOpts = append(traceOpts, otlptracehttp.WithInsecure())
		metricOpts = append(metricOpts, otlpmetrichttp.WithInsecure())
	}
	if target.path != "" && target.path != "/" {
		traceOpts = append(traceOpts, otlptracehttp.WithURLPath(target.path))
		metricOpts = append(metricOpts, otlpmetrichttp.WithURLPath(target.path))
	}

	traceExporter, err := otlptracehttp.New(ctx, traceOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry: start trace exporter (http): %w", err)
	}
	metricExporter, err := otlpmetrichttp.New(ctx, metricOpts...)
	if err != nil {
		_ = traceExporter.Shutdown(ctx)
		return nil, nil, fmt.Errorf("telemetry: start metric exporter (http): %w", err)
	}

	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1.0))),
		sdktrace.WithBatcher(traceExporter),
	)
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(15*time.Second))),
	)
	return traceProvider, meterProvider, nil
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
