package querysuite

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	querytestdata "pkt.systems/lockd/integration/query/testdata"
	"pkt.systems/lockd/lql"
	"pkt.systems/lockd/namespaces"
)

// ServerFactory starts a backend-specific lockd server for the query suite.
type ServerFactory func(testing.TB) *lockd.TestServer

type runConfig struct {
	datasetProfile querytestdata.DatasetProfile
}

// Option customizes the behavior of suite runners.
type Option func(*runConfig)

// WithDatasetProfile sets the dataset profile used for domain seeding.
func WithDatasetProfile(profile querytestdata.DatasetProfile) Option {
	return func(cfg *runConfig) {
		cfg.datasetProfile = profile
	}
}

// WithReducedDataset applies a lighter dataset profile (useful for slow backends).
func WithReducedDataset() Option {
	return WithDatasetProfile(querytestdata.DatasetReduced)
}

// RunSelectors seeds simple documents and ensures selector matching behaves consistently.
func RunSelectors(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 15*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		querytestdata.SeedState(t, ctx, ts.Client, "", "orders-open-1", map[string]any{"status": "open", "amount": 150.0, "region": "us"})
		querytestdata.SeedState(t, ctx, ts.Client, "", "orders-open-2", map[string]any{"status": "open", "amount": 80.0})
		querytestdata.SeedState(t, ctx, ts.Client, "", "orders-closed", map[string]any{"status": "closed", "amount": 200.0})

		selector, found, err := lql.ParseSelectorString(`
and.eq{field=status,value=open},
and.range{field=amount,gte=100,lt=200}`)
		if err != nil || !found {
			t.Fatalf("parse selector: %v (found=%v)", err, found)
		}

		resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
			Namespace: namespaces.Default,
			Selector:  selector,
			Limit:     10,
		})
		if len(resp.Keys) != 1 || resp.Keys[0] != "orders-open-1" {
			t.Fatalf("expected only orders-open-1, got %+v", resp.Keys)
		}
		if resp.Cursor != "" {
			t.Fatalf("expected empty cursor for single page, got %q", resp.Cursor)
		}
	})
}

// RunPagination verifies cursor pagination works across all backends.
func RunPagination(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 15*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		for i := 0; i < 4; i++ {
			key := fmt.Sprintf("batch-%02d", i)
			querytestdata.SeedState(t, ctx, ts.Client, "", key, map[string]any{"status": "open", "index": i})
		}
		selector, found, err := lql.ParseSelectorString(`eq{field=status,value=open}`)
		if err != nil || !found {
			t.Fatalf("parse selector: %v (found=%v)", err, found)
		}

		var (
			cursor    string
			collected []string
		)
		seen := make(map[string]struct{})
		produced := 0
		for {
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  selector,
				Limit:     2,
				Cursor:    cursor,
			})
			for _, key := range resp.Keys {
				if _, ok := seen[key]; ok {
					t.Fatalf("duplicate key %s encountered during pagination", key)
				}
				seen[key] = struct{}{}
				collected = append(collected, key)
				produced++
			}
			if resp.Cursor == "" {
				break
			}
			if produced == 0 {
				t.Fatalf("pagination returned a cursor (%q) but no keys", resp.Cursor)
			}
			produced = 0
			cursor = resp.Cursor
			if len(collected) > 10 {
				t.Fatalf("pagination runaway: %+v", collected)
			}
		}
		expected := []string{"batch-00", "batch-01", "batch-02", "batch-03"}
		querytestdata.ExpectKeySet(t, collected, expected)
	})
}

// RunNamespaceIsolation ensures results stay scoped to the requested namespace.
func RunNamespaceIsolation(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 15*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		querytestdata.SeedState(t, ctx, ts.Client, "alpha", "job-1", map[string]any{"status": "open"})
		querytestdata.SeedState(t, ctx, ts.Client, "alpha", "job-2", map[string]any{"status": "open"})
		querytestdata.SeedState(t, ctx, ts.Client, "beta", "job-3", map[string]any{"status": "open"})

		sel, found, err := lql.ParseSelectorString(`eq{field=status,value=open}`)
		if err != nil || !found {
			t.Fatalf("parse selector: %v (found=%v)", err, found)
		}

		alpha := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
			Namespace: "alpha",
			Selector:  sel,
			Limit:     10,
		})
		querytestdata.ExpectKeySet(t, alpha.Keys, []string{"job-1", "job-2"})

		beta := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
			Namespace: "beta",
			Selector:  sel,
			Limit:     10,
		})
		querytestdata.ExpectKeySet(t, beta.Keys, []string{"job-3"})
	})
}

// RunPublicRead checks that public queries can be fetched via GetPublic without leases.
func RunPublicRead(t *testing.T, factory ServerFactory) {
	withServer(t, factory, 15*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		key := "report-" + time.Now().Format("150405")
		querytestdata.SeedState(t, ctx, ts.Client, "", key, map[string]any{"status": "published", "payload": "value"})

		sel, found, err := lql.ParseSelectorString(`eq{field=status,value=published}`)
		if err != nil || !found {
			t.Fatalf("parse selector: %v", err)
		}

		resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
			Namespace: namespaces.Default,
			Selector:  sel,
			Limit:     1,
		})
		if len(resp.Keys) != 1 || resp.Keys[0] != key {
			t.Fatalf("expected %s in query response, got %+v", key, resp.Keys)
		}

		reader, _, _, err := ts.Client.GetPublicWithNamespace(ctx, namespaces.Default, key)
		if err != nil {
			t.Fatalf("public get: %v", err)
		}
		defer reader.Close()
		var payload map[string]any
		if err := json.NewDecoder(reader).Decode(&payload); err != nil {
			t.Fatalf("decode public payload: %v", err)
		}
		if payload["payload"] != "value" {
			t.Fatalf("unexpected payload: %+v", payload)
		}
	})
}

// RunDomainDatasets seeds richer domain data (finance, firmware, SALUTE, flight) and evaluates selectors.
func RunDomainDatasets(t *testing.T, factory ServerFactory, opts ...Option) {
	cfg := runConfig{datasetProfile: querytestdata.DatasetFull}
	for _, opt := range opts {
		opt(&cfg)
	}
	shouldRun := func(name string) bool {
		if cfg.datasetProfile != querytestdata.DatasetReduced {
			return true
		}
		switch name {
		case "vouchers-unposted", "firmware-draining", "salute-opfor", "flight-manual-guidance":
			return true
		default:
			return false
		}
	}
	withServer(t, factory, 30*time.Second, func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client) {
		querytestdata.SeedVoucherData(t, ctx, ts.Client, cfg.datasetProfile)
		querytestdata.SeedFirmwareData(t, ctx, ts.Client, cfg.datasetProfile)
		querytestdata.SeedSaluteData(t, ctx, ts.Client, cfg.datasetProfile)
		querytestdata.SeedFlightData(t, ctx, ts.Client, cfg.datasetProfile)

		t.Run("vouchers-unposted", func(t *testing.T) {
			if !shouldRun("vouchers-unposted") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, found, err := lql.ParseSelectorString(`
and.eq{field=voucher.book,value=GENERAL},
and.eq{field=voucher.header.period,value=2025-11},
and.eq{field=voucher.header.posted,value=false},
and.range{field=voucher.lines.10.amount,gte=3000}`)
			if err != nil || !found {
				t.Fatalf("selector voucher: %v (found=%v)", err, found)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			want := []string{"voucher-ap-2025-1101"}
			if !slices.Equal(resp.Keys, want) {
				t.Fatalf("expected %v, got %v", want, resp.Keys)
			}
		})

		t.Run("vouchers-posted", func(t *testing.T) {
			if !shouldRun("vouchers-posted") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, found, err := lql.ParseSelectorString(`
and.eq{field=voucher.header.posted,value=true}`)
			if err != nil || !found {
				t.Fatalf("selector vouchers posted: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			querytestdata.ExpectKeySet(t, resp.Keys, []string{
				"voucher-ap-2025-1102",
				"voucher-bulk-00",
				"voucher-bulk-02",
				"voucher-bulk-04",
			})
		})

		t.Run("vouchers-unposted-nested-or", func(t *testing.T) {
			if !shouldRun("vouchers-unposted-nested-or") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, found, err := lql.ParseSelectorString(`
and.0.eq{field=voucher.header.posted,value=false},
and.1.or.0.eq{field=voucher.book,value=GENERAL},
and.1.or.1.eq{field=voucher.book,value=US-LEDGER}`)
			if err != nil || !found {
				t.Fatalf("selector vouchers nested or: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			querytestdata.ExpectKeySet(t, resp.Keys, []string{
				"voucher-ap-2025-1101",
				"voucher-bulk-01",
				"voucher-bulk-03",
				"voucher-bulk-05",
				"voucher-us-2025-1103",
			})
		})

		t.Run("firmware-draining", func(t *testing.T) {
			if !shouldRun("firmware-draining") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, found, err := lql.ParseSelectorString(`
and.eq{field=device.firmware.channel,value=stable},
and.range{field=device.rollout.progress.percent,gte=50},
and.range{field=device.telemetry.battery_mv,gte=3600},
and.eq{field=device.rollout.progress.status,value=draining}`)
			if err != nil || !found {
				t.Fatalf("firmware selector: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			want := []string{"device-gw-2048"}
			if !slices.Equal(resp.Keys, want) {
				t.Fatalf("expected %v, got %v", want, resp.Keys)
			}
		})

		t.Run("firmware-draining-or-west", func(t *testing.T) {
			if !shouldRun("firmware-draining-or-west") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, found, err := lql.ParseSelectorString(`
and.eq{field=device.rollout.progress.status,value=draining},
or.eq{field=device.location.region,value=us-west}`)
			if err != nil || !found {
				t.Fatalf("firmware draining or west selector: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			querytestdata.ExpectKeySet(t, resp.Keys, []string{
				"device-gw-1024",
				"device-gw-2048",
				"device-gw-512",
				"device-gw-600",
			})
		})

		t.Run("firmware-low-battery", func(t *testing.T) {
			if !shouldRun("firmware-low-battery") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, found, err := lql.ParseSelectorString(`
and.eq{field=device.firmware.channel,value=stable},
and.range{field=device.telemetry.battery_mv,lte=3600}`)
			if err != nil || !found {
				t.Fatalf("firmware low battery selector: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			querytestdata.ExpectKeySet(t, resp.Keys, []string{"device-gw-600", "device-gw-700", "device-gw-8192"})
		})

		t.Run("salute-opfor", func(t *testing.T) {
			if !shouldRun("salute-opfor") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, found, err := lql.ParseSelectorString(`
and.eq{field=report.type,value=salute},
and.eq{field=report.affiliation,value=opfor},
and.prefix{field=report.location.grid,value=42SXD}`)
			if err != nil || !found {
				t.Fatalf("salute selector: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			want := []string{"salute-report-opfor"}
			if !slices.Equal(resp.Keys, want) {
				t.Fatalf("expected %v, got %v", want, resp.Keys)
			}
		})

		t.Run("salute-friendly-grid", func(t *testing.T) {
			if !shouldRun("salute-friendly-grid") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, found, err := lql.ParseSelectorString(`
and.eq{field=report.affiliation,value=friendly},
and.prefix{field=report.location.grid,value=42SY}`)
			if err != nil || !found {
				t.Fatalf("salute friendly selector: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			querytestdata.ExpectKeySet(t, resp.Keys, []string{"salute-report-friendly-south"})
		})

		t.Run("flight-critical", func(t *testing.T) {
			if !shouldRun("flight-critical") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, found, err := lql.ParseSelectorString(`
and.eq{field=vehicle.mission,value=delta-heavy},
and.eq{field=telemetry.stage,value=boost},
and.range{field=telemetry.apoapsis_km,gte=150},
and.eq{field=telemetry.guidance.mode,value=auto}`)
			if err != nil || !found {
				t.Fatalf("flight selector: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			want := []string{"flight-boost-02"}
			if !slices.Equal(resp.Keys, want) {
				t.Fatalf("expected %v, got %v", want, resp.Keys)
			}
		})

		t.Run("flight-manual-guidance", func(t *testing.T) {
			if !shouldRun("flight-manual-guidance") {
				t.Skip("reduced dataset")
			}
			t.Parallel()
			sel, found, err := lql.ParseSelectorString(`
and.eq{field=telemetry.guidance.mode,value=manual}`)
			if err != nil || !found {
				t.Fatalf("flight manual selector: %v", err)
			}
			resp := doQuery(t, httpClient, ts.URL(), api.QueryRequest{
				Namespace: namespaces.Default,
				Selector:  sel,
			})
			querytestdata.ExpectKeySet(t, resp.Keys, []string{"flight-descent-01", "flight-entry-01", "flight-fairing-01"})
		})
	})
}

func withServer(t testing.TB, factory ServerFactory, timeout time.Duration, fn func(ctx context.Context, ts *lockd.TestServer, httpClient *http.Client)) {
	t.Helper()
	ts := factory(t)
	if ts.Client == nil {
		t.Fatalf("test server missing client")
	}
	httpClient := newHTTPClient(t, ts)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	fn(ctx, ts, httpClient)
}

func newHTTPClient(t testing.TB, ts *lockd.TestServer) *http.Client {
	t.Helper()
	client, err := ts.NewHTTPClient()
	if err != nil {
		t.Fatalf("http client: %v", err)
	}
	return client
}

func doQuery(t testing.TB, httpClient *http.Client, baseURL string, req api.QueryRequest) api.QueryResponse {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal query request: %v", err)
	}
	httpReq, err := http.NewRequest(http.MethodPost, baseURL+"/v1/query", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("prepare http request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		t.Fatalf("query request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status %d: %s", resp.StatusCode, data)
	}
	var out api.QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return out
}
