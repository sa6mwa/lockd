package querydata

import (
	"context"
	"fmt"
	"testing"
	"time"

	"pkt.systems/lockd/client"
)

type datasetEntry struct {
	Key        string
	Data       map[string]any
	MinProfile DatasetProfile
}

// SeedVoucherData populates a rich finance dataset (multiple books, currencies,
// workflows, attachments, and multi-line vouchers) so selectors can exercise
// complex paths.
func SeedVoucherData(ctx context.Context, t testing.TB, cli *client.Client, profile DatasetProfile) {
	t.Helper()
	entries := make([]datasetEntry, 0, len(voucherRecords))
	for _, record := range voucherRecords {
		if !record.allowed(profile) {
			continue
		}
		entries = append(entries, record.toDatasetEntry())
	}
	seedEntries(ctx, t, cli, "", entries)
}

// SeedFirmwareData populates IoT devices across rollout states and regions.
func SeedFirmwareData(ctx context.Context, t testing.TB, cli *client.Client, profile DatasetProfile) {
	t.Helper()
	entries := make([]datasetEntry, 0, len(firmwareRecords))
	for _, record := range firmwareRecords {
		if !record.allowed(profile) {
			continue
		}
		entries = append(entries, record.toDatasetEntry())
	}
	seedEntries(ctx, t, cli, "", entries)
}

// SeedSaluteData seeds SALUTE reports across affiliations and grids.
func SeedSaluteData(ctx context.Context, t testing.TB, cli *client.Client, profile DatasetProfile) {
	entries := make([]datasetEntry, 0, len(saluteRecords))
	for _, record := range saluteRecords {
		if !record.allowed(profile) {
			continue
		}
		entries = append(entries, record.toDatasetEntry())
	}
	seedEntries(ctx, t, cli, "", entries)
}

// SeedFlightData seeds telemetry snapshots for multiple stages/modes.
func SeedFlightData(ctx context.Context, t testing.TB, cli *client.Client, profile DatasetProfile) {
	entries := make([]datasetEntry, 0, len(flightRecords))
	for _, record := range flightRecords {
		if !record.allowed(profile) {
			continue
		}
		entries = append(entries, record.toDatasetEntry())
	}
	seedEntries(ctx, t, cli, "", entries)
}

// voucher dataset ----------------------------------------------------------------

type voucherRecord struct {
	Key           string
	Book          string
	Period        string
	Region        string
	Currency      string
	Posted        bool
	Workflow      string
	HasAttachment bool
	MinProfile    DatasetProfile
	Lines         []voucherLine
}

type voucherLine struct {
	Slot     string
	Account  string
	Amount   float64
	CostCtr  string
	TaxCode  string
	VendorID string
}

func (vr voucherRecord) allowed(profile DatasetProfile) bool {
	return profile >= vr.MinProfile
}

func (vr voucherRecord) toDatasetEntry() datasetEntry {
	lines := make(map[string]any, len(vr.Lines))
	for _, line := range vr.Lines {
		lines[line.Slot] = map[string]any{
			"account":     line.Account,
			"amount":      line.Amount,
			"cost_center": line.CostCtr,
			"tax_code":    line.TaxCode,
			"vendor_id":   line.VendorID,
		}
	}
	attachments := []map[string]any{}
	if vr.HasAttachment {
		attachments = append(attachments, map[string]any{
			"filename":    fmt.Sprintf("%s.pdf", vr.Key),
			"uploaded_at": time.Date(2025, 11, 3, 14, 5, 0, 0, time.UTC).Format(time.RFC3339),
		})
	}
	return datasetEntry{
		Key:        vr.Key,
		MinProfile: vr.MinProfile,
		Data: map[string]any{
			"voucher": map[string]any{
				"book":     vr.Book,
				"currency": vr.Currency,
				"header": map[string]any{
					"period":      vr.Period,
					"posted":      vr.Posted,
					"region":      vr.Region,
					"workflow":    vr.Workflow,
					"attachments": attachments,
				},
				"lines": lines,
			},
		},
	}
}

var voucherRecords = []voucherRecord{
	{Key: "voucher-ap-2025-1101", Book: "GENERAL", Period: "2025-11", Region: "EMEA", Currency: "EUR", Posted: false, Workflow: "awaiting_approval", HasAttachment: true, MinProfile: DatasetReduced, Lines: []voucherLine{{"10", "6001", 3500, "PAR-FIN", "FR-STD", "ACME-EU"}, {"20", "2010", -3500, "PAR-FIN", "FR-STD", "ACME-EU"}}},
	{Key: "voucher-ap-2025-1102", Book: "GENERAL", Period: "2025-11", Region: "EMEA", Currency: "EUR", Posted: true, Workflow: "posted", MinProfile: DatasetFull, Lines: []voucherLine{{"10", "6001", 1800, "MAD-SUP", "ES-RED", "IBERIA"}, {"20", "2100", -1800, "MAD-SUP", "ES-RED", "IBERIA"}}},
	{Key: "voucher-ap-2025-1103", Book: "GENERAL", Period: "2025-10", Region: "APAC", Currency: "JPY", Posted: true, Workflow: "posted", MinProfile: DatasetExtended, Lines: []voucherLine{{"10", "6010", 950, "TOK-RND", "JP-STD", "NIPTECH"}, {"20", "2990", -950, "TOK-RND", "JP-STD", "NIPTECH"}}},
	{Key: "voucher-us-2025-1103", Book: "US-LEDGER", Period: "2025-11", Region: "US", Currency: "USD", Posted: false, Workflow: "awaiting_post", HasAttachment: true, MinProfile: DatasetFull, Lines: []voucherLine{{"10", "4100", 5000, "NYC-SALES", "US-NY", "BIGBOX"}, {"20", "2400", -5000, "NYC-SALES", "US-NY", "BIGBOX"}, {"30", "1100", 2500, "NYC-SALES", "US-NY", "BIGBOX"}, {"40", "5100", -2500, "NYC-SALES", "US-NY", "BIGBOX"}}},
	{Key: "voucher-eu-2025-1104", Book: "EU-LEDGER", Period: "2025-11", Region: "EMEA", Currency: "EUR", Posted: true, Workflow: "posted", MinProfile: DatasetExtended, Lines: []voucherLine{{"10", "5500", 2200, "BER-OPS", "DE-LOW", "AUTO-GMBH"}, {"20", "3300", -2200, "BER-OPS", "DE-LOW", "AUTO-GMBH"}}},
	{Key: "voucher-eu-2025-1105", Book: "EU-LEDGER", Period: "2025-11", Region: "EMEA", Currency: "GBP", Posted: false, Workflow: "awaiting_review", MinProfile: DatasetExtended, Lines: []voucherLine{{"10", "6100", 1750, "LON-CAP", "UK-STD", "BRITNET"}, {"20", "3010", -1750, "LON-CAP", "UK-STD", "BRITNET"}, {"30", "6250", 220, "LON-CAP", "UK-RED", "BRITNET"}, {"40", "3010", -220, "LON-CAP", "UK-RED", "BRITNET"}}},
	{Key: "voucher-ap-2025-1201", Book: "GENERAL", Period: "2025-12", Region: "APAC", Currency: "AUD", Posted: false, Workflow: "awaiting_approval", MinProfile: DatasetExtended, Lines: []voucherLine{{"10", "6400", 4200, "SYD-OPS", "AU-STD", "KOALA"}, {"20", "2050", -4200, "SYD-OPS", "AU-STD", "KOALA"}}},
	{Key: "voucher-latam-2025-1102", Book: "LATAM-LEDGER", Period: "2025-11", Region: "LATAM", Currency: "BRL", Posted: true, Workflow: "posted", MinProfile: DatasetExtended, Lines: []voucherLine{{"10", "7100", 3100, "SAO-FIN", "BR-STD", "RIO-CONS"}, {"20", "2600", -3100, "SAO-FIN", "BR-STD", "RIO-CONS"}}},
	{Key: "voucher-bulk-00", Book: "GENERAL", Period: "2025-11", Region: "GLOBAL", Currency: "USD", Posted: true, Workflow: "posted", MinProfile: DatasetFull, Lines: []voucherLine{{"10", "6001", 1000, "OPS", "US-UT", "BULK-A"}, {"20", "2010", -1000, "OPS", "US-UT", "BULK-A"}}},
	{Key: "voucher-bulk-01", Book: "GENERAL", Period: "2025-11", Region: "GLOBAL", Currency: "USD", Posted: false, Workflow: "awaiting_post", MinProfile: DatasetFull, Lines: []voucherLine{{"10", "6001", 1100, "OPS", "US-UT", "BULK-B"}, {"20", "2010", -1100, "OPS", "US-UT", "BULK-B"}}},
	{Key: "voucher-bulk-02", Book: "US-LEDGER", Period: "2025-11", Region: "US", Currency: "USD", Posted: true, Workflow: "posted", MinProfile: DatasetFull, Lines: []voucherLine{{"10", "6200", 1300, "DEN-SVC", "US-CO", "BULK-C"}, {"20", "2100", -1300, "DEN-SVC", "US-CO", "BULK-C"}}},
	{Key: "voucher-bulk-03", Book: "GENERAL", Period: "2025-11", Region: "GLOBAL", Currency: "USD", Posted: false, Workflow: "awaiting_approval", MinProfile: DatasetFull, Lines: []voucherLine{{"10", "6300", 1250, "ROM-OPS", "IT-STD", "BULK-D"}, {"20", "3200", -1250, "ROM-OPS", "IT-STD", "BULK-D"}}},
	{Key: "voucher-bulk-04", Book: "GENERAL", Period: "2025-11", Region: "GLOBAL", Currency: "USD", Posted: true, Workflow: "posted", MinProfile: DatasetFull, Lines: []voucherLine{{"10", "6001", 1500, "OPS", "US-UT", "BULK-E"}, {"20", "2010", -1500, "OPS", "US-UT", "BULK-E"}}},
	{Key: "voucher-bulk-05", Book: "US-LEDGER", Period: "2025-11", Region: "US", Currency: "USD", Posted: false, Workflow: "awaiting_review", MinProfile: DatasetFull, Lines: []voucherLine{{"10", "6100", 1650, "DAL-OPS", "US-TX", "BULK-F"}, {"20", "2110", -1650, "DAL-OPS", "US-TX", "BULK-F"}}},
	{Key: "voucher-bulk-06", Book: "GENERAL", Period: "2025-11", Region: "GLOBAL", Currency: "USD", Posted: true, Workflow: "posted", MinProfile: DatasetExtended, Lines: []voucherLine{{"10", "6001", 1600, "OPS", "US-UT", "BULK-G"}, {"20", "2010", -1600, "OPS", "US-UT", "BULK-G"}}},
	{Key: "voucher-bulk-07", Book: "GENERAL", Period: "2025-11", Region: "GLOBAL", Currency: "USD", Posted: false, Workflow: "awaiting_post", MinProfile: DatasetExtended, Lines: []voucherLine{{"10", "6001", 1700, "OPS", "US-UT", "BULK-H"}, {"20", "2010", -1700, "OPS", "US-UT", "BULK-H"}}},
}

// firmware dataset -------------------------------------------------------------

type firmwareRecord struct {
	Key        string
	Channel    string
	Region     string
	Percent    int
	Battery    int
	Status     string
	Alerts     []string
	MinProfile DatasetProfile
}

func (fr firmwareRecord) allowed(profile DatasetProfile) bool {
	return profile >= fr.MinProfile
}

func (fr firmwareRecord) toDatasetEntry() datasetEntry {
	return datasetEntry{
		Key:        fr.Key,
		MinProfile: fr.MinProfile,
		Data: map[string]any{
			"device": map[string]any{
				"firmware": map[string]any{"channel": fr.Channel},
				"rollout": map[string]any{
					"progress": map[string]any{
						"percent": fr.Percent,
						"status":  fr.Status,
					},
					"alerts": fr.Alerts,
				},
				"telemetry": map[string]any{
					"battery_mv": fr.Battery,
				},
				"location": map[string]any{
					"region": fr.Region,
				},
			},
		},
	}
}

var firmwareRecords = []firmwareRecord{
	{Key: "device-gw-2048", Channel: "stable", Region: "us-east", Percent: 75, Battery: 3800, Status: "draining", Alerts: []string{"constraint"}, MinProfile: DatasetReduced},
	{Key: "device-gw-1024", Channel: "stable", Region: "us-west", Percent: 20, Battery: 4000, Status: "draining", Alerts: []string{"backoff"}, MinProfile: DatasetFull},
	{Key: "device-gw-512", Channel: "beta", Region: "us-central", Percent: 10, Battery: 4100, Status: "draining", Alerts: nil, MinProfile: DatasetFull},
	{Key: "device-gw-600", Channel: "stable", Region: "us-west", Percent: 35, Battery: 3500, Status: "paused", Alerts: []string{"low_battery"}, MinProfile: DatasetFull},
	{Key: "device-gw-700", Channel: "stable", Region: "us-south", Percent: 15, Battery: 3400, Status: "paused", Alerts: []string{"low_battery"}, MinProfile: DatasetFull},
	{Key: "device-gw-8192", Channel: "stable", Region: "us-central", Percent: 80, Battery: 3300, Status: "ready", Alerts: nil, MinProfile: DatasetFull},
	{Key: "device-gw-900", Channel: "canary", Region: "eu-west", Percent: 5, Battery: 3600, Status: "deploying", Alerts: []string{"unstable"}, MinProfile: DatasetExtended},
	{Key: "device-gw-1001", Channel: "stable", Region: "ap-south", Percent: 55, Battery: 3700, Status: "draining", Alerts: []string{"network"}, MinProfile: DatasetExtended},
	{Key: "device-gw-1500", Channel: "stable", Region: "ca-central", Percent: 62, Battery: 3650, Status: "draining", Alerts: []string{"thermal"}, MinProfile: DatasetExtended},
	{Key: "device-gw-1700", Channel: "stable", Region: "eu-north", Percent: 48, Battery: 3725, Status: "draining", Alerts: []string{"packet_loss"}, MinProfile: DatasetExtended},
	{Key: "device-gw-1800", Channel: "beta", Region: "us-east", Percent: 30, Battery: 3890, Status: "deploying", Alerts: []string{"slow_upgrade"}, MinProfile: DatasetExtended},
	{Key: "device-gw-1900", Channel: "stable", Region: "ap-northeast", Percent: 90, Battery: 3975, Status: "ready", Alerts: nil, MinProfile: DatasetExtended},
}

// salute dataset ---------------------------------------------------------------

type saluteRecord struct {
	Key         string
	Affiliation string
	Grid        string
	Status      string
	MinProfile  DatasetProfile
}

func (sr saluteRecord) allowed(profile DatasetProfile) bool {
	return profile >= sr.MinProfile
}

func (sr saluteRecord) toDatasetEntry() datasetEntry {
	return datasetEntry{
		Key:        sr.Key,
		MinProfile: sr.MinProfile,
		Data: map[string]any{
			"report": map[string]any{
				"type":        "salute",
				"affiliation": sr.Affiliation,
				"location": map[string]any{
					"grid": sr.Grid,
				},
				"status": sr.Status,
			},
		},
	}
}

var saluteRecords = []saluteRecord{
	{Key: "salute-report-opfor", Affiliation: "opfor", Grid: "42SXD1345", Status: "confirmed", MinProfile: DatasetReduced},
	{Key: "salute-report-friendly-south", Affiliation: "friendly", Grid: "42SY9981", Status: "tracking", MinProfile: DatasetFull},
	{Key: "salute-report-friendly-north", Affiliation: "friendly", Grid: "42SZ1020", Status: "tracking", MinProfile: DatasetExtended},
	{Key: "salute-report-opfor-deep", Affiliation: "opfor", Grid: "42SXD8888", Status: "confirmed", MinProfile: DatasetExtended},
	{Key: "salute-report-opfor-east", Affiliation: "opfor", Grid: "42SY1100", Status: "investigating", MinProfile: DatasetExtended},
	{Key: "salute-report-friendly-west", Affiliation: "friendly", Grid: "42RX2000", Status: "confirmed", MinProfile: DatasetExtended},
}

// flight dataset ---------------------------------------------------------------

type flightRecord struct {
	Key        string
	Mission    string
	Stage      string
	Apoapsis   int
	Guidance   string
	MinProfile DatasetProfile
}

func (fr flightRecord) allowed(profile DatasetProfile) bool {
	return profile >= fr.MinProfile
}

func (fr flightRecord) toDatasetEntry() datasetEntry {
	return datasetEntry{
		Key:        fr.Key,
		MinProfile: fr.MinProfile,
		Data: map[string]any{
			"vehicle": map[string]any{
				"mission": fr.Mission,
			},
			"telemetry": map[string]any{
				"stage":       fr.Stage,
				"apoapsis_km": fr.Apoapsis,
				"guidance": map[string]any{
					"mode": fr.Guidance,
				},
			},
		},
	}
}

var flightRecords = []flightRecord{
	{Key: "flight-boost-02", Mission: "delta-heavy", Stage: "boost", Apoapsis: 200, Guidance: "auto", MinProfile: DatasetFull},
	{Key: "flight-descent-01", Mission: "delta-heavy", Stage: "descent", Apoapsis: 80, Guidance: "manual", MinProfile: DatasetReduced},
	{Key: "flight-entry-01", Mission: "delta-heavy", Stage: "entry", Apoapsis: 60, Guidance: "manual", MinProfile: DatasetReduced},
	{Key: "flight-fairing-01", Mission: "delta-heavy", Stage: "fairing", Apoapsis: 40, Guidance: "manual", MinProfile: DatasetReduced},
	{Key: "flight-boost-01", Mission: "delta-heavy", Stage: "boost", Apoapsis: 140, Guidance: "auto", MinProfile: DatasetExtended},
	{Key: "flight-boost-03", Mission: "delta-light", Stage: "boost", Apoapsis: 170, Guidance: "auto", MinProfile: DatasetExtended},
	{Key: "flight-orbit-01", Mission: "delta-heavy", Stage: "orbit", Apoapsis: 420, Guidance: "auto", MinProfile: DatasetExtended},
	{Key: "flight-entry-02", Mission: "delta-light", Stage: "entry", Apoapsis: 55, Guidance: "manual", MinProfile: DatasetExtended},
}

// shared helpers ----------------------------------------------------------------

func seedEntries(ctx context.Context, t testing.TB, cli *client.Client, namespace string, entries []datasetEntry) {
	t.Helper()
	for _, entry := range entries {
		SeedState(ctx, t, cli, namespace, entry.Key, entry.Data)
	}
}

// ExpectKeySet compares unordered key slices and fails when they diverge.
func ExpectKeySet(t testing.TB, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("expected %d keys %v, got %d %v", len(want), want, len(got), got)
	}
	lookup := make(map[string]int, len(got))
	for _, key := range got {
		lookup[key]++
	}
	for _, key := range want {
		if lookup[key] == 0 {
			t.Fatalf("missing key %s in %v", key, got)
		}
		lookup[key]--
	}
	for key, count := range lookup {
		if count != 0 {
			t.Fatalf("unexpected duplicate %s in %v", key, got)
		}
	}
}
