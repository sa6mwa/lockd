package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	defaultFullBaselinePreset     = "full-v1"
	defaultFastBaselinePreset     = "fast-v1"
	defaultBaselinePreset         = defaultFullBaselinePreset
	defaultBaselineHistoryPath    = "docs/performance/lockd-bench-full-baseline-history.jsonl"
	defaultBaselineRunHistoryPath = "docs/performance/lockd-bench-full-run-history.jsonl"
)

var (
	baselineStatsLineRE = regexp.MustCompile(`^([^:]+): ops=(\d+) ops/s=([0-9.]+) avg=([^ ]+) p50=([^ ]+) p90=([^ ]+) p95=([^ ]+) p99=([^ ]+) p99\.9=([^ ]+) min=([^ ]+) max=([^ ]+) errors=(\d+)$`)
)

type baselineCasePlan struct {
	Workload        string
	Size            int
	Ops             int
	PayloadBytes    int
	AttachmentBytes int
}

type baselineBackendProfile struct {
	Concurrency           int
	Runs                  int
	Warmup                int
	AttachmentTargetBytes int
	LockTargetBytes       int
	MinOps                int
	MaxOps                int
	FixedOps              int
}

type baselineGitInfo struct {
	ShortSHA string `json:"short_sha"`
	Dirty    bool   `json:"dirty"`
}

type baselineRunConfig struct {
	HAMode      string `json:"ha_mode"`
	Concurrency int    `json:"concurrency"`
	Runs        int    `json:"runs"`
	Warmup      int    `json:"warmup"`
	QueryReturn string `json:"query_return,omitempty"`
	QRFDisabled bool   `json:"qrf_disabled,omitempty"`
}

type baselineStatsSnapshot struct {
	Ops       int     `json:"ops"`
	OpsPerSec float64 `json:"ops_per_sec"`
	AvgNs     int64   `json:"avg_ns"`
	MinNs     int64   `json:"min_ns"`
	MaxNs     int64   `json:"max_ns"`
	P50Ns     int64   `json:"p50_ns"`
	P90Ns     int64   `json:"p90_ns"`
	P95Ns     int64   `json:"p95_ns"`
	P99Ns     int64   `json:"p99_ns"`
	P999Ns    int64   `json:"p999_ns"`
	Errors    int64   `json:"errors"`
}

type baselineCaseResult struct {
	Workload        string                           `json:"workload"`
	Size            int                              `json:"size,omitempty"`
	Ops             int                              `json:"ops"`
	PayloadBytes    int                              `json:"payload_bytes,omitempty"`
	AttachmentBytes int                              `json:"attachment_bytes,omitempty"`
	Phases          map[string]baselineStatsSnapshot `json:"phases"`
}

type baselineRunRecord struct {
	RunID         string               `json:"run_id"`
	Timestamp     string               `json:"timestamp"`
	HistoryBranch string               `json:"history_branch,omitempty"`
	Preset        string               `json:"preset"`
	Backend       string               `json:"backend"`
	Store         string               `json:"store,omitempty"`
	DiskRoot      string               `json:"disk_root,omitempty"`
	Git           baselineGitInfo      `json:"git"`
	Golden        bool                 `json:"golden,omitempty"`
	Config        baselineRunConfig    `json:"config"`
	Cases         []baselineCaseResult `json:"cases"`
}

func defaultBaselineBackends() []string {
	return []string{"disk", "minio"}
}

func supportedBaselineBackends() []string {
	return []string{"disk", "mem", "minio", "aws", "azure"}
}

func historyBranchForPreset(preset string) string {
	switch strings.TrimSpace(strings.ToLower(preset)) {
	case defaultFastBaselinePreset:
		return "fast"
	case defaultFullBaselinePreset:
		return "full"
	default:
		return strings.TrimSpace(strings.ToLower(preset))
	}
}

func runBaselineMode(cfg benchConfig) error {
	backends, err := parseBaselineBackends(cfg.baselineBackends)
	if err != nil {
		return err
	}
	if len(backends) > 1 && (strings.TrimSpace(cfg.envFile) != "" || strings.TrimSpace(cfg.storeURL) != "") {
		return fmt.Errorf("-env-file and -store are only supported with a single baseline backend")
	}
	history, err := loadBaselineHistory(cfg.baselineHistory)
	if err != nil {
		return err
	}
	runHistory, err := loadBaselineHistory(cfg.baselineRunHistory)
	if err != nil {
		return err
	}
	gitInfo := detectBaselineGitInfo()
	runTS := time.Now().UTC().Format("20060102T150405Z")

	for _, backend := range backends {
		profile, err := baselineProfileForBackend(cfg.baselinePreset, backend)
		if err != nil {
			return err
		}
		presetCases, err := baselinePresetCases(cfg.baselinePreset, backend)
		if err != nil {
			return err
		}
		runID := fmt.Sprintf("%s-%s-%s-%s-%s", runTS, gitInfo.ShortSHA, dirtySuffix(gitInfo.Dirty), historyBranchForPreset(cfg.baselinePreset), backend)
		record, err := executeBaselineBackend(cfg, backend, runID, gitInfo, profile, presetCases)
		if err != nil {
			return err
		}
		last := findLatestBaselineRecord(history, record.Preset, record.Backend, false)
		golden := findLatestBaselineRecord(history, record.Preset, record.Backend, true)
		printBaselineComparisons(record, last, golden)
		history, err = maybeAppendBaselineHistory(cfg.baselineHistory, history, record, cfg.baselineAppendHistory)
		if err != nil {
			return err
		}
		runHistory, err = maybeAppendBaselineHistory(cfg.baselineRunHistory, runHistory, record, cfg.baselineAppendRunHistory)
		if err != nil {
			return err
		}
	}
	return nil
}

func runBaselineReport(cfg benchConfig) error {
	history, err := loadBaselineHistory(cfg.baselineHistory)
	if err != nil {
		return err
	}
	fmt.Printf("frozen %s baseline history: %s\n", historyBranchForPreset(cfg.baselinePreset), cfg.baselineHistory)
	return printLatestBaselineHistoryReport(history, cfg.baselinePreset, "disk")
}

func runBaselineRunReport(cfg benchConfig) error {
	history, err := loadBaselineHistory(cfg.baselineRunHistory)
	if err != nil {
		return err
	}
	fmt.Printf("%s run history: %s\n", historyBranchForPreset(cfg.baselinePreset), cfg.baselineRunHistory)
	if err := printRecentBaselineRunHistoryReport(history, cfg.baselinePreset, historyBranchForPreset(cfg.baselinePreset), cfg.baselineReportLimit); err != nil {
		if strings.HasPrefix(err.Error(), "no ") {
			fmt.Println(err.Error())
			return nil
		}
		return err
	}
	return nil
}

func printLatestBaselineHistoryReport(history []baselineRunRecord, preset, backend string) error {
	current, previous := latestAndPreviousBaselineRecords(history, preset, backend)
	if current == nil {
		return fmt.Errorf("no %s baseline records found in history", backend)
	}
	fmt.Printf("baseline run: branch=%s preset=%s backend=%s run_id=%s git=%s-%s\n",
		recordHistoryBranch(*current),
		current.Preset,
		current.Backend,
		current.RunID,
		current.Git.ShortSHA,
		dirtySuffix(current.Git.Dirty),
	)
	for _, currentCase := range current.Cases {
		currentTotal, ok := currentCase.Phases["total"]
		if !ok {
			continue
		}
		label := currentCase.Workload
		if currentCase.Size > 0 {
			label = fmt.Sprintf("%s/%d", currentCase.Workload, currentCase.Size)
		}
		lastText := "n/a"
		if lastStats, ok := matchingBaselineStats(previous, currentCase); ok {
			lastText = fmt.Sprintf("%.1f (%s)", lastStats.OpsPerSec, percentDeltaText(currentTotal.OpsPerSec, lastStats.OpsPerSec))
		}
		fmt.Printf("  %-28s current=%.1f last=%s\n", label, currentTotal.OpsPerSec, lastText)
	}
	return nil
}

func printRecentBaselineRunHistoryReport(history []baselineRunRecord, preset, branch string, limit int) error {
	if limit <= 0 {
		limit = 3
	}
	filtered := make([]baselineRunRecord, 0, len(history))
	for _, record := range history {
		if record.Preset == preset && matchesHistoryBranch(record, branch) {
			filtered = append(filtered, record)
		}
	}
	if len(filtered) == 0 {
		return fmt.Errorf("no %s run records found in history", branch)
	}
	start := 0
	if len(filtered) > limit {
		start = len(filtered) - limit
	}
	for _, record := range filtered[start:] {
		fmt.Printf("run: branch=%s timestamp=%s run_id=%s git=%s-%s\n",
			recordHistoryBranch(record),
			record.Timestamp,
			record.RunID,
			record.Git.ShortSHA,
			dirtySuffix(record.Git.Dirty),
		)
		for _, currentCase := range record.Cases {
			currentTotal, ok := currentCase.Phases["total"]
			if !ok {
				continue
			}
			label := currentCase.Workload
			if currentCase.Size > 0 {
				label = fmt.Sprintf("%s/%d", currentCase.Workload, currentCase.Size)
			}
			fmt.Printf("  %-28s ops/s=%.1f\n", label, currentTotal.OpsPerSec)
		}
	}
	return nil
}

func freezeLatestBaselineRun(cfg benchConfig) error {
	record, err := loadLatestRunRecord(cfg.baselineRunHistory, historyBranchForPreset(cfg.baselinePreset), cfg.baselinePreset)
	if err != nil {
		return err
	}
	history, err := loadBaselineHistory(cfg.baselineHistory)
	if err != nil {
		return err
	}
	for _, existing := range history {
		if existing.RunID == record.RunID {
			fmt.Printf("disk baseline already frozen: run_id=%s history=%s\n", record.RunID, cfg.baselineHistory)
			return nil
		}
	}
	if err := appendBaselineHistory(cfg.baselineHistory, record); err != nil {
		return err
	}
	fmt.Printf("disk baseline frozen from latest run-history: run_id=%s history=%s\n", record.RunID, cfg.baselineHistory)
	return nil
}

func maybeAppendBaselineHistory(historyPath string, history []baselineRunRecord, record baselineRunRecord, appendHistory bool) ([]baselineRunRecord, error) {
	if !appendHistory {
		return history, nil
	}
	if err := appendBaselineHistory(historyPath, record); err != nil {
		return history, err
	}
	return append(history, record), nil
}

func markBaselineGolden(historyPath, runID string) error {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("-baseline-run-id is required")
	}
	records, err := loadBaselineHistory(historyPath)
	if err != nil {
		return err
	}
	found := false
	for i := range records {
		if records[i].RunID == runID {
			records[i].Golden = true
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("run id %q not found in %s", runID, historyPath)
	}
	if err := rewriteBaselineHistory(historyPath, records); err != nil {
		return err
	}
	fmt.Printf("baseline golden: run_id=%s history=%s\n", runID, historyPath)
	return nil
}

func parseBaselineBackends(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return slices.Clone(defaultBaselineBackends()), nil
	}
	parts := strings.Split(raw, ",")
	backends := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	supported := supportedBaselineBackends()
	for _, part := range parts {
		backend := strings.ToLower(strings.TrimSpace(part))
		if backend == "" {
			continue
		}
		if !slices.Contains(supported, backend) {
			return nil, fmt.Errorf("unsupported baseline backend %q", backend)
		}
		if _, ok := seen[backend]; ok {
			continue
		}
		seen[backend] = struct{}{}
		backends = append(backends, backend)
	}
	if len(backends) == 0 {
		return nil, fmt.Errorf("no baseline backends selected")
	}
	return backends, nil
}

func baselineProfileForBackend(preset, backend string) (baselineBackendProfile, error) {
	switch strings.TrimSpace(strings.ToLower(preset)) {
	case defaultFullBaselinePreset:
		switch strings.TrimSpace(strings.ToLower(backend)) {
		case "disk", "mem":
			return baselineBackendProfile{
				Concurrency:           8,
				Runs:                  2,
				Warmup:                1,
				AttachmentTargetBytes: 512 * 1024 * 1024,
				LockTargetBytes:       512 * 1024 * 1024,
				MinOps:                32,
				MaxOps:                10000,
				FixedOps:              1000,
			}, nil
		case "minio", "aws", "azure":
			return baselineBackendProfile{
				Concurrency:           4,
				Runs:                  2,
				Warmup:                1,
				AttachmentTargetBytes: 8 * 1024 * 1024,
				LockTargetBytes:       4 * 1024 * 1024,
				MinOps:                16,
				MaxOps:                256,
				FixedOps:              100,
			}, nil
		default:
			return baselineBackendProfile{}, fmt.Errorf("unsupported baseline backend %q", backend)
		}
	case defaultFastBaselinePreset:
		switch strings.TrimSpace(strings.ToLower(backend)) {
		case "disk":
			return baselineBackendProfile{
				Concurrency:           8,
				Runs:                  1,
				Warmup:                1,
				AttachmentTargetBytes: 0,
				LockTargetBytes:       0,
				MinOps:                0,
				MaxOps:                0,
				FixedOps:              500,
			}, nil
		default:
			return baselineBackendProfile{}, fmt.Errorf("unsupported fast baseline backend %q", backend)
		}
	default:
		return baselineBackendProfile{}, fmt.Errorf("unsupported baseline preset %q", preset)
	}
}

func baselinePresetCases(preset, backend string) ([]baselineCasePlan, error) {
	switch strings.TrimSpace(strings.ToLower(preset)) {
	case defaultFullBaselinePreset:
		profile, err := baselineProfileForBackend(preset, backend)
		if err != nil {
			return nil, err
		}
		var cases []baselineCasePlan
		for _, workload := range []string{"attachments", "attachments-public", "lock"} {
			for _, size := range []int{4096, 65536, 1048576, 8388608} {
				cases = append(cases, baselineCasePlan{
					Workload:        workload,
					Size:            size,
					Ops:             baselineOpsForCase(profile, workload, size),
					PayloadBytes:    size,
					AttachmentBytes: attachmentBytesForWorkload(workload, size),
				})
			}
		}
		for _, workload := range []string{"public-read", "query-index", "query-scan", "xa-commit", "xa-rollback"} {
			cases = append(cases, baselineCasePlan{Workload: workload, Ops: profile.FixedOps})
		}
		return cases, nil
	case defaultFastBaselinePreset:
		if strings.TrimSpace(strings.ToLower(backend)) != "disk" {
			return nil, fmt.Errorf("unsupported fast baseline backend %q", backend)
		}
		return []baselineCasePlan{
			{Workload: "attachments", Size: 4096, Ops: 4000, PayloadBytes: 4096, AttachmentBytes: 4096},
			{Workload: "attachments", Size: 1048576, Ops: 128, PayloadBytes: 1048576, AttachmentBytes: 1048576},
			{Workload: "lock", Size: 4096, Ops: 4000, PayloadBytes: 4096},
			{Workload: "lock", Size: 1048576, Ops: 128, PayloadBytes: 1048576},
			{Workload: "public-read", Ops: 1000},
			{Workload: "query-index", Ops: 500},
			{Workload: "query-scan", Ops: 500},
			{Workload: "xa-commit", Ops: 500},
			{Workload: "xa-rollback", Ops: 500},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported baseline preset %q", preset)
	}
}

func baselineOpsForCase(profile baselineBackendProfile, workload string, size int) int {
	targetBytes := 0
	multiplier := 1
	switch workload {
	case "attachments", "attachments-public":
		targetBytes = profile.AttachmentTargetBytes
		multiplier = 2
	case "lock":
		targetBytes = profile.LockTargetBytes
	default:
		return profile.FixedOps
	}
	ops := targetBytes / (size * multiplier)
	if ops < profile.MinOps {
		ops = profile.MinOps
	}
	if ops > profile.MaxOps {
		ops = profile.MaxOps
	}
	return ops
}

func attachmentBytesForWorkload(workload string, size int) int {
	switch workload {
	case "attachments", "attachments-public":
		return size
	default:
		return 0
	}
}

func executeBaselineBackend(cfg benchConfig, backend, runID string, gitInfo baselineGitInfo, profile baselineBackendProfile, cases []baselineCasePlan) (baselineRunRecord, error) {
	record := baselineRunRecord{
		RunID:         runID,
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		HistoryBranch: historyBranchForPreset(cfg.baselinePreset),
		Preset:        cfg.baselinePreset,
		Backend:       backend,
		Store:         effectiveStoreURL(cfg),
		Git:           gitInfo,
		Config: baselineRunConfig{
			HAMode:      cfg.haMode,
			Concurrency: profile.Concurrency,
			Runs:        profile.Runs,
			Warmup:      profile.Warmup,
			QueryReturn: cfg.queryReturn,
			QRFDisabled: cfg.qrfDisabled,
		},
		Cases: make([]baselineCaseResult, 0, len(cases)),
	}
	for _, plan := range cases {
		fmt.Printf("baseline case: backend=%s workload=%s", backend, plan.Workload)
		if plan.Size > 0 {
			fmt.Printf(" size=%d", plan.Size)
		}
		fmt.Printf(" ops=%d\n", plan.Ops)
		out, err := runBaselineChild(cfg, backend, profile, plan)
		if err != nil {
			return baselineRunRecord{}, err
		}
		fmt.Print(string(out))
		caseResult, err := parseBaselineCaseResult(plan, out)
		if err != nil {
			return baselineRunRecord{}, fmt.Errorf("parse baseline output backend=%s workload=%s size=%d: %w", backend, plan.Workload, plan.Size, err)
		}
		if err := validateBaselineCaseResult(caseResult); err != nil {
			return baselineRunRecord{}, fmt.Errorf("baseline case backend=%s workload=%s size=%d: %w", backend, plan.Workload, plan.Size, err)
		}
		if record.DiskRoot == "" {
			record.DiskRoot = extractBenchDiskRoot(out)
		}
		record.Cases = append(record.Cases, caseResult)
	}
	return record, nil
}

func runBaselineChild(cfg benchConfig, backend string, profile baselineBackendProfile, plan baselineCasePlan) ([]byte, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	args := []string{
		"-backend", backend,
		"-ha", cfg.haMode,
		"-workload", plan.Workload,
		"-ops", strconv.Itoa(plan.Ops),
		"-runs", strconv.Itoa(profile.Runs),
		"-warmup", strconv.Itoa(profile.Warmup),
		"-concurrency", strconv.Itoa(profile.Concurrency),
		"-log-level", cfg.logLevel,
		"-query-return", cfg.queryReturn,
		"-query-limit", strconv.Itoa(cfg.queryLimit),
		"-query-doc-prefetch", strconv.Itoa(cfg.queryDocPrefetch),
		"-http-timeout", cfg.httpTimeout.String(),
	}
	if plan.PayloadBytes > 0 {
		args = append(args, "-payload-bytes", strconv.Itoa(plan.PayloadBytes))
	}
	if plan.AttachmentBytes > 0 {
		args = append(args, "-attachment-bytes", strconv.Itoa(plan.AttachmentBytes))
	}
	if cfg.qrfDisabled {
		args = append(args, "-qrf-disabled")
	}
	if strings.TrimSpace(cfg.envFile) != "" {
		args = append(args, "-env-file", cfg.envFile)
	}
	if strings.TrimSpace(cfg.storeURL) != "" {
		args = append(args, "-store", cfg.storeURL)
	}
	cmd := exec.Command(exe, args...)
	cmd.Env = os.Environ()
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return out.Bytes(), fmt.Errorf("backend=%s workload=%s size=%d: %w", backend, plan.Workload, plan.Size, err)
	}
	return out.Bytes(), nil
}

func parseBaselineCaseResult(plan baselineCasePlan, output []byte) (baselineCaseResult, error) {
	phases := make(map[string]baselineStatsSnapshot)
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		match := baselineStatsLineRE.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		stats, err := baselineStatsFromMatch(match)
		if err != nil {
			return baselineCaseResult{}, err
		}
		phases[match[1]] = stats
	}
	if err := scanner.Err(); err != nil {
		return baselineCaseResult{}, err
	}
	if len(phases) == 0 {
		return baselineCaseResult{}, errors.New("no benchmark stats found in output")
	}
	return baselineCaseResult{
		Workload:        plan.Workload,
		Size:            plan.Size,
		Ops:             plan.Ops,
		PayloadBytes:    plan.PayloadBytes,
		AttachmentBytes: plan.AttachmentBytes,
		Phases:          phases,
	}, nil
}

func baselineStatsFromMatch(match []string) (baselineStatsSnapshot, error) {
	parseDuration := func(raw string) (int64, error) {
		dur, err := time.ParseDuration(raw)
		if err != nil {
			return 0, err
		}
		return int64(dur), nil
	}
	ops, err := strconv.Atoi(match[2])
	if err != nil {
		return baselineStatsSnapshot{}, err
	}
	opsPerSec, err := strconv.ParseFloat(match[3], 64)
	if err != nil {
		return baselineStatsSnapshot{}, err
	}
	avgNs, err := parseDuration(match[4])
	if err != nil {
		return baselineStatsSnapshot{}, err
	}
	p50Ns, err := parseDuration(match[5])
	if err != nil {
		return baselineStatsSnapshot{}, err
	}
	p90Ns, err := parseDuration(match[6])
	if err != nil {
		return baselineStatsSnapshot{}, err
	}
	p95Ns, err := parseDuration(match[7])
	if err != nil {
		return baselineStatsSnapshot{}, err
	}
	p99Ns, err := parseDuration(match[8])
	if err != nil {
		return baselineStatsSnapshot{}, err
	}
	p999Ns, err := parseDuration(match[9])
	if err != nil {
		return baselineStatsSnapshot{}, err
	}
	minNs, err := parseDuration(match[10])
	if err != nil {
		return baselineStatsSnapshot{}, err
	}
	maxNs, err := parseDuration(match[11])
	if err != nil {
		return baselineStatsSnapshot{}, err
	}
	errs, err := strconv.ParseInt(match[12], 10, 64)
	if err != nil {
		return baselineStatsSnapshot{}, err
	}
	return baselineStatsSnapshot{
		Ops:       ops,
		OpsPerSec: opsPerSec,
		AvgNs:     avgNs,
		MinNs:     minNs,
		MaxNs:     maxNs,
		P50Ns:     p50Ns,
		P90Ns:     p90Ns,
		P95Ns:     p95Ns,
		P99Ns:     p99Ns,
		P999Ns:    p999Ns,
		Errors:    errs,
	}, nil
}

func printBaselineComparisons(current baselineRunRecord, last, golden *baselineRunRecord) {
	fmt.Printf("baseline run: preset=%s backend=%s run_id=%s git=%s-%s\n",
		current.Preset,
		current.Backend,
		current.RunID,
		current.Git.ShortSHA,
		dirtySuffix(current.Git.Dirty),
	)
	for _, currentCase := range current.Cases {
		currentTotal, ok := currentCase.Phases["total"]
		if !ok {
			continue
		}
		label := currentCase.Workload
		if currentCase.Size > 0 {
			label = fmt.Sprintf("%s/%d", currentCase.Workload, currentCase.Size)
		}
		lastText := "n/a"
		if lastStats, ok := matchingBaselineStats(last, currentCase); ok {
			lastText = fmt.Sprintf("%.1f (%s)", lastStats.OpsPerSec, percentDeltaText(currentTotal.OpsPerSec, lastStats.OpsPerSec))
		}
		goldenText := "n/a"
		if goldenStats, ok := matchingBaselineStats(golden, currentCase); ok {
			goldenText = fmt.Sprintf("%.1f (%s)", goldenStats.OpsPerSec, percentDeltaText(currentTotal.OpsPerSec, goldenStats.OpsPerSec))
		}
		fmt.Printf("  %-28s current=%.1f last=%s golden=%s\n", label, currentTotal.OpsPerSec, lastText, goldenText)
	}
}

func matchingBaselineStats(record *baselineRunRecord, currentCase baselineCaseResult) (baselineStatsSnapshot, bool) {
	if record == nil {
		return baselineStatsSnapshot{}, false
	}
	for _, candidate := range record.Cases {
		if candidate.Workload != currentCase.Workload || candidate.Size != currentCase.Size {
			continue
		}
		stats, ok := candidate.Phases["total"]
		return stats, ok
	}
	return baselineStatsSnapshot{}, false
}

func percentDeltaText(current, previous float64) string {
	if previous == 0 {
		return "n/a"
	}
	delta := ((current - previous) / previous) * 100
	return fmt.Sprintf("%+.1f%%", delta)
}

func validateBaselineCaseResult(result baselineCaseResult) error {
	for phase, stats := range result.Phases {
		if stats.Errors == 0 {
			continue
		}
		return fmt.Errorf("phase %s recorded %d errors", phase, stats.Errors)
	}
	return nil
}

func findLatestBaselineRecord(records []baselineRunRecord, preset, backend string, goldenOnly bool) *baselineRunRecord {
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if record.Preset != preset || record.Backend != backend {
			continue
		}
		if goldenOnly && !record.Golden {
			continue
		}
		return &records[i]
	}
	return nil
}

func latestAndPreviousBaselineRecords(records []baselineRunRecord, preset, backend string) (*baselineRunRecord, *baselineRunRecord) {
	var current *baselineRunRecord
	for i := len(records) - 1; i >= 0; i-- {
		record := &records[i]
		if record.Preset != preset || record.Backend != backend {
			continue
		}
		if current == nil {
			current = record
			continue
		}
		return current, record
	}
	return current, nil
}

func findBaselineCase(cases []baselineCaseResult, workload string, size int) (baselineCaseResult, bool) {
	for _, candidate := range cases {
		if candidate.Workload == workload && candidate.Size == size {
			return candidate, true
		}
	}
	return baselineCaseResult{}, false
}

func recordHistoryBranch(record baselineRunRecord) string {
	if strings.TrimSpace(record.HistoryBranch) != "" {
		return record.HistoryBranch
	}
	return record.Backend
}

func matchesHistoryBranch(record baselineRunRecord, branch string) bool {
	actual := recordHistoryBranch(record)
	if actual == branch {
		return true
	}
	return branch == "full" && strings.TrimSpace(record.HistoryBranch) == "" && record.Backend == "disk"
}

func effectiveStoreURL(cfg benchConfig) string {
	if strings.TrimSpace(cfg.storeURL) != "" {
		return strings.TrimSpace(cfg.storeURL)
	}
	return strings.TrimSpace(os.Getenv("LOCKD_STORE"))
}

func extractBenchDiskRoot(output []byte) string {
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "bench disk root: ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "bench disk root: "))
		}
	}
	return ""
}

func loadLatestRunRecord(path, branch, preset string) (baselineRunRecord, error) {
	records, err := loadBaselineHistory(path)
	if err != nil {
		return baselineRunRecord{}, err
	}
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if record.Preset != preset {
			continue
		}
		if !matchesHistoryBranch(record, branch) {
			continue
		}
		return record, nil
	}
	return baselineRunRecord{}, fmt.Errorf("no %s run records found in %s", branch, path)
}

func loadBaselineHistory(path string) ([]baselineRunRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var records []baselineRunRecord
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record baselineRunRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, fmt.Errorf("history decode: %w", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func appendBaselineHistory(path string, record baselineRunRecord) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	return nil
}

func rewriteBaselineHistory(path string, records []baselineRunRecord) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	for _, record := range records {
		data, err := json.Marshal(record)
		if err != nil {
			_ = f.Close()
			return err
		}
		if _, err := f.Write(append(data, '\n')); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func detectBaselineGitInfo() baselineGitInfo {
	info := baselineGitInfo{ShortSHA: "nogit", Dirty: true}
	sha, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err == nil {
		info.ShortSHA = strings.TrimSpace(string(sha))
	}
	status, err := exec.Command("git", gitDirtyStatusArgs()...).Output()
	if err == nil {
		info.Dirty = len(bytes.TrimSpace(status)) > 0
	}
	return info
}

func gitDirtyStatusArgs() []string {
	return []string{"status", "--porcelain", "--untracked-files=normal", "--", ".", ":(exclude)docs/"}
}

func dirtySuffix(dirty bool) string {
	if dirty {
		return "dirty"
	}
	return "clean"
}
