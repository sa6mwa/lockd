package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestBaselinePresetCasesFullV1Disk(t *testing.T) {
	t.Parallel()

	cases, err := baselinePresetCases(defaultFullBaselinePreset, "disk")
	if err != nil {
		t.Fatalf("baselinePresetCases: %v", err)
	}
	wantOps := map[string]int{
		"attachments/4096":           10000,
		"attachments/65536":          4096,
		"attachments/1048576":        256,
		"attachments/8388608":        32,
		"attachments-public/8388608": 32,
		"lock/65536":                 8192,
		"lock/1048576":               512,
		"lock/8388608":               64,
	}
	gotOps := make(map[string]int)
	for _, tc := range cases {
		key := tc.Workload
		if tc.Size > 0 {
			key = key + "/" + itoa(tc.Size)
		}
		gotOps[key] = tc.Ops
	}
	for key, want := range wantOps {
		if gotOps[key] != want {
			t.Fatalf("%s ops=%d want=%d", key, gotOps[key], want)
		}
	}
}

func TestBaselinePresetCasesFullV1Minio(t *testing.T) {
	t.Parallel()

	cases, err := baselinePresetCases(defaultFullBaselinePreset, "minio")
	if err != nil {
		t.Fatalf("baselinePresetCases: %v", err)
	}
	wantOps := map[string]int{
		"attachments/4096":           256,
		"attachments/65536":          64,
		"attachments/1048576":        16,
		"attachments/8388608":        16,
		"attachments-public/4096":    256,
		"attachments-public/65536":   64,
		"attachments-public/1048576": 16,
		"attachments-public/8388608": 16,
		"lock/4096":                  256,
		"lock/65536":                 64,
		"lock/1048576":               16,
		"lock/8388608":               16,
		"public-read":                100,
		"query-index":                100,
	}
	gotOps := make(map[string]int)
	for _, tc := range cases {
		key := tc.Workload
		if tc.Size > 0 {
			key = key + "/" + itoa(tc.Size)
		}
		gotOps[key] = tc.Ops
	}
	for key, want := range wantOps {
		if gotOps[key] != want {
			t.Fatalf("%s ops=%d want=%d", key, gotOps[key], want)
		}
	}
}

func TestBaselineProfileForBackendFullAndFast(t *testing.T) {
	t.Parallel()

	diskProfile, err := baselineProfileForBackend(defaultFullBaselinePreset, "disk")
	if err != nil {
		t.Fatalf("baselineProfileForBackend disk: %v", err)
	}
	if diskProfile.Concurrency != 8 || diskProfile.Runs != 2 || diskProfile.Warmup != 1 {
		t.Fatalf("unexpected disk profile: %+v", diskProfile)
	}

	minioProfile, err := baselineProfileForBackend(defaultFullBaselinePreset, "minio")
	if err != nil {
		t.Fatalf("baselineProfileForBackend minio: %v", err)
	}
	if minioProfile.Concurrency != 4 || minioProfile.Runs != 2 || minioProfile.Warmup != 1 {
		t.Fatalf("unexpected minio profile: %+v", minioProfile)
	}

	fastProfile, err := baselineProfileForBackend(defaultFastBaselinePreset, "disk")
	if err != nil {
		t.Fatalf("baselineProfileForBackend fast disk: %v", err)
	}
	if fastProfile.Concurrency != 8 || fastProfile.Runs != 1 || fastProfile.Warmup != 1 {
		t.Fatalf("unexpected fast profile: %+v", fastProfile)
	}
}

func TestBaselinePresetCasesFastV1Disk(t *testing.T) {
	t.Parallel()

	cases, err := baselinePresetCases(defaultFastBaselinePreset, "disk")
	if err != nil {
		t.Fatalf("baselinePresetCases fast: %v", err)
	}
	wantOps := map[string]int{
		"attachments/4096":    4000,
		"attachments/1048576": 128,
		"lock/4096":           4000,
		"lock/1048576":        128,
		"public-read":         1000,
		"query-index":         500,
		"query-scan":          500,
		"xa-commit":           500,
		"xa-rollback":         500,
	}
	gotOps := make(map[string]int)
	for _, tc := range cases {
		key := tc.Workload
		if tc.Size > 0 {
			key = key + "/" + itoa(tc.Size)
		}
		gotOps[key] = tc.Ops
	}
	for key, want := range wantOps {
		if gotOps[key] != want {
			t.Fatalf("%s ops=%d want=%d", key, gotOps[key], want)
		}
	}
}

func TestParseBaselineBackendsDefaultAndValidation(t *testing.T) {
	t.Parallel()

	backends, err := parseBaselineBackends("")
	if err != nil {
		t.Fatalf("parseBaselineBackends default: %v", err)
	}
	if len(backends) != 2 || backends[0] != "disk" || backends[1] != "minio" {
		t.Fatalf("unexpected default backends: %#v", backends)
	}
	if _, err := parseBaselineBackends("disk,unknown"); err == nil {
		t.Fatal("expected unknown backend error")
	}
}

func TestFindLatestBaselineRecordAndGolden(t *testing.T) {
	t.Parallel()

	records := []baselineRunRecord{
		{RunID: "old-disk", Preset: defaultBaselinePreset, Backend: "disk"},
		{RunID: "old-disk-golden", Preset: defaultBaselinePreset, Backend: "disk", Golden: true},
		{RunID: "latest-minio", Preset: defaultBaselinePreset, Backend: "minio"},
		{RunID: "latest-disk", Preset: defaultBaselinePreset, Backend: "disk"},
	}
	last := findLatestBaselineRecord(records, defaultBaselinePreset, "disk", false)
	if last == nil || last.RunID != "latest-disk" {
		t.Fatalf("unexpected last record: %+v", last)
	}
	golden := findLatestBaselineRecord(records, defaultBaselinePreset, "disk", true)
	if golden == nil || golden.RunID != "old-disk-golden" {
		t.Fatalf("unexpected golden record: %+v", golden)
	}
}

func TestMarkBaselineGolden(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	history := filepath.Join(dir, "history.jsonl")
	records := []baselineRunRecord{
		{RunID: "run-a", Preset: defaultBaselinePreset, Backend: "disk"},
		{RunID: "run-b", Preset: defaultBaselinePreset, Backend: "minio"},
	}
	if err := rewriteBaselineHistory(history, records); err != nil {
		t.Fatalf("rewriteBaselineHistory: %v", err)
	}
	if err := markBaselineGolden(history, "run-b"); err != nil {
		t.Fatalf("markBaselineGolden: %v", err)
	}
	got, err := loadBaselineHistory(history)
	if err != nil {
		t.Fatalf("loadBaselineHistory: %v", err)
	}
	if len(got) != 2 || got[1].RunID != "run-b" || !got[1].Golden {
		t.Fatalf("unexpected history after golden mark: %+v", got)
	}
}

func TestMaybeAppendBaselineHistoryDisabled(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	historyPath := filepath.Join(dir, "history.jsonl")
	initial := []baselineRunRecord{{RunID: "run-a", Preset: defaultBaselinePreset, Backend: "disk"}}
	if err := rewriteBaselineHistory(historyPath, initial); err != nil {
		t.Fatalf("rewriteBaselineHistory: %v", err)
	}
	infoBefore, err := os.Stat(historyPath)
	if err != nil {
		t.Fatalf("Stat before: %v", err)
	}
	record := baselineRunRecord{RunID: "run-b", Preset: defaultBaselinePreset, Backend: "disk"}
	got, err := maybeAppendBaselineHistory(historyPath, append([]baselineRunRecord(nil), initial...), record, false)
	if err != nil {
		t.Fatalf("maybeAppendBaselineHistory: %v", err)
	}
	if len(got) != len(initial) {
		t.Fatalf("history len=%d want=%d", len(got), len(initial))
	}
	infoAfter, err := os.Stat(historyPath)
	if err != nil {
		t.Fatalf("Stat after: %v", err)
	}
	if !infoBefore.ModTime().Equal(infoAfter.ModTime()) || infoBefore.Size() != infoAfter.Size() {
		t.Fatal("history file changed despite appendHistory=false")
	}
}

func TestMaybeAppendBaselineHistoryEnabled(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	historyPath := filepath.Join(dir, "history.jsonl")
	initial := []baselineRunRecord{{RunID: "run-a", Preset: defaultBaselinePreset, Backend: "disk"}}
	if err := rewriteBaselineHistory(historyPath, initial); err != nil {
		t.Fatalf("rewriteBaselineHistory: %v", err)
	}
	record := baselineRunRecord{RunID: "run-b", Preset: defaultBaselinePreset, Backend: "disk"}
	got, err := maybeAppendBaselineHistory(historyPath, append([]baselineRunRecord(nil), initial...), record, true)
	if err != nil {
		t.Fatalf("maybeAppendBaselineHistory: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("history len=%d want=2", len(got))
	}
	if got[1].RunID != "run-b" {
		t.Fatalf("last run id=%q want=run-b", got[1].RunID)
	}
	onDisk, err := loadBaselineHistory(historyPath)
	if err != nil {
		t.Fatalf("loadBaselineHistory: %v", err)
	}
	if len(onDisk) != 2 || onDisk[1].RunID != "run-b" {
		t.Fatalf("unexpected on-disk history: %+v", onDisk)
	}
}

func TestRecordHistoryBranchFallsBackToBackend(t *testing.T) {
	t.Parallel()

	if got := recordHistoryBranch(baselineRunRecord{Backend: "disk"}); got != "disk" {
		t.Fatalf("fallback branch=%q want=disk", got)
	}
	if got := recordHistoryBranch(baselineRunRecord{HistoryBranch: "fast", Backend: "disk"}); got != "fast" {
		t.Fatalf("explicit branch=%q want=fast", got)
	}
}

func TestHistoryBranchForPreset(t *testing.T) {
	t.Parallel()

	if got := historyBranchForPreset(defaultFastBaselinePreset); got != "fast" {
		t.Fatalf("fast branch=%q want=fast", got)
	}
	if got := historyBranchForPreset(defaultFullBaselinePreset); got != "full" {
		t.Fatalf("full branch=%q want=full", got)
	}
}

func TestLatestAndPreviousBaselineRecords(t *testing.T) {
	t.Parallel()

	records := []baselineRunRecord{
		{RunID: "disk-a", Preset: defaultBaselinePreset, Backend: "disk"},
		{RunID: "minio-a", Preset: defaultBaselinePreset, Backend: "minio"},
		{RunID: "disk-b", Preset: defaultBaselinePreset, Backend: "disk"},
	}
	current, previous := latestAndPreviousBaselineRecords(records, defaultBaselinePreset, "disk")
	if current == nil || current.RunID != "disk-b" {
		t.Fatalf("current=%+v want=disk-b", current)
	}
	if previous == nil || previous.RunID != "disk-a" {
		t.Fatalf("previous=%+v want=disk-a", previous)
	}
}

func TestFreezeLatestBaselineRun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	runHistory := filepath.Join(dir, "run-history.jsonl")
	baselineHistory := filepath.Join(dir, "baseline-history.jsonl")
	records := []baselineRunRecord{
		{RunID: "disk-old", HistoryBranch: "full", Preset: defaultBaselinePreset, Backend: "disk"},
		{RunID: "disk-new", HistoryBranch: "full", Preset: defaultBaselinePreset, Backend: "disk"},
	}
	if err := rewriteBaselineHistory(runHistory, records); err != nil {
		t.Fatalf("rewrite runHistory: %v", err)
	}
	cfg := benchConfig{
		baselinePreset:     defaultBaselinePreset,
		baselineRunHistory: runHistory,
		baselineHistory:    baselineHistory,
	}
	if err := freezeLatestBaselineRun(cfg); err != nil {
		t.Fatalf("freezeLatestBaselineRun: %v", err)
	}
	got, err := loadBaselineHistory(baselineHistory)
	if err != nil {
		t.Fatalf("loadBaselineHistory: %v", err)
	}
	if len(got) != 1 || got[0].RunID != "disk-new" {
		t.Fatalf("unexpected frozen history: %+v", got)
	}
}

func TestPrintRecentBaselineRunHistoryReport(t *testing.T) {
	t.Parallel()

	history := []baselineRunRecord{
		{RunID: "disk-a", Timestamp: "2026-03-20T00:00:00Z", Preset: defaultBaselinePreset, Backend: "disk", Git: baselineGitInfo{ShortSHA: "aaaa111", Dirty: false}, Cases: []baselineCaseResult{{Workload: "query-index", Phases: map[string]baselineStatsSnapshot{"total": {OpsPerSec: 100}}}}},
		{RunID: "disk-b", Timestamp: "2026-03-21T00:00:00Z", Preset: defaultBaselinePreset, Backend: "disk", Git: baselineGitInfo{ShortSHA: "bbbb222", Dirty: true}, Cases: []baselineCaseResult{{Workload: "query-index", Phases: map[string]baselineStatsSnapshot{"total": {OpsPerSec: 200}}}}},
		{RunID: "disk-c", Timestamp: "2026-03-22T00:00:00Z", Preset: defaultBaselinePreset, Backend: "disk", Git: baselineGitInfo{ShortSHA: "cccc333", Dirty: false}, Cases: []baselineCaseResult{{Workload: "query-scan", Phases: map[string]baselineStatsSnapshot{"total": {OpsPerSec: 300}}}}},
	}
	origStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	os.Stdout = w
	printErr := printRecentBaselineRunHistoryReport(history, defaultBaselinePreset, "disk", 2)
	_ = w.Close()
	os.Stdout = origStdout
	if printErr != nil {
		t.Fatalf("printRecentBaselineRunHistoryReport: %v", printErr)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	output := buf.String()
	for _, want := range []string{
		"run: branch=disk timestamp=2026-03-21T00:00:00Z run_id=disk-b git=bbbb222-dirty",
		"query-index",
		"ops/s=200.0",
		"run: branch=disk timestamp=2026-03-22T00:00:00Z run_id=disk-c git=cccc333-clean",
		"query-scan",
		"ops/s=300.0",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
	if strings.Contains(output, "disk-a") {
		t.Fatalf("output should not include older runs beyond limit:\n%s", output)
	}
}

func TestGitDirtyStatusArgsExcludeDocs(t *testing.T) {
	t.Parallel()

	args := gitDirtyStatusArgs()
	want := []string{"status", "--porcelain", "--untracked-files=normal", "--", ".", ":(exclude)docs/"}
	if len(args) != len(want) {
		t.Fatalf("args len=%d want=%d (%v)", len(args), len(want), args)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("args[%d]=%q want=%q (%v)", i, args[i], want[i], args)
		}
	}
}

func TestValidateBaselineCaseResultRejectsErrors(t *testing.T) {
	t.Parallel()

	clean := baselineCaseResult{
		Workload: "attachments",
		Size:     65536,
		Phases: map[string]baselineStatsSnapshot{
			"total":  {Errors: 0},
			"attach": {Errors: 0},
		},
	}
	if err := validateBaselineCaseResult(clean); err != nil {
		t.Fatalf("clean case rejected: %v", err)
	}

	dirty := baselineCaseResult{
		Workload: "attachments",
		Size:     65536,
		Phases: map[string]baselineStatsSnapshot{
			"total":  {Errors: 1},
			"attach": {Errors: 1},
		},
	}
	if err := validateBaselineCaseResult(dirty); err == nil {
		t.Fatal("expected error-bearing case to be rejected")
	}
}

func itoa(v int) string {
	return strconv.Itoa(v)
}
