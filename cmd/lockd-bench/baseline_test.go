package main

import (
	"path/filepath"
	"strconv"
	"testing"
)

func TestBaselinePresetCasesEmbeddedV1Disk(t *testing.T) {
	t.Parallel()

	cases, err := baselinePresetCases(defaultBaselinePreset, "disk")
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

func TestBaselinePresetCasesEmbeddedV1Minio(t *testing.T) {
	t.Parallel()

	cases, err := baselinePresetCases(defaultBaselinePreset, "minio")
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

func TestBaselineProfileForBackendEmbeddedV1(t *testing.T) {
	t.Parallel()

	diskProfile, err := baselineProfileForBackend(defaultBaselinePreset, "disk")
	if err != nil {
		t.Fatalf("baselineProfileForBackend disk: %v", err)
	}
	if diskProfile.Concurrency != 8 || diskProfile.Runs != 2 || diskProfile.Warmup != 1 {
		t.Fatalf("unexpected disk profile: %+v", diskProfile)
	}

	minioProfile, err := baselineProfileForBackend(defaultBaselinePreset, "minio")
	if err != nil {
		t.Fatalf("baselineProfileForBackend minio: %v", err)
	}
	if minioProfile.Concurrency != 4 || minioProfile.Runs != 2 || minioProfile.Warmup != 1 {
		t.Fatalf("unexpected minio profile: %+v", minioProfile)
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
