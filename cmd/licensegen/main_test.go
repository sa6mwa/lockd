package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIsLicenseFile(t *testing.T) {
	t.Parallel()

	cases := map[string]bool{
		"LICENSE":         true,
		"license.txt":     true,
		"licence.md":      true,
		"NOTICE":          true,
		"license-apache":  true,
		"random.txt":      false,
		"README":          false,
		"copying":         true,
		"copying.TXT":     true,
		"doc-license.md":  false,
		"license-addon":   true,
		"notlicensefile":  false,
		"licence-notes":   true,
		"somethingNOTICE": false,
	}
	for name, want := range cases {
		got := isLicenseFile(name)
		if got != want {
			t.Fatalf("isLicenseFile(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestFindLicenseFilesReturnsSortedMatches(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	files := []string{"README.md", "license", "LICENSE", "notice.txt", "doc.txt"}
	for _, name := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("content"), 0o600); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
	}
	got := findLicenseFiles(dir)
	want := []string{"LICENSE", "license", "notice.txt"}
	if len(got) != len(want) {
		t.Fatalf("findLicenseFiles len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i, name := range want {
		if got[i] != name {
			t.Fatalf("findLicenseFiles[%d] = %q, want %q (full: %v)", i, got[i], name, got)
		}
	}
}

func TestEffectiveDirPrefersReplacement(t *testing.T) {
	t.Parallel()

	replacement := module{Dir: "/tmp/replace"}
	m := module{Dir: "/tmp/original", Replace: &replacement}
	if got := effectiveDir(m); got != "/tmp/replace" {
		t.Fatalf("effectiveDir returned %q, want /tmp/replace", got)
	}

	m = module{Dir: "/tmp/original"}
	if got := effectiveDir(m); got != "/tmp/original" {
		t.Fatalf("effectiveDir returned %q, want /tmp/original", got)
	}
}

func TestNormalizeNewlines(t *testing.T) {
	t.Parallel()

	in := "line1\r\nline2\r\n"
	want := "line1\nline2\n"
	if got := string(normalizeNewlines([]byte(in))); got != want {
		t.Fatalf("normalizeNewlines = %q, want %q", got, want)
	}
}

func TestWriteReportProducesSummary(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "licenses.md")
	blobs := []licenseBlob{
		{
			ModulePath: "example.com/mod",
			Version:    "v1.2.3",
			Files:      []string{"LICENSE"},
			Content: map[string][]byte{
				"LICENSE": []byte("Example License\nLine2\n"),
			},
			Indirect: true,
		},
	}
	if err := writeReport(path, blobs); err != nil {
		t.Fatalf("writeReport: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	text := string(data)
	fragments := []string{
		"# Third-Party Licenses",
		"## Summary",
		"| example.com/mod | v1.2.3 | LICENSE | true |",
		"## example.com/mod v1.2.3",
		"### LICENSE",
		"Example License\nLine2",
	}
	for _, fragment := range fragments {
		if !strings.Contains(text, fragment) {
			t.Fatalf("report missing fragment %q\nreport:\n%s", fragment, text)
		}
	}
}
