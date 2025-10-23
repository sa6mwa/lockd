package lsf

import (
	"strings"
	"testing"
)

func TestParseMeminfoUsesMemAvailable(t *testing.T) {
	const data = `MemTotal:       32768 kB
MemAvailable:   16384 kB
MemFree:         8192 kB
Buffers:          512 kB
Cached:          2048 kB
`
	mi, err := parseMeminfo(strings.NewReader(data))
	if err != nil {
		t.Fatalf("parseMeminfo returned error: %v", err)
	}
	if mi.totalBytes != 32768*1024 {
		t.Fatalf("expected total bytes %d, got %d", 32768*1024, mi.totalBytes)
	}
	if mi.availableBytes != 16384*1024 {
		t.Fatalf("expected available bytes %d, got %d", 16384*1024, mi.availableBytes)
	}
	if !mi.includesReclaimableData {
		t.Fatalf("expected includesReclaimableData true when MemAvailable present")
	}
}

func TestParseMeminfoFallbackEstimation(t *testing.T) {
	const data = `MemTotal:       40960 kB
MemFree:         4096 kB
Buffers:         2048 kB
Cached:          8192 kB
SReclaimable:    1024 kB
Shmem:            512 kB
`
	mi, err := parseMeminfo(strings.NewReader(data))
	if err != nil {
		t.Fatalf("parseMeminfo returned error: %v", err)
	}
	if mi.totalBytes != 40960*1024 {
		t.Fatalf("expected total bytes %d, got %d", 40960*1024, mi.totalBytes)
	}
	expectedAvailable := (4096 + 2048 + 8192 + 1024 - 512) * 1024
	if mi.availableBytes != uint64(expectedAvailable) {
		t.Fatalf("expected available bytes %d, got %d", expectedAvailable, mi.availableBytes)
	}
	if !mi.includesReclaimableData {
		t.Fatalf("expected includesReclaimableData true when caches included")
	}
}
