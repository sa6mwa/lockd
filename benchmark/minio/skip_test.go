//go:build bench && !minio

package miniointegration

import "testing"

func TestMinioBenchmarksRequireMinioTag(t *testing.T) {
	t.Skip("minio benchmarks require -tags 'bench minio'")
}
