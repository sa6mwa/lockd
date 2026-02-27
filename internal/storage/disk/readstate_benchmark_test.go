package disk

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/namespaces"
)

func BenchmarkStoreReadState(b *testing.B) {
	ctx := context.Background()
	sizes := []int{1024, 16 * 1024}
	for _, size := range sizes {
		size := size
		b.Run(bytesLabel(size), func(b *testing.B) {
			root := filepath.Join(b.TempDir(), "store")
			store, err := New(Config{Root: root})
			if err != nil {
				b.Fatalf("new store: %v", err)
			}
			defer store.Close()

			key := "bench-read-state"
			payload := bytes.Repeat([]byte("x"), size)
			if _, err := store.WriteState(ctx, namespaces.Default, key, bytes.NewReader(payload), storage.PutStateOptions{}); err != nil {
				b.Fatalf("write state: %v", err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				res, err := store.ReadState(ctx, namespaces.Default, key)
				if err != nil {
					b.Fatalf("read state: %v", err)
				}
				n, copyErr := io.Copy(io.Discard, res.Reader)
				closeErr := res.Reader.Close()
				if copyErr != nil {
					b.Fatalf("drain state: %v", copyErr)
				}
				if closeErr != nil {
					b.Fatalf("close state reader: %v", closeErr)
				}
				if n != int64(size) {
					b.Fatalf("read bytes = %d, want %d", n, size)
				}
			}
		})
	}
}

func bytesLabel(n int) string {
	if n < 1024 {
		return fmt.Sprintf("%dB", n)
	}
	return fmt.Sprintf("%dKiB", n/1024)
}
