package storage

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
	"os"

	"pkt.systems/kryptograf"
	"pkt.systems/kryptograf/keymgmt"
)

// This stress test mirrors the buffer-pool usage pattern we saw in lockd
// benchmarks: many goroutines reuse a single sync.Pool while encrypting and
// decrypting varying payload sizes. It is deliberately lightweight by default
// to keep CI fast; increase LOCKD_POOL_STRESS_ITERS to hunt rare failures.
func TestStorageCryptoBufferPoolStress(t *testing.T) {
	const (
		defaultGoroutines = 32
		defaultIter       = 100
		minChunk          = 2048
		maxChunk          = 32 * 1024
	)

	iter := defaultIter
	if v, ok := parsePositiveEnv("LOCKD_POOL_STRESS_ITERS"); ok && v > iter {
		iter = v
	}

	root, _ := keymgmt.GenerateRootKey()
	var pool sync.Pool
	kg := kryptograf.New(root).WithOptions(
		kryptograf.WithChunkSize(minChunk),
		kryptograf.WithBufferPool(&pool),
	)

	errCh := make(chan error, defaultGoroutines)
	wg := sync.WaitGroup{}
	wg.Add(defaultGoroutines)

	for g := 0; g < defaultGoroutines; g++ {
		seed := time.Now().UnixNano() + int64(g)
		r := rand.New(rand.NewSource(seed))

		go func(seed int64, r *rand.Rand) {
			defer wg.Done()
			for i := 0; i < iter; i++ {
				chunk := minChunk + r.Intn(maxChunk-minChunk)
				plen := r.Intn(chunk*3) + 1 // up to ~3 chunks
				payload := make([]byte, plen)
				if _, err := r.Read(payload); err != nil {
					errCh <- fmt.Errorf("rand read: %w", err)
					return
				}

				mat, err := kg.MintDEK([]byte(fmt.Sprintf("pool-stress-%d-%d", seed, i)))
				if err != nil {
					errCh <- err
					return
				}

				var cipherBuf bytes.Buffer
				w, err := kg.EncryptWriter(&cipherBuf, mat, kryptograf.WithChunkSize(chunk))
				if err != nil {
					errCh <- err
					return
				}
				if _, err := w.Write(payload); err != nil {
					errCh <- err
					return
				}
				if err := w.Close(); err != nil {
					errCh <- err
					return
				}

				// Churn the pool.
				if i%25 == 0 {
					runtime.GC()
				} else {
					runtime.Gosched()
				}

				rdr, err := kg.DecryptReader(bytes.NewReader(cipherBuf.Bytes()), mat, kryptograf.WithChunkSize(chunk))
				if err != nil {
					errCh <- err
					return
				}
				plain, err := io.ReadAll(rdr)
				rdr.Close()
				if err != nil {
					errCh <- err
					return
				}
				if !bytes.Equal(plain, payload) {
					errCh <- fmt.Errorf("payload mismatch seed=%d iter=%d chunk=%d len=%d", seed, i, chunk, plen)
					return
				}
			}
		}(seed, r)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("pool stress failure: %v", err)
		}
	}
}

func parsePositiveEnv(key string) (int, bool) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}
