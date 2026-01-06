package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"pkt.systems/lockd/internal/uuidv7"
)

const (
	backendHashNamespace   = ".lockd"
	backendHashKey         = "backend-id"
	backendHashContentType = "text/plain"
)

// BackendHashResult captures the resolved backend hash and derived hash.
type BackendHashResult struct {
	Hash        string
	DerivedHash string
}

// ResolveBackendHash returns the backend hash, plus the derived hash (when a descriptor was supplied).
// The returned hash may come from an existing backend-id marker, a derived hash, or a generated UUID.
func ResolveBackendHash(ctx context.Context, backend Backend, derivedDescriptor string) (BackendHashResult, error) {
	derivedDescriptor = strings.TrimSpace(derivedDescriptor)
	derivedHash := ""
	if derivedDescriptor != "" {
		sum := sha256.Sum256([]byte(derivedDescriptor))
		derivedHash = hex.EncodeToString(sum[:])
	}

	stored, err := readBackendHash(ctx, backend)
	if err == nil && stored != "" {
		return BackendHashResult{Hash: stored, DerivedHash: derivedHash}, nil
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		if derivedHash != "" {
			return BackendHashResult{Hash: derivedHash, DerivedHash: derivedHash}, fmt.Errorf("backend hash read failed: %w", err)
		}
		return BackendHashResult{DerivedHash: derivedHash}, fmt.Errorf("backend hash read failed: %w", err)
	}

	candidate := derivedHash
	if candidate == "" {
		candidate = uuidv7.NewString()
	}
	if err := writeBackendHash(ctx, backend, candidate); err != nil {
		if errors.Is(err, ErrCASMismatch) {
			stored, readErr := readBackendHash(ctx, backend)
			if readErr == nil && stored != "" {
				return BackendHashResult{Hash: stored, DerivedHash: derivedHash}, nil
			}
			if derivedHash != "" {
				if readErr != nil {
					return BackendHashResult{Hash: derivedHash, DerivedHash: derivedHash}, fmt.Errorf("backend hash read after cas failed: %w", readErr)
				}
				return BackendHashResult{Hash: derivedHash, DerivedHash: derivedHash}, nil
			}
			if readErr != nil {
				return BackendHashResult{DerivedHash: derivedHash}, fmt.Errorf("backend hash read after cas failed: %w", readErr)
			}
			return BackendHashResult{DerivedHash: derivedHash}, fmt.Errorf("backend hash read after cas failed")
		}
		if derivedHash != "" {
			return BackendHashResult{Hash: derivedHash, DerivedHash: derivedHash}, fmt.Errorf("backend hash write failed: %w", err)
		}
		return BackendHashResult{DerivedHash: derivedHash}, fmt.Errorf("backend hash write failed: %w", err)
	}
	return BackendHashResult{Hash: candidate, DerivedHash: derivedHash}, nil
}

func readBackendHash(ctx context.Context, backend Backend) (string, error) {
	obj, err := backend.GetObject(ctx, backendHashNamespace, backendHashKey)
	if err != nil {
		return "", err
	}
	defer obj.Reader.Close()
	data, err := io.ReadAll(obj.Reader)
	if err != nil {
		return "", err
	}
	val := strings.TrimSpace(string(data))
	if val == "" {
		return "", ErrNotFound
	}
	return val, nil
}

func writeBackendHash(ctx context.Context, backend Backend, hash string) error {
	hash = strings.TrimSpace(hash)
	if hash == "" {
		return fmt.Errorf("backend hash required")
	}
	_, err := backend.PutObject(ctx, backendHashNamespace, backendHashKey, strings.NewReader(hash), PutObjectOptions{
		IfNotExists: true,
		ContentType: backendHashContentType,
	})
	return err
}
