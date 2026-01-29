package storage

import (
	"bytes"
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
func ResolveBackendHash(ctx context.Context, backend Backend, derivedDescriptor string, crypto *Crypto) (BackendHashResult, error) {
	derivedDescriptor = strings.TrimSpace(derivedDescriptor)
	derivedHash := ""
	if derivedDescriptor != "" {
		sum := sha256.Sum256([]byte(derivedDescriptor))
		derivedHash = hex.EncodeToString(sum[:])
	}

	stored, etag, contentType, err := readBackendHash(ctx, backend, crypto)
	if err == nil && stored != "" {
		if crypto != nil && crypto.Enabled() && contentType != ContentTypeTextEncrypted {
			_ = writeBackendHash(ctx, backend, stored, crypto, etag, true)
		}
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
	if err := writeBackendHash(ctx, backend, candidate, crypto, "", false); err != nil {
		if errors.Is(err, ErrCASMismatch) {
			stored, _, _, readErr := readBackendHash(ctx, backend, crypto)
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

func readBackendHash(ctx context.Context, backend Backend, crypto *Crypto) (string, string, string, error) {
	obj, err := backend.GetObject(ctx, backendHashNamespace, backendHashKey)
	if err != nil {
		return "", "", "", err
	}
	defer obj.Reader.Close()
	data, err := io.ReadAll(obj.Reader)
	if err != nil {
		return "", "", "", err
	}
	contentType := ""
	etag := ""
	if obj.Info != nil {
		contentType = obj.Info.ContentType
		etag = obj.Info.ETag
	}
	if crypto != nil && crypto.Enabled() && contentType == ContentTypeTextEncrypted {
		data, err = crypto.DecryptMetadata(data)
		if err != nil {
			return "", "", contentType, err
		}
	}
	val := strings.TrimSpace(string(data))
	if val == "" {
		return "", etag, contentType, ErrNotFound
	}
	return val, etag, contentType, nil
}

func writeBackendHash(ctx context.Context, backend Backend, hash string, crypto *Crypto, expectedETag string, allowOverwrite bool) error {
	hash = strings.TrimSpace(hash)
	if hash == "" {
		return fmt.Errorf("backend hash required")
	}
	payload := []byte(hash)
	contentType := backendHashContentType
	if crypto != nil && crypto.Enabled() {
		var err error
		payload, err = crypto.EncryptMetadata(payload)
		if err != nil {
			return err
		}
		contentType = ContentTypeTextEncrypted
	}
	opts := PutObjectOptions{
		ContentType: contentType,
	}
	if expectedETag != "" {
		opts.ExpectedETag = expectedETag
	} else if !allowOverwrite {
		opts.IfNotExists = true
	}
	_, err := backend.PutObject(ctx, backendHashNamespace, backendHashKey, bytes.NewReader(payload), opts)
	return err
}
