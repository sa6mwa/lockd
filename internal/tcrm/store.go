package tcrm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	"pkt.systems/lockd/internal/storage"
	"pkt.systems/lockd/internal/tccluster"
	"pkt.systems/pslog"
)

const (
	rmNamespace       = ".lockd"
	rmKey             = "tc-rm-members"
	rmContentType     = "application/json"
	rmMaxMergeTries   = 50
	rmMergeBackoff    = 5 * time.Millisecond
	rmMergeBackoffMax = 100 * time.Millisecond
)

// BackendMembers tracks RM endpoints for a backend hash.
type BackendMembers struct {
	BackendHash   string   `json:"backend_hash"`
	Endpoints     []string `json:"endpoints"`
	UpdatedAtUnix int64    `json:"updated_at_unix,omitempty"`
}

// Members captures the persisted RM registry.
type Members struct {
	Backends      []BackendMembers `json:"backends"`
	UpdatedAtUnix int64            `json:"updated_at_unix,omitempty"`
}

// LoadResult reports the registry record alongside storage metadata.
type LoadResult struct {
	Members Members
	ETag    string
	Found   bool
}

// UpdateResult reports the outcome of a register/unregister operation.
type UpdateResult struct {
	Members Members
	ETag    string
	Changed bool
	Deleted bool
}

// Store manages RM registry entries in the backend.
type Store struct {
	backend storage.Backend
	logger  pslog.Logger
}

// NewStore constructs an RM registry store for the supplied backend.
func NewStore(backend storage.Backend, logger pslog.Logger) *Store {
	return &Store{backend: backend, logger: logger}
}

// Load reads the stored registry.
func (s *Store) Load(ctx context.Context) (LoadResult, error) {
	if s == nil || s.backend == nil {
		return LoadResult{}, errors.New("tcrm: backend not configured")
	}
	obj, err := s.backend.GetObject(ctx, rmNamespace, rmKey)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return LoadResult{Members: Members{Backends: nil}, Found: false}, nil
		}
		return LoadResult{}, err
	}
	defer obj.Reader.Close()
	payload, err := io.ReadAll(obj.Reader)
	if err != nil {
		return LoadResult{}, err
	}
	var members Members
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &members); err != nil {
			return LoadResult{}, fmt.Errorf("tcrm: decode members: %w", err)
		}
	}
	members.Backends = normalizeBackends(members.Backends)
	return LoadResult{
		Members: members,
		ETag:    obj.Info.ETag,
		Found:   true,
	}, nil
}

// Register adds or refreshes an RM endpoint for a backend hash.
func (s *Store) Register(ctx context.Context, backendHash, endpoint string) (UpdateResult, error) {
	backendHash = strings.TrimSpace(backendHash)
	normalizedEndpoint, endpointErr := tccluster.NormalizeEndpoint(endpoint)
	if s == nil || s.backend == nil {
		return UpdateResult{}, errors.New("tcrm: backend not configured")
	}
	if backendHash == "" {
		return UpdateResult{}, errors.New("tcrm: backend hash required")
	}
	if endpointErr != nil {
		return UpdateResult{}, fmt.Errorf("tcrm: %w", endpointErr)
	}
	endpoint = normalizedEndpoint
	_, hasDeadline := ctx.Deadline()
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return UpdateResult{}, ctx.Err()
		}
		if !hasDeadline && attempt >= rmMaxMergeTries {
			return UpdateResult{}, errors.New("tcrm: register failed after retries")
		}
		current, err := s.Load(ctx)
		if err != nil {
			return UpdateResult{}, err
		}
		now := time.Now().UnixMilli()
		updated := upsertBackend(current.Members.Backends, backendHash, endpoint, now)
		if current.Found && slices.EqualFunc(current.Members.Backends, updated, backendEqual) {
			return UpdateResult{Members: current.Members, ETag: current.ETag, Changed: false}, nil
		}
		record := Members{
			Backends:      updated,
			UpdatedAtUnix: now,
		}
		payload, err := json.Marshal(record)
		if err != nil {
			return UpdateResult{}, err
		}
		opts := storage.PutObjectOptions{ContentType: rmContentType}
		if current.Found && current.ETag != "" {
			opts.ExpectedETag = current.ETag
		} else {
			opts.IfNotExists = true
		}
		info, err := s.backend.PutObject(ctx, rmNamespace, rmKey, bytes.NewReader(payload), opts)
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) || errors.Is(err, storage.ErrNotFound) {
				if err := sleepMergeBackoff(ctx, mergeBackoff(attempt+1)); err != nil {
					return UpdateResult{}, err
				}
				continue
			}
			return UpdateResult{}, err
		}
		if info == nil {
			info = &storage.ObjectInfo{}
		}
		return UpdateResult{Members: record, ETag: info.ETag, Changed: true}, nil
	}
}

// Unregister removes an RM endpoint from the registry.
func (s *Store) Unregister(ctx context.Context, backendHash, endpoint string) (UpdateResult, error) {
	backendHash = strings.TrimSpace(backendHash)
	normalizedEndpoint, endpointErr := tccluster.NormalizeEndpoint(endpoint)
	if s == nil || s.backend == nil {
		return UpdateResult{}, errors.New("tcrm: backend not configured")
	}
	if backendHash == "" {
		return UpdateResult{}, errors.New("tcrm: backend hash required")
	}
	if endpointErr != nil {
		return UpdateResult{}, fmt.Errorf("tcrm: %w", endpointErr)
	}
	endpoint = normalizedEndpoint
	_, hasDeadline := ctx.Deadline()
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return UpdateResult{}, ctx.Err()
		}
		if !hasDeadline && attempt >= rmMaxMergeTries {
			return UpdateResult{}, errors.New("tcrm: unregister failed after retries")
		}
		current, err := s.Load(ctx)
		if err != nil {
			return UpdateResult{}, err
		}
		if !current.Found || len(current.Members.Backends) == 0 {
			return UpdateResult{Members: Members{Backends: nil}, Changed: false}, nil
		}
		remaining := removeEndpoint(current.Members.Backends, backendHash, endpoint)
		if slices.EqualFunc(current.Members.Backends, remaining, backendEqual) {
			return UpdateResult{Members: current.Members, ETag: current.ETag, Changed: false}, nil
		}
		if len(remaining) == 0 {
			err = s.backend.DeleteObject(ctx, rmNamespace, rmKey, storage.DeleteObjectOptions{
				ExpectedETag: current.ETag,
			})
			if err != nil {
				if errors.Is(err, storage.ErrCASMismatch) || errors.Is(err, storage.ErrNotFound) {
					if err := sleepMergeBackoff(ctx, mergeBackoff(attempt+1)); err != nil {
						return UpdateResult{}, err
					}
					continue
				}
				return UpdateResult{}, err
			}
			return UpdateResult{Members: Members{Backends: nil}, Changed: true, Deleted: true}, nil
		}
		record := Members{
			Backends:      remaining,
			UpdatedAtUnix: time.Now().UnixMilli(),
		}
		payload, err := json.Marshal(record)
		if err != nil {
			return UpdateResult{}, err
		}
		info, err := s.backend.PutObject(ctx, rmNamespace, rmKey, bytes.NewReader(payload), storage.PutObjectOptions{
			ExpectedETag: current.ETag,
			ContentType:  rmContentType,
		})
		if err != nil {
			if errors.Is(err, storage.ErrCASMismatch) || errors.Is(err, storage.ErrNotFound) {
				if err := sleepMergeBackoff(ctx, mergeBackoff(attempt+1)); err != nil {
					return UpdateResult{}, err
				}
				continue
			}
			return UpdateResult{}, err
		}
		if info == nil {
			info = &storage.ObjectInfo{}
		}
		return UpdateResult{Members: record, ETag: info.ETag, Changed: true}, nil
	}
}

// Endpoints returns the endpoints registered for a backend hash.
func (s *Store) Endpoints(ctx context.Context, backendHash string) ([]string, error) {
	backendHash = strings.TrimSpace(backendHash)
	if backendHash == "" {
		return nil, errors.New("tcrm: backend hash required")
	}
	result, err := s.Load(ctx)
	if err != nil {
		return nil, err
	}
	for _, backend := range result.Members.Backends {
		if backend.BackendHash == backendHash {
			return append([]string(nil), backend.Endpoints...), nil
		}
	}
	return nil, nil
}

func normalizeEndpoint(raw string) string {
	normalized, err := tccluster.NormalizeEndpoint(raw)
	if err != nil {
		return ""
	}
	return normalized
}

func normalizeBackends(backends []BackendMembers) []BackendMembers {
	normalized := make([]BackendMembers, 0, len(backends))
	for _, backend := range backends {
		hash := strings.TrimSpace(backend.BackendHash)
		if hash == "" {
			continue
		}
		endpoints := normalizeEndpoints(backend.Endpoints)
		if len(endpoints) == 0 {
			continue
		}
		normalized = append(normalized, BackendMembers{
			BackendHash:   hash,
			Endpoints:     endpoints,
			UpdatedAtUnix: backend.UpdatedAtUnix,
		})
	}
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i].BackendHash < normalized[j].BackendHash
	})
	return normalized
}

func normalizeEndpoints(endpoints []string) []string {
	seen := make(map[string]struct{}, len(endpoints))
	out := make([]string, 0, len(endpoints))
	for _, raw := range endpoints {
		trimmed := normalizeEndpoint(raw)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	sort.Strings(out)
	return out
}

func mergeBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}
	backoff := rmMergeBackoff
	for i := 1; i < attempt; i++ {
		backoff *= 2
		if backoff >= rmMergeBackoffMax {
			return rmMergeBackoffMax
		}
	}
	if backoff > rmMergeBackoffMax {
		return rmMergeBackoffMax
	}
	return backoff
}

func sleepMergeBackoff(ctx context.Context, backoff time.Duration) error {
	if backoff <= 0 {
		return nil
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func upsertBackend(backends []BackendMembers, backendHash, endpoint string, updatedAt int64) []BackendMembers {
	normalized := normalizeBackends(backends)
	for i, backend := range normalized {
		if backend.BackendHash != backendHash {
			continue
		}
		if !slices.Contains(backend.Endpoints, endpoint) {
			backend.Endpoints = append(backend.Endpoints, endpoint)
			backend.Endpoints = normalizeEndpoints(backend.Endpoints)
			backend.UpdatedAtUnix = updatedAt
			normalized[i] = backend
			return normalized
		}
		if backend.UpdatedAtUnix == 0 {
			backend.UpdatedAtUnix = updatedAt
			normalized[i] = backend
		}
		return normalized
	}
	normalized = append(normalized, BackendMembers{
		BackendHash:   backendHash,
		Endpoints:     []string{endpoint},
		UpdatedAtUnix: updatedAt,
	})
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i].BackendHash < normalized[j].BackendHash
	})
	return normalized
}

func removeEndpoint(backends []BackendMembers, backendHash, endpoint string) []BackendMembers {
	normalized := normalizeBackends(backends)
	out := make([]BackendMembers, 0, len(normalized))
	for _, backend := range normalized {
		if backend.BackendHash != backendHash {
			out = append(out, backend)
			continue
		}
		remaining := make([]string, 0, len(backend.Endpoints))
		for _, candidate := range backend.Endpoints {
			if candidate == endpoint {
				continue
			}
			remaining = append(remaining, candidate)
		}
		remaining = normalizeEndpoints(remaining)
		if len(remaining) == 0 {
			continue
		}
		backend.Endpoints = remaining
		out = append(out, backend)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].BackendHash < out[j].BackendHash
	})
	return out
}

func backendEqual(a, b BackendMembers) bool {
	if a.BackendHash != b.BackendHash {
		return false
	}
	if a.UpdatedAtUnix != b.UpdatedAtUnix {
		return false
	}
	return slices.Equal(a.Endpoints, b.Endpoints)
}
