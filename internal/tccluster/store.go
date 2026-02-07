package tccluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	pathpkg "path"
	"sort"
	"strings"
	"sync"
	"time"

	"pkt.systems/lockd/internal/clock"
	"pkt.systems/lockd/internal/storage"
	"pkt.systems/pslog"
)

const (
	clusterNamespace   = ".lockd"
	clusterPrefix      = "tc-cluster/leases/"
	clusterContentType = "application/json"
	clusterListLimit   = 512
)

// LeaseRecord captures a TC membership lease.
type LeaseRecord struct {
	Identity      string `json:"identity,omitempty"`
	Endpoint      string `json:"endpoint"`
	UpdatedAtUnix int64  `json:"updated_at_unix,omitempty"`
	ExpiresAtUnix int64  `json:"expires_at_unix,omitempty"`
}

// ListResult reports active cluster membership.
type ListResult struct {
	Records       []LeaseRecord
	Endpoints     []string
	UpdatedAtUnix int64
}

// Store manages TC cluster membership leases in the backend.
type Store struct {
	backend    storage.Backend
	logger     pslog.Logger
	clock      clock.Clock
	announceMu sync.Mutex
	mu         sync.RWMutex
	paused     map[string]struct{}
}

// NewStore constructs a Store for the supplied backend.
func NewStore(backend storage.Backend, logger pslog.Logger, clk clock.Clock) *Store {
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	if clk == nil {
		clk = clock.Real{}
	}
	return &Store{backend: backend, logger: logger, clock: clk}
}

// ErrPaused indicates announcements are paused for the identity.
var ErrPaused = errors.New("tccluster: identity paused")

// Announce records or renews a membership lease for the supplied identity.
// Announce resumes a paused identity.
func (s *Store) Announce(ctx context.Context, identity, endpoint string, ttl time.Duration) (LeaseRecord, error) {
	return s.announce(ctx, identity, endpoint, ttl, true)
}

// AnnounceIfNotPaused records or renews a membership lease unless the identity is paused.
// It never resumes a paused identity.
func (s *Store) AnnounceIfNotPaused(ctx context.Context, identity, endpoint string, ttl time.Duration) (LeaseRecord, error) {
	return s.announce(ctx, identity, endpoint, ttl, false)
}

func (s *Store) announce(ctx context.Context, identity, endpoint string, ttl time.Duration, resume bool) (LeaseRecord, error) {
	if s == nil || s.backend == nil {
		return LeaseRecord{}, errors.New("tccluster: backend not configured")
	}
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return LeaseRecord{}, errors.New("tccluster: identity required")
	}
	normalizedEndpoint, err := NormalizeEndpoint(endpoint)
	if err != nil {
		return LeaseRecord{}, fmt.Errorf("tccluster: %w", err)
	}
	endpoint = normalizedEndpoint
	if ttl <= 0 {
		return LeaseRecord{}, errors.New("tccluster: ttl must be > 0")
	}
	s.announceMu.Lock()
	defer s.announceMu.Unlock()
	if resume {
		s.resume(identity)
	} else if s.isPaused(identity) {
		return LeaseRecord{}, ErrPaused
	}
	now := s.clock.Now()
	expiresAt := now.Add(ttl)
	record := LeaseRecord{
		Identity:      identity,
		Endpoint:      endpoint,
		UpdatedAtUnix: now.UnixMilli(),
		ExpiresAtUnix: expiresAt.UnixMilli(),
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return LeaseRecord{}, err
	}
	_, err = s.backend.PutObject(ctx, clusterNamespace, leaseKey(identity), bytes.NewReader(payload), storage.PutObjectOptions{
		ContentType: clusterContentType,
	})
	if err != nil {
		return LeaseRecord{}, err
	}
	return record, nil
}

// Pause stops auto-announcements for the supplied identity.
func (s *Store) Pause(identity string) {
	if s == nil {
		return
	}
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return
	}
	s.announceMu.Lock()
	defer s.announceMu.Unlock()
	s.mu.Lock()
	if s.paused == nil {
		s.paused = make(map[string]struct{})
	}
	s.paused[identity] = struct{}{}
	s.mu.Unlock()
}

// Resume re-enables auto-announcements for the supplied identity.
func (s *Store) Resume(identity string) {
	if s == nil {
		return
	}
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return
	}
	s.resume(identity)
}

// IsPaused reports whether auto-announcements are suppressed for identity.
func (s *Store) IsPaused(identity string) bool {
	return s.isPaused(identity)
}

// Leave deletes the membership lease for the supplied identity.
func (s *Store) Leave(ctx context.Context, identity string) error {
	if s == nil || s.backend == nil {
		return errors.New("tccluster: backend not configured")
	}
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return errors.New("tccluster: identity required")
	}
	return s.backend.DeleteObject(ctx, clusterNamespace, leaseKey(identity), storage.DeleteObjectOptions{
		IgnoreNotFound: true,
	})
}

// Active returns the active membership leases as of the current clock.
func (s *Store) Active(ctx context.Context) (ListResult, error) {
	if s == nil {
		return ListResult{}, errors.New("tccluster: backend not configured")
	}
	return s.activeAt(ctx, s.clock.Now())
}

func (s *Store) activeAt(ctx context.Context, now time.Time) (ListResult, error) {
	records, err := s.list(ctx)
	if err != nil {
		return ListResult{}, err
	}
	active := make([]LeaseRecord, 0, len(records))
	endpoints := make([]string, 0, len(records))
	updatedAt := int64(0)
	nowMillis := now.UnixMilli()
	for _, record := range records {
		if record.Endpoint == "" || record.ExpiresAtUnix == 0 {
			continue
		}
		if record.ExpiresAtUnix <= nowMillis {
			continue
		}
		active = append(active, record)
		endpoints = append(endpoints, record.Endpoint)
		if record.UpdatedAtUnix > updatedAt {
			updatedAt = record.UpdatedAtUnix
		}
	}
	endpoints = NormalizeEndpoints(endpoints)
	return ListResult{
		Records:       active,
		Endpoints:     endpoints,
		UpdatedAtUnix: updatedAt,
	}, nil
}

func (s *Store) resume(identity string) {
	s.mu.Lock()
	if len(s.paused) == 0 {
		s.mu.Unlock()
		return
	}
	delete(s.paused, identity)
	s.mu.Unlock()
}

func (s *Store) isPaused(identity string) bool {
	if s == nil {
		return false
	}
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return false
	}
	s.mu.RLock()
	_, ok := s.paused[identity]
	s.mu.RUnlock()
	return ok
}

func (s *Store) list(ctx context.Context) ([]LeaseRecord, error) {
	if s == nil || s.backend == nil {
		return nil, errors.New("tccluster: backend not configured")
	}
	var records []LeaseRecord
	startAfter := ""
	for {
		result, err := s.backend.ListObjects(ctx, clusterNamespace, storage.ListOptions{
			Prefix:     clusterPrefix,
			StartAfter: startAfter,
			Limit:      clusterListLimit,
		})
		if err != nil {
			return nil, err
		}
		if result == nil {
			return nil, errors.New("tccluster: list returned nil result")
		}
		for _, obj := range result.Objects {
			key := strings.TrimSpace(obj.Key)
			if key == "" {
				continue
			}
			identity := strings.TrimPrefix(key, clusterPrefix)
			if identity == "" || identity == key {
				continue
			}
			record, err := s.loadLease(ctx, key, identity)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					continue
				}
				if s.logger != nil {
					s.logger.Warn("tc.cluster.lease.read_failed", "key", key, "error", err)
				}
				continue
			}
			if record.Identity == "" {
				record.Identity = identity
			}
			record.Endpoint = normalizeEndpoint(record.Endpoint)
			if record.Endpoint == "" {
				continue
			}
			records = append(records, record)
		}
		if !result.Truncated {
			break
		}
		startAfter = strings.TrimSpace(result.NextStartAfter)
		if startAfter == "" {
			if len(result.Objects) == 0 {
				break
			}
			startAfter = result.Objects[len(result.Objects)-1].Key
		}
	}
	return records, nil
}

func (s *Store) loadLease(ctx context.Context, key, identity string) (LeaseRecord, error) {
	obj, err := s.backend.GetObject(ctx, clusterNamespace, key)
	if err != nil {
		return LeaseRecord{}, err
	}
	defer obj.Reader.Close()
	payload, err := io.ReadAll(obj.Reader)
	if err != nil {
		return LeaseRecord{}, err
	}
	if len(payload) == 0 {
		return LeaseRecord{}, nil
	}
	var record LeaseRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return LeaseRecord{}, fmt.Errorf("tccluster: decode lease %s: %w", identity, err)
	}
	return record, nil
}

func leaseKey(identity string) string {
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return clusterPrefix
	}
	return clusterPrefix + identity
}

// NormalizeEndpoints trims, dedupes, and sorts endpoint entries.
func NormalizeEndpoints(endpoints []string) []string {
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

// ContainsEndpoint reports whether endpoints contains target.
func ContainsEndpoint(endpoints []string, target string) bool {
	target = normalizeEndpoint(target)
	if target == "" {
		return false
	}
	for _, item := range endpoints {
		if item == target {
			return true
		}
	}
	return false
}

func normalizeEndpoint(raw string) string {
	normalized, err := NormalizeEndpoint(raw)
	if err != nil {
		return ""
	}
	return normalized
}

// NormalizeEndpoint canonicalizes and validates a TC/RM endpoint URL.
func NormalizeEndpoint(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.New("endpoint required")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", errors.New("endpoint scheme must be http or https")
	}
	if parsed.Host == "" {
		return "", errors.New("endpoint host required")
	}
	if parsed.User != nil {
		return "", errors.New("endpoint userinfo is not allowed")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("endpoint must not include query or fragment")
	}
	cleanPath := pathpkg.Clean("/" + strings.TrimLeft(parsed.Path, "/"))
	if cleanPath == "/" {
		parsed.Path = ""
		parsed.RawPath = ""
	} else {
		parsed.Path = cleanPath
		parsed.RawPath = ""
	}
	return strings.TrimSuffix(parsed.String(), "/"), nil
}

// JoinEndpoint validates base and appends a cleaned path suffix.
func JoinEndpoint(base, suffix string) (string, error) {
	normalized, err := NormalizeEndpoint(base)
	if err != nil {
		return "", err
	}
	ref, err := url.Parse(strings.TrimSpace(suffix))
	if err != nil {
		return "", fmt.Errorf("invalid endpoint suffix: %w", err)
	}
	if ref.IsAbs() || ref.Host != "" || ref.Scheme != "" || ref.User != nil {
		return "", errors.New("endpoint suffix must be a relative path")
	}
	if ref.RawQuery != "" || ref.Fragment != "" {
		return "", errors.New("endpoint suffix must not include query or fragment")
	}
	if ref.Path == "" {
		return "", errors.New("endpoint suffix path required")
	}
	baseURL, err := url.Parse(normalized)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint URL: %w", err)
	}
	basePath := strings.TrimSuffix(baseURL.Path, "/")
	suffixPath := pathpkg.Clean("/" + strings.TrimLeft(ref.Path, "/"))
	joinedPath := pathpkg.Clean(basePath + "/" + strings.TrimLeft(suffixPath, "/"))
	if !strings.HasPrefix(joinedPath, "/") {
		joinedPath = "/" + joinedPath
	}
	baseURL.Path = joinedPath
	baseURL.RawPath = ""
	baseURL.RawQuery = ""
	baseURL.Fragment = ""
	return baseURL.String(), nil
}
