package tcleader

import (
	"testing"
	"time"
)

func TestLeaseStoreFollow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()

	t.Run("accepts new leader", func(t *testing.T) {
		store := &LeaseStore{}
		rec, ok := store.Follow(now, "leader", "http://leader", 2, now.Add(5*time.Second))
		if !ok {
			t.Fatalf("expected follow to succeed")
		}
		if rec.LeaderID != "leader" || rec.LeaderEndpoint != "http://leader" || rec.Term != 2 || !rec.Observed {
			t.Fatalf("unexpected record: %+v", rec)
		}
	})

	t.Run("rejects lower term", func(t *testing.T) {
		store := &LeaseStore{record: LeaseRecord{LeaderID: "leader", LeaderEndpoint: "http://leader", Term: 3, ExpiresAt: now.Add(5 * time.Second)}}
		if _, ok := store.Follow(now, "other", "http://other", 2, now.Add(5*time.Second)); ok {
			t.Fatalf("expected follow to reject lower term")
		}
	})

	t.Run("rejects same term with active different leader", func(t *testing.T) {
		store := &LeaseStore{record: LeaseRecord{LeaderID: "leader", LeaderEndpoint: "http://leader", Term: 2, ExpiresAt: now.Add(5 * time.Second)}}
		if _, ok := store.Follow(now, "other", "http://other", 2, now.Add(5*time.Second)); ok {
			t.Fatalf("expected follow to reject conflicting leader")
		}
	})

	t.Run("accepts same term when expired", func(t *testing.T) {
		store := &LeaseStore{record: LeaseRecord{LeaderID: "leader", LeaderEndpoint: "http://leader", Term: 2, ExpiresAt: now.Add(-time.Second)}}
		rec, ok := store.Follow(now, "other", "http://other", 2, now.Add(5*time.Second))
		if !ok {
			t.Fatalf("expected follow to accept expired record")
		}
		if rec.LeaderID != "other" || rec.Term != 2 {
			t.Fatalf("unexpected record: %+v", rec)
		}
	})
}

func TestLeaseStoreAcquireRenewRelease(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	store := &LeaseStore{}
	ttl := 10 * time.Second

	rec, err := store.Acquire(now, "leader", "http://leader", 1, ttl)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if rec.Observed {
		t.Fatalf("expected acquire to leave record unobserved")
	}
	if rec.ExpiresAt != now.Add(ttl) {
		t.Fatalf("unexpected expiry: %v", rec.ExpiresAt)
	}

	renewAt := now.Add(3 * time.Second)
	rec, err = store.Renew(renewAt, "leader", 1, ttl)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if !rec.Observed {
		t.Fatalf("expected renew to mark record observed")
	}
	if rec.ExpiresAt != renewAt.Add(ttl) {
		t.Fatalf("unexpected renewal expiry: %v", rec.ExpiresAt)
	}

	releaseAt := renewAt.Add(500 * time.Millisecond)
	rec, err = store.Release(releaseAt, "leader", 1)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if rec.LeaderID != "" || rec.LeaderEndpoint != "" {
		t.Fatalf("expected lease cleared, got %+v", rec)
	}
	if rec.Observed {
		t.Fatalf("expected release to clear observation")
	}
	if rec.ExpiresAt != releaseAt {
		t.Fatalf("unexpected release expiry: %v", rec.ExpiresAt)
	}
}

func TestLeaseStoreAcquireRequiresHigherTermAfterExpiry(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	store := &LeaseStore{
		record: LeaseRecord{
			LeaderID:       "leader",
			LeaderEndpoint: "http://leader",
			Term:           3,
			ExpiresAt:      now.Add(-time.Second),
			Observed:       true,
		},
	}

	if _, err := store.Acquire(now, "candidate", "http://candidate", 3, 5*time.Second); err == nil {
		t.Fatalf("expected acquire to reject same term after expiry")
	}
	rec, err := store.Acquire(now, "candidate", "http://candidate", 4, 5*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if rec.Term != 4 || rec.LeaderID != "candidate" {
		t.Fatalf("unexpected record: %+v", rec)
	}
}

func TestLeaseStoreExpiredUnobservedClearsTerm(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	store := &LeaseStore{
		record: LeaseRecord{
			LeaderID:       "candidate",
			LeaderEndpoint: "http://candidate",
			Term:           5,
			ExpiresAt:      now.Add(-time.Second),
			Observed:       false,
		},
	}

	rec, err := store.Acquire(now, "leader", "http://leader", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if rec.Term != 1 || rec.LeaderID != "leader" {
		t.Fatalf("unexpected record: %+v", rec)
	}
}

func TestLeaseStoreReleaseUnobservedResetsTerm(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	store := &LeaseStore{}
	ttl := 5 * time.Second

	rec, err := store.Acquire(now, "candidate", "http://candidate", 2, ttl)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if rec.Observed {
		t.Fatalf("expected unobserved record after acquire")
	}
	rec, err = store.Release(now.Add(time.Second), "candidate", 2)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if rec.Term != 0 {
		t.Fatalf("expected term reset after unobserved release, got %d", rec.Term)
	}
	if rec.LeaderID != "" || rec.LeaderEndpoint != "" {
		t.Fatalf("expected cleared leader after release, got %+v", rec)
	}
}

func TestLeaseStoreObservedTermPersistsAfterUnobservedRelease(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	store := &LeaseStore{
		record: LeaseRecord{
			LeaderID:       "leader",
			LeaderEndpoint: "http://leader",
			Term:           2,
			ExpiresAt:      now.Add(-time.Second),
			Observed:       true,
		},
	}

	rec, err := store.Acquire(now, "candidate", "http://candidate", 3, 5*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if rec.ObservedTerm != 2 {
		t.Fatalf("expected observed term to remain 2, got %d", rec.ObservedTerm)
	}
	rec, err = store.Release(now.Add(time.Second), "candidate", 3)
	if err != nil {
		t.Fatalf("release: %v", err)
	}
	if rec.Term != 2 || rec.ObservedTerm != 2 {
		t.Fatalf("expected term to reset to observed term, got %+v", rec)
	}
	if rec.LeaderID != "" || rec.LeaderEndpoint != "" {
		t.Fatalf("expected cleared leader after release, got %+v", rec)
	}
}

func TestLeaseStoreAcquireTermFencing(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	store := &LeaseStore{
		record: LeaseRecord{
			LeaderID:       "leader",
			LeaderEndpoint: "http://leader",
			Term:           5,
			ExpiresAt:      now.Add(5 * time.Second),
			Observed:       true,
		},
	}

	if _, err := store.Acquire(now, "candidate", "http://candidate", 4, 5*time.Second); err == nil {
		t.Fatalf("expected lower term to be rejected")
	}
	if _, err := store.Acquire(now, "candidate", "http://candidate", 6, 5*time.Second); err == nil {
		t.Fatalf("expected active lease to block new leader even with higher term")
	}
	rec, err := store.Acquire(now, "leader", "http://leader", 5, 10*time.Second)
	if err != nil {
		t.Fatalf("expected same leader/term acquire to succeed: %v", err)
	}
	if rec.ExpiresAt != now.Add(10*time.Second) {
		t.Fatalf("unexpected expiry after refresh: %v", rec.ExpiresAt)
	}
}
