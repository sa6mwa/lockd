package core

import (
	"sync/atomic"
	"time"
)

type queueDecisionCacheEntry struct {
	lastEmpty atomic.Int64
}

func (s *Service) queueDecisionCacheEntry(key string) *queueDecisionCacheEntry {
	if s == nil {
		return nil
	}
	if val, ok := s.queueDecisionCache.Load(key); ok {
		if entry, ok := val.(*queueDecisionCacheEntry); ok {
			return entry
		}
	}
	entry := &queueDecisionCacheEntry{}
	actual, _ := s.queueDecisionCache.LoadOrStore(key, entry)
	if stored, ok := actual.(*queueDecisionCacheEntry); ok {
		return stored
	}
	return entry
}

func (s *Service) shouldCheckQueueDecision(key string, now time.Time) bool {
	if s == nil {
		return false
	}
	if s.queueDecisionCacheTTL <= 0 {
		return true
	}
	entry := s.queueDecisionCacheEntry(key)
	if entry == nil {
		return true
	}
	last := entry.lastEmpty.Load()
	if last == 0 {
		return true
	}
	if now.Sub(time.Unix(0, last)) >= s.queueDecisionCacheTTL {
		return true
	}
	return false
}

func (s *Service) markQueueDecisionEmpty(key string, now time.Time) {
	if s == nil {
		return
	}
	entry := s.queueDecisionCacheEntry(key)
	if entry == nil {
		return
	}
	entry.lastEmpty.Store(now.UnixNano())
}

func (s *Service) clearQueueDecisionEmpty(key string) {
	if s == nil {
		return
	}
	entry := s.queueDecisionCacheEntry(key)
	if entry == nil {
		return
	}
	entry.lastEmpty.Store(0)
}
