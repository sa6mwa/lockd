package clock

import (
	"sync"
	"time"
)

// Manual provides a controllable clock for deterministic tests.
type Manual struct {
	mu     sync.Mutex
	now    time.Time
	timers []*manualTimer
}

type manualTimer struct {
	at time.Time
	ch chan time.Time
}

// NewManual constructs a Manual clock starting at the supplied time.
func NewManual(start time.Time) *Manual {
	return &Manual{now: start.UTC()}
}

// Now returns the current manual time.
func (m *Manual) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// After returns a channel that fires when the manual clock advances by d.
func (m *Manual) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	m.mu.Lock()
	if d <= 0 {
		now := m.now
		m.mu.Unlock()
		ch <- now
		return ch
	}
	timer := &manualTimer{
		at: m.now.Add(d),
		ch: ch,
	}
	m.timers = append(m.timers, timer)
	m.mu.Unlock()
	return ch
}

// Sleep blocks until the manual clock advances by at least d.
func (m *Manual) Sleep(d time.Duration) {
	<-m.After(d)
}

// Advance moves time forward by d and fires any due timers.
func (m *Manual) Advance(d time.Duration) time.Time {
	if d < 0 {
		d = 0
	}
	m.mu.Lock()
	m.now = m.now.Add(d)
	now := m.now
	if len(m.timers) == 0 {
		m.mu.Unlock()
		return now
	}
	remaining := m.timers[:0]
	for _, timer := range m.timers {
		if timer.at.After(now) {
			remaining = append(remaining, timer)
			continue
		}
		timer.ch <- now
	}
	m.timers = remaining
	m.mu.Unlock()
	return now
}

// Pending returns the number of scheduled timers.
func (m *Manual) Pending() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.timers)
}
