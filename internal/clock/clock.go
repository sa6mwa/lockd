package clock

import "time"

// Clock abstracts time-related functions for easier testing.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
}

// Real implements Clock using the standard library.
type Real struct{}

// Now returns the current UTC time.
func (Real) Now() time.Time {
	return time.Now().UTC()
}

// After mirrors time.After while satisfying the Clock interface.
func (Real) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// Sleep blocks for at least the supplied duration.
func (Real) Sleep(d time.Duration) {
	time.Sleep(d)
}
