package core

import "net/http"

// applyShutdownGuard returns a Failure when shutdown/draining rules forbid work.
func (s *Service) applyShutdownGuard(kind string) error {
	if s == nil || s.shutdownState == nil {
		return nil
	}
	state := s.shutdownState()
	if !state.Draining {
		return nil
	}
	retry := durationToSeconds(state.Remaining)
	if retry <= 0 {
		retry = 1
	}
	return Failure{
		Code:       "shutdown_draining",
		Detail:     "server is draining existing leases",
		RetryAfter: retry,
		HTTPStatus: http.StatusServiceUnavailable,
	}
}
