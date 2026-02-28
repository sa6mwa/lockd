package core

const (
	indexAsyncWorkers   = 4
	indexAsyncQueueSize = 4096
)

func (s *Service) startIndexAsyncWorkers() {
	if s == nil || s.indexManager == nil {
		return
	}
	if s.indexAsyncQueue != nil {
		return
	}
	q := make(chan func(), indexAsyncQueueSize)
	s.indexAsyncQueue = q
	for i := 0; i < indexAsyncWorkers; i++ {
		go func() {
			for fn := range q {
				if fn != nil {
					fn()
				}
			}
		}()
	}
}

func (s *Service) scheduleIndexAsync(fn func()) {
	if s == nil || fn == nil {
		return
	}
	q := s.indexAsyncQueue
	if q == nil {
		fn()
		return
	}
	select {
	case q <- fn:
	default:
		// Backpressure fallback: execute inline to avoid dropping index work.
		fn()
	}
}
