package storage

import (
	"context"
	"sync"
)

type commitGroupKey struct{}

const commitGroupInlineLimit = 8

type committerEntry struct {
	key any
	fn  func() <-chan error
}

// CommitGroup collects storage commit notifications so callers can wait once.
type CommitGroup struct {
	mu         sync.Mutex
	wait       []<-chan error
	committers map[any]func() <-chan error
	commitList []committerEntry
	finalizers []func() error
	committed  bool
	pooled     bool
}

// NewCommitGroup creates a commit group for batching storage commits.
var commitGroupPool = sync.Pool{
	New: func() any {
		return &CommitGroup{pooled: true}
	},
}

// NewCommitGroup creates a commit group for batching storage commits.
func NewCommitGroup() *CommitGroup {
	group := commitGroupPool.Get().(*CommitGroup)
	group.reset()
	return group
}

// Add registers a commit notification channel with the group.
func (g *CommitGroup) Add(ch <-chan error) {
	if g == nil || ch == nil {
		return
	}
	g.mu.Lock()
	g.wait = append(g.wait, ch)
	g.mu.Unlock()
}

// AddCommitter registers a deferred commit action keyed by key.
// The commit action is invoked once when Wait is called.
// Returns true when a committer was registered or invoked.
func (g *CommitGroup) AddCommitter(key any, fn func() <-chan error) bool {
	if g == nil || fn == nil {
		return false
	}
	g.mu.Lock()
	if g.committed {
		g.mu.Unlock()
		if ch := fn(); ch != nil {
			go func() {
				<-ch
			}()
		}
		return true
	}
	if g.committers != nil {
		if _, exists := g.committers[key]; !exists {
			g.committers[key] = fn
			g.mu.Unlock()
			return true
		}
		g.mu.Unlock()
		return false
	}
	for i := range g.commitList {
		if g.commitList[i].key == key {
			g.mu.Unlock()
			return false
		}
	}
	g.commitList = append(g.commitList, committerEntry{key: key, fn: fn})
	if len(g.commitList) > commitGroupInlineLimit {
		g.committers = make(map[any]func() <-chan error, len(g.commitList))
		for _, entry := range g.commitList {
			if entry.fn == nil {
				continue
			}
			g.committers[entry.key] = entry.fn
		}
		g.commitList = nil
	}
	g.mu.Unlock()
	return true
}

// AddFinalizer registers a callback that runs after committers complete successfully.
func (g *CommitGroup) AddFinalizer(fn func() error) {
	if g == nil || fn == nil {
		return
	}
	g.mu.Lock()
	if g.committed {
		g.mu.Unlock()
		_ = fn()
		return
	}
	g.finalizers = append(g.finalizers, fn)
	g.mu.Unlock()
}

// Wait blocks until all registered commits finish, returning the first error.
func (g *CommitGroup) Wait() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	g.committed = true
	committers := g.committers
	g.committers = nil
	commitList := append([]committerEntry(nil), g.commitList...)
	g.commitList = nil
	waiters := append([]<-chan error(nil), g.wait...)
	g.wait = nil
	finalizers := append([]func() error(nil), g.finalizers...)
	g.finalizers = nil
	g.mu.Unlock()
	for _, entry := range commitList {
		if entry.fn == nil {
			continue
		}
		if ch := entry.fn(); ch != nil {
			waiters = append(waiters, ch)
		}
	}
	for _, fn := range committers {
		if fn == nil {
			continue
		}
		if ch := fn(); ch != nil {
			waiters = append(waiters, ch)
		}
	}
	var firstErr error
	for _, ch := range waiters {
		if ch == nil {
			continue
		}
		if err := <-ch; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		g.release()
		return firstErr
	}
	for _, fn := range finalizers {
		if fn == nil {
			continue
		}
		if err := fn(); err != nil {
			g.release()
			return err
		}
	}
	g.release()
	return firstErr
}

func (g *CommitGroup) reset() {
	g.wait = nil
	g.committers = nil
	g.commitList = nil
	g.finalizers = nil
	g.committed = false
}

func (g *CommitGroup) release() {
	if g == nil || !g.pooled {
		return
	}
	g.reset()
	commitGroupPool.Put(g)
}

// ContextWithCommitGroup attaches a commit group to ctx.
func ContextWithCommitGroup(ctx context.Context, group *CommitGroup) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, commitGroupKey{}, group)
}

// CommitGroupFromContext retrieves a commit group from ctx when present.
func CommitGroupFromContext(ctx context.Context) (*CommitGroup, bool) {
	if ctx == nil {
		return nil, false
	}
	group, ok := ctx.Value(commitGroupKey{}).(*CommitGroup)
	return group, ok
}
