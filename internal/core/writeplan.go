package core

import (
	"context"

	"pkt.systems/lockd/internal/storage"
)

type writePlan struct {
	commitGroup *storage.CommitGroup
	commitCtx   context.Context
	ownsGroup   bool
}

func (s *Service) newWritePlan(ctx context.Context) *writePlan {
	if group, ok := storage.CommitGroupFromContext(ctx); ok && group != nil {
		return &writePlan{
			commitGroup: group,
			commitCtx:   ctx,
			ownsGroup:   false,
		}
	}
	group := storage.NewCommitGroup()
	commitCtx := storage.ContextWithCommitGroup(ctx, group)
	commitCtx = s.maybeNoSync(commitCtx)
	return &writePlan{
		commitGroup: group,
		commitCtx:   commitCtx,
		ownsGroup:   true,
	}
}

func (p *writePlan) Context() context.Context {
	if p == nil {
		return context.Background()
	}
	if p.commitCtx == nil {
		return context.Background()
	}
	return p.commitCtx
}

func (p *writePlan) AddFinalizer(fn func() error) {
	if p == nil || p.commitGroup == nil || fn == nil {
		return
	}
	p.commitGroup.AddFinalizer(fn)
}

func (p *writePlan) Wait(err error) error {
	if p == nil || !p.ownsGroup || p.commitGroup == nil {
		return err
	}
	if err != nil {
		if waitErr := p.commitGroup.Wait(); waitErr != nil {
			return waitErr
		}
		return err
	}
	if waitErr := p.commitGroup.Wait(); waitErr != nil {
		return waitErr
	}
	return nil
}
