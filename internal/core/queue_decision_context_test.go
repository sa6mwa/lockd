package core

import (
	"context"
	"testing"
	"time"
)

func TestQueueDecisionContextIgnoresParentCancel(t *testing.T) {
	svc := &Service{queueDecisionApplyTimeout: 500 * time.Millisecond}
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := svc.queueDecisionContext(parent)
	if cancel == nil {
		t.Fatalf("expected cancel func for queue decision context")
	}
	cancelParent()
	if err := ctx.Err(); err != nil {
		t.Fatalf("expected decision context to ignore parent cancel, got %v", err)
	}
	cancel()
}
