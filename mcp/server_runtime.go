package mcp

import (
	"context"
	"fmt"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func (s *server) handleInitialized(ctx context.Context, req *mcpsdk.InitializedRequest) {
	if req == nil || req.Session == nil || s.subscriptions == nil {
		return
	}
	namespace := s.resolveNamespace("")
	queue := s.resolveQueue("")
	clientID := requestClientID(req.Extra)
	if _, err := s.subscriptions.Subscribe(ctx, req.Session, clientID, namespace, queue); err != nil {
		s.subscribeLog.Warn("mcp.queue.subscription.auto_subscribe_failed",
			"session_id", req.Session.ID(),
			"client_id", clientID,
			"namespace", namespace,
			"queue", queue,
			"error", err,
		)
		return
	}
	s.subscribeLog.Info("mcp.queue.subscription.auto_subscribed",
		"session_id", req.Session.ID(),
		"client_id", clientID,
		"namespace", namespace,
		"queue", queue,
	)
}

func (s *server) resolveNamespace(raw string) string {
	ns := strings.TrimSpace(raw)
	if ns != "" {
		return ns
	}
	return s.cfg.DefaultNamespace
}

func (s *server) resolveQueue(raw string) string {
	queue := strings.TrimSpace(raw)
	if queue != "" {
		return queue
	}
	return s.cfg.AgentBusQueue
}

func requestClientID(extra *mcpsdk.RequestExtra) string {
	if extra == nil || extra.TokenInfo == nil {
		return ""
	}
	return strings.TrimSpace(extra.TokenInfo.UserID)
}

func defaultOwner(clientID string) string {
	clientID = strings.TrimSpace(clientID)
	if clientID == "" {
		return "mcp-worker"
	}
	return fmt.Sprintf("mcp-%s", clientID)
}
