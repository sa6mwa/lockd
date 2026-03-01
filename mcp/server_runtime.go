package mcp

import (
	"context"
	"fmt"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	mcpoauth "pkt.systems/lockd/mcp/oauth"
)

func (s *server) handleInitialized(ctx context.Context, req *mcpsdk.InitializedRequest) {
	if req == nil || req.Session == nil || s.subscriptions == nil {
		return
	}
	namespace := s.resolveNamespace("", req.Extra)
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

func (s *server) resolveNamespace(raw string, extra *mcpsdk.RequestExtra) string {
	ns := strings.TrimSpace(raw)
	if ns != "" {
		return ns
	}
	ns = requestClientNamespace(extra)
	if ns != "" {
		return ns
	}
	if s.oauthManager != nil {
		clientID := requestClientID(extra)
		if clientID != "" {
			namespace, err := s.oauthManager.ClientNamespace(clientID)
			if err == nil && strings.TrimSpace(namespace) != "" {
				return strings.TrimSpace(namespace)
			}
		}
	}
	return s.cfg.DefaultNamespace
}

func (s *server) resolveNamespaceForCall(raw string, req *mcpsdk.CallToolRequest) string {
	if req == nil {
		return s.resolveNamespace(raw, nil)
	}
	return s.resolveNamespace(raw, req.Extra)
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

func requestClientNamespace(extra *mcpsdk.RequestExtra) string {
	if extra == nil || extra.TokenInfo == nil || len(extra.TokenInfo.Extra) == 0 {
		return ""
	}
	raw, ok := extra.TokenInfo.Extra[mcpoauth.TokenInfoExtraDefaultNamespace]
	if !ok {
		return ""
	}
	value, ok := raw.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func defaultOwner(clientID string) string {
	clientID = strings.TrimSpace(clientID)
	if clientID == "" {
		return "mcp-worker"
	}
	return fmt.Sprintf("mcp-%s", clientID)
}
