package mcp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	mcpoauth "pkt.systems/lockd/mcp/oauth"
	"pkt.systems/pslog"
)

const (
	headerForwarded           = "Forwarded"
	headerXForwardedFor       = "X-Forwarded-For"
	headerXRealIP             = "X-Real-IP"
	headerLockdRemoteAddr     = "X-Lockd-Remote-Addr"
	headerLockdResolvedRealIP = "X-Lockd-Resolved-Real-IP"
)

type accessLogResponseWriter struct {
	http.ResponseWriter
	statusCode int
	bytesOut   int64
}

func (w *accessLogResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *accessLogResponseWriter) Write(data []byte) (int, error) {
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(data)
	w.bytesOut += int64(n)
	return n, err
}

func (w *accessLogResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *accessLogResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *accessLogResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("hijacker not supported")
	}
	return hijacker.Hijack()
}

func (w *accessLogResponseWriter) Push(target string, opts *http.PushOptions) error {
	pusher, ok := w.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, opts)
}

func (w *accessLogResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	if rf, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		n, err := rf.ReadFrom(r)
		w.bytesOut += n
		return n, err
	}
	n, err := io.Copy(w.ResponseWriter, r)
	w.bytesOut += n
	return n, err
}

func (s *server) withHTTPAccessLogs(next http.Handler) http.Handler {
	if next == nil {
		return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		remoteAddr := strings.TrimSpace(r.RemoteAddr)
		realIP := resolveRealIP(r.Header, remoteAddr)

		headers := r.Header.Clone()
		if headers.Get(headerLockdRemoteAddr) == "" && remoteAddr != "" {
			headers.Set(headerLockdRemoteAddr, remoteAddr)
		}
		if headers.Get(headerLockdResolvedRealIP) == "" && realIP != "" {
			headers.Set(headerLockdResolvedRealIP, realIP)
		}
		r.Header = headers

		capture := &accessLogResponseWriter{ResponseWriter: w}
		next.ServeHTTP(capture, r)

		statusCode := capture.statusCode
		if statusCode == 0 {
			statusCode = http.StatusOK
		}
		path := s.sanitizeRequestPath(r.URL.Path)
		durationMS := time.Since(start).Milliseconds()
		fields := []any{
			"method", r.Method,
			"path", path,
			"status", statusCode,
			"duration_ms", durationMS,
			"bytes_out", capture.bytesOut,
			"real_ip", realIP,
			"remote_addr", remoteAddr,
		}
		if statusCode >= 400 {
			s.transportLog.Warn("mcp.http.request", fields...)
			return
		}
		s.transportLog.Debug("mcp.http.request", fields...)
	})
}

func (s *server) sanitizeRequestPath(rawPath string) string {
	path := strings.TrimSpace(rawPath)
	if path == "" {
		return "/"
	}
	transferPrefix := strings.TrimSpace(s.transferPath)
	if transferPrefix != "" && strings.HasPrefix(path, transferPrefix+"/") {
		return transferPrefix + "/:capability"
	}
	return path
}

func resolveRealIP(headers http.Header, remoteAddr string) string {
	if ip := realIPFromHeaders(headers); ip != "" {
		return ip
	}
	return hostOnly(remoteAddr)
}

func realIPFromHeaders(headers http.Header) string {
	if headers == nil {
		return ""
	}
	if ip := parseForwardedFor(headers.Get(headerForwarded)); ip != "" {
		return ip
	}
	if ip := parseXForwardedFor(headers.Get(headerXForwardedFor)); ip != "" {
		return ip
	}
	if ip := hostOnly(headers.Get(headerXRealIP)); ip != "" {
		return ip
	}
	if ip := hostOnly(headers.Get(headerLockdResolvedRealIP)); ip != "" {
		return ip
	}
	if ip := hostOnly(headers.Get(headerLockdRemoteAddr)); ip != "" {
		return ip
	}
	return ""
}

func parseForwardedFor(forwarded string) string {
	forwarded = strings.TrimSpace(forwarded)
	if forwarded == "" {
		return ""
	}
	parts := strings.Split(forwarded, ",")
	for _, part := range parts {
		segments := strings.Split(part, ";")
		for _, segment := range segments {
			kv := strings.SplitN(strings.TrimSpace(segment), "=", 2)
			if len(kv) != 2 {
				continue
			}
			if !strings.EqualFold(strings.TrimSpace(kv[0]), "for") {
				continue
			}
			value := strings.Trim(strings.TrimSpace(kv[1]), "\"")
			if strings.HasPrefix(value, "[") {
				end := strings.Index(value, "]")
				if end > 1 {
					value = value[1:end]
				}
			}
			if strings.HasPrefix(strings.ToLower(value), "_") {
				continue
			}
			if host := hostOnly(value); host != "" {
				return host
			}
		}
	}
	return ""
}

func parseXForwardedFor(forwardedFor string) string {
	forwardedFor = strings.TrimSpace(forwardedFor)
	if forwardedFor == "" {
		return ""
	}
	parts := strings.Split(forwardedFor, ",")
	for _, part := range parts {
		if host := hostOnly(part); host != "" {
			return host
		}
	}
	return ""
}

func hostOnly(value string) string {
	value = strings.Trim(strings.TrimSpace(value), "\"")
	if value == "" {
		return ""
	}
	if strings.HasPrefix(value, "[") && strings.Contains(value, "]") {
		value = strings.TrimPrefix(value, "[")
		value = strings.SplitN(value, "]", 2)[0]
		return strings.TrimSpace(value)
	}
	if host, _, err := net.SplitHostPort(value); err == nil {
		return strings.TrimSpace(host)
	}
	if ip := net.ParseIP(value); ip != nil {
		return ip.String()
	}
	if strings.Count(value, ":") > 1 {
		return value
	}
	if strings.Contains(value, ":") {
		host := strings.SplitN(value, ":", 2)[0]
		return strings.TrimSpace(host)
	}
	return value
}

func withToolObservability[In, Out any](s *server, toolName string, h mcpsdk.ToolHandlerFor[In, Out]) mcpsdk.ToolHandlerFor[In, Out] {
	return func(ctx context.Context, req *mcpsdk.CallToolRequest, input In) (*mcpsdk.CallToolResult, Out, error) {
		toolLog := s.transportLog
		if toolLog == nil {
			toolLog = pslog.NoopLogger()
		}
		start := time.Now()
		clientID := ""
		sessionID := ""
		realIP := ""
		if req != nil {
			sessionID = normalizeMCPServerSessionID(req.Session)
			if req.Extra != nil {
				clientID = requestClientID(req.Extra)
				realIP = realIPFromHeaders(req.Extra.Header)
			}
		}
		if err := s.ensureRequestClientActive(clientID); err != nil {
			var zero Out
			toolLog.Warn("mcp.tool.invoke",
				"tool", toolName,
				"session_id", sessionID,
				"client_id", clientID,
				"real_ip", realIP,
				"duration_ms", time.Since(start).Milliseconds(),
				"error_code", "unauthorized",
				"error", err.Error(),
			)
			return nil, zero, fmt.Errorf("unauthorized: %w", err)
		}
		res, out, err := h(ctx, req, input)
		durationMS := time.Since(start).Milliseconds()
		if err != nil {
			env := classifyToolError(err)
			toolLog.Warn("mcp.tool.invoke",
				"tool", toolName,
				"session_id", sessionID,
				"client_id", clientID,
				"real_ip", realIP,
				"duration_ms", durationMS,
				"error_code", env.ErrorCode,
				"retryable", env.Retryable,
				"http_status", env.HTTPStatus,
			)
			return res, out, err
		}
		toolLog.Info("mcp.tool.invoke",
			"tool", toolName,
			"session_id", sessionID,
			"client_id", clientID,
			"real_ip", realIP,
			"duration_ms", durationMS,
		)
		return res, out, nil
	}
}

func withObservedTool[In, Out any](s *server, toolName string, h mcpsdk.ToolHandlerFor[In, Out]) mcpsdk.ToolHandlerFor[In, Out] {
	return withStructuredToolErrors(withToolObservability(s, toolName, h))
}

func (s *server) ensureRequestClientActive(clientID string) error {
	clientID = strings.TrimSpace(clientID)
	if clientID == "" || s.oauthManager == nil {
		return nil
	}
	if err := s.oauthManager.EnsureClientActive(clientID); err != nil {
		if errors.Is(err, mcpoauth.ErrClientInactive) {
			return err
		}
		return fmt.Errorf("check oauth client activity: %w", err)
	}
	return nil
}
