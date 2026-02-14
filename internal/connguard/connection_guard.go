package connguard

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"pkt.systems/lockd/internal/svcfields"
	"pkt.systems/pslog"
)

// ConnectionGuardConfig controls connection-level protection used before the HTTP
// layer handles traffic.
type ConnectionGuardConfig struct {
	// Enabled toggles guard enforcement.
	Enabled bool
	// FailureThreshold is the number of suspicious events before hard blocking.
	FailureThreshold int
	// FailureWindow defines the period for counting suspicious events.
	FailureWindow time.Duration
	// BlockDuration is how long a blocked IP remains blocked.
	BlockDuration time.Duration
	// ProbeTimeout is the timeout for pre-classification probes on plain TCP.
	ProbeTimeout time.Duration
}

type connectionEvent struct {
	failures     []time.Time
	blockedUntil time.Time
}

// ConnectionGuard stores suspicious-connection state and can wrap a listener.
type ConnectionGuard struct {
	cfg    ConnectionGuardConfig
	logger pslog.Logger
	mu     sync.Mutex
	now    func() time.Time
	events map[string]*connectionEvent
}

// NewConnectionGuard constructs a connection guard with supplied config.
func NewConnectionGuard(cfg ConnectionGuardConfig, logger pslog.Logger) *ConnectionGuard {
	if cfg.FailureThreshold < 0 {
		cfg.FailureThreshold = 0
	}
	if cfg.FailureWindow <= 0 {
		cfg.FailureWindow = 1 * time.Second
	}
	if cfg.BlockDuration <= 0 {
		cfg.BlockDuration = 5 * time.Minute
	}
	if cfg.ProbeTimeout < 0 {
		cfg.ProbeTimeout = 0
	}
	if logger == nil {
		logger = pslog.NoopLogger()
	}
	return &ConnectionGuard{
		cfg:    cfg,
		logger: svcfields.WithSubsystem(logger, "control.connguard"),
		now:    time.Now,
		events: make(map[string]*connectionEvent),
	}
}

// WrapListener returns a listener enforcing connection guard behavior.
func (g *ConnectionGuard) WrapListener(ln net.Listener, tlsConfig *tls.Config) net.Listener {
	if g == nil || !g.cfg.Enabled || ln == nil {
		return ln
	}
	return &guardedListener{
		Listener:  ln,
		guard:     g,
		tlsConfig: tlsConfig,
	}
}

// classifyFailure records a suspicious event and returns whether the remote is blocked.
func (g *ConnectionGuard) classifyFailure(remote string, reason string) bool {
	if g == nil || g.cfg.FailureThreshold <= 0 {
		return false
	}
	remote = normalizeRemoteAddr(remote)
	if remote == "" {
		return false
	}
	now := g.now()

	g.mu.Lock()
	defer g.mu.Unlock()

	state := g.events[remote]
	if state == nil {
		state = &connectionEvent{}
		g.events[remote] = state
	}
	if !state.blockedUntil.IsZero() && state.blockedUntil.After(now) {
		return true
	}
	state.blockedUntil = time.Time{}

	cutoff := now.Add(-g.cfg.FailureWindow)
	for len(state.failures) > 0 && state.failures[0].Before(cutoff) {
		state.failures = state.failures[1:]
	}
	state.failures = append(state.failures, now)
	if len(state.failures) < g.cfg.FailureThreshold {
		g.logger.Warn("lockd.connguard.suspicious",
			"remote", remote,
			"reason", reason,
			"count", len(state.failures),
			"threshold", g.cfg.FailureThreshold)
		return false
	}

	state.blockedUntil = now.Add(g.cfg.BlockDuration)
	state.failures = nil
	g.logger.Warn("lockd.connguard.engaged",
		"remote", remote,
		"threshold", g.cfg.FailureThreshold,
		"window", g.cfg.FailureWindow,
		"duration", g.cfg.BlockDuration,
		"reason", reason)
	g.logger.Warn("lockd.connguard.blocked",
		"remote", remote,
		"threshold", g.cfg.FailureThreshold,
		"window", g.cfg.FailureWindow,
		"duration", g.cfg.BlockDuration,
		"reason", reason)
	return true
}

func (g *ConnectionGuard) isBlocked(remote string) bool {
	if g == nil || !g.cfg.Enabled {
		return false
	}
	remote = normalizeRemoteAddr(remote)
	if remote == "" {
		return false
	}
	now := g.now()

	g.mu.Lock()
	defer g.mu.Unlock()

	state := g.events[remote]
	if state == nil || state.blockedUntil.IsZero() {
		return false
	}
	if state.blockedUntil.After(now) {
		return true
	}
	state.blockedUntil = time.Time{}
	g.logger.Warn("lockd.connguard.disengaged",
		"remote", remote)
	if len(state.failures) == 0 {
		delete(g.events, remote)
	}
	return false
}

// normalizeRemoteAddr extracts just the host component.
func normalizeRemoteAddr(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(raw)
	if err == nil {
		return host
	}
	return raw
}

type guardedListener struct {
	net.Listener
	guard     *ConnectionGuard
	tlsConfig *tls.Config
}

// Accept blocks suspicious traffic before returning a connection to http.Server.
func (l *guardedListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		accepted, rejected, wrapErr := l.wrapConnection(conn)
		if !rejected && wrapErr == nil {
			return accepted, nil
		}
		if accepted != nil {
			_ = accepted.Close()
		}
	}
}

func (l *guardedListener) wrapConnection(conn net.Conn) (net.Conn, bool, error) {
	if l.guard == nil || conn == nil {
		return conn, false, nil
	}
	remote := remoteAddress(conn)
	if l.guard.isBlocked(remote) {
		l.guard.logger.Warn("lockd.connguard.rejected", "remote", remote, "reason", "blocked")
		return nil, true, errors.New("connection blocked")
	}

	if l.tlsConfig != nil {
		return l.wrapTLSConnection(conn, remote)
	}
	return l.wrapPlainConnection(conn, remote)
}

func remoteAddress(conn net.Conn) string {
	if conn == nil {
		return ""
	}
	remote := conn.RemoteAddr()
	if remote == nil {
		return ""
	}
	return remote.String()
}

func (l *guardedListener) wrapTLSConnection(conn net.Conn, remote string) (net.Conn, bool, error) {
	tlsConn := tls.Server(conn, l.tlsConfig)
	if l.guard.cfg.ProbeTimeout > 0 {
		deadline := l.guard.now().Add(l.guard.cfg.ProbeTimeout)
		if err := tlsConn.SetReadDeadline(deadline); err != nil {
			l.guard.logger.Warn("lockd.connguard.deadline", "remote", remote, "error", err)
		}
	}
	err := tlsConn.Handshake()
	_ = tlsConn.SetReadDeadline(time.Time{})
	if err == nil {
		return tlsConn, false, nil
	}
	_ = l.guard.classifyFailure(remote, "tls_handshake")
	return tlsConn, true, err
}

func (l *guardedListener) wrapPlainConnection(conn net.Conn, remote string) (net.Conn, bool, error) {
	if l.guard.cfg.ProbeTimeout <= 0 {
		return conn, false, nil
	}
	deadline := l.guard.now().Add(l.guard.cfg.ProbeTimeout)
	if err := conn.SetReadDeadline(deadline); err != nil {
		l.guard.logger.Warn("lockd.connguard.deadline", "remote", remote, "error", err)
		return conn, false, nil
	}
	buffer := make([]byte, 1)
	n, err := conn.Read(buffer)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		l.guard.classifyFailure(remote, "zero_connect")
		return conn, true, err
	}
	if n == 0 {
		l.guard.classifyFailure(remote, "zero_connect")
		return conn, true, io.EOF
	}
	return &prefixedConn{
		Conn:   conn,
		prefix: buffer[:n],
		used:   0,
	}, false, nil
}

type prefixedConn struct {
	net.Conn
	prefix []byte
	used   int
}

func (c *prefixedConn) Read(p []byte) (int, error) {
	if len(c.prefix) > c.used {
		n := copy(p, c.prefix[c.used:])
		c.used += n
		if n < len(p) {
			next, err := c.Conn.Read(p[n:])
			n += next
			return n, err
		}
		return n, nil
	}
	return c.Conn.Read(p)
}
