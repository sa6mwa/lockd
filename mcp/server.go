package mcp

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"pkt.systems/lockd"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/svcfields"
	"pkt.systems/lockd/mcp/oauth"
	"pkt.systems/lockd/mcp/state"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"
)

// Config controls lockd MCP server runtime behavior.
type Config struct {
	Listen                    string
	DisableTLS                bool
	BundlePath                string
	UpstreamServer            string
	DefaultNamespace          string
	AgentBusQueue             string
	UpstreamDisableMTLS       bool
	UpstreamClientBundlePath  string
	UpstreamHTTPTimeout       time.Duration
	UpstreamCloseTimeout      time.Duration
	UpstreamKeepAliveTimeout  time.Duration
	OAuthStatePath            string
	OAuthRefreshStorePath     string
	MCPPath                   string
	OAuthProtectedResourceURL string
}

// Server is the MCP facade service contract.
type Server interface {
	Run(context.Context) error
}

// NewServerRequest wraps constructor inputs.
type NewServerRequest struct {
	Config Config
	Logger pslog.Logger
}

type server struct {
	cfg            Config
	logger         pslog.Logger
	oauthLogger    pslog.Logger
	lifecycleLog   pslog.Logger
	transportLog   pslog.Logger
	subscribeLog   pslog.Logger
	upstream       *lockdclient.Client
	oauthManager   *oauth.Manager
	subscriptions  *subscriptionManager
	httpServer     *http.Server
	tlsBundlePath  string
	resourceID     string
	metadataPath   string
	mcpHTTPPath    string
	oauthWellKnown string
}

// NewServer constructs the lockd MCP facade service.
func NewServer(req NewServerRequest) (Server, error) {
	cfg := req.Config
	applyDefaults(&cfg)
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	logger := req.Logger
	if logger == nil {
		logger = pslog.NewStructured(context.Background(), os.Stderr).With("app", "lockd")
	}

	s := &server{
		cfg:            cfg,
		logger:         logger,
		oauthLogger:    svcfields.WithSubsystem(logger, "mcp.oauth"),
		lifecycleLog:   svcfields.WithSubsystem(logger, "server.lifecycle.mcp"),
		transportLog:   svcfields.WithSubsystem(logger, "mcp.transport.http"),
		subscribeLog:   svcfields.WithSubsystem(logger, "mcp.queue.subscriptions"),
		mcpHTTPPath:    cleanHTTPPath(cfg.MCPPath),
		metadataPath:   "/.well-known/oauth-protected-resource",
		oauthWellKnown: "/.well-known/oauth-authorization-server",
	}

	upstream, err := newUpstreamClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	s.upstream = upstream
	s.subscriptions = newSubscriptionManager(upstream, s.subscribeLog)

	if !cfg.DisableTLS {
		oauthMgr, err := oauth.NewManager(oauth.ManagerConfig{
			StatePath:    cfg.OAuthStatePath,
			RefreshStore: cfg.OAuthRefreshStorePath,
			Logger:       s.oauthLogger,
		})
		if err != nil {
			if errors.Is(err, state.ErrNotBootstrapped) {
				return nil, fmt.Errorf("mcp oauth state missing: run 'lockd mcp bootstrap' before starting the MCP server")
			}
			return nil, err
		}
		s.oauthManager = oauthMgr
	}

	if !cfg.DisableTLS {
		bundlePath := strings.TrimSpace(cfg.BundlePath)
		if bundlePath == "" {
			var err error
			bundlePath, err = lockd.DefaultBundlePath()
			if err != nil {
				return nil, fmt.Errorf("resolve default server bundle path: %w", err)
			}
		}
		bundlePath, err = absPath(bundlePath)
		if err != nil {
			return nil, err
		}
		s.tlsBundlePath = bundlePath
	}

	if s.oauthManager != nil {
		issuer, err := s.oauthManager.Issuer()
		if err != nil {
			return nil, err
		}
		s.resourceID = strings.TrimSpace(cfg.OAuthProtectedResourceURL)
		if s.resourceID == "" {
			s.resourceID = joinURL(issuer, s.mcpHTTPPath)
		}
	}

	mux, err := s.buildMux()
	if err != nil {
		return nil, err
	}
	s.httpServer = &http.Server{
		Addr:    cfg.Listen,
		Handler: mux,
	}

	return s, nil
}

func (s *server) Run(ctx context.Context) error {
	s.lifecycleLog.Info("starting lockd MCP facade", "listen", s.cfg.Listen, "disable_tls", s.cfg.DisableTLS, "mcp_path", s.mcpHTTPPath)
	defer func() {
		if s.subscriptions != nil {
			s.subscriptions.Close()
		}
		if s.upstream != nil {
			_ = s.upstream.Close()
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		if s.cfg.DisableTLS {
			errCh <- s.httpServer.ListenAndServe()
			return
		}

		bundle, err := tlsutil.LoadBundle(s.tlsBundlePath, "")
		if err != nil {
			errCh <- fmt.Errorf("load mcp TLS bundle: %w", err)
			return
		}
		tlsConfig := &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{bundle.ServerCertificate},
		}
		ln, err := net.Listen("tcp", s.cfg.Listen)
		if err != nil {
			errCh <- fmt.Errorf("listen %s: %w", s.cfg.Listen, err)
			return
		}
		errCh <- s.httpServer.Serve(tls.NewListener(ln, tlsConfig))
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return nil
	case err := <-errCh:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (s *server) buildMux() (*http.ServeMux, error) {
	mcpSrv := mcpsdk.NewServer(&mcpsdk.Implementation{
		Name:    "lockd-mcp-facade",
		Version: "0.1.0",
	}, &mcpsdk.ServerOptions{
		Instructions:       defaultServerInstructions(s.cfg),
		InitializedHandler: s.handleInitialized,
	})
	s.registerResources(mcpSrv)
	s.registerTools(mcpSrv)

	streamable := mcpsdk.NewStreamableHTTPHandler(func(_ *http.Request) *mcpsdk.Server {
		return mcpSrv
	}, nil)

	var mcpHandler http.Handler = streamable
	if s.oauthManager != nil {
		mcpHandler = mcpauth.RequireBearerToken(
			s.oauthManager.VerifyToken,
			&mcpauth.RequireBearerTokenOptions{ResourceMetadataURL: s.metadataPath},
		)(mcpHandler)
	}

	mux := http.NewServeMux()
	mux.Handle(s.mcpHTTPPath, mcpHandler)
	if s.oauthManager != nil {
		mux.HandleFunc("/authorize", s.oauthManager.HandleAuthorize)
		mux.HandleFunc("/token", s.oauthManager.HandleToken)
		mux.HandleFunc(s.oauthWellKnown, s.oauthManager.HandleAuthServerMetadata)
		mux.HandleFunc(s.metadataPath, s.handleProtectedResourceMetadata)
	}
	return mux, nil
}

func (s *server) handleProtectedResourceMetadata(w http.ResponseWriter, r *http.Request) {
	meta, err := s.oauthManager.ProtectedResourceMetadata(s.resourceID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	mcpauth.ProtectedResourceMetadataHandler(meta).ServeHTTP(w, r)
}

func (s *server) registerTools(srv *mcpsdk.Server) {
	descriptions := buildToolDescriptions(s.cfg)
	desc := func(name string) string {
		description, ok := descriptions[name]
		if !ok {
			panic(fmt.Sprintf("missing MCP tool description for %q", name))
		}
		return description
	}

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolLockAcquire,
		Description: desc(toolLockAcquire),
	}, s.handleLockAcquireTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolLockKeepAlive,
		Description: desc(toolLockKeepAlive),
	}, s.handleLockKeepAliveTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolLockRelease,
		Description: desc(toolLockRelease),
	}, s.handleLockReleaseTool)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolGet,
		Description: desc(toolGet),
	}, s.handleGetTool)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQuery,
		Description: desc(toolQuery),
	}, s.handleQueryTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolStateUpdate,
		Description: desc(toolStateUpdate),
	}, s.handleStateUpdateTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolStateMetadata,
		Description: desc(toolStateMetadata),
	}, s.handleStateMetadataTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolStateRemove,
		Description: desc(toolStateRemove),
	}, s.handleStateRemoveTool)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsPut,
		Description: desc(toolAttachmentsPut),
	}, s.handleAttachmentPutTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsList,
		Description: desc(toolAttachmentsList),
	}, s.handleAttachmentListTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsGet,
		Description: desc(toolAttachmentsGet),
	}, s.handleAttachmentGetTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsDelete,
		Description: desc(toolAttachmentsDelete),
	}, s.handleAttachmentDeleteTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsDeleteAll,
		Description: desc(toolAttachmentsDeleteAll),
	}, s.handleAttachmentDeleteAllTool)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolNamespaceGet,
		Description: desc(toolNamespaceGet),
	}, s.handleNamespaceGetTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolNamespaceUpdate,
		Description: desc(toolNamespaceUpdate),
	}, s.handleNamespaceUpdateTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolIndexFlush,
		Description: desc(toolIndexFlush),
	}, s.handleIndexFlushTool)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolHelp,
		Description: desc(toolHelp),
	}, s.handleHelpTool)

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueEnqueue,
		Description: desc(toolQueueEnqueue),
	}, s.handleQueueEnqueueTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueDequeue,
		Description: desc(toolQueueDequeue),
	}, s.handleQueueDequeueTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueAck,
		Description: desc(toolQueueAck),
	}, s.handleQueueAckTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueDefer,
		Description: desc(toolQueueDefer),
	}, s.handleQueueDeferTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueSubscribe,
		Description: desc(toolQueueSubscribe),
	}, s.handleQueueSubscribeTool)
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueUnsubscribe,
		Description: desc(toolQueueUnsubscribe),
	}, s.handleQueueUnsubscribeTool)
}

type getToolInput struct {
	Key       string `json:"key" jsonschema:"Lock key to read"`
	Namespace string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	Public    bool   `json:"public,omitempty" jsonschema:"Allow public-read fallback when true"`
	LeaseID   string `json:"lease_id,omitempty" jsonschema:"Optional lease ID for lease-bound reads"`
}

type getToolOutput struct {
	Namespace string          `json:"namespace"`
	Key       string          `json:"key"`
	Found     bool            `json:"found"`
	ETag      string          `json:"etag,omitempty"`
	Version   string          `json:"version,omitempty"`
	Value     json.RawMessage `json:"value,omitempty"`
}

func (s *server) handleGetTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input getToolInput) (*mcpsdk.CallToolResult, getToolOutput, error) {
	key := strings.TrimSpace(input.Key)
	if key == "" {
		return nil, getToolOutput{}, fmt.Errorf("key is required")
	}

	opts := []lockdclient.GetOption{}
	if ns := strings.TrimSpace(input.Namespace); ns != "" {
		opts = append(opts, lockdclient.WithGetNamespace(ns))
	}
	if lease := strings.TrimSpace(input.LeaseID); lease != "" {
		opts = append(opts, lockdclient.WithGetLeaseID(lease))
	}
	if !input.Public {
		opts = append(opts, lockdclient.WithGetPublicDisabled(true))
	}

	resp, err := s.upstream.Get(ctx, key, opts...)
	if err != nil {
		return nil, getToolOutput{}, err
	}
	defer resp.Close()

	out := getToolOutput{
		Namespace: resp.Namespace,
		Key:       resp.Key,
		Found:     resp.HasState,
		ETag:      resp.ETag,
		Version:   resp.Version,
	}
	if !resp.HasState {
		return nil, out, nil
	}
	body, err := resp.Bytes()
	if err != nil {
		return nil, getToolOutput{}, err
	}
	if len(body) > 0 {
		out.Value = json.RawMessage(append([]byte(nil), body...))
	}
	return nil, out, nil
}

type queryToolInput struct {
	Query     string `json:"query" jsonschema:"LQL query expression"`
	Namespace string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
	Limit     int    `json:"limit,omitempty" jsonschema:"Maximum rows to return"`
	Cursor    string `json:"cursor,omitempty" jsonschema:"Continuation cursor"`
}

type queryToolOutput struct {
	Namespace string   `json:"namespace"`
	Keys      []string `json:"keys"`
	Cursor    string   `json:"cursor,omitempty"`
	IndexSeq  uint64   `json:"index_seq,omitempty"`
}

func (s *server) handleQueryTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input queryToolInput) (*mcpsdk.CallToolResult, queryToolOutput, error) {
	expr := strings.TrimSpace(input.Query)
	if expr == "" {
		return nil, queryToolOutput{}, fmt.Errorf("query is required")
	}

	opts := []lockdclient.QueryOption{lockdclient.WithQuery(expr)}
	if ns := strings.TrimSpace(input.Namespace); ns != "" {
		opts = append(opts, lockdclient.WithQueryNamespace(ns))
	}
	if input.Limit > 0 {
		opts = append(opts, lockdclient.WithQueryLimit(input.Limit))
	}
	if cursor := strings.TrimSpace(input.Cursor); cursor != "" {
		opts = append(opts, lockdclient.WithQueryCursor(cursor))
	}

	resp, err := s.upstream.Query(ctx, opts...)
	if err != nil {
		return nil, queryToolOutput{}, err
	}
	defer resp.Close()

	return nil, queryToolOutput{
		Namespace: resp.Namespace,
		Keys:      resp.Keys(),
		Cursor:    resp.Cursor,
		IndexSeq:  resp.IndexSeq,
	}, nil
}

func newUpstreamClient(cfg Config, logger pslog.Logger) (*lockdclient.Client, error) {
	endpoints, err := lockdclient.ParseEndpoints(cfg.UpstreamServer, cfg.UpstreamDisableMTLS)
	if err != nil {
		return nil, err
	}
	opts := []lockdclient.Option{
		lockdclient.WithDisableMTLS(cfg.UpstreamDisableMTLS),
		lockdclient.WithDefaultNamespace(cfg.DefaultNamespace),
		lockdclient.WithHTTPTimeout(cfg.UpstreamHTTPTimeout),
		lockdclient.WithCloseTimeout(cfg.UpstreamCloseTimeout),
		lockdclient.WithKeepAliveTimeout(cfg.UpstreamKeepAliveTimeout),
		lockdclient.WithDrainAwareShutdown(true),
		lockdclient.WithLogger(svcfields.WithSubsystem(logger, "client.sdk")),
	}

	if !cfg.UpstreamDisableMTLS {
		bundlePath, err := lockd.ResolveClientBundlePathWithHint(lockd.ClientBundleRoleSDK, cfg.UpstreamClientBundlePath, "--client-bundle")
		if err != nil {
			return nil, fmt.Errorf("resolve mcp upstream client bundle: %w", err)
		}
		opts = append(opts, lockdclient.WithBundlePath(bundlePath))
	}

	return lockdclient.NewWithEndpoints(endpoints, opts...)
}

func applyDefaults(cfg *Config) {
	if strings.TrimSpace(cfg.Listen) == "" {
		cfg.Listen = "127.0.0.1:19341"
	}
	if strings.TrimSpace(cfg.UpstreamServer) == "" {
		cfg.UpstreamServer = "https://127.0.0.1:9341"
	}
	if strings.TrimSpace(cfg.DefaultNamespace) == "" {
		cfg.DefaultNamespace = "mcp"
	}
	cfg.DefaultNamespace = strings.TrimSpace(cfg.DefaultNamespace)
	if strings.TrimSpace(cfg.AgentBusQueue) == "" {
		cfg.AgentBusQueue = "lockd.agent.bus"
	}
	cfg.AgentBusQueue = strings.TrimSpace(cfg.AgentBusQueue)
	if cfg.UpstreamHTTPTimeout <= 0 {
		cfg.UpstreamHTTPTimeout = lockdclient.DefaultHTTPTimeout
	}
	if cfg.UpstreamCloseTimeout <= 0 {
		cfg.UpstreamCloseTimeout = lockdclient.DefaultCloseTimeout
	}
	if cfg.UpstreamKeepAliveTimeout <= 0 {
		cfg.UpstreamKeepAliveTimeout = lockdclient.DefaultKeepAliveTimeout
	}
	if strings.TrimSpace(cfg.MCPPath) == "" {
		cfg.MCPPath = "/mcp"
	}
}

func validateConfig(cfg Config) error {
	if strings.TrimSpace(cfg.Listen) == "" {
		return fmt.Errorf("listen address required")
	}
	if strings.TrimSpace(cfg.UpstreamServer) == "" {
		return fmt.Errorf("upstream lockd server required")
	}
	return nil
}

func cleanHTTPPath(raw string) string {
	p := strings.TrimSpace(raw)
	if p == "" {
		return "/mcp"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}

func joinURL(baseURL, p string) string {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return p
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return baseURL + p
	}
	u.Path = path.Join(u.Path, strings.TrimPrefix(p, "/"))
	return u.String()
}

func absPath(p string) (string, error) {
	return filepath.Abs(p)
}
