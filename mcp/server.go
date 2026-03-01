package mcp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
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
	BaseURL                   string
	AllowHTTP                 bool
	BundlePath                string
	UpstreamServer            string
	InlineMaxBytes            int64
	DefaultNamespace          string
	AgentBusQueue             string
	UpstreamDisableMTLS       bool
	UpstreamClientBundlePath  string
	UpstreamHTTPTimeout       time.Duration
	UpstreamCloseTimeout      time.Duration
	UpstreamKeepAliveTimeout  time.Duration
	UpstreamDrainAware        *bool
	OAuthStatePath            string
	OAuthTokenStorePath       string
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
	cfg                Config
	logger             pslog.Logger
	oauthLogger        pslog.Logger
	lifecycleLog       pslog.Logger
	transportLog       pslog.Logger
	transferLog        pslog.Logger
	subscribeLog       pslog.Logger
	writeStreamLog     pslog.Logger
	upstream           *lockdclient.Client
	oauthManager       *oauth.Manager
	subscriptions      *subscriptionManager
	writeStreams       *writeStreamManager
	transfers          *transferManager
	httpServer         *http.Server
	tlsBundlePath      string
	resourceID         string
	metadataPath       string
	metadataURL        string
	authHTTPPath       string
	mcpHTTPPath        string
	mcpServePath       string
	oauthWellKnown     string
	openIDWellKnown    string
	oauthAuthorizePath string
	oauthTokenPath     string
	oauthRegisterPath  string
	baseURL            *url.URL
	transferPath       string
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
		transferLog:    svcfields.WithSubsystem(logger, "mcp.transport.transfer"),
		subscribeLog:   svcfields.WithSubsystem(logger, "mcp.queue.subscriptions"),
		writeStreamLog: svcfields.WithSubsystem(logger, "mcp.write.streams"),
		mcpHTTPPath:    cleanHTTPPath(cfg.MCPPath),
	}
	baseURL, err := parseBaseURL(cfg.BaseURL, cfg.AllowHTTP)
	if err != nil {
		return nil, err
	}
	s.baseURL = baseURL
	s.authHTTPPath = cleanHTTPPath(baseURL.Path)
	s.mcpServePath = scopedHTTPPath(s.authHTTPPath, s.mcpHTTPPath)
	s.transferPath = path.Join(s.mcpServePath, "transfer")
	s.oauthAuthorizePath = prefixedOAuthEndpointPath(s.authHTTPPath, "authorize")
	s.oauthTokenPath = prefixedOAuthEndpointPath(s.authHTTPPath, "token")
	s.oauthRegisterPath = prefixedOAuthEndpointPath(s.authHTTPPath, "register")

	upstream, err := newUpstreamClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	s.upstream = upstream
	s.subscriptions = newSubscriptionManager(upstream, s.subscribeLog)
	s.writeStreams = newWriteStreamManager(s.writeStreamLog)
	s.transfers = newTransferManager(s.transferLog)

	if !cfg.DisableTLS {
		oauthMgr, err := oauth.NewManager(oauth.ManagerConfig{
			StatePath:  cfg.OAuthStatePath,
			TokenStore: cfg.OAuthTokenStorePath,
			Logger:     s.oauthLogger,
		})
		if err != nil {
			if errors.Is(err, state.ErrNotBootstrapped) {
				return nil, fmt.Errorf("mcp oauth state missing: initialize with 'lockd mcp issuer set <url>' or 'lockd mcp client add'")
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
		issuerPath := urlPathPrefix(issuer)
		if issuerPath != s.authHTTPPath {
			return nil, fmt.Errorf("issuer path %q must match --base-url path %q for MCP scoped routing (run 'lockd mcp issuer set <url>' with matching path)", issuerPath, s.authHTTPPath)
		}
		s.resourceID = strings.TrimSpace(cfg.OAuthProtectedResourceURL)
		if s.resourceID == "" {
			s.resourceID = joinURL(issuer, s.mcpHTTPPath)
		}
		s.oauthWellKnown = oauthAuthorizationServerWellKnownPath(s.authHTTPPath, issuerPath)
		s.openIDWellKnown = openIDConfigurationWellKnownPath(s.authHTTPPath)
		s.metadataPath = oauthProtectedResourceWellKnownPath(s.authHTTPPath, urlPathPrefix(s.resourceID))
		s.metadataURL = absoluteURLForPath(s.baseURL, s.metadataPath)
	}

	mux, err := s.buildMux()
	if err != nil {
		return nil, err
	}
	s.httpServer = &http.Server{
		Addr:    cfg.Listen,
		Handler: s.withHTTPAccessLogs(mux),
	}

	return s, nil
}

func (s *server) Run(ctx context.Context) error {
	s.lifecycleLog.Info("starting lockd MCP facade", "listen", s.cfg.Listen, "disable_tls", s.cfg.DisableTLS, "mcp_path", s.mcpHTTPPath)
	defer func() {
		if s.subscriptions != nil {
			s.subscriptions.Close()
		}
		if s.writeStreams != nil {
			s.writeStreams.Close()
		}
		if s.transfers != nil {
			s.transfers.Close()
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
			&mcpauth.RequireBearerTokenOptions{ResourceMetadataURL: s.metadataURL},
		)(mcpHandler)
	}

	mux := http.NewServeMux()
	mux.Handle(s.mcpServePath, mcpHandler)
	mux.HandleFunc(s.transferPath+"/", s.handleTransfer)
	if s.oauthManager != nil {
		mux.HandleFunc(s.oauthAuthorizePath, s.oauthManager.HandleAuthorize)
		mux.HandleFunc(s.oauthTokenPath, s.oauthManager.HandleToken)
		mux.HandleFunc(s.oauthRegisterPath, s.oauthManager.HandleRegister)
		mux.HandleFunc(s.oauthWellKnown, s.oauthManager.HandleAuthServerMetadata)
		mux.HandleFunc(s.openIDWellKnown, s.oauthManager.HandleOpenIDConfiguration)
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
	tool := func(name string) *mcpsdk.Tool {
		return &mcpsdk.Tool{
			Name:        name,
			Description: desc(name),
			Annotations: toolAnnotations(name),
		}
	}

	mcpsdk.AddTool(srv, tool(toolLockAcquire), withObservedTool(s, toolLockAcquire, s.handleLockAcquireTool))
	mcpsdk.AddTool(srv, tool(toolLockKeepAlive), withObservedTool(s, toolLockKeepAlive, s.handleLockKeepAliveTool))
	mcpsdk.AddTool(srv, tool(toolLockRelease), withObservedTool(s, toolLockRelease, s.handleLockReleaseTool))

	mcpsdk.AddTool(srv, tool(toolGet), withObservedTool(s, toolGet, s.handleGetTool))
	mcpsdk.AddTool(srv, tool(toolDescribe), withObservedTool(s, toolDescribe, s.handleDescribeTool))

	mcpsdk.AddTool(srv, tool(toolQuery), withObservedTool(s, toolQuery, s.handleQueryTool))
	mcpsdk.AddTool(srv, tool(toolQueryStream), withObservedTool(s, toolQueryStream, s.handleQueryStreamTool))
	mcpsdk.AddTool(srv, tool(toolStatePut), withObservedTool(s, toolStatePut, s.handleStatePutTool))
	mcpsdk.AddTool(srv, tool(toolStateUpdate), withObservedTool(s, toolStateUpdate, s.handleStateUpdateTool))
	mcpsdk.AddTool(srv, tool(toolStateMutate), withObservedTool(s, toolStateMutate, s.handleStateMutateTool))
	mcpsdk.AddTool(srv, tool(toolStatePatch), withObservedTool(s, toolStatePatch, s.handleStatePatchTool))
	mcpsdk.AddTool(srv, tool(toolStateWriteStreamBegin), withObservedTool(s, toolStateWriteStreamBegin, s.handleStateWriteStreamBeginTool))
	mcpsdk.AddTool(srv, tool(toolStateWriteStreamStatus), withObservedTool(s, toolStateWriteStreamStatus, s.handleStateWriteStreamStatusTool))
	mcpsdk.AddTool(srv, tool(toolStateWriteStreamCommit), withObservedTool(s, toolStateWriteStreamCommit, s.handleStateWriteStreamCommitTool))
	mcpsdk.AddTool(srv, tool(toolStateWriteStreamAbort), withObservedTool(s, toolStateWriteStreamAbort, s.handleStateWriteStreamAbortTool))
	mcpsdk.AddTool(srv, tool(toolStateStream), withObservedTool(s, toolStateStream, s.handleStateStreamTool))
	mcpsdk.AddTool(srv, tool(toolStateMetadata), withObservedTool(s, toolStateMetadata, s.handleStateMetadataTool))
	mcpsdk.AddTool(srv, tool(toolStateDelete), withObservedTool(s, toolStateDelete, s.handleStateDeleteTool))
	mcpsdk.AddTool(srv, tool(toolStateRemove), withObservedTool(s, toolStateRemove, s.handleStateRemoveTool))

	mcpsdk.AddTool(srv, tool(toolAttachmentsPut), withObservedTool(s, toolAttachmentsPut, s.handleAttachmentPutTool))
	mcpsdk.AddTool(srv, tool(toolAttachmentsWriteStreamBegin), withObservedTool(s, toolAttachmentsWriteStreamBegin, s.handleAttachmentsWriteStreamBeginTool))
	mcpsdk.AddTool(srv, tool(toolAttachmentsWriteStreamStatus), withObservedTool(s, toolAttachmentsWriteStreamStatus, s.handleAttachmentsWriteStreamStatusTool))
	mcpsdk.AddTool(srv, tool(toolAttachmentsWriteStreamCommit), withObservedTool(s, toolAttachmentsWriteStreamCommit, s.handleAttachmentsWriteStreamCommitTool))
	mcpsdk.AddTool(srv, tool(toolAttachmentsWriteStreamAbort), withObservedTool(s, toolAttachmentsWriteStreamAbort, s.handleAttachmentsWriteStreamAbortTool))
	mcpsdk.AddTool(srv, tool(toolAttachmentsList), withObservedTool(s, toolAttachmentsList, s.handleAttachmentListTool))
	mcpsdk.AddTool(srv, tool(toolAttachmentsHead), withObservedTool(s, toolAttachmentsHead, s.handleAttachmentHeadTool))
	mcpsdk.AddTool(srv, tool(toolAttachmentsChecksum), withObservedTool(s, toolAttachmentsChecksum, s.handleAttachmentChecksumTool))
	mcpsdk.AddTool(srv, tool(toolAttachmentsGet), withObservedTool(s, toolAttachmentsGet, s.handleAttachmentGetTool))
	mcpsdk.AddTool(srv, tool(toolAttachmentsStream), withObservedTool(s, toolAttachmentsStream, s.handleAttachmentStreamTool))
	mcpsdk.AddTool(srv, tool(toolAttachmentsDelete), withObservedTool(s, toolAttachmentsDelete, s.handleAttachmentDeleteTool))
	mcpsdk.AddTool(srv, tool(toolAttachmentsDeleteAll), withObservedTool(s, toolAttachmentsDeleteAll, s.handleAttachmentDeleteAllTool))

	mcpsdk.AddTool(srv, tool(toolNamespaceGet), withObservedTool(s, toolNamespaceGet, s.handleNamespaceGetTool))
	mcpsdk.AddTool(srv, tool(toolNamespaceUpdate), withObservedTool(s, toolNamespaceUpdate, s.handleNamespaceUpdateTool))
	mcpsdk.AddTool(srv, tool(toolIndexFlush), withObservedTool(s, toolIndexFlush, s.handleIndexFlushTool))

	mcpsdk.AddTool(srv, tool(toolHint), withObservedTool(s, toolHint, s.handleHintTool))
	mcpsdk.AddTool(srv, tool(toolHelp), withObservedTool(s, toolHelp, s.handleHelpTool))

	mcpsdk.AddTool(srv, tool(toolQueueEnqueue), withObservedTool(s, toolQueueEnqueue, s.handleQueueEnqueueTool))
	mcpsdk.AddTool(srv, tool(toolQueueWriteStreamBegin), withObservedTool(s, toolQueueWriteStreamBegin, s.handleQueueWriteStreamBeginTool))
	mcpsdk.AddTool(srv, tool(toolQueueWriteStreamStatus), withObservedTool(s, toolQueueWriteStreamStatus, s.handleQueueWriteStreamStatusTool))
	mcpsdk.AddTool(srv, tool(toolQueueWriteStreamCommit), withObservedTool(s, toolQueueWriteStreamCommit, s.handleQueueWriteStreamCommitTool))
	mcpsdk.AddTool(srv, tool(toolQueueWriteStreamAbort), withObservedTool(s, toolQueueWriteStreamAbort, s.handleQueueWriteStreamAbortTool))
	mcpsdk.AddTool(srv, tool(toolQueueDequeue), withObservedTool(s, toolQueueDequeue, s.handleQueueDequeueTool))
	mcpsdk.AddTool(srv, tool(toolQueueStats), withObservedTool(s, toolQueueStats, s.handleQueueStatsTool))
	mcpsdk.AddTool(srv, tool(toolQueueWatch), withObservedTool(s, toolQueueWatch, s.handleQueueWatchTool))
	mcpsdk.AddTool(srv, tool(toolQueueAck), withObservedTool(s, toolQueueAck, s.handleQueueAckTool))
	mcpsdk.AddTool(srv, tool(toolQueueNack), withObservedTool(s, toolQueueNack, s.handleQueueNackTool))
	mcpsdk.AddTool(srv, tool(toolQueueDefer), withObservedTool(s, toolQueueDefer, s.handleQueueDeferTool))
	mcpsdk.AddTool(srv, tool(toolQueueExtend), withObservedTool(s, toolQueueExtend, s.handleQueueExtendTool))
	mcpsdk.AddTool(srv, tool(toolQueueSubscribe), withObservedTool(s, toolQueueSubscribe, s.handleQueueSubscribeTool))
	mcpsdk.AddTool(srv, tool(toolQueueUnsubscribe), withObservedTool(s, toolQueueUnsubscribe, s.handleQueueUnsubscribeTool))
}

var readOnlyTools = map[string]struct{}{
	toolGet:                 {},
	toolDescribe:            {},
	toolQuery:               {},
	toolQueryStream:         {},
	toolStateStream:         {},
	toolStateMetadata:       {},
	toolAttachmentsList:     {},
	toolAttachmentsHead:     {},
	toolAttachmentsChecksum: {},
	toolAttachmentsGet:      {},
	toolAttachmentsStream:   {},
	toolNamespaceGet:        {},
	toolHint:                {},
	toolHelp:                {},
	toolQueueStats:          {},
	toolQueueWatch:          {},
}

func toolAnnotations(name string) *mcpsdk.ToolAnnotations {
	destructive := name == toolStateDelete || name == toolStateRemove || name == toolAttachmentsDelete || name == toolAttachmentsDeleteAll
	ann := &mcpsdk.ToolAnnotations{
		DestructiveHint: boolRef(destructive),
		OpenWorldHint:   boolRef(false),
	}
	if _, ok := readOnlyTools[name]; ok {
		ann.ReadOnlyHint = true
	}
	return ann
}

func boolRef(v bool) *bool {
	b := v
	return &b
}

type getToolInput struct {
	Key         string `json:"key" jsonschema:"Lock key to read"`
	Namespace   string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to client default namespace, then server default namespace)"`
	Public      *bool  `json:"public,omitempty" jsonschema:"Read mode selector: true for public read (default), false for lease-bound read"`
	LeaseID     string `json:"lease_id,omitempty" jsonschema:"Lease ID required when public=false"`
	PayloadMode string `json:"payload_mode,omitempty" jsonschema:"Payload mode: auto (default), inline, stream, or none"`
}

type getToolOutput struct {
	Namespace             string `json:"namespace"`
	Key                   string `json:"key"`
	Found                 bool   `json:"found"`
	ETag                  string `json:"etag,omitempty"`
	Version               int64  `json:"version,omitempty"`
	PayloadMode           string `json:"payload_mode"`
	PayloadBytes          int64  `json:"payload_bytes,omitempty"`
	PayloadText           string `json:"payload_text,omitempty"`
	PayloadBase64         string `json:"payload_base64,omitempty"`
	DownloadURL           string `json:"download_url,omitempty"`
	DownloadMethod        string `json:"download_method,omitempty"`
	DownloadExpiresAtUnix int64  `json:"download_expires_at_unix,omitempty"`
}

func (s *server) handleGetTool(ctx context.Context, req *mcpsdk.CallToolRequest, input getToolInput) (*mcpsdk.CallToolResult, getToolOutput, error) {
	key := strings.TrimSpace(input.Key)
	if key == "" {
		return nil, getToolOutput{}, fmt.Errorf("key is required")
	}
	mode, err := parsePayloadMode(input.PayloadMode, payloadModeAuto)
	if err != nil {
		return nil, getToolOutput{}, err
	}
	leaseID := strings.TrimSpace(input.LeaseID)
	publicRead := resolvePublicReadMode(input.Public)
	if publicRead && leaseID != "" {
		return nil, getToolOutput{}, fmt.Errorf("lease_id must be empty when public=true")
	}
	if !publicRead && leaseID == "" {
		return nil, getToolOutput{}, fmt.Errorf("lease_id is required when public=false")
	}

	resolvedNamespace := s.resolveNamespaceForCall(input.Namespace, req)
	opts := []lockdclient.GetOption{
		lockdclient.WithGetNamespace(resolvedNamespace),
	}
	if leaseID != "" {
		opts = append(opts, lockdclient.WithGetLeaseID(leaseID))
	}
	if !publicRead {
		opts = append(opts, lockdclient.WithGetPublicDisabled(true))
	}

	resp, err := s.upstream.Get(ctx, key, opts...)
	if err != nil {
		return nil, getToolOutput{}, err
	}

	out := getToolOutput{
		Namespace:   resp.Namespace,
		Key:         resp.Key,
		Found:       resp.HasState,
		ETag:        resp.ETag,
		PayloadMode: string(payloadModeNone),
	}
	if ver := strings.TrimSpace(resp.Version); ver != "" {
		parsed, parseErr := strconv.ParseInt(ver, 10, 64)
		if parseErr != nil {
			_ = resp.Close()
			return nil, getToolOutput{}, fmt.Errorf("invalid upstream version %q: %w", ver, parseErr)
		}
		out.Version = parsed
	}
	if !resp.HasState {
		_ = resp.Close()
		return nil, out, nil
	}
	switch mode {
	case payloadModeNone:
		_ = resp.Close()
		return nil, out, nil
	case payloadModeInline:
		inline, inlineErr := readInlinePayloadStrict(resp.Reader(), s.cfg.InlineMaxBytes, toolGet, toolStateStream)
		_ = resp.Close()
		if inlineErr != nil {
			return nil, getToolOutput{}, inlineErr
		}
		out.PayloadMode = string(payloadModeInline)
		out.PayloadBytes = inline.Bytes
		out.PayloadText = inline.Text
		out.PayloadBase64 = inline.Base64
		return nil, out, nil
	case payloadModeAuto:
		inline, tooLarge, inlineErr := readInlinePayloadAuto(resp.Reader(), s.cfg.InlineMaxBytes)
		if inlineErr != nil {
			_ = resp.Close()
			return nil, getToolOutput{}, inlineErr
		}
		if !tooLarge {
			_ = resp.Close()
			out.PayloadMode = string(payloadModeInline)
			out.PayloadBytes = inline.Bytes
			out.PayloadText = inline.Text
			out.PayloadBase64 = inline.Base64
			return nil, out, nil
		}
		_ = resp.Close()
		if req == nil || req.Session == nil {
			return nil, getToolOutput{}, inlinePayloadTooLargeError(toolGet, s.cfg.InlineMaxBytes+1, normalizedInlineMaxBytes(s.cfg.InlineMaxBytes), toolStateStream)
		}
		return s.handleStateStreamFallback(req, out, key, leaseID, publicRead)
	case payloadModeStream:
		_ = resp.Close()
		if req == nil || req.Session == nil {
			return nil, getToolOutput{}, fmt.Errorf("payload_mode=stream requires an active MCP session")
		}
		return s.handleStateStreamFallback(req, out, key, leaseID, publicRead)
	default:
		_ = resp.Close()
		return nil, getToolOutput{}, fmt.Errorf("unsupported payload_mode %q", mode)
	}
}

func (s *server) handleStateStreamFallback(req *mcpsdk.CallToolRequest, out getToolOutput, key, leaseID string, publicRead bool) (*mcpsdk.CallToolResult, getToolOutput, error) {
	transferCtx, cancelTransfer := context.WithCancel(context.Background())
	opts := []lockdclient.GetOption{}
	if ns := strings.TrimSpace(out.Namespace); ns != "" {
		opts = append(opts, lockdclient.WithGetNamespace(ns))
	}
	if leaseID != "" {
		opts = append(opts, lockdclient.WithGetLeaseID(leaseID))
	}
	if !publicRead {
		opts = append(opts, lockdclient.WithGetPublicDisabled(true))
	}
	stateResp, err := s.upstream.Get(transferCtx, key, opts...)
	if err != nil {
		cancelTransfer()
		return nil, getToolOutput{}, err
	}
	if !stateResp.HasState {
		_ = stateResp.Close()
		cancelTransfer()
		out.PayloadMode = string(payloadModeNone)
		return nil, out, nil
	}
	reg, err := s.ensureTransferManager().RegisterDownload(req.Session, stateResp.Reader(), transferDownloadRequest{
		ContentType: "application/json",
		Cleanup:     cancelTransfer,
	})
	if err != nil {
		_ = stateResp.Close()
		cancelTransfer()
		return nil, getToolOutput{}, err
	}
	out.PayloadMode = string(payloadModeStream)
	out.DownloadURL = s.transferURL(reg.ID)
	out.DownloadMethod = reg.Method
	out.DownloadExpiresAtUnix = reg.ExpiresAtUnix
	return nil, out, nil
}

func resolvePublicReadMode(raw *bool) bool {
	if raw == nil {
		return true
	}
	return *raw
}

type describeToolInput struct {
	Key       string `json:"key" jsonschema:"Lock key to inspect"`
	Namespace string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to client default namespace, then server default namespace)"`
}

type describeToolOutput struct {
	Namespace   string `json:"namespace"`
	Key         string `json:"key"`
	Owner       string `json:"owner,omitempty"`
	LeaseID     string `json:"lease_id,omitempty"`
	ExpiresAt   int64  `json:"expires_at_unix,omitempty"`
	Version     int64  `json:"version"`
	StateETag   string `json:"state_etag,omitempty"`
	UpdatedAt   int64  `json:"updated_at_unix,omitempty"`
	QueryHidden *bool  `json:"query_hidden,omitempty"`
}

func (s *server) handleDescribeTool(ctx context.Context, req *mcpsdk.CallToolRequest, input describeToolInput) (*mcpsdk.CallToolResult, describeToolOutput, error) {
	key := strings.TrimSpace(input.Key)
	if key == "" {
		return nil, describeToolOutput{}, fmt.Errorf("key is required")
	}
	var extra *mcpsdk.RequestExtra
	if req != nil {
		extra = req.Extra
	}
	upstream, done, err := s.upstreamForNamespace(input.Namespace, extra)
	if err != nil {
		return nil, describeToolOutput{}, err
	}
	defer done()

	describe, err := upstream.Describe(ctx, key)
	if err != nil {
		return nil, describeToolOutput{}, err
	}
	return nil, describeToolOutput{
		Namespace:   describe.Namespace,
		Key:         describe.Key,
		Owner:       describe.Owner,
		LeaseID:     describe.LeaseID,
		ExpiresAt:   describe.ExpiresAt,
		Version:     describe.Version,
		StateETag:   describe.StateETag,
		UpdatedAt:   describe.UpdatedAt,
		QueryHidden: describe.Metadata.QueryHidden,
	}, nil
}

type queryToolInput struct {
	Query     string         `json:"query" jsonschema:"LQL query expression (empty string matches all keys). Examples: in{field=/tags,any=planning|finance}, contains{f=/summary,a=\"renewal|key phrase 2\"}, date{field=/updated_at,since=yesterday}, /updated_at>=2025-01-01T00:00:00Z"`
	Namespace string         `json:"namespace,omitempty" jsonschema:"Namespace (defaults to client default namespace, then server default namespace)"`
	Limit     int            `json:"limit,omitempty" jsonschema:"Maximum rows to return"`
	Cursor    string         `json:"cursor,omitempty" jsonschema:"Continuation cursor"`
	Engine    string         `json:"engine,omitempty" jsonschema:"Optional query engine override: auto, index, or scan"`
	Refresh   string         `json:"refresh,omitempty" jsonschema:"Optional refresh policy, for example wait_for"`
	Fields    map[string]any `json:"fields,omitempty" jsonschema:"Optional query field projection map"`
}

type queryToolOutput struct {
	Namespace string            `json:"namespace"`
	Keys      []string          `json:"keys,omitempty"`
	Cursor    string            `json:"cursor,omitempty"`
	IndexSeq  uint64            `json:"index_seq,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

func (s *server) handleQueryTool(ctx context.Context, req *mcpsdk.CallToolRequest, input queryToolInput) (*mcpsdk.CallToolResult, queryToolOutput, error) {
	expr := strings.TrimSpace(input.Query)
	resolvedNamespace := s.resolveNamespaceForCall(input.Namespace, req)
	opts := []lockdclient.QueryOption{
		lockdclient.WithQuery(expr),
		lockdclient.WithQueryNamespace(resolvedNamespace),
	}
	if input.Limit > 0 {
		opts = append(opts, lockdclient.WithQueryLimit(input.Limit))
	}
	if cursor := strings.TrimSpace(input.Cursor); cursor != "" {
		opts = append(opts, lockdclient.WithQueryCursor(cursor))
	}
	if len(input.Fields) > 0 {
		opts = append(opts, lockdclient.WithQueryFields(cloneAnyMap(input.Fields)))
	}
	if engine := strings.ToLower(strings.TrimSpace(input.Engine)); engine != "" {
		switch engine {
		case "auto", "index", "scan":
			opts = append(opts, lockdclient.WithQueryEngine(engine))
		default:
			return nil, queryToolOutput{}, fmt.Errorf("invalid engine %q (expected auto|index|scan)", input.Engine)
		}
	}
	if refresh := strings.TrimSpace(input.Refresh); refresh != "" {
		opts = append(opts, lockdclient.WithQueryRefresh(refresh))
	}
	opts = append(opts, lockdclient.WithQueryReturnKeys())

	resp, err := s.upstream.Query(ctx, opts...)
	if err != nil {
		return nil, queryToolOutput{}, err
	}
	defer resp.Close()

	out := queryToolOutput{
		Namespace: resp.Namespace,
		Cursor:    resp.Cursor,
		IndexSeq:  resp.IndexSeq,
		Metadata:  cloneStringMap(resp.Metadata),
	}
	out.Keys = resp.Keys()
	return nil, out, nil
}

type queryStreamToolInput struct {
	Query     string         `json:"query" jsonschema:"LQL query expression (empty string matches all documents). Supports contains/icontains any= lists and temporal date/range selectors (e.g. date{field=/updated_at,since=today})"`
	Namespace string         `json:"namespace,omitempty" jsonschema:"Namespace (defaults to client default namespace, then server default namespace)"`
	Limit     int            `json:"limit,omitempty" jsonschema:"Maximum rows to return"`
	Cursor    string         `json:"cursor,omitempty" jsonschema:"Continuation cursor"`
	Engine    string         `json:"engine,omitempty" jsonschema:"Optional query engine override: auto, index, or scan"`
	Refresh   string         `json:"refresh,omitempty" jsonschema:"Optional refresh policy, for example wait_for"`
	Fields    map[string]any `json:"fields,omitempty" jsonschema:"Optional query field projection map"`
}

type queryStreamToolOutput struct {
	Namespace             string            `json:"namespace"`
	Cursor                string            `json:"cursor,omitempty"`
	IndexSeq              uint64            `json:"index_seq,omitempty"`
	Metadata              map[string]string `json:"metadata,omitempty"`
	DownloadURL           string            `json:"download_url"`
	DownloadMethod        string            `json:"download_method"`
	DownloadExpiresAtUnix int64             `json:"download_expires_at_unix"`
}

func (s *server) handleQueryStreamTool(ctx context.Context, req *mcpsdk.CallToolRequest, input queryStreamToolInput) (*mcpsdk.CallToolResult, queryStreamToolOutput, error) {
	if req == nil || req.Session == nil {
		return nil, queryStreamToolOutput{}, fmt.Errorf("query streaming requires an active MCP session")
	}
	expr := strings.TrimSpace(input.Query)
	resolvedNamespace := s.resolveNamespaceForCall(input.Namespace, req)

	opts := []lockdclient.QueryOption{
		lockdclient.WithQuery(expr),
		lockdclient.WithQueryNamespace(resolvedNamespace),
		lockdclient.WithQueryReturnDocuments(),
	}
	if input.Limit > 0 {
		opts = append(opts, lockdclient.WithQueryLimit(input.Limit))
	}
	if cursor := strings.TrimSpace(input.Cursor); cursor != "" {
		opts = append(opts, lockdclient.WithQueryCursor(cursor))
	}
	if len(input.Fields) > 0 {
		opts = append(opts, lockdclient.WithQueryFields(cloneAnyMap(input.Fields)))
	}
	if engine := strings.ToLower(strings.TrimSpace(input.Engine)); engine != "" {
		switch engine {
		case "auto", "index", "scan":
			opts = append(opts, lockdclient.WithQueryEngine(engine))
		default:
			return nil, queryStreamToolOutput{}, fmt.Errorf("invalid engine %q (expected auto|index|scan)", input.Engine)
		}
	}
	if refresh := strings.TrimSpace(input.Refresh); refresh != "" {
		opts = append(opts, lockdclient.WithQueryRefresh(refresh))
	}

	resp, err := s.upstream.Query(ctx, opts...)
	if err != nil {
		return nil, queryStreamToolOutput{}, err
	}
	out := queryStreamToolOutput{
		Namespace: resp.Namespace,
		Cursor:    resp.Cursor,
		IndexSeq:  resp.IndexSeq,
		Metadata:  cloneStringMap(resp.Metadata),
	}
	reader, cleanup := streamQueryRowsNDJSON(resp)
	reg, err := s.ensureTransferManager().RegisterDownload(req.Session, reader, transferDownloadRequest{
		ContentType: "application/x-ndjson",
		Filename:    "lockd-query.ndjson",
		Headers: map[string]string{
			"X-Lockd-Query-Namespace": resp.Namespace,
		},
		Cleanup: cleanup,
	})
	if err != nil {
		_ = reader.Close()
		if cleanup != nil {
			cleanup()
		}
		return nil, queryStreamToolOutput{}, err
	}
	out.DownloadURL = s.transferURL(reg.ID)
	out.DownloadMethod = reg.Method
	out.DownloadExpiresAtUnix = reg.ExpiresAtUnix
	return nil, out, nil
}

func (s *server) upstreamForNamespace(raw string, extra *mcpsdk.RequestExtra) (*lockdclient.Client, func(), error) {
	ns := s.resolveNamespace(raw, extra)
	if ns == s.cfg.DefaultNamespace {
		return s.upstream, func() {}, nil
	}
	cfg := s.cfg
	cfg.DefaultNamespace = ns
	cli, err := newUpstreamClient(cfg, s.logger)
	if err != nil {
		return nil, nil, err
	}
	return cli, func() {
		_ = cli.Close()
	}, nil
}

func cloneAnyMap(source map[string]any) map[string]any {
	if len(source) == 0 {
		return nil
	}
	out := make(map[string]any, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
}

func cloneStringMap(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	out := make(map[string]string, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
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
		lockdclient.WithDrainAwareShutdown(resolveUpstreamDrainAware(cfg.UpstreamDrainAware)),
		lockdclient.WithLogger(svcfields.WithSubsystem(logger, "client.sdk")),
	}

	if !cfg.UpstreamDisableMTLS {
		bundlePath, err := resolveUpstreamClientBundlePath(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, lockdclient.WithBundlePath(bundlePath))
	}

	return lockdclient.NewWithEndpoints(endpoints, opts...)
}

func resolveUpstreamClientBundlePath(cfg Config) (string, error) {
	bundlePath, err := lockd.ResolveClientBundlePathWithHint(lockd.ClientBundleRoleSDK, cfg.UpstreamClientBundlePath, "--client-bundle")
	if err != nil {
		return "", fmt.Errorf("resolve mcp upstream client bundle: %w", err)
	}
	return bundlePath, nil
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
	cfg.InlineMaxBytes = normalizedInlineMaxBytes(cfg.InlineMaxBytes)
	if cfg.UpstreamHTTPTimeout <= 0 {
		cfg.UpstreamHTTPTimeout = lockdclient.DefaultHTTPTimeout
	}
	if cfg.UpstreamCloseTimeout <= 0 {
		cfg.UpstreamCloseTimeout = lockdclient.DefaultCloseTimeout
	}
	if cfg.UpstreamKeepAliveTimeout <= 0 {
		cfg.UpstreamKeepAliveTimeout = lockdclient.DefaultKeepAliveTimeout
	}
	if cfg.UpstreamDrainAware == nil {
		drainAware := true
		cfg.UpstreamDrainAware = &drainAware
	}
	if strings.TrimSpace(cfg.MCPPath) == "" {
		cfg.MCPPath = "/"
	}
}

func resolveUpstreamDrainAware(v *bool) bool {
	if v == nil {
		return true
	}
	return *v
}

func validateConfig(cfg Config) error {
	if strings.TrimSpace(cfg.Listen) == "" {
		return fmt.Errorf("listen address required")
	}
	if strings.TrimSpace(cfg.UpstreamServer) == "" {
		return fmt.Errorf("upstream lockd server required")
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return fmt.Errorf("mcp base URL required: set --base-url (or LOCKD_MCP_BASE_URL) to the externally reachable MCP URL")
	}
	if _, err := parseBaseURL(cfg.BaseURL, cfg.AllowHTTP); err != nil {
		return err
	}
	return nil
}

func cleanHTTPPath(raw string) string {
	p := strings.TrimSpace(raw)
	if p == "" {
		return "/"
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

func oauthAuthorizationServerWellKnownPath(rootPath, issuerPath string) string {
	return scopedWellKnownPath(rootPath, "oauth-authorization-server", issuerPath)
}

func oauthProtectedResourceWellKnownPath(rootPath, resourcePath string) string {
	return scopedWellKnownPath(rootPath, "oauth-protected-resource", resourcePath)
}

func openIDConfigurationWellKnownPath(rootPath string) string {
	root := cleanHTTPPath(rootPath)
	return path.Join(root, ".well-known", "openid-configuration")
}

func scopedWellKnownPath(rootPath, kind, targetPath string) string {
	root := cleanHTTPPath(rootPath)
	base := path.Join(root, ".well-known", strings.Trim(strings.TrimSpace(kind), "/"))
	relative := scopedRelativePath(root, targetPath)
	if relative == "/" {
		return base
	}
	return path.Join(base, strings.TrimPrefix(relative, "/"))
}

func scopedRelativePath(rootPath, targetPath string) string {
	root := cleanHTTPPath(rootPath)
	target := cleanHTTPPath(targetPath)
	if target == root {
		return "/"
	}
	if root != "/" && strings.HasPrefix(target, root+"/") {
		return "/" + strings.TrimPrefix(target, root+"/")
	}
	if root == "/" {
		return target
	}
	return "/" + strings.TrimPrefix(target, "/")
}

func scopedHTTPPath(rootPath, relativePath string) string {
	root := cleanHTTPPath(rootPath)
	relative := cleanHTTPPath(relativePath)
	if relative == "/" {
		return root
	}
	return path.Join(root, strings.TrimPrefix(relative, "/"))
}

func urlPathPrefix(rawURL string) string {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "/"
	}
	p := cleanHTTPPath(u.Path)
	if p == "." || p == "" {
		return "/"
	}
	return p
}

func prefixedOAuthEndpointPath(prefix, endpoint string) string {
	p := cleanHTTPPath(prefix)
	if p == "/" {
		return "/" + strings.TrimPrefix(endpoint, "/")
	}
	return path.Join(p, strings.TrimPrefix(endpoint, "/"))
}

func absoluteURLForPath(base *url.URL, p string) string {
	if base == nil {
		return cleanHTTPPath(p)
	}
	u := *base
	u.Path = cleanHTTPPath(p)
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	u.RawFragment = ""
	return u.String()
}

func absPath(p string) (string, error) {
	return filepath.Abs(p)
}

func parseBaseURL(raw string, allowHTTP bool) (*url.URL, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil, fmt.Errorf("mcp base URL required: set --base-url (or LOCKD_MCP_BASE_URL) to the externally reachable MCP URL")
	}
	if !strings.Contains(value, "://") {
		value = "https://" + value
	}
	u, err := url.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("parse mcp base URL %q: %w", raw, err)
	}
	if strings.TrimSpace(u.Scheme) == "" {
		return nil, fmt.Errorf("mcp base URL %q requires a scheme", raw)
	}
	scheme := strings.ToLower(strings.TrimSpace(u.Scheme))
	if scheme != "https" {
		if scheme != "http" {
			return nil, fmt.Errorf("mcp base URL %q has unsupported scheme %q (expected https)", raw, scheme)
		}
		if !allowHTTP {
			return nil, fmt.Errorf("mcp base URL %q uses http; set --allow-http to permit non-TLS transfer URLs", raw)
		}
	}
	if strings.TrimSpace(u.Host) == "" {
		return nil, fmt.Errorf("mcp base URL %q requires a host", raw)
	}
	u.Fragment = ""
	u.RawFragment = ""
	u.RawQuery = ""
	u.Path = cleanHTTPPath(u.Path)
	return u, nil
}
