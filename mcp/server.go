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
	transferLog    pslog.Logger
	subscribeLog   pslog.Logger
	writeStreamLog pslog.Logger
	upstream       *lockdclient.Client
	oauthManager   *oauth.Manager
	subscriptions  *subscriptionManager
	writeStreams   *writeStreamManager
	transfers      *transferManager
	httpServer     *http.Server
	tlsBundlePath  string
	resourceID     string
	metadataPath   string
	mcpHTTPPath    string
	oauthWellKnown string
	baseURL        *url.URL
	transferPath   string
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
		metadataPath:   "/.well-known/oauth-protected-resource",
		oauthWellKnown: "/.well-known/oauth-authorization-server",
	}
	baseURL, err := parseBaseURL(cfg.BaseURL, cfg.AllowHTTP)
	if err != nil {
		return nil, err
	}
	s.baseURL = baseURL
	s.transferPath = path.Join(s.mcpHTTPPath, "transfer")

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
			&mcpauth.RequireBearerTokenOptions{ResourceMetadataURL: s.metadataPath},
		)(mcpHandler)
	}

	mux := http.NewServeMux()
	mux.Handle(s.mcpHTTPPath, mcpHandler)
	mux.HandleFunc(s.transferPath+"/", s.handleTransfer)
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
	}, withStructuredToolErrors(s.handleLockAcquireTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolLockKeepAlive,
		Description: desc(toolLockKeepAlive),
	}, withStructuredToolErrors(s.handleLockKeepAliveTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolLockRelease,
		Description: desc(toolLockRelease),
	}, withStructuredToolErrors(s.handleLockReleaseTool))

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolGet,
		Description: desc(toolGet),
	}, withStructuredToolErrors(s.handleGetTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolDescribe,
		Description: desc(toolDescribe),
	}, withStructuredToolErrors(s.handleDescribeTool))

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQuery,
		Description: desc(toolQuery),
	}, withStructuredToolErrors(s.handleQueryTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueryStream,
		Description: desc(toolQueryStream),
	}, withStructuredToolErrors(s.handleQueryStreamTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolStateUpdate,
		Description: desc(toolStateUpdate),
	}, withStructuredToolErrors(s.handleStateUpdateTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolStateMutate,
		Description: desc(toolStateMutate),
	}, withStructuredToolErrors(s.handleStateMutateTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolStatePatch,
		Description: desc(toolStatePatch),
	}, withStructuredToolErrors(s.handleStatePatchTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolStateWriteStreamBegin,
		Description: desc(toolStateWriteStreamBegin),
	}, withStructuredToolErrors(s.handleStateWriteStreamBeginTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolStateWriteStreamStatus,
		Description: desc(toolStateWriteStreamStatus),
	}, withStructuredToolErrors(s.handleStateWriteStreamStatusTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolStateWriteStreamCommit,
		Description: desc(toolStateWriteStreamCommit),
	}, withStructuredToolErrors(s.handleStateWriteStreamCommitTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolStateWriteStreamAbort,
		Description: desc(toolStateWriteStreamAbort),
	}, withStructuredToolErrors(s.handleStateWriteStreamAbortTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolStateStream,
		Description: desc(toolStateStream),
	}, withStructuredToolErrors(s.handleStateStreamTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolStateMetadata,
		Description: desc(toolStateMetadata),
	}, withStructuredToolErrors(s.handleStateMetadataTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolStateRemove,
		Description: desc(toolStateRemove),
	}, withStructuredToolErrors(s.handleStateRemoveTool))

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsPut,
		Description: desc(toolAttachmentsPut),
	}, withStructuredToolErrors(s.handleAttachmentPutTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsWriteStreamBegin,
		Description: desc(toolAttachmentsWriteStreamBegin),
	}, withStructuredToolErrors(s.handleAttachmentsWriteStreamBeginTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsWriteStreamStatus,
		Description: desc(toolAttachmentsWriteStreamStatus),
	}, withStructuredToolErrors(s.handleAttachmentsWriteStreamStatusTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsWriteStreamCommit,
		Description: desc(toolAttachmentsWriteStreamCommit),
	}, withStructuredToolErrors(s.handleAttachmentsWriteStreamCommitTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsWriteStreamAbort,
		Description: desc(toolAttachmentsWriteStreamAbort),
	}, withStructuredToolErrors(s.handleAttachmentsWriteStreamAbortTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsList,
		Description: desc(toolAttachmentsList),
	}, withStructuredToolErrors(s.handleAttachmentListTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsHead,
		Description: desc(toolAttachmentsHead),
	}, withStructuredToolErrors(s.handleAttachmentHeadTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsChecksum,
		Description: desc(toolAttachmentsChecksum),
	}, withStructuredToolErrors(s.handleAttachmentChecksumTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsGet,
		Description: desc(toolAttachmentsGet),
	}, withStructuredToolErrors(s.handleAttachmentGetTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsStream,
		Description: desc(toolAttachmentsStream),
	}, withStructuredToolErrors(s.handleAttachmentStreamTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsDelete,
		Description: desc(toolAttachmentsDelete),
	}, withStructuredToolErrors(s.handleAttachmentDeleteTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolAttachmentsDeleteAll,
		Description: desc(toolAttachmentsDeleteAll),
	}, withStructuredToolErrors(s.handleAttachmentDeleteAllTool))

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolNamespaceGet,
		Description: desc(toolNamespaceGet),
	}, withStructuredToolErrors(s.handleNamespaceGetTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolNamespaceUpdate,
		Description: desc(toolNamespaceUpdate),
	}, withStructuredToolErrors(s.handleNamespaceUpdateTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolIndexFlush,
		Description: desc(toolIndexFlush),
	}, withStructuredToolErrors(s.handleIndexFlushTool))

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolHint,
		Description: desc(toolHint),
	}, withStructuredToolErrors(s.handleHintTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolHelp,
		Description: desc(toolHelp),
	}, withStructuredToolErrors(s.handleHelpTool))

	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueEnqueue,
		Description: desc(toolQueueEnqueue),
	}, withStructuredToolErrors(s.handleQueueEnqueueTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueWriteStreamBegin,
		Description: desc(toolQueueWriteStreamBegin),
	}, withStructuredToolErrors(s.handleQueueWriteStreamBeginTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueWriteStreamStatus,
		Description: desc(toolQueueWriteStreamStatus),
	}, withStructuredToolErrors(s.handleQueueWriteStreamStatusTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueWriteStreamCommit,
		Description: desc(toolQueueWriteStreamCommit),
	}, withStructuredToolErrors(s.handleQueueWriteStreamCommitTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueWriteStreamAbort,
		Description: desc(toolQueueWriteStreamAbort),
	}, withStructuredToolErrors(s.handleQueueWriteStreamAbortTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueDequeue,
		Description: desc(toolQueueDequeue),
	}, withStructuredToolErrors(s.handleQueueDequeueTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueStats,
		Description: desc(toolQueueStats),
	}, withStructuredToolErrors(s.handleQueueStatsTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueWatch,
		Description: desc(toolQueueWatch),
	}, withStructuredToolErrors(s.handleQueueWatchTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueAck,
		Description: desc(toolQueueAck),
	}, withStructuredToolErrors(s.handleQueueAckTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueNack,
		Description: desc(toolQueueNack),
	}, withStructuredToolErrors(s.handleQueueNackTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueDefer,
		Description: desc(toolQueueDefer),
	}, withStructuredToolErrors(s.handleQueueDeferTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueExtend,
		Description: desc(toolQueueExtend),
	}, withStructuredToolErrors(s.handleQueueExtendTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueSubscribe,
		Description: desc(toolQueueSubscribe),
	}, withStructuredToolErrors(s.handleQueueSubscribeTool))
	mcpsdk.AddTool(srv, &mcpsdk.Tool{
		Name:        toolQueueUnsubscribe,
		Description: desc(toolQueueUnsubscribe),
	}, withStructuredToolErrors(s.handleQueueUnsubscribeTool))
}

type getToolInput struct {
	Key         string `json:"key" jsonschema:"Lock key to read"`
	Namespace   string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
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

	opts := []lockdclient.GetOption{}
	if ns := strings.TrimSpace(input.Namespace); ns != "" {
		opts = append(opts, lockdclient.WithGetNamespace(ns))
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
	Namespace string `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
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

func (s *server) handleDescribeTool(ctx context.Context, _ *mcpsdk.CallToolRequest, input describeToolInput) (*mcpsdk.CallToolResult, describeToolOutput, error) {
	key := strings.TrimSpace(input.Key)
	if key == "" {
		return nil, describeToolOutput{}, fmt.Errorf("key is required")
	}
	upstream, done, err := s.upstreamForNamespace(input.Namespace)
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
	Query     string         `json:"query" jsonschema:"LQL query expression"`
	Namespace string         `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
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
	Query     string         `json:"query" jsonschema:"LQL query expression"`
	Namespace string         `json:"namespace,omitempty" jsonschema:"Namespace (defaults to server default namespace)"`
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
	if expr == "" {
		return nil, queryStreamToolOutput{}, fmt.Errorf("query is required")
	}

	opts := []lockdclient.QueryOption{
		lockdclient.WithQuery(expr),
		lockdclient.WithQueryReturnDocuments(),
	}
	if ns := strings.TrimSpace(input.Namespace); ns != "" {
		opts = append(opts, lockdclient.WithQueryNamespace(ns))
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

func (s *server) upstreamForNamespace(raw string) (*lockdclient.Client, func(), error) {
	ns := s.resolveNamespace(raw)
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
		lockdclient.WithDrainAwareShutdown(true),
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
	if strings.TrimSpace(cfg.MCPPath) == "" {
		cfg.MCPPath = "/"
	}
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
