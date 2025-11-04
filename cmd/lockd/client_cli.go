package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/loggingutil"
	"pkt.systems/lockd/internal/tlsutil"
	"pkt.systems/pslog"
)

const (
	clientServerKey           = "client.server"
	clientBundleKey           = "client.bundle"
	clientDisableMTLSKey      = "client.disable_mtls"
	clientTimeoutKey          = "client.timeout"
	clientCloseTimeoutKey     = "client.close_timeout"
	clientKeepAliveTimeoutKey = "client.keepalive_timeout"
	clientDrainAwareKey       = "client.drain_aware_shutdown"
	clientLogLevelKey         = "client.log_level"
	clientLogOutputKey        = "client.log_output"

	envLeaseID           = "LOCKD_CLIENT_LEASE"
	envKey               = "LOCKD_CLIENT_KEY"
	envOwner             = "LOCKD_CLIENT_OWNER"
	envVersion           = "LOCKD_CLIENT_VERSION"
	envETag              = "LOCKD_CLIENT_ETAG"
	envExpires           = "LOCKD_CLIENT_EXPIRES_UNIX"
	envServerURL         = "LOCKD_CLIENT_SERVER"
	envRetryAfter        = "LOCKD_CLIENT_RETRY_AFTER"
	envFencingToken      = "LOCKD_CLIENT_FENCING_TOKEN"
	envCorrelation       = "LOCKD_CLIENT_CORRELATION_ID"
	envQueueName         = "LOCKD_QUEUE_NAME"
	envQueueMessageID    = "LOCKD_QUEUE_MESSAGE_ID"
	envQueueLease        = "LOCKD_QUEUE_LEASE"
	envQueueFencing      = "LOCKD_QUEUE_FENCING_TOKEN"
	envQueueMetaETag     = "LOCKD_QUEUE_META_ETAG"
	envQueuePayloadPath  = "LOCKD_QUEUE_PAYLOAD_PATH"
	envQueueCursor       = "LOCKD_QUEUE_CURSOR"
	envQueueStateLease   = "LOCKD_QUEUE_STATE_LEASE"
	envQueueStateFencing = "LOCKD_QUEUE_STATE_FENCING_TOKEN"
	envQueueStateETag    = "LOCKD_QUEUE_STATE_ETAG"

	defaultBlockDuration = lockd.DefaultClientBlock
	defaultAcquireTTL    = lockd.DefaultDefaultTTL
	defaultKeepAliveTTL  = lockd.DefaultDefaultTTL
)

func newClientCommand() *cobra.Command {
	cfg := &clientCLIConfig{}
	var verbose bool
	cmd := &cobra.Command{
		Use:   "client",
		Short: "Interact with a running lockd server",
	}

	flags := cmd.PersistentFlags()
	flags.String("server", "https://127.0.0.1:9341", "lockd server base URL")
	flags.String("bundle", "", "path to client bundle PEM (default auto-discover under $HOME/.lockd)")
	flags.Bool("disable-mtls", false, "disable mutual TLS (plain HTTP by default for bare endpoints)")
	flags.Duration("timeout", lockdclient.DefaultHTTPTimeout, "HTTP client timeout")
	flags.Duration("close-timeout", lockdclient.DefaultCloseTimeout, "timeout to wait for lease release during Close()")
	flags.Duration("keepalive-timeout", lockdclient.DefaultKeepAliveTimeout, "timeout to wait for keepalive responses")
	flags.Bool("drain-aware-shutdown", true, "automatically release leases when the server is draining")
	flags.String("log-level", "none", "client log level (trace|debug|info|warn|error|none)")
	flags.String("log-output", "", "client log output path (default stderr)")
	flags.BoolVarP(&verbose, "verbose", "v", false, "enable verbose (trace) client logging")

	mustBindFlag(clientServerKey, "LOCKD_CLIENT_SERVER", flags.Lookup("server"))
	mustBindFlag(clientBundleKey, "LOCKD_CLIENT_BUNDLE", flags.Lookup("bundle"))
	mustBindFlag(clientDisableMTLSKey, "LOCKD_CLIENT_DISABLE_MTLS", flags.Lookup("disable-mtls"))
	mustBindFlag(clientTimeoutKey, "LOCKD_CLIENT_TIMEOUT", flags.Lookup("timeout"))
	mustBindFlag(clientCloseTimeoutKey, "LOCKD_CLIENT_CLOSE_TIMEOUT", flags.Lookup("close-timeout"))
	mustBindFlag(clientKeepAliveTimeoutKey, "LOCKD_CLIENT_KEEPALIVE_TIMEOUT", flags.Lookup("keepalive-timeout"))
	mustBindFlag(clientDrainAwareKey, "LOCKD_CLIENT_DRAIN_AWARE", flags.Lookup("drain-aware-shutdown"))
	mustBindFlag(clientLogLevelKey, "LOCKD_CLIENT_LOG_LEVEL", flags.Lookup("log-level"))
	mustBindFlag(clientLogOutputKey, "LOCKD_CLIENT_LOG_OUTPUT", flags.Lookup("log-output"))

	cfg.verboseFlag = &verbose

	cmd.AddCommand(
		newClientQueueCommand(cfg),
		newClientAcquireCommand(cfg),
		newClientKeepAliveCommand(cfg),
		newClientReleaseCommand(cfg),
		newClientGetCommand(cfg),
		newClientUpdateCommand(cfg),
		newClientRemoveCommand(cfg),
		newClientSetCommand(cfg),
		newClientEditCommand(cfg),
	)

	return cmd
}

func mustBindFlag(key, env string, flag *pflag.Flag) {
	if flag == nil {
		panic(fmt.Sprintf("flag for key %s not found", key))
	}
	if err := viper.BindPFlag(key, flag); err != nil {
		panic(err)
	}
	if env != "" {
		if err := viper.BindEnv(key, env); err != nil {
			panic(err)
		}
	}
}

type clientCLIConfig struct {
	loaded           bool
	server           string
	servers          []string
	bundle           string
	disableMTLS      bool
	timeout          time.Duration
	closeTimeout     time.Duration
	keepAliveTimeout time.Duration
	drainAware       bool
	cachedHTTPClient *http.Client
	logLevel         string
	logOutput        string
	logger           pslog.Base
	logClosers       []io.Closer
	loggerReady      bool
	verboseFlag      *bool
}

type keyResolveMode int

const (
	keyAllowAutogen keyResolveMode = iota
	keyRequireExisting
)

func resolveKeyInput(flagValue string, mode keyResolveMode) (string, error) {
	key := strings.TrimSpace(flagValue)
	if key != "" {
		return key, nil
	}
	if mode == keyAllowAutogen {
		return "", nil
	}
	envVal := strings.TrimSpace(os.Getenv(envKey))
	if envVal == "" {
		return "", fmt.Errorf("key required (specify --key/-k or export %s)", envKey)
	}
	return envVal, nil
}

func (c *clientCLIConfig) load() error {
	if c.loaded {
		return nil
	}
	server := viper.GetString(clientServerKey)
	if server == "" {
		server = "https://127.0.0.1:9341"
	}
	c.disableMTLS = viper.GetBool(clientDisableMTLSKey)
	endpoints, err := lockdclient.ParseEndpoints(server, c.disableMTLS)
	if err != nil {
		return err
	}
	c.servers = endpoints
	c.server = strings.Join(endpoints, ",")
	c.bundle = viper.GetString(clientBundleKey)
	timeout := viper.GetDuration(clientTimeoutKey)
	if timeout <= 0 {
		timeout = lockdclient.DefaultHTTPTimeout
		if len(c.servers) > 1 && !viper.IsSet(clientTimeoutKey) {
			timeout = 3 * time.Second
		}
	}
	c.timeout = timeout
	closeTimeout := viper.GetDuration(clientCloseTimeoutKey)
	if closeTimeout <= 0 {
		closeTimeout = lockdclient.DefaultCloseTimeout
	}
	c.closeTimeout = closeTimeout
	ka := viper.GetDuration(clientKeepAliveTimeoutKey)
	if ka <= 0 {
		ka = lockdclient.DefaultKeepAliveTimeout
	}
	c.keepAliveTimeout = ka
	c.drainAware = viper.GetBool(clientDrainAwareKey)
	if !viper.IsSet(clientDrainAwareKey) {
		c.drainAware = true
	}
	c.logOutput = viper.GetString(clientLogOutputKey)
	c.logLevel = strings.TrimSpace(viper.GetString(clientLogLevelKey))
	if c.verboseFlag != nil && *c.verboseFlag {
		c.logLevel = "trace"
	}
	if err := c.setupLogger(); err != nil {
		return err
	}
	c.loaded = true
	return nil
}

func (c *clientCLIConfig) ensureBundlePath() (string, error) {
	if c.bundle != "" {
		return c.bundle, nil
	}
	paths, err := resolveClientBundlePaths(nil)
	if err != nil {
		return "", err
	}
	if len(paths) == 0 {
		return "", errors.New("no client bundle found; provide --bundle")
	}
	if len(paths) > 1 {
		return "", fmt.Errorf("multiple client bundles found (%s); specify --bundle", strings.Join(paths, ", "))
	}
	c.bundle = paths[0]
	return c.bundle, nil
}

func (c *clientCLIConfig) setupLogger() error {
	if c.loggerReady {
		return nil
	}
	levelStr := strings.TrimSpace(strings.ToLower(c.logLevel))
	if levelStr == "" {
		levelStr = "none"
	}
	if levelStr == "none" || levelStr == "disabled" || levelStr == "off" {
		c.logger = nil
		c.loggerReady = true
		return nil
	}
	level, ok := pslog.ParseLevel(levelStr)
	if !ok {
		return fmt.Errorf("invalid client log level %q", c.logLevel)
	}
	if level == pslog.NoLevel || level == pslog.Disabled {
		c.logger = nil
		c.loggerReady = true
		return nil
	}
	var writer io.Writer = os.Stderr
	if c.logOutput != "" {
		switch c.logOutput {
		case "-":
			writer = os.Stdout
		case "stderr":
			writer = os.Stderr
		case "stdout":
			writer = os.Stdout
		default:
			f, err := os.OpenFile(c.logOutput, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
			if err != nil {
				return fmt.Errorf("open log file: %w", err)
			}
			c.logClosers = append(c.logClosers, f)
			writer = f
		}
	}
	logger := loggingutil.WithSubsystem(pslog.NewStructured(writer), "client.cli").LogLevel(level)
	c.logger = logger
	c.loggerReady = true
	return nil
}

func (c *clientCLIConfig) cleanup() {
	for _, closer := range c.logClosers {
		_ = closer.Close()
	}
	c.logClosers = nil
	c.logger = nil
	c.loggerReady = false
	c.loaded = false
	c.cachedHTTPClient = nil
}

func (c *clientCLIConfig) buildHTTPClient() (*http.Client, error) {
	if c.cachedHTTPClient != nil {
		return c.cachedHTTPClient, nil
	}
	if err := c.load(); err != nil {
		return nil, err
	}
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("http default transport unexpected type")
	}
	tr := transport.Clone()
	if !c.disableMTLS {
		bundlePath, err := c.ensureBundlePath()
		if err != nil {
			return nil, err
		}
		clientBundle, err := tlsutil.LoadClientBundle(bundlePath)
		if err != nil {
			return nil, err
		}
		tr.TLSClientConfig = buildClientTLS(clientBundle)
	} else {
		tr.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}
	httpClient := &http.Client{
		Timeout:   c.timeout,
		Transport: tr,
	}
	c.cachedHTTPClient = httpClient
	return httpClient, nil
}

func (c *clientCLIConfig) client() (*lockdclient.Client, error) {
	if err := c.load(); err != nil {
		return nil, err
	}
	httpClient, err := c.buildHTTPClient()
	if err != nil {
		return nil, err
	}
	opts := []lockdclient.Option{
		lockdclient.WithDisableMTLS(c.disableMTLS),
		lockdclient.WithHTTPClient(httpClient),
		lockdclient.WithHTTPTimeout(c.timeout),
		lockdclient.WithCloseTimeout(c.closeTimeout),
		lockdclient.WithKeepAliveTimeout(c.keepAliveTimeout),
		lockdclient.WithDrainAwareShutdown(c.drainAware),
	}
	if c.logger != nil {
		opts = append(opts, lockdclient.WithLogger(c.logger))
	}
	return lockdclient.NewWithEndpoints(c.servers, opts...)
}

func buildClientTLS(bundle *tlsutil.ClientBundle) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		Certificates:       []tls.Certificate{bundle.Certificate},
		RootCAs:            bundle.CAPool,
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return verifyServerCertificate(rawCerts, bundle.CAPool)
		},
	}
}

func verifyServerCertificate(rawCerts [][]byte, roots *x509.CertPool) error {
	if len(rawCerts) == 0 {
		return errors.New("mtls: missing server certificate")
	}
	certs := make([]*x509.Certificate, 0, len(rawCerts))
	for _, raw := range rawCerts {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return fmt.Errorf("mtls: parse server certificate: %w", err)
		}
		certs = append(certs, cert)
	}
	leaf := certs[0]
	opts := x509.VerifyOptions{
		Roots:         roots,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		Intermediates: x509.NewCertPool(),
		CurrentTime:   time.Now(),
	}
	for _, cert := range certs[1:] {
		opts.Intermediates.AddCert(cert)
	}
	if _, err := leaf.Verify(opts); err != nil {
		return fmt.Errorf("mtls: verify server certificate: %w", err)
	}
	return nil
}

type outputMode string

const (
	outputText outputMode = "text"
	outputJSON outputMode = "json"
)

type stateFormat string

const (
	stateFormatAuto stateFormat = "auto"
	stateFormatRaw  stateFormat = "raw"
	stateFormatJSON stateFormat = "json"
	stateFormatYAML stateFormat = "yaml"
)

func formatTime(ts int64) string {
	if ts == 0 {
		return ""
	}
	return time.Unix(ts, 0).UTC().Format(time.RFC3339)
}

func writeJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func mustDurationSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	secs := int64(d / time.Second)
	if d%time.Second != 0 {
		secs++
	}
	if secs <= 0 {
		secs = 1
	}
	return secs
}

func parseBlockSeconds(raw string) (int64, error) {
	val := strings.TrimSpace(raw)
	if val == "" {
		return lockdclient.BlockWaitForever, nil
	}
	lower := strings.ToLower(val)
	switch lower {
	case "-1", "nowait", "no-wait":
		return lockdclient.BlockNoWait, nil
	case "0", "forever", "waitforever", "wait-forever", "infinite":
		return lockdclient.BlockWaitForever, nil
	}
	if secs, err := strconv.ParseInt(val, 10, 64); err == nil {
		if secs < 0 {
			return lockdclient.BlockNoWait, nil
		}
		if secs == 0 {
			return lockdclient.BlockWaitForever, nil
		}
		return secs, nil
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return 0, fmt.Errorf("--block: %w", err)
	}
	if d < 0 {
		return lockdclient.BlockNoWait, nil
	}
	if d == 0 {
		return lockdclient.BlockWaitForever, nil
	}
	secs := int64(d / time.Second)
	if d%time.Second != 0 {
		secs++
	}
	if secs <= 0 {
		secs = 1
	}
	return secs, nil
}

func defaultOwner() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "lockd"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func resolveLease(lease string) (string, error) {
	if lease != "" {
		return lease, nil
	}
	if env := os.Getenv(envLeaseID); env != "" {
		return env, nil
	}
	return "", fmt.Errorf("--lease is required (or set %s)", envLeaseID)
}

func resolveFencing(token string) (string, error) {
	if token != "" {
		return token, nil
	}
	if env := os.Getenv(envFencingToken); env != "" {
		return env, nil
	}
	return "", fmt.Errorf("--fencing-token is required (or set %s)", envFencingToken)
}

func parseQueueAttributes(pairs []string) (map[string]any, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	attrs := make(map[string]any, len(pairs))
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, value, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("invalid attribute %q (expected key=value)", pair)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("attribute key cannot be empty (%q)", pair)
		}
		attrs[key] = strings.TrimSpace(value)
	}
	if len(attrs) == 0 {
		return nil, nil
	}
	return attrs, nil
}

func resolveQueueString(arg, envVar, flag string, required bool) (string, error) {
	val := strings.TrimSpace(arg)
	if val == "" {
		val = strings.TrimSpace(os.Getenv(envVar))
	}
	if val == "" && required {
		return "", fmt.Errorf("--%s is required (or set %s)", flag, envVar)
	}
	return val, nil
}

func resolveQueueInt64(arg, envVar, flag string, required bool) (int64, error) {
	val := strings.TrimSpace(arg)
	if val == "" {
		val = strings.TrimSpace(os.Getenv(envVar))
	}
	if val == "" {
		if required {
			return 0, fmt.Errorf("--%s is required (or set %s)", flag, envVar)
		}
		return 0, nil
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("--%s: %w", flag, err)
	}
	return n, nil
}

func resolveCorrelationID() string {
	if env := strings.TrimSpace(os.Getenv(envCorrelation)); env != "" {
		if normalized, ok := lockdclient.NormalizeCorrelationID(env); ok {
			return normalized
		}
	}
	return lockdclient.GenerateCorrelationID()
}

func commandContextWithCorrelation(cmd *cobra.Command) (context.Context, string) {
	id := resolveCorrelationID()
	ctx := lockdclient.WithCorrelationID(cmd.Context(), id)
	return ctx, id
}

func newClientQueueCommand(cfg *clientCLIConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue",
		Short: "Interact with lockd queues",
	}
	cmd.AddCommand(
		newClientQueueEnqueueCommand(cfg),
		newClientQueueDequeueCommand(cfg),
		newClientQueueAckCommand(cfg),
		newClientQueueNackCommand(cfg),
		newClientQueueExtendCommand(cfg),
	)
	return cmd
}

func newClientQueueEnqueueCommand(cfg *clientCLIConfig) *cobra.Command {
	var queue string
	var data string
	var payloadFile string
	var contentType string
	var delay time.Duration
	var visibility time.Duration
	var ttl time.Duration
	var maxAttempts int
	var output string
	var attrs []string

	cmd := &cobra.Command{
		Use:   "enqueue",
		Short: "Enqueue a payload into a queue",
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := strings.TrimSpace(queue)
			if queueName == "" {
				return fmt.Errorf("--queue is required")
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			var reader io.Reader
			var closer io.Closer
			if payloadFile != "" {
				var file io.ReadCloser
				if payloadFile == "-" {
					file = io.NopCloser(os.Stdin)
				} else {
					f, err := os.Open(payloadFile)
					if err != nil {
						return fmt.Errorf("open payload file: %w", err)
					}
					file = f
				}
				reader = file
				closer = file
			} else {
				reader = strings.NewReader(data)
			}
			attrMap, err := parseQueueAttributes(attrs)
			if err != nil {
				return err
			}
			opts := lockdclient.EnqueueOptions{
				Delay:       delay,
				Visibility:  visibility,
				TTL:         ttl,
				MaxAttempts: maxAttempts,
				Attributes:  attrMap,
				ContentType: contentType,
			}
			ctx, _ := commandContextWithCorrelation(cmd)
			res, err := cli.Enqueue(ctx, queueName, reader, opts)
			if err != nil {
				return err
			}
			if closer != nil {
				closer.Close()
			}
			switch outputMode(strings.ToLower(output)) {
			case outputJSON:
				return writeJSON(cmd.OutOrStdout(), res)
			default:
				fmt.Fprintf(cmd.OutOrStdout(), "message_id=%s attempts=%d max_attempts=%d\n", res.MessageID, res.Attempts, res.MaxAttempts)
				return nil
			}
		},
	}
	cmd.Flags().StringVarP(&queue, "queue", "q", "", "queue name (required)")
	cmd.Flags().StringVarP(&data, "data", "d", "", "inline payload data (defaults to empty)")
	cmd.Flags().StringVar(&payloadFile, "file", "", "path to payload file (overrides --data)")
	cmd.Flags().StringVar(&contentType, "content-type", "", "payload content type")
	cmd.Flags().DurationVar(&delay, "delay", 0, "initial visibility delay")
	cmd.Flags().DurationVar(&visibility, "visibility", 0, "visibility timeout")
	cmd.Flags().DurationVar(&ttl, "ttl", 0, "message TTL before DLQ")
	cmd.Flags().IntVar(&maxAttempts, "max-attempts", 0, "override maximum delivery attempts")
	cmd.Flags().StringSliceVar(&attrs, "attr", nil, "additional message attributes (key=value)")
	cmd.Flags().StringVar(&output, "output", string(outputText), "output format (text|json)")
	return cmd
}

func newClientQueueDequeueCommand(cfg *clientCLIConfig) *cobra.Command {
	var owner string
	var visibility time.Duration
	var block string
	var pageSize int
	var startAfter string
	var stateful bool
	var payloadOut string
	var output string

	cmd := &cobra.Command{
		Use:   "dequeue <queue>",
		Short: "Dequeue the next available message",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := strings.TrimSpace(args[0])
			if queueName == "" {
				return fmt.Errorf("queue is required")
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			ownerVal := strings.TrimSpace(owner)
			if ownerVal == "" {
				ownerVal = defaultOwner()
			}
			blockSecs, err := parseBlockSeconds(block)
			if err != nil {
				return err
			}
			opts := lockdclient.DequeueOptions{
				Owner:        ownerVal,
				Visibility:   visibility,
				BlockSeconds: blockSecs,
				PageSize:     pageSize,
				StartAfter:   startAfter,
			}
			ctx, _ := commandContextWithCorrelation(cmd)
			var msg *lockdclient.QueueMessage
			if stateful {
				msg, err = cli.DequeueWithState(ctx, queueName, opts)
			} else {
				msg, err = cli.Dequeue(ctx, queueName, opts)
			}
			if err != nil {
				var apiErr *lockdclient.APIError
				if errors.As(err, &apiErr) && apiErr.Response.ErrorCode == "waiting" {
					fmt.Fprintln(cmd.ErrOrStderr(), "no messages available (try again later)")
					return nil
				}
				return err
			}
			if msg == nil {
				fmt.Fprintln(cmd.ErrOrStderr(), "no message available")
				return nil
			}
			var payloadPath string
			var payloadWriter *os.File
			if payloadOut != "" {
				file, err := os.OpenFile(payloadOut, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
				if err != nil {
					return fmt.Errorf("open payload file: %w", err)
				}
				payloadWriter = file
				payloadPath = payloadOut
			} else {
				file, err := os.CreateTemp("", "lockd-queue-*.bin")
				if err != nil {
					return fmt.Errorf("create temp payload file: %w", err)
				}
				payloadWriter = file
				payloadPath = file.Name()
			}
			bytesWritten, err := msg.WritePayloadTo(payloadWriter)
			if cerr := payloadWriter.Close(); cerr != nil {
				return fmt.Errorf("close payload file: %w", cerr)
			}
			if err != nil {
				msg.ClosePayload()
				return fmt.Errorf("stream payload: %w", err)
			}
			if payloadOut == "" {
				fmt.Fprintf(cmd.ErrOrStderr(), "payload written to %s\n", payloadPath)
			}
			if err := msg.ClosePayload(); err != nil {
				return fmt.Errorf("close payload reader: %w", err)
			}
			summary := map[string]any{
				"queue":                   msg.Queue(),
				"message_id":              msg.MessageID(),
				"attempts":                msg.Attempts(),
				"max_attempts":            msg.MaxAttempts(),
				"visibility_timeout_secs": msg.VisibilityTimeout().Seconds(),
				"not_visible_until_unix":  msg.NotVisibleUntil().Unix(),
				"lease_id":                msg.LeaseID(),
				"lease_expires_at_unix":   msg.LeaseExpiresAt(),
				"fencing_token":           msg.FencingToken(),
				"meta_etag":               msg.MetaETag(),
				"cursor":                  msg.Cursor(),
				"payload_path":            payloadPath,
				"payload_content_type":    msg.ContentType(),
				"payload_bytes":           bytesWritten,
			}
			if state := msg.StateHandle(); state != nil {
				summary["state_lease_id"] = state.LeaseID()
				summary["state_fencing_token"] = state.FencingToken()
				summary["state_etag"] = state.ETag()
				summary["state_lease_expires_at_unix"] = state.LeaseExpiresAt()
			}
			switch outputMode(strings.ToLower(output)) {
			case outputJSON:
				return writeJSON(cmd.OutOrStdout(), summary)
			default:
				exports := []struct{ name, value string }{
					{name: envQueueName, value: msg.Queue()},
					{name: envQueueMessageID, value: msg.MessageID()},
					{name: envQueueLease, value: msg.LeaseID()},
					{name: envQueueFencing, value: strconv.FormatInt(msg.FencingToken(), 10)},
					{name: envQueueMetaETag, value: msg.MetaETag()},
					{name: envQueuePayloadPath, value: payloadPath},
					{name: envQueueCursor, value: msg.Cursor()},
				}
				if state := msg.StateHandle(); state != nil {
					exports = append(exports,
						struct{ name, value string }{name: envQueueStateLease, value: state.LeaseID()},
						struct{ name, value string }{name: envQueueStateFencing, value: strconv.FormatInt(state.FencingToken(), 10)},
						struct{ name, value string }{name: envQueueStateETag, value: state.ETag()},
					)
				}
				out := cmd.OutOrStdout()
				for _, e := range exports {
					if e.value == "" {
						continue
					}
					fmt.Fprintf(out, "export %s=%q\n", e.name, e.value)
				}
				return nil
			}
		},
	}
	cmd.Flags().StringVar(&owner, "owner", "", "owner identity (default hostname + pid)")
	cmd.Flags().StringVar(&block, "block", "forever", "time to wait for messages (e.g. 30s, 2m, 0 for forever, -1 for nowait)")
	cmd.Flags().DurationVar(&visibility, "visibility", 0, "visibility timeout override")
	cmd.Flags().IntVar(&pageSize, "page-size", 32, "queue page size override")
	cmd.Flags().StringVar(&startAfter, "start-after", "", "start listing after the given message ID")
	cmd.Flags().BoolVar(&stateful, "stateful", false, "acquire workflow state lease")
	cmd.Flags().StringVar(&payloadOut, "payload-out", "", "write payload to file")
	cmd.Flags().StringVar(&output, "output", string(outputText), "output format (text|json)")
	return cmd
}

func newClientQueueAckCommand(cfg *clientCLIConfig) *cobra.Command {
	var queue string
	var messageID string
	var leaseID string
	var metaETag string
	var fencingToken string
	var stateLease string
	var stateETag string
	var stateFencing string

	cmd := &cobra.Command{
		Use:   "ack",
		Short: "Acknowledge and remove a queue message",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			queueName, err := resolveQueueString(queue, envQueueName, "queue", true)
			if err != nil {
				return err
			}
			msgID, err := resolveQueueString(messageID, envQueueMessageID, "message", true)
			if err != nil {
				return err
			}
			lease, err := resolveQueueString(leaseID, envQueueLease, "lease", true)
			if err != nil {
				return err
			}
			meta, err := resolveQueueString(metaETag, envQueueMetaETag, "meta-etag", true)
			if err != nil {
				return err
			}
			fencing, err := resolveQueueInt64(fencingToken, envQueueFencing, "fencing-token", true)
			if err != nil {
				return err
			}
			stateLeaseID, err := resolveQueueString(stateLease, envQueueStateLease, "state-lease", false)
			if err != nil {
				return err
			}
			stateFence, err := resolveQueueInt64(stateFencing, envQueueStateFencing, "state-fencing-token", stateLeaseID != "")
			if err != nil {
				return err
			}
			stateETagVal, err := resolveQueueString(stateETag, envQueueStateETag, "state-etag", false)
			if err != nil {
				return err
			}
			ctx, _ := commandContextWithCorrelation(cmd)
			req := api.AckRequest{
				Queue:        queueName,
				MessageID:    msgID,
				LeaseID:      lease,
				MetaETag:     meta,
				FencingToken: fencing,
				StateETag:    stateETagVal,
			}
			if stateLeaseID != "" {
				req.StateLeaseID = stateLeaseID
				req.StateFencingToken = stateFence
			}
			if _, err := cli.QueueAck(ctx, req); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "acknowledged")
			return nil
		},
	}
	cmd.Flags().StringVar(&queue, "queue", "", "queue name")
	cmd.Flags().StringVar(&messageID, "message", "", "message id")
	cmd.Flags().StringVar(&leaseID, "lease", "", "message lease id")
	cmd.Flags().StringVar(&metaETag, "meta-etag", "", "message meta etag")
	cmd.Flags().StringVar(&fencingToken, "fencing-token", "", "message fencing token")
	cmd.Flags().StringVar(&stateLease, "state-lease", "", "state lease id")
	cmd.Flags().StringVar(&stateETag, "state-etag", "", "state etag")
	cmd.Flags().StringVar(&stateFencing, "state-fencing-token", "", "state fencing token")
	return cmd
}

func newClientQueueNackCommand(cfg *clientCLIConfig) *cobra.Command {
	var queue string
	var messageID string
	var leaseID string
	var metaETag string
	var fencingToken string
	var stateLease string
	var stateFencing string
	var delay time.Duration
	var reason string

	cmd := &cobra.Command{
		Use:   "nack",
		Short: "Release a message back to the queue",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			queueName, err := resolveQueueString(queue, envQueueName, "queue", true)
			if err != nil {
				return err
			}
			msgID, err := resolveQueueString(messageID, envQueueMessageID, "message", true)
			if err != nil {
				return err
			}
			lease, err := resolveQueueString(leaseID, envQueueLease, "lease", true)
			if err != nil {
				return err
			}
			meta, err := resolveQueueString(metaETag, envQueueMetaETag, "meta-etag", true)
			if err != nil {
				return err
			}
			fencing, err := resolveQueueInt64(fencingToken, envQueueFencing, "fencing-token", true)
			if err != nil {
				return err
			}
			stateLeaseID, err := resolveQueueString(stateLease, envQueueStateLease, "state-lease", false)
			if err != nil {
				return err
			}
			stateFence, err := resolveQueueInt64(stateFencing, envQueueStateFencing, "state-fencing-token", stateLeaseID != "")
			if err != nil {
				return err
			}
			ctx, _ := commandContextWithCorrelation(cmd)
			req := api.NackRequest{
				Queue:        queueName,
				MessageID:    msgID,
				LeaseID:      lease,
				MetaETag:     meta,
				FencingToken: fencing,
				DelaySeconds: mustDurationSeconds(delay),
			}
			if reason != "" {
				req.LastError = map[string]any{"detail": reason}
			}
			if stateLeaseID != "" {
				req.StateLeaseID = stateLeaseID
				req.StateFencingToken = stateFence
			}
			if _, err := cli.QueueNack(ctx, req); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "requeued")
			return nil
		},
	}
	cmd.Flags().StringVar(&queue, "queue", "", "queue name")
	cmd.Flags().StringVar(&messageID, "message", "", "message id")
	cmd.Flags().StringVar(&leaseID, "lease", "", "message lease id")
	cmd.Flags().StringVar(&metaETag, "meta-etag", "", "message meta etag")
	cmd.Flags().StringVar(&fencingToken, "fencing-token", "", "message fencing token")
	cmd.Flags().StringVar(&stateLease, "state-lease", "", "state lease id")
	cmd.Flags().StringVar(&stateFencing, "state-fencing-token", "", "state fencing token")
	cmd.Flags().DurationVar(&delay, "delay", 0, "nack visibility delay")
	cmd.Flags().StringVar(&reason, "reason", "", "optional nack reason")
	return cmd
}

func newClientQueueExtendCommand(cfg *clientCLIConfig) *cobra.Command {
	var queue string
	var messageID string
	var leaseID string
	var metaETag string
	var fencingToken string
	var stateLease string
	var stateFencing string
	var extendBy time.Duration

	cmd := &cobra.Command{
		Use:   "extend",
		Short: "Extend a queue message lease",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			queueName, err := resolveQueueString(queue, envQueueName, "queue", true)
			if err != nil {
				return err
			}
			msgID, err := resolveQueueString(messageID, envQueueMessageID, "message", true)
			if err != nil {
				return err
			}
			lease, err := resolveQueueString(leaseID, envQueueLease, "lease", true)
			if err != nil {
				return err
			}
			meta, err := resolveQueueString(metaETag, envQueueMetaETag, "meta-etag", true)
			if err != nil {
				return err
			}
			fencing, err := resolveQueueInt64(fencingToken, envQueueFencing, "fencing-token", true)
			if err != nil {
				return err
			}
			stateLeaseID, err := resolveQueueString(stateLease, envQueueStateLease, "state-lease", false)
			if err != nil {
				return err
			}
			stateFence, err := resolveQueueInt64(stateFencing, envQueueStateFencing, "state-fencing-token", stateLeaseID != "")
			if err != nil {
				return err
			}
			ctx, _ := commandContextWithCorrelation(cmd)
			req := api.ExtendRequest{
				Queue:           queueName,
				MessageID:       msgID,
				LeaseID:         lease,
				MetaETag:        meta,
				FencingToken:    fencing,
				ExtendBySeconds: mustDurationSeconds(extendBy),
			}
			if stateLeaseID != "" {
				req.StateLeaseID = stateLeaseID
				req.StateFencingToken = stateFence
			}
			res, err := cli.QueueExtend(ctx, req)
			if err != nil {
				return err
			}
			if res != nil {
				fmt.Fprintf(cmd.OutOrStdout(), "lease_expires_at_unix=%d visibility_timeout_seconds=%d\n", res.LeaseExpiresAtUnix, res.VisibilityTimeoutSeconds)
			} else {
				fmt.Fprintln(cmd.OutOrStdout(), "extended")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&queue, "queue", "", "queue name")
	cmd.Flags().StringVar(&messageID, "message", "", "message id")
	cmd.Flags().StringVar(&leaseID, "lease", "", "message lease id")
	cmd.Flags().StringVar(&metaETag, "meta-etag", "", "message meta etag")
	cmd.Flags().StringVar(&fencingToken, "fencing-token", "", "message fencing token")
	cmd.Flags().StringVar(&stateLease, "state-lease", "", "state lease id")
	cmd.Flags().StringVar(&stateFencing, "state-fencing-token", "", "state fencing token")
	cmd.Flags().DurationVar(&extendBy, "extend", 0, "additional visibility duration")
	return cmd
}

func newClientAcquireCommand(cfg *clientCLIConfig) *cobra.Command {
	var ttl time.Duration
	var block string
	var owner string
	var output string
	var failureRetries int
	var idempotency string
	var keyFlag string

	cmd := &cobra.Command{
		Use:   "acquire",
		Short: "Acquire a lease for a key",
		Example: `  # Acquire a lease and export environment variables
  eval "$(lockd client acquire --server https://127.0.0.1:9341 --ttl 30s --key orders)"`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := resolveKeyInput(keyFlag, keyAllowAutogen)
			if err != nil {
				return err
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			ownerVal := owner
			if ownerVal == "" {
				ownerVal = defaultOwner()
			}
			blockSecs, err := parseBlockSeconds(block)
			if err != nil {
				return err
			}
			req := api.AcquireRequest{
				Key:         key,
				Owner:       ownerVal,
				TTLSeconds:  mustDurationSeconds(ttl),
				BlockSecs:   blockSecs,
				Idempotency: idempotency,
			}
			if req.TTLSeconds <= 0 {
				return fmt.Errorf("ttl must be > 0")
			}
			ctx, _ := commandContextWithCorrelation(cmd)
			var opts []lockdclient.AcquireOption
			if cmd.Flags().Changed("retries") {
				opts = append(opts, lockdclient.WithAcquireFailureRetries(failureRetries))
			}
			sess, err := cli.Acquire(ctx, req, opts...)
			if err != nil {
				return err
			}
			switch outputMode(strings.ToLower(output)) {
			case outputJSON:
				return writeJSON(cmd.OutOrStdout(), sess.AcquireResponse)
			default:
				out := cmd.OutOrStdout()
				exports := []struct {
					name  string
					value string
				}{
					{name: envLeaseID, value: sess.LeaseID},
					{name: envKey, value: sess.Key},
					{name: envOwner, value: sess.Owner},
					{name: envFencingToken, value: sess.FencingTokenString()},
					{name: envServerURL, value: cfg.server},
					{name: envVersion, value: strconv.FormatInt(sess.Version, 10)},
					{name: envETag, value: sess.StateETag},
					{name: envExpires, value: strconv.FormatInt(sess.ExpiresAt, 10)},
				}
				if sess.RetryAfter > 0 {
					exports = append(exports, struct {
						name  string
						value string
					}{name: envRetryAfter, value: strconv.FormatInt(sess.RetryAfter, 10)})
				}
				for _, ex := range exports {
					fmt.Fprintf(out, "export %s=%q\n", ex.name, ex.value)
				}
				if sess.CorrelationID != "" {
					fmt.Fprintf(out, "export %s=%q\n", envCorrelation, sess.CorrelationID)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&keyFlag, "key", "k", "", "key to lease (omit for server-generated)")
	cmd.Flags().DurationVar(&ttl, "ttl", defaultAcquireTTL, "lease TTL duration")
	cmd.Flags().StringVar(&block, "block", defaultBlockDuration.String(), "time to wait on conflicts before retrying (e.g. 30s, 2m, 0 for forever, -1 for nowait)")
	cmd.Flags().StringVar(&owner, "owner", "", "owner identity (default hostname + pid)")
	cmd.Flags().StringVar(&output, "output", string(outputText), "output format (text|json)")
	cmd.Flags().IntVar(&failureRetries, "retries", lockdclient.DefaultAcquireFailureRetries, "max retries on non-conflict failures (-1 disables)")
	cmd.Flags().StringVar(&idempotency, "idempotency", "", "optional idempotency token")
	return cmd
}

func newClientKeepAliveCommand(cfg *clientCLIConfig) *cobra.Command {
	var ttl time.Duration
	var leaseID string
	var output string
	var fencing string
	var keyFlag string
	cmd := &cobra.Command{
		Use:   "keepalive",
		Short: "Keep an existing lease alive",
		Example: `  # Extend the current lease using the exported environment variable
  lockd client keepalive --ttl 1m --key orders`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := resolveKeyInput(keyFlag, keyRequireExisting)
			if err != nil {
				return err
			}
			lease, err := resolveLease(leaseID)
			if err != nil {
				return err
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			token, err := resolveFencing(fencing)
			if err != nil {
				return err
			}
			cli.RegisterLeaseToken(lease, token)
			req := api.KeepAliveRequest{
				Key:        key,
				LeaseID:    lease,
				TTLSeconds: mustDurationSeconds(ttl),
			}
			if req.TTLSeconds <= 0 {
				return fmt.Errorf("ttl must be > 0")
			}
			ctx, _ := commandContextWithCorrelation(cmd)
			resp, err := cli.KeepAlive(ctx, req)
			if err != nil {
				return err
			}
			summary := map[string]any{
				"expires_at_unix": resp.ExpiresAt,
			}
			if iso := formatTime(resp.ExpiresAt); iso != "" {
				summary["expires_at_iso"] = iso
			}
			switch outputMode(strings.ToLower(output)) {
			case outputJSON:
				return writeJSON(cmd.OutOrStdout(), summary)
			default:
				fmt.Fprintf(cmd.OutOrStdout(), "expires_at_unix: %d (%s)\n", resp.ExpiresAt, formatTime(resp.ExpiresAt))
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&keyFlag, "key", "k", "", "key to keep alive (falls back to "+envKey+")")
	cmd.Flags().StringVar(&leaseID, "lease", "", "lease identifier")
	cmd.Flags().DurationVar(&ttl, "ttl", defaultKeepAliveTTL, "new TTL duration")
	cmd.Flags().StringVar(&output, "output", string(outputJSON), "output format (text|json)")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	return cmd
}

func newClientReleaseCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var output string
	var fencing string
	var keyFlag string
	cmd := &cobra.Command{
		Use:   "release",
		Short: "Release a lease",
		Example: `  # Release the current lease
  lockd client release --key orders`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := resolveKeyInput(keyFlag, keyRequireExisting)
			if err != nil {
				return err
			}
			lease, err := resolveLease(leaseID)
			if err != nil {
				return err
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			token, err := resolveFencing(fencing)
			if err != nil {
				return err
			}
			cli.RegisterLeaseToken(lease, token)
			req := api.ReleaseRequest{Key: key, LeaseID: lease}
			ctx, _ := commandContextWithCorrelation(cmd)
			resp, err := cli.Release(ctx, req)
			if err != nil {
				return err
			}
			summary := map[string]any{"released": resp.Released}
			switch outputMode(strings.ToLower(output)) {
			case outputJSON:
				return writeJSON(cmd.OutOrStdout(), summary)
			default:
				fmt.Fprintf(cmd.OutOrStdout(), "released: %t\n", resp.Released)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&keyFlag, "key", "k", "", "key to release (falls back to "+envKey+")")
	cmd.Flags().StringVar(&leaseID, "lease", "", "lease identifier")
	cmd.Flags().StringVar(&output, "output", string(outputJSON), "output format (text|json)")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	return cmd
}

func newClientGetCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var outputPath string
	var format string
	var showMeta bool
	var fencing string
	var keyFlag string
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Fetch state JSON for a key",
		Example: `  # Pretty-print the current state to stdout
  lockd client get --key orders -o -`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := resolveKeyInput(keyFlag, keyRequireExisting)
			if err != nil {
				return err
			}
			lease, err := resolveLease(leaseID)
			if err != nil {
				return err
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			token, err := resolveFencing(fencing)
			if err != nil {
				return err
			}
			cli.RegisterLeaseToken(lease, token)
			ctx, _ := commandContextWithCorrelation(cmd)
			reader, etag, version, err := cli.GetState(ctx, key, lease)
			if err != nil {
				return err
			}
			var data []byte
			if reader != nil {
				defer reader.Close()
				data, err = io.ReadAll(reader)
				if err != nil {
					return err
				}
			}
			fmtChoice := parseStateFormat(stateFormat(format), outputPath)
			payload, err := formatStatePayload(data, fmtChoice)
			if err != nil {
				return err
			}
			if err := writeStateOutput(outputPath, payload); err != nil {
				return err
			}
			summary := map[string]any{
				"version": version,
				"bytes":   len(data),
			}
			if etag != "" {
				summary["etag"] = etag
			}
			if err := writeJSON(cmd.ErrOrStderr(), summary); err != nil {
				return err
			}
			if showMeta {
				errOut := cmd.ErrOrStderr()
				fmt.Fprintf(errOut, "version: %s\n", version)
				if etag != "" {
					fmt.Fprintf(errOut, "etag: %s\n", etag)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&keyFlag, "key", "k", "", "key to fetch (falls back to "+envKey+")")
	cmd.Flags().StringVar(&leaseID, "lease", "", "active lease identifier")
	cmd.Flags().StringVarP(&outputPath, "output", "o", "-", "output file (use - for stdout)")
	cmd.Flags().StringVar(&format, "type", string(stateFormatAuto), "output type (auto|raw|json|yaml)")
	cmd.Flags().BoolVar(&showMeta, "show-meta", false, "print version/etag metadata to stderr")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	return cmd
}

func newClientUpdateCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var format string
	var ifVersion string
	var ifETag string
	var fencing string
	var output string
	var keyFlag string
	cmd := &cobra.Command{
		Use:   "update [input]",
		Short: "Upload new state from a file",
		Example: `  # Pipe edited state back into the server
  lockd client get orders -o - \
    | lockd client edit status.counter++ \
    | lockd client update --key orders`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := resolveKeyInput(keyFlag, keyRequireExisting)
			if err != nil {
				return err
			}
			lease, err := resolveLease(leaseID)
			if err != nil {
				return err
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			token, err := resolveFencing(fencing)
			if err != nil {
				return err
			}
			cli.RegisterLeaseToken(lease, token)
			path := "-"
			if len(args) == 1 {
				path = args[0]
			}
			fmtChoice := parseStateFormat(stateFormat(format), path)
			payload, err := loadStatePayload(path, fmtChoice)
			if err != nil {
				return err
			}
			opts := lockdclient.UpdateStateOptions{IfVersion: ifVersion, IfETag: ifETag, FencingToken: token}
			ctx, _ := commandContextWithCorrelation(cmd)
			result, err := cli.UpdateStateBytes(ctx, key, lease, payload, opts)
			if err != nil {
				return err
			}
			summary := map[string]any{
				"version": result.NewVersion,
				"bytes":   result.BytesWritten,
			}
			if result.NewStateETag != "" {
				summary["etag"] = result.NewStateETag
			}
			switch outputMode(strings.ToLower(output)) {
			case outputJSON:
				return writeJSON(cmd.OutOrStdout(), summary)
			default:
				fmt.Fprintf(cmd.OutOrStdout(), "version: %d\nbytes: %d\netag: %s\n", result.NewVersion, result.BytesWritten, result.NewStateETag)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&keyFlag, "key", "k", "", "key to update (falls back to "+envKey+")")
	cmd.Flags().StringVar(&leaseID, "lease", "", "active lease identifier")
	cmd.Flags().StringVar(&format, "type", string(stateFormatAuto), "input type (auto|raw|json|yaml)")
	cmd.Flags().StringVar(&ifVersion, "if-version", "", "conditional update on version")
	cmd.Flags().StringVar(&ifETag, "if-etag", "", "conditional update on state etag")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	cmd.Flags().StringVar(&output, "output", string(outputJSON), "output format (text|json)")
	return cmd
}

func newClientRemoveCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var ifVersion string
	var ifETag string
	var fencing string
	var output string
	var keyFlag string
	cmd := &cobra.Command{
		Use:     "remove",
		Aliases: []string{"delete", "rm"},
		Short:   "Remove the stored state for a key",
		Example: `  # Remove the current state while holding a lease
  lockd client remove --key orders --lease "$LOCKD_CLIENT_LEASE"`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := resolveKeyInput(keyFlag, keyRequireExisting)
			if err != nil {
				return err
			}
			lease, err := resolveLease(leaseID)
			if err != nil {
				return err
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			token, err := resolveFencing(fencing)
			if err != nil {
				return err
			}
			cli.RegisterLeaseToken(lease, token)
			opts := lockdclient.RemoveStateOptions{IfVersion: ifVersion, IfETag: ifETag, FencingToken: token}
			ctx, _ := commandContextWithCorrelation(cmd)
			result, err := cli.RemoveState(ctx, key, lease, opts)
			if err != nil {
				return err
			}
			summary := map[string]any{
				"removed": result.Removed,
				"version": result.NewVersion,
			}
			switch outputMode(strings.ToLower(output)) {
			case outputJSON:
				return writeJSON(cmd.OutOrStdout(), summary)
			default:
				fmt.Fprintf(cmd.OutOrStdout(), "removed: %t\nversion: %d\n", result.Removed, result.NewVersion)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&keyFlag, "key", "k", "", "key to remove (falls back to "+envKey+")")
	cmd.Flags().StringVar(&leaseID, "lease", "", "active lease identifier")
	cmd.Flags().StringVar(&ifVersion, "if-version", "", "conditional remove on version")
	cmd.Flags().StringVar(&ifETag, "if-etag", "", "conditional remove on state etag")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	cmd.Flags().StringVar(&output, "output", string(outputJSON), "output format (text|json)")
	return cmd
}

func newClientSetCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var ifVersion string
	var ifETag string
	var noCAS bool
	var fencing string
	var output string
	var keyFlag string
	cmd := &cobra.Command{
		Use:   "set mutation [mutation...]",
		Short: "Mutate JSON state fields for an active lease",
		Example: `  # Increment a counter and stamp the current time
  lockd client set --key orders progress.counter++ time:progress.updated=NOW`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := resolveKeyInput(keyFlag, keyRequireExisting)
			if err != nil {
				return err
			}
			mutations, err := parseMutations(args, time.Now())
			if err != nil {
				return err
			}
			lease, err := resolveLease(leaseID)
			if err != nil {
				return err
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			token, err := resolveFencing(fencing)
			if err != nil {
				return err
			}
			cli.RegisterLeaseToken(lease, token)
			ctx := cmd.Context()
			stateBytes, etag, version, err := cli.GetStateBytes(ctx, key, lease)
			if err != nil {
				return err
			}
			doc, err := parseJSONObject(stateBytes)
			if err != nil {
				return fmt.Errorf("parse state: %w", err)
			}
			if doc == nil {
				doc = map[string]any{}
			}
			if err := applyMutations(doc, mutations); err != nil {
				return err
			}
			payload, err := marshalCompactJSON(doc)
			if err != nil {
				return err
			}
			opts := lockdclient.UpdateStateOptions{FencingToken: token}
			if !noCAS {
				opts.IfVersion = version
				opts.IfETag = etag
			}
			if ifVersion != "" {
				opts.IfVersion = ifVersion
			}
			if ifETag != "" {
				opts.IfETag = ifETag
			}
			result, err := cli.UpdateStateBytes(ctx, key, lease, payload, opts)
			if err != nil {
				return err
			}
			summary := map[string]any{
				"version": result.NewVersion,
				"bytes":   result.BytesWritten,
			}
			if result.NewStateETag != "" {
				summary["etag"] = result.NewStateETag
			}
			switch outputMode(strings.ToLower(output)) {
			case outputJSON:
				return writeJSON(cmd.OutOrStdout(), summary)
			default:
				fmt.Fprintf(cmd.OutOrStdout(), "version: %d\nbytes: %d\netag: %s\n", result.NewVersion, result.BytesWritten, result.NewStateETag)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&keyFlag, "key", "k", "", "key to update (falls back to "+envKey+")")
	cmd.Flags().StringVar(&leaseID, "lease", "", "active lease identifier")
	cmd.Flags().StringVar(&ifVersion, "if-version", "", "override version used for CAS (default from fetched state)")
	cmd.Flags().StringVar(&ifETag, "if-etag", "", "override ETag used for CAS (default from fetched state)")
	cmd.Flags().BoolVar(&noCAS, "no-cas", false, "skip conditional update using fetched version/etag (fencing token still required)")
	cmd.Flags().StringVar(&output, "output", string(outputJSON), "output format (text|json)")
	return cmd
}

func newClientEditCommand(_ *clientCLIConfig) *cobra.Command {
	var inputPath string
	var format string
	cmd := &cobra.Command{
		Use:   "edit mutation [mutation...]",
		Short: "Apply JSON field mutations to a local file",
		Example: `  # Increment counter in place
  lockd client edit --file checkpoint.json status.counter++`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if format != "" && strings.ToLower(format) != "json" && strings.ToLower(format) != "auto" {
				return fmt.Errorf("only json format is supported for edit")
			}
			path := inputPath
			if path == "" {
				path = "-"
			}
			mutations, err := parseMutations(args, time.Now())
			if err != nil {
				return err
			}
			data, mode, err := readInput(path)
			if err != nil {
				return err
			}
			doc, err := parseJSONObject(data)
			if err != nil {
				return fmt.Errorf("parse json: %w", err)
			}
			if doc == nil {
				doc = map[string]any{}
			}
			if err := applyMutations(doc, mutations); err != nil {
				return err
			}
			pretty, err := marshalPrettyJSON(doc)
			if err != nil {
				return err
			}
			if err := writeOutputFile(path, append(pretty, '\n'), mode); err != nil {
				return err
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&inputPath, "file", "f", "", "input/output file path (default stdin/stdout)")
	cmd.Flags().StringVar(&format, "type", "json", "input type (currently only json supported)")
	return cmd
}

func parseStateFormat(spec stateFormat, outputPath string) stateFormat {
	switch spec {
	case stateFormatRaw, stateFormatJSON, stateFormatYAML:
		return spec
	default:
		if ext := formatExt(outputPath); ext != "" {
			switch ext {
			case ".yaml", ".yml":
				return stateFormatYAML
			case ".json":
				return stateFormatJSON
			}
		}
		return stateFormatJSON
	}
}

func formatStatePayload(data []byte, format stateFormat) ([]byte, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		trimmed = []byte("null")
	}
	switch format {
	case stateFormatRaw:
		if !bytes.HasSuffix(trimmed, []byte("\n")) {
			trimmed = append(trimmed, '\n')
		}
		return trimmed, nil
	case stateFormatYAML:
		jsonBytes, err := ensureJSONBytes(trimmed)
		if err != nil {
			return nil, err
		}
		yamlBytes, err := convertJSONToYAML(jsonBytes)
		if err != nil {
			return nil, err
		}
		return yamlBytes, nil
	default:
		jsonBytes, err := ensureJSONBytes(trimmed)
		if err != nil {
			return nil, err
		}
		var buf bytes.Buffer
		if err := json.Indent(&buf, jsonBytes, "", "  "); err != nil {
			return nil, err
		}
		buf.WriteByte('\n')
		return buf.Bytes(), nil
	}
}

func ensureJSONBytes(data []byte) ([]byte, error) {
	var tmp any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&tmp); err != nil {
		return nil, err
	}
	return json.Marshal(tmp)
}

func writeStateOutput(path string, data []byte) error {
	if path == "-" || path == "" {
		_, err := os.Stdout.Write(data)
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func loadStatePayload(path string, format stateFormat) ([]byte, error) {
	data, _, err := readInput(path)
	if err != nil {
		return nil, err
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		trimmed = []byte("null")
	}
	switch format {
	case stateFormatRaw:
		return append([]byte(nil), trimmed...), nil
	case stateFormatYAML:
		return convertYAMLToJSON(path, trimmed)
	case stateFormatJSON, stateFormatAuto:
		return ensureJSONBytes(trimmed)
	default:
		return ensureJSONBytes(trimmed)
	}
}
