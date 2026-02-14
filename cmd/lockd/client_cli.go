package main

import (
	"bufio"
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
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/namespaces"
	"pkt.systems/lockd/tlsutil"
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
	envTxnID             = "LOCKD_CLIENT_TXN_ID"
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
	envNamespace         = "LOCKD_CLIENT_NAMESPACE"
	envQueueNamespace    = "LOCKD_QUEUE_NAMESPACE"

	defaultBlockDuration = lockd.DefaultClientBlock
	defaultAcquireTTL    = lockd.DefaultDefaultTTL
	defaultKeepAliveTTL  = lockd.DefaultDefaultTTL
)

func newClientCommand(baseLogger pslog.Logger) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "client",
		Short: "Interact with a running lockd server",
	}
	cfg := addClientConnectionFlags(cmd, true, baseLogger)

	cmd.AddCommand(
		newClientQueueCommand(cfg),
		newClientAcquireCommand(cfg),
		newClientKeepAliveCommand(cfg),
		newClientReleaseCommand(cfg),
		newClientGetCommand(cfg),
		newClientAttachmentsCommand(cfg),
		newClientUpdateCommand(cfg),
		newClientRemoveCommand(cfg),
		newClientSetCommand(cfg),
		newClientEditCommand(cfg),
		newClientQueryCommand(cfg),
		newClientTxnCommand(cfg), // hidden alias; canonical is root-level txn
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

func addClientConnectionFlags(cmd *cobra.Command, includeVerbose bool, baseLogger pslog.Logger) *clientCLIConfig {
	cfg := &clientCLIConfig{baseLogger: baseLogger}
	flags := cmd.PersistentFlags()
	flags.StringP("server", "s", "https://127.0.0.1:9341", "lockd server base URL")
	flags.StringP("bundle", "b", "", "path to client bundle PEM (default auto-discover under $HOME/.lockd)")
	flags.Bool("disable-mtls", false, "disable mutual TLS (plain HTTP by default for bare endpoints)")
	flags.Duration("timeout", lockdclient.DefaultHTTPTimeout, "HTTP client timeout")
	flags.Duration("close-timeout", lockdclient.DefaultCloseTimeout, "timeout to wait for lease release during Close()")
	flags.Duration("keepalive-timeout", lockdclient.DefaultKeepAliveTimeout, "timeout to wait for keepalive responses")
	flags.Bool("drain-aware-shutdown", true, "automatically release leases when the server is draining")
	flags.String("log-level", "none", "client log level (trace|debug|info|warn|error|none)")
	flags.String("log-output", "", "client log output path (default stderr)")
	mustBindFlag(clientServerKey, "LOCKD_CLIENT_SERVER", flags.Lookup("server"))
	mustBindFlag(clientBundleKey, "LOCKD_CLIENT_BUNDLE", flags.Lookup("bundle"))
	mustBindFlag(clientDisableMTLSKey, "LOCKD_CLIENT_DISABLE_MTLS", flags.Lookup("disable-mtls"))
	mustBindFlag(clientTimeoutKey, "LOCKD_CLIENT_TIMEOUT", flags.Lookup("timeout"))
	mustBindFlag(clientCloseTimeoutKey, "LOCKD_CLIENT_CLOSE_TIMEOUT", flags.Lookup("close-timeout"))
	mustBindFlag(clientKeepAliveTimeoutKey, "LOCKD_CLIENT_KEEPALIVE_TIMEOUT", flags.Lookup("keepalive-timeout"))
	mustBindFlag(clientDrainAwareKey, "LOCKD_CLIENT_DRAIN_AWARE", flags.Lookup("drain-aware-shutdown"))
	cfg.logLevelFlag = flags.Lookup("log-level")
	cfg.logOutputFlag = flags.Lookup("log-output")
	mustBindFlag(clientLogLevelKey, "LOCKD_CLIENT_LOG_LEVEL", cfg.logLevelFlag)
	mustBindFlag(clientLogOutputKey, "LOCKD_CLIENT_LOG_OUTPUT", cfg.logOutputFlag)
	if includeVerbose {
		var verbose bool
		flags.BoolVarP(&verbose, "verbose", "v", false, "enable verbose (trace) client logging")
		cfg.verboseFlag = &verbose
	}
	return cfg
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
	baseLogger       pslog.Logger
	logLevelFlag     *pflag.Flag
	logOutputFlag    *pflag.Flag
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

func resolveNamespaceInput(flagValue string) string {
	ns := strings.TrimSpace(flagValue)
	if ns != "" {
		return ns
	}
	return strings.TrimSpace(os.Getenv(envNamespace))
}

func resolveQueueNamespaceInput(flagValue string) string {
	ns := strings.TrimSpace(flagValue)
	if ns != "" {
		return ns
	}
	if env := strings.TrimSpace(os.Getenv(envQueueNamespace)); env != "" {
		return env
	}
	return strings.TrimSpace(os.Getenv(envNamespace))
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
	path, err := lockd.ResolveClientBundlePath(lockd.ClientBundleRoleSDK, c.bundle)
	if err != nil {
		return "", err
	}
	c.bundle = path
	return c.bundle, nil
}

func (c *clientCLIConfig) setupLogger() error {
	if c.loggerReady {
		return nil
	}

	logLevelExplicit := false
	if c.logLevelFlag != nil && c.logLevelFlag.Changed {
		logLevelExplicit = true
	}
	if _, ok := os.LookupEnv("LOCKD_CLIENT_LOG_LEVEL"); ok {
		logLevelExplicit = true
	}
	if viper.InConfig(clientLogLevelKey) {
		logLevelExplicit = true
	}
	if c.verboseFlag != nil && *c.verboseFlag {
		logLevelExplicit = true
	}

	logOutputExplicit := false
	if c.logOutputFlag != nil && c.logOutputFlag.Changed {
		logOutputExplicit = true
	}
	if _, ok := os.LookupEnv("LOCKD_CLIENT_LOG_OUTPUT"); ok {
		logOutputExplicit = true
	}
	if viper.InConfig(clientLogOutputKey) {
		logOutputExplicit = true
	}

	if !logLevelExplicit {
		c.logger = nil
		c.loggerReady = true
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

	if logOutputExplicit && c.logOutput != "" {
		c.logger = pslog.LoggerFromEnv(
			pslog.WithEnvPrefix("LOCKD_LOG_"),
			pslog.WithEnvOptions(pslog.Options{Mode: pslog.ModeStructured, MinLevel: level}),
			pslog.WithEnvWriter(writer),
		).With("app", "lockd").LogLevel(level)
		c.loggerReady = true
		return nil
	}

	c.logger = c.baseLogger.LogLevel(level)
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

func resolveTxn(txn string) (string, error) {
	if txn != "" {
		return txn, nil
	}
	if env := os.Getenv(envTxnID); env != "" {
		return env, nil
	}
	return "", fmt.Errorf("--txn-id is required (xid, 20-char base32) or set %s", envTxnID)
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

func queueNamespaceFromCmd(cmd *cobra.Command) string {
	if cmd == nil {
		return resolveQueueNamespaceInput("")
	}
	if f := cmd.Flags().Lookup("namespace"); f != nil {
		if val := strings.TrimSpace(f.Value.String()); val != "" {
			return resolveQueueNamespaceInput(val)
		}
	}
	if f := cmd.InheritedFlags().Lookup("namespace"); f != nil {
		if val := strings.TrimSpace(f.Value.String()); val != "" {
			return resolveQueueNamespaceInput(val)
		}
	}
	return resolveQueueNamespaceInput("")
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
		Use:     "queue",
		Aliases: []string{"q"},
		Short:   "Interact with lockd queues",
	}
	cmd.PersistentFlags().StringP("namespace", "n", "", "namespace used for queue operations (defaults to server configuration or "+envNamespace+")")
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
			ns := queueNamespaceFromCmd(cmd)
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
				Namespace:   ns,
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
	cmd.Flags().StringVarP(&payloadFile, "file", "f", "", "path to payload file (overrides --data)")
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
	var queue string
	var owner string
	var visibility time.Duration
	var block string
	var pageSize int
	var startAfter string
	var stateful bool
	var payloadOut string
	var output string

	cmd := &cobra.Command{
		Use:   "dequeue",
		Short: "Dequeue the next available message",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName, err := resolveQueueString(queue, envQueueName, "queue", true)
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
			ns := queueNamespaceFromCmd(cmd)
			ownerVal := strings.TrimSpace(owner)
			if ownerVal == "" {
				ownerVal = defaultOwner()
			}
			blockSecs, err := parseBlockSeconds(block)
			if err != nil {
				return err
			}
			opts := lockdclient.DequeueOptions{
				Namespace:    ns,
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
				"namespace":               msg.Namespace(),
				"queue":                   msg.Queue(),
				"message_id":              msg.MessageID(),
				"attempts":                msg.Attempts(),
				"failure_attempts":        msg.FailureAttempts(),
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
					{name: envQueueNamespace, value: msg.Namespace()},
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
	cmd.Flags().StringVarP(&payloadOut, "payload-out", "o", "", "write payload to file")
	cmd.Flags().StringVar(&output, "output", string(outputText), "output format (text|json)")
	cmd.Flags().StringVarP(&queue, "queue", "q", "", "queue name (required)")
	cmd.Example = strings.TrimSpace(`
  # Dequeue a message from a specific namespace
  lockd client queue dequeue --queue my.queue --namespace myns

  # Capture exports for follow-up commands
  eval "$(lockd client queue dequeue -q my.queue -n myns)"
`)
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
			ns := queueNamespaceFromCmd(cmd)
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
				Namespace:    ns,
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
	cmd.Flags().StringVarP(&queue, "queue", "q", "", "queue name")
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
	var intent string

	cmd := &cobra.Command{
		Use:   "nack",
		Short: "Release a message back to the queue",
		RunE: func(cmd *cobra.Command, args []string) error {
			ns := queueNamespaceFromCmd(cmd)
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
			intentVal := api.NackIntent(strings.ToLower(strings.TrimSpace(intent)))
			switch intentVal {
			case "", api.NackIntentFailure:
				intentVal = api.NackIntentFailure
			case api.NackIntentDefer:
			default:
				return fmt.Errorf("invalid --intent %q (expected failure|defer)", intent)
			}
			if intentVal == api.NackIntentDefer && strings.TrimSpace(reason) != "" {
				return fmt.Errorf("--reason is only supported when --intent=failure")
			}
			req := api.NackRequest{
				Namespace:    ns,
				Queue:        queueName,
				MessageID:    msgID,
				LeaseID:      lease,
				MetaETag:     meta,
				FencingToken: fencing,
				DelaySeconds: mustDurationSeconds(delay),
				Intent:       intentVal,
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
	cmd.Flags().StringVarP(&queue, "queue", "q", "", "queue name")
	cmd.Flags().StringVar(&messageID, "message", "", "message id")
	cmd.Flags().StringVar(&leaseID, "lease", "", "message lease id")
	cmd.Flags().StringVar(&metaETag, "meta-etag", "", "message meta etag")
	cmd.Flags().StringVar(&fencingToken, "fencing-token", "", "message fencing token")
	cmd.Flags().StringVar(&stateLease, "state-lease", "", "state lease id")
	cmd.Flags().StringVar(&stateFencing, "state-fencing-token", "", "state fencing token")
	cmd.Flags().DurationVar(&delay, "delay", 0, "nack visibility delay")
	cmd.Flags().StringVarP(&intent, "intent", "i", string(api.NackIntentFailure), "nack intent: failure|defer")
	cmd.Flags().StringVar(&reason, "reason", "", "optional failure reason (intent=failure)")
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
			ns := queueNamespaceFromCmd(cmd)
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
				Namespace:       ns,
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
	cmd.Flags().StringVarP(&queue, "queue", "q", "", "queue name")
	cmd.Flags().StringVar(&messageID, "message", "", "message id")
	cmd.Flags().StringVar(&leaseID, "lease", "", "message lease id")
	cmd.Flags().StringVar(&metaETag, "meta-etag", "", "message meta etag")
	cmd.Flags().StringVar(&fencingToken, "fencing-token", "", "message fencing token")
	cmd.Flags().StringVar(&stateLease, "state-lease", "", "state lease id")
	cmd.Flags().StringVar(&stateFencing, "state-fencing-token", "", "state fencing token")
	cmd.Flags().DurationVar(&extendBy, "extend", 0, "additional visibility duration")
	return cmd
}

func newTxnCommandBase(cfg *clientCLIConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "txn",
		Short: "Transaction coordination helpers",
	}
	cmd.AddCommand(
		newClientTxPrepareCommand(cfg),
		newClientTxCommitCommand(cfg),
		newClientTxRollbackCommand(cfg),
		newClientTxReplayCommand(cfg),
	)
	return cmd
}

func newClientTxnCommand(cfg *clientCLIConfig) *cobra.Command {
	cmd := newTxnCommandBase(cfg)
	cmd.Hidden = true
	return cmd
}

func newClientTxPrepareCommand(cfg *clientCLIConfig) *cobra.Command {
	var txnID string
	var participants []string
	var expires int64
	var namespace string
	cmd := &cobra.Command{
		Use:   "prepare",
		Short: "Record a pending transaction decision",
		RunE: func(cmd *cobra.Command, args []string) error {
			ns := namespace
			if ns == "" {
				ns = namespaces.Default
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			txn, err := resolveTxn(txnID)
			if err != nil {
				return err
			}
			parts, err := parseParticipants(participants, ns)
			if err != nil {
				return err
			}
			ctx, _ := commandContextWithCorrelation(cmd)
			resp, err := cli.TxnPrepare(ctx, api.TxnDecisionRequest{
				TxnID:         txn,
				Participants:  parts,
				ExpiresAtUnix: expires,
			})
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "txn_id=%s state=%s\n", resp.TxnID, resp.State)
			return nil
		},
	}
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id (xid, 20-char base32; default from "+envTxnID+")")
	cmd.Flags().StringSliceVarP(&participants, "participant", "p", nil, "participant entry (ns:key or key; optional @backend_hash; repeatable)")
	cmd.Flags().Int64Var(&expires, "expires-unix", 0, "expiry unix timestamp for pending decision (optional)")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", namespaces.Default, "default namespace for participants")
	return cmd
}

func newClientTxCommitCommand(cfg *clientCLIConfig) *cobra.Command {
	var txnID string
	var participants []string
	var namespace string
	cmd := &cobra.Command{
		Use:   "commit",
		Short: "Commit a transaction",
		RunE: func(cmd *cobra.Command, args []string) error {
			ns := namespace
			if ns == "" {
				ns = namespaces.Default
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			txn, err := resolveTxn(txnID)
			if err != nil {
				return err
			}
			parts, err := parseParticipants(participants, ns)
			if err != nil {
				return err
			}
			ctx, _ := commandContextWithCorrelation(cmd)
			resp, err := cli.TxnCommit(ctx, api.TxnDecisionRequest{
				TxnID:        txn,
				Participants: parts,
			})
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "txn_id=%s state=%s\n", resp.TxnID, resp.State)
			return nil
		},
	}
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id (xid, 20-char base32; default from "+envTxnID+")")
	cmd.Flags().StringSliceVarP(&participants, "participant", "p", nil, "participant entry (ns:key or key; optional @backend_hash; repeatable)")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", namespaces.Default, "default namespace for participants")
	return cmd
}

func newClientTxRollbackCommand(cfg *clientCLIConfig) *cobra.Command {
	var txnID string
	var participants []string
	var namespace string
	cmd := &cobra.Command{
		Use:   "rollback",
		Short: "Rollback a transaction",
		RunE: func(cmd *cobra.Command, args []string) error {
			ns := namespace
			if ns == "" {
				ns = namespaces.Default
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			txn, err := resolveTxn(txnID)
			if err != nil {
				return err
			}
			parts, err := parseParticipants(participants, ns)
			if err != nil {
				return err
			}
			ctx, _ := commandContextWithCorrelation(cmd)
			resp, err := cli.TxnRollback(ctx, api.TxnDecisionRequest{
				TxnID:        txn,
				Participants: parts,
			})
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "txn_id=%s state=%s\n", resp.TxnID, resp.State)
			return nil
		},
	}
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id (xid, 20-char base32; default from "+envTxnID+")")
	cmd.Flags().StringSliceVarP(&participants, "participant", "p", nil, "participant entry (ns:key or key; optional @backend_hash; repeatable)")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", namespaces.Default, "default namespace for participants")
	return cmd
}

func newClientTxReplayCommand(cfg *clientCLIConfig) *cobra.Command {
	var txnID string
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay a recorded transaction decision",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			txn, err := resolveTxn(txnID)
			if err != nil {
				return err
			}
			ctx, _ := commandContextWithCorrelation(cmd)
			resp, err := cli.TxnReplay(ctx, txn)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "txn_id=%s state=%s\n", resp.TxnID, resp.State)
			return nil
		},
	}
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id (xid, 20-char base32; default from "+envTxnID+")")
	return cmd
}

func parseParticipants(values []string, defaultNS string) ([]api.TxnParticipant, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]api.TxnParticipant, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		backendHash := ""
		if at := strings.LastIndex(v, "@"); at >= 0 {
			backendHash = strings.TrimSpace(v[at+1:])
			v = strings.TrimSpace(v[:at])
			if backendHash == "" {
				return nil, fmt.Errorf("invalid participant %q (empty backend hash)", v)
			}
		}
		ns := defaultNS
		key := v
		if parts := strings.SplitN(v, ":", 2); len(parts) == 2 {
			ns = strings.TrimSpace(parts[0])
			key = strings.TrimSpace(parts[1])
		}
		if key == "" {
			return nil, fmt.Errorf("invalid participant %q", v)
		}
		out = append(out, api.TxnParticipant{Namespace: ns, Key: key, BackendHash: backendHash})
	}
	return out, nil
}

func newClientAcquireCommand(cfg *clientCLIConfig) *cobra.Command {
	var ttl time.Duration
	var block string
	var owner string
	var output string
	var failureRetries int
	var keyFlag string
	var namespace string
	var txnID string

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
			ns := resolveNamespaceInput(namespace)
			txn := strings.TrimSpace(txnID)
			if txn == "" {
				txn = strings.TrimSpace(os.Getenv(envTxnID))
			}
			req := api.AcquireRequest{
				Namespace:  ns,
				Key:        key,
				Owner:      ownerVal,
				TTLSeconds: mustDurationSeconds(ttl),
				BlockSecs:  blockSecs,
				TxnID:      txn,
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
					{name: envTxnID, value: sess.TxnID},
					{name: envServerURL, value: cfg.server},
					{name: envVersion, value: strconv.FormatInt(sess.Version, 10)},
					{name: envETag, value: sess.StateETag},
					{name: envExpires, value: strconv.FormatInt(sess.ExpiresAt, 10)},
				}
				if sess.Namespace != "" {
					exports = append(exports, struct {
						name  string
						value string
					}{name: envNamespace, value: sess.Namespace})
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
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id to join (xid, 20-char base32; defaults to $LOCKD_TXN_ID)")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace for the key (defaults to server configuration)")
	return cmd
}

func newClientKeepAliveCommand(cfg *clientCLIConfig) *cobra.Command {
	var ttl time.Duration
	var leaseID string
	var output string
	var fencing string
	var keyFlag string
	var namespace string
	var txnID string
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
			ns := resolveNamespaceInput(namespace)
			txn := strings.TrimSpace(txnID)
			if txn == "" {
				txn = strings.TrimSpace(os.Getenv(envTxnID))
			}
			req := api.KeepAliveRequest{
				Namespace:  ns,
				Key:        key,
				LeaseID:    lease,
				TTLSeconds: mustDurationSeconds(ttl),
				TxnID:      txn,
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
	cmd.Flags().StringVar(&leaseID, "lease", "", "lease identifier (xid, 20-char base32)")
	cmd.Flags().DurationVar(&ttl, "ttl", defaultKeepAliveTTL, "new TTL duration")
	cmd.Flags().StringVar(&output, "output", string(outputJSON), "output format (text|json)")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace for the lease (defaults to server configuration)")
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id to echo (required when the lease is enlisted; defaults to $LOCKD_TXN_ID)")
	return cmd
}

func newClientReleaseCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var output string
	var fencing string
	var keyFlag string
	var namespace string
	var rollback bool
	var txnID string
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
			txn, err := resolveTxn(txnID)
			if err != nil {
				return err
			}
			cli.RegisterLeaseToken(lease, token)
			ns := resolveNamespaceInput(namespace)
			req := api.ReleaseRequest{Namespace: ns, Key: key, LeaseID: lease, TxnID: txn, Rollback: rollback}
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
	cmd.Flags().StringVar(&leaseID, "lease", "", "lease identifier (xid, 20-char base32)")
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id (xid, 20-char base32; default from "+envTxnID+")")
	cmd.Flags().StringVar(&output, "output", string(outputJSON), "output format (text|json)")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	cmd.Flags().BoolVar(&rollback, "rollback", false, "rollback staged changes instead of committing")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace for the lease (defaults to server configuration)")
	return cmd
}

func newClientGetCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var outputPath string
	var format string
	var showMeta bool
	var fencing string
	var keyFlag string
	var namespace string
	var public bool
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
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			var lease string
			opts := []lockdclient.GetOption{lockdclient.WithGetNamespace(resolveNamespaceInput(namespace))}
			if !public {
				lease, err = resolveLease(leaseID)
				if err != nil {
					return err
				}
				token, err := resolveFencing(fencing)
				if err != nil {
					return err
				}
				cli.RegisterLeaseToken(lease, token)
				opts = append(opts, lockdclient.WithGetLeaseID(lease), lockdclient.WithGetPublicDisabled(true))
			}
			ctx, _ := commandContextWithCorrelation(cmd)
			resp, err := cli.Get(ctx, key, opts...)
			if err != nil {
				return err
			}
			if resp != nil {
				defer resp.Close()
			}
			var data []byte
			if resp != nil {
				data, err = resp.Bytes()
				if err != nil {
					return err
				}
			}
			etag := ""
			version := ""
			if resp != nil {
				etag = resp.ETag
				version = resp.Version
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
	cmd.Flags().StringVar(&leaseID, "lease", "", "active lease identifier (xid, 20-char base32)")
	cmd.Flags().StringVarP(&outputPath, "output", "o", "-", "output file (use - for stdout)")
	cmd.Flags().StringVar(&format, "type", string(stateFormatAuto), "output type (auto|raw|json|yaml)")
	cmd.Flags().BoolVar(&showMeta, "show-meta", false, "print version/etag metadata to stderr")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace for the key (defaults to server configuration)")
	cmd.Flags().BoolVar(&public, "public", false, "read the latest published snapshot without acquiring a lease")
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
	var namespace string
	var txnID string
	cmd := &cobra.Command{
		Use:   "update [input]",
		Short: "Upload new state from a file",
		Example: `  # Pipe edited state back into the server
  eval "$(lockd client acquire --key orders)" \
    && lockd client get --output - \
    | lockd client edit /status/counter++ \
    | lockd client update`,
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
			txn, err := resolveTxn(txnID)
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
			ns := resolveNamespaceInput(namespace)
			opts := lockdclient.UpdateOptions{Namespace: ns, IfVersion: ifVersion, IfETag: ifETag, FencingToken: token, TxnID: txn}
			ctx, _ := commandContextWithCorrelation(cmd)
			result, err := cli.UpdateBytes(ctx, key, lease, payload, opts)
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
	cmd.Flags().StringVar(&leaseID, "lease", "", "active lease identifier (xid, 20-char base32)")
	cmd.Flags().StringVar(&format, "type", string(stateFormatAuto), "input type (auto|raw|json|yaml)")
	cmd.Flags().StringVar(&ifVersion, "if-version", "", "conditional update on version")
	cmd.Flags().StringVar(&ifETag, "if-etag", "", "conditional update on state etag")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id (xid, 20-char base32; default from "+envTxnID+")")
	cmd.Flags().StringVar(&output, "output", string(outputJSON), "output format (text|json)")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace for the key (defaults to server configuration)")
	return cmd
}

func newClientRemoveCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var ifVersion string
	var ifETag string
	var fencing string
	var output string
	var keyFlag string
	var namespace string
	var txnID string
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
			txn, err := resolveTxn(txnID)
			if err != nil {
				return err
			}
			cli.RegisterLeaseToken(lease, token)
			ns := resolveNamespaceInput(namespace)
			opts := lockdclient.RemoveOptions{Namespace: ns, IfVersion: ifVersion, IfETag: ifETag, FencingToken: token, TxnID: txn}
			ctx, _ := commandContextWithCorrelation(cmd)
			result, err := cli.Remove(ctx, key, lease, opts)
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
	cmd.Flags().StringVar(&leaseID, "lease", "", "active lease identifier (xid, 20-char base32)")
	cmd.Flags().StringVar(&ifVersion, "if-version", "", "conditional remove on version")
	cmd.Flags().StringVar(&ifETag, "if-etag", "", "conditional remove on state etag")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id (xid, 20-char base32; default from "+envTxnID+")")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace for the key (defaults to server configuration)")
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
	var namespace string
	var txnID string
	cmd := &cobra.Command{
		Use:   "set mutation [mutation...]",
		Short: "Mutate JSON state fields for an active lease",
		Example: `  # Increment a counter and stamp the current time
  lockd client set --key orders /progress/counter++ time:/progress/updated=NOW

  # Mutate multiple fields with brace shorthand + quoted keys
  lockd client set --key ledger '/data{/hello key="mars traveler",/count++}' /meta/previous=world`,
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
			txn, err := resolveTxn(txnID)
			if err != nil {
				return err
			}
			cli.RegisterLeaseToken(lease, token)
			ns := resolveNamespaceInput(namespace)
			ctx := cmd.Context()
			resp, err := cli.Get(ctx, key,
				lockdclient.WithGetNamespace(ns),
				lockdclient.WithGetLeaseID(lease),
				lockdclient.WithGetPublicDisabled(true),
			)
			if err != nil {
				return err
			}
			if resp != nil {
				defer resp.Close()
			}
			var stateBytes []byte
			if resp != nil {
				stateBytes, err = resp.Bytes()
				if err != nil {
					return err
				}
			}
			etag := ""
			version := ""
			if resp != nil {
				etag = resp.ETag
				version = resp.Version
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
			opts := lockdclient.UpdateOptions{Namespace: ns, FencingToken: token, TxnID: txn}
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
			result, err := cli.UpdateBytes(ctx, key, lease, payload, opts)
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
	cmd.Flags().StringVar(&leaseID, "lease", "", "active lease identifier (xid, 20-char base32)")
	cmd.Flags().StringVar(&ifVersion, "if-version", "", "override version used for CAS (default from fetched state)")
	cmd.Flags().StringVar(&ifETag, "if-etag", "", "override ETag used for CAS (default from fetched state)")
	cmd.Flags().BoolVar(&noCAS, "no-cas", false, "skip conditional update using fetched version/etag (fencing token still required)")
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id (xid, 20-char base32; default from "+envTxnID+")")
	cmd.Flags().StringVar(&output, "output", string(outputJSON), "output format (text|json)")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace for the key (defaults to server configuration)")
	return cmd
}

func newClientEditCommand(_ *clientCLIConfig) *cobra.Command {
	var inputPath string
	var format string
	cmd := &cobra.Command{
		Use:   "edit mutation [mutation...]",
		Short: "Apply JSON field mutations to a local file",
		Example: `  # Increment counter in place
  lockd client edit --file checkpoint.json /status/counter++`,
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

func newClientQueryCommand(cfg *clientCLIConfig) *cobra.Command {
	var namespace string
	var outputPath string
	var outputFormat string
	var documents bool
	var directory string
	cmd := &cobra.Command{
		Use:   "query [selector ...]",
		Short: "Execute LQL selectors and stream matching keys or documents",
		Example: `  # List keys that match a selector
  lockd client query '/status/state="staged"'

  # Stream documents as NDJSON
  lockd client query --documents '/progress/count>=10'

  # Iterate over keys via text output
  lockd client query --output text '/status/state="staged"' \
    | while read -r key; do lockd client get --public --key "$key"; done`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			selector := strings.Join(args, "\n")
			mode := outputMode(strings.ToLower(outputFormat))
			if mode != outputJSON && mode != outputText {
				return fmt.Errorf("invalid --output %q (expected json or text)", outputFormat)
			}
			dir := strings.TrimSpace(directory)
			docsMode := documents || dir != ""
			if docsMode && mode == outputText {
				return fmt.Errorf("--output text is only supported for key listings; omit --documents or switch to json")
			}
			if dir != "" && outputPath != "" && outputPath != "-" {
				return fmt.Errorf("--directory cannot be combined with --file/-f output")
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			ns := resolveNamespaceInput(namespace)
			opts := []lockdclient.QueryOption{
				lockdclient.WithQueryNamespace(ns),
				lockdclient.WithQuery(selector),
			}
			if documents {
				opts = append(opts, lockdclient.WithQueryReturnDocuments())
			}
			ctx, _ := commandContextWithCorrelation(cmd)
			resp, err := cli.Query(ctx, opts...)
			if err != nil {
				return err
			}
			if resp == nil {
				return nil
			}
			defer resp.Close()
			if docsMode {
				if dir != "" {
					return writeQueryDocumentsDirectory(resp, dir)
				}
				return writeQueryDocumentsStream(cmd, resp, outputPath)
			}
			return writeQueryKeys(cmd, outputPath, mode, resp.Keys())
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace to query (defaults to configured namespace)")
	cmd.Flags().StringVarP(&outputPath, "file", "f", "-", "output file (use - for stdout)")
	cmd.Flags().StringVarP(&outputFormat, "output", "o", string(outputJSON), "output format for keys (json|text)")
	cmd.Flags().BoolVarP(&documents, "documents", "d", false, "stream matching documents as NDJSON instead of keys")
	cmd.Flags().StringVarP(&directory, "directory", "F", "", "directory to write individual document files (enables documents mode)")
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

func writeQueryKeys(cmd *cobra.Command, path string, mode outputMode, keys []string) error {
	if mode == outputText {
		var buf bytes.Buffer
		for _, key := range keys {
			buf.WriteString(key)
			buf.WriteByte('\n')
		}
		return writeQueryBytes(cmd, path, buf.Bytes())
	}
	if keys == nil {
		keys = []string{}
	}
	payload, err := json.Marshal(keys)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	return writeQueryBytes(cmd, path, payload)
}

func writeQueryBytes(cmd *cobra.Command, path string, data []byte) error {
	if path == "" || path == "-" {
		if len(data) == 0 {
			return nil
		}
		_, err := cmd.OutOrStdout().Write(data)
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func writeQueryDocumentsStream(cmd *cobra.Command, resp *lockdclient.QueryResponse, path string) error {
	if resp == nil {
		return nil
	}
	writer, err := openQueryWriter(cmd, path)
	if err != nil {
		return err
	}
	defer writer.Close()
	buf := bufio.NewWriter(writer)
	if err := resp.ForEach(func(row lockdclient.QueryRow) error {
		reader, err := row.DocumentReader()
		if err != nil {
			return err
		}
		defer reader.Close()
		if _, err := io.Copy(buf, reader); err != nil {
			return err
		}
		return buf.WriteByte('\n')
	}); err != nil {
		_ = buf.Flush()
		return err
	}
	return buf.Flush()
}

func writeQueryDocumentsDirectory(resp *lockdclient.QueryResponse, dir string) error {
	if resp == nil {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	counts := map[string]int{}
	return resp.ForEach(func(row lockdclient.QueryRow) error {
		reader, err := row.DocumentReader()
		if err != nil {
			return err
		}
		defer reader.Close()
		base := sanitizeKeyFilename(row.Key)
		if base == "" {
			base = "doc"
		}
		idx := counts[base]
		counts[base] = idx + 1
		name := base
		if idx > 0 {
			name = fmt.Sprintf("%s-%d", base, idx)
		}
		tmp, err := os.CreateTemp(dir, name+"-*.tmp")
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(tmp, reader)
		closeErr := tmp.Close()
		if copyErr != nil {
			os.Remove(tmp.Name())
			return copyErr
		}
		if closeErr != nil {
			os.Remove(tmp.Name())
			return closeErr
		}
		finalPath := filepath.Join(dir, name+".json")
		if err := os.Rename(tmp.Name(), finalPath); err != nil {
			os.Remove(tmp.Name())
			return err
		}
		return nil
	})
}

func sanitizeKeyFilename(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		case unicode.IsSpace(r):
			b.WriteByte('-')
		case r == '/' || r == '\\':
			b.WriteByte('-')
		default:
			b.WriteByte('_')
		}
	}
	return strings.Trim(b.String(), "-_.")
}

func openQueryWriter(cmd *cobra.Command, path string) (io.WriteCloser, error) {
	if path == "" || path == "-" {
		return nopWriteCloser{Writer: cmd.OutOrStdout()}, nil
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return file, nil
}

type nopWriteCloser struct {
	io.Writer
}

func (n nopWriteCloser) Close() error { return nil }

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

func newClientAttachmentsCommand(cfg *clientCLIConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "attachments",
		Short: "Manage attachments for a key",
	}
	cmd.AddCommand(
		newClientAttachmentsListCommand(cfg),
		newClientAttachmentsGetCommand(cfg),
		newClientAttachmentsPutCommand(cfg),
		newClientAttachmentsDeleteCommand(cfg),
		newClientAttachmentsDeleteAllCommand(cfg),
	)
	return cmd
}

func newClientAttachmentsListCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var txnID string
	var fencing string
	var keyFlag string
	var namespace string
	var public bool
	var output string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List attachments for a key",
		Example: `  # List attachments while holding a lease
  eval "$(lockd client acquire --key orders)"
  lockd client attachments list

  # Public list after release
  lockd client attachments list --key orders --public`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := resolveKeyInput(keyFlag, keyRequireExisting)
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
			ctx, _ := commandContextWithCorrelation(cmd)
			req := lockdclient.ListAttachmentsRequest{
				Namespace: resolveNamespaceInput(namespace),
				Key:       key,
				Public:    public,
			}
			if !public {
				lease, err := resolveLease(leaseID)
				if err != nil {
					return err
				}
				txn, err := resolveTxn(txnID)
				if err != nil {
					return err
				}
				token, err := resolveFencing(fencing)
				if err != nil {
					return err
				}
				cli.RegisterLeaseToken(lease, token)
				req.LeaseID = lease
				req.TxnID = txn
			}
			res, err := cli.ListAttachments(ctx, req)
			if err != nil {
				return err
			}
			if output == "text" {
				out := cmd.OutOrStdout()
				for _, att := range res.Attachments {
					fmt.Fprintf(out, "%s\t%d\t%s\n", att.Name, att.Size, att.ID)
				}
				return nil
			}
			return writeJSON(cmd.OutOrStdout(), res)
		},
	}
	cmd.Flags().StringVarP(&keyFlag, "key", "k", "", "key to list attachments for (falls back to "+envKey+")")
	cmd.Flags().StringVar(&leaseID, "lease", "", "lease identifier (xid, 20-char base32)")
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id (xid, 20-char base32; default from "+envTxnID+")")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace for the key (defaults to server configuration)")
	cmd.Flags().BoolVar(&public, "public", false, "list attachments without a lease")
	cmd.Flags().StringVar(&output, "output", "json", "output format (text|json)")
	return cmd
}

func newClientAttachmentsGetCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var txnID string
	var fencing string
	var keyFlag string
	var namespace string
	var public bool
	var name string
	var id string
	var outputPath string
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Download an attachment",
		Example: `  # Download an attachment by name
  eval "$(lockd client acquire --key orders)"
  lockd client attachments get --name invoice.pdf -o invoice.pdf

  # Public download after release
  lockd client attachments get --key orders --name invoice.pdf --public -o invoice.pdf`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := resolveKeyInput(keyFlag, keyRequireExisting)
			if err != nil {
				return err
			}
			if strings.TrimSpace(name) == "" && strings.TrimSpace(id) == "" {
				return fmt.Errorf("--name or --id required")
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			ctx, _ := commandContextWithCorrelation(cmd)
			req := lockdclient.GetAttachmentRequest{
				Namespace: resolveNamespaceInput(namespace),
				Key:       key,
				Public:    public,
				Selector:  lockdclient.AttachmentSelector{Name: name, ID: id},
			}
			if !public {
				lease, err := resolveLease(leaseID)
				if err != nil {
					return err
				}
				txn, err := resolveTxn(txnID)
				if err != nil {
					return err
				}
				token, err := resolveFencing(fencing)
				if err != nil {
					return err
				}
				cli.RegisterLeaseToken(lease, token)
				req.LeaseID = lease
				req.TxnID = txn
			}
			att, err := cli.GetAttachment(ctx, req)
			if err != nil {
				return err
			}
			if att != nil {
				defer att.Close()
			}
			writer, err := openQueryWriter(cmd, outputPath)
			if err != nil {
				return err
			}
			defer writer.Close()
			_, err = io.Copy(writer, att)
			return err
		},
	}
	cmd.Flags().StringVarP(&keyFlag, "key", "k", "", "key to read attachments for (falls back to "+envKey+")")
	cmd.Flags().StringVar(&leaseID, "lease", "", "lease identifier (xid, 20-char base32)")
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id (xid, 20-char base32; default from "+envTxnID+")")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace for the key (defaults to server configuration)")
	cmd.Flags().BoolVar(&public, "public", false, "read attachments without a lease")
	cmd.Flags().StringVar(&name, "name", "", "attachment name")
	cmd.Flags().StringVar(&id, "id", "", "attachment id")
	cmd.Flags().StringVarP(&outputPath, "output", "o", "-", "output path (use - for stdout)")
	return cmd
}

func newClientAttachmentsPutCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var txnID string
	var fencing string
	var keyFlag string
	var namespace string
	var name string
	var path string
	var contentType string
	var preventOverwrite bool
	var maxBytes int64
	cmd := &cobra.Command{
		Use:   "put",
		Short: "Upload an attachment",
		Example: `  # Upload a file while holding a lease
  eval "$(lockd client acquire --key orders)"
  lockd client attachments put --name invoice.pdf --file ./invoice.pdf --content-type application/pdf

  # Stream from stdin
  cat invoice.pdf | lockd client attachments put --name invoice.pdf --file -`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := resolveKeyInput(keyFlag, keyRequireExisting)
			if err != nil {
				return err
			}
			if strings.TrimSpace(name) == "" {
				return fmt.Errorf("--name required")
			}
			if strings.TrimSpace(path) == "" {
				return fmt.Errorf("--file required")
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
			if path == "-" {
				reader = cmd.InOrStdin()
			} else {
				file, err := os.Open(path)
				if err != nil {
					return err
				}
				reader = file
				closer = file
			}
			if closer != nil {
				defer closer.Close()
			}
			lease, err := resolveLease(leaseID)
			if err != nil {
				return err
			}
			txn, err := resolveTxn(txnID)
			if err != nil {
				return err
			}
			token, err := resolveFencing(fencing)
			if err != nil {
				return err
			}
			cli.RegisterLeaseToken(lease, token)
			ctx, _ := commandContextWithCorrelation(cmd)
			var maxPtr *int64
			if cmd.Flags().Changed("max-bytes") {
				value := maxBytes
				maxPtr = &value
			}
			res, err := cli.Attach(ctx, lockdclient.AttachRequest{
				Namespace:        resolveNamespaceInput(namespace),
				Key:              key,
				LeaseID:          lease,
				TxnID:            txn,
				Name:             name,
				Body:             reader,
				ContentType:      contentType,
				MaxBytes:         maxPtr,
				PreventOverwrite: preventOverwrite,
			})
			if err != nil {
				return err
			}
			return writeJSON(cmd.OutOrStdout(), res)
		},
	}
	cmd.Flags().StringVarP(&keyFlag, "key", "k", "", "key to attach to (falls back to "+envKey+")")
	cmd.Flags().StringVar(&leaseID, "lease", "", "lease identifier (xid, 20-char base32)")
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id (xid, 20-char base32; default from "+envTxnID+")")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace for the key (defaults to server configuration)")
	cmd.Flags().StringVar(&name, "name", "", "attachment name")
	cmd.Flags().StringVarP(&path, "file", "f", "", "path to attachment payload (use - for stdin)")
	cmd.Flags().StringVar(&contentType, "content-type", "", "attachment content type (default application/octet-stream)")
	cmd.Flags().BoolVar(&preventOverwrite, "prevent-overwrite", false, "skip if attachment already exists")
	cmd.Flags().Int64Var(&maxBytes, "max-bytes", 0, "maximum attachment size (0 = unlimited)")
	return cmd
}

func newClientAttachmentsDeleteCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var txnID string
	var fencing string
	var keyFlag string
	var namespace string
	var name string
	var id string
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete an attachment",
		Example: `  # Delete a single attachment by name
  eval "$(lockd client acquire --key orders)"
  lockd client attachments delete --name invoice.pdf`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := resolveKeyInput(keyFlag, keyRequireExisting)
			if err != nil {
				return err
			}
			if strings.TrimSpace(name) == "" && strings.TrimSpace(id) == "" {
				return fmt.Errorf("--name or --id required")
			}
			if err := cfg.load(); err != nil {
				return err
			}
			defer cfg.cleanup()
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			lease, err := resolveLease(leaseID)
			if err != nil {
				return err
			}
			txn, err := resolveTxn(txnID)
			if err != nil {
				return err
			}
			token, err := resolveFencing(fencing)
			if err != nil {
				return err
			}
			cli.RegisterLeaseToken(lease, token)
			ctx, _ := commandContextWithCorrelation(cmd)
			res, err := cli.DeleteAttachment(ctx, lockdclient.DeleteAttachmentRequest{
				Namespace: resolveNamespaceInput(namespace),
				Key:       key,
				LeaseID:   lease,
				TxnID:     txn,
				Selector:  lockdclient.AttachmentSelector{Name: name, ID: id},
			})
			if err != nil {
				return err
			}
			return writeJSON(cmd.OutOrStdout(), res)
		},
	}
	cmd.Flags().StringVarP(&keyFlag, "key", "k", "", "key to delete from (falls back to "+envKey+")")
	cmd.Flags().StringVar(&leaseID, "lease", "", "lease identifier (xid, 20-char base32)")
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id (xid, 20-char base32; default from "+envTxnID+")")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace for the key (defaults to server configuration)")
	cmd.Flags().StringVar(&name, "name", "", "attachment name")
	cmd.Flags().StringVar(&id, "id", "", "attachment id")
	return cmd
}

func newClientAttachmentsDeleteAllCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var txnID string
	var fencing string
	var keyFlag string
	var namespace string
	cmd := &cobra.Command{
		Use:   "delete-all",
		Short: "Delete all attachments for a key",
		Example: `  # Delete all attachments for a key
  eval "$(lockd client acquire --key orders)"
  lockd client attachments delete-all`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := resolveKeyInput(keyFlag, keyRequireExisting)
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
			lease, err := resolveLease(leaseID)
			if err != nil {
				return err
			}
			txn, err := resolveTxn(txnID)
			if err != nil {
				return err
			}
			token, err := resolveFencing(fencing)
			if err != nil {
				return err
			}
			cli.RegisterLeaseToken(lease, token)
			ctx, _ := commandContextWithCorrelation(cmd)
			res, err := cli.DeleteAllAttachments(ctx, lockdclient.DeleteAllAttachmentsRequest{
				Namespace: resolveNamespaceInput(namespace),
				Key:       key,
				LeaseID:   lease,
				TxnID:     txn,
			})
			if err != nil {
				return err
			}
			return writeJSON(cmd.OutOrStdout(), res)
		},
	}
	cmd.Flags().StringVarP(&keyFlag, "key", "k", "", "key to delete from (falls back to "+envKey+")")
	cmd.Flags().StringVar(&leaseID, "lease", "", "lease identifier (xid, 20-char base32)")
	cmd.Flags().StringVar(&txnID, "txn-id", "", "transaction id (xid, 20-char base32; default from "+envTxnID+")")
	cmd.Flags().StringVar(&fencing, "fencing-token", "", "fencing token (default from "+envFencingToken+")")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace for the key (defaults to server configuration)")
	return cmd
}
