package main

import (
	"bytes"
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

	lockdclient "pkt.systems/lockd/client"
	"pkt.systems/lockd/internal/tlsutil"
)

const (
	clientServerKey  = "client.server"
	clientBundleKey  = "client.bundle"
	clientMTLSKey    = "client.mtls"
	clientTimeoutKey = "client.timeout"

	envLeaseID    = "LOCKD_CLIENT_LEASE"
	envKey        = "LOCKD_CLIENT_KEY"
	envOwner      = "LOCKD_CLIENT_OWNER"
	envVersion    = "LOCKD_CLIENT_VERSION"
	envETag       = "LOCKD_CLIENT_ETAG"
	envExpires    = "LOCKD_CLIENT_EXPIRES_UNIX"
	envServerURL  = "LOCKD_CLIENT_SERVER"
	envRetryAfter = "LOCKD_CLIENT_RETRY_AFTER"
)

func newClientCommand() *cobra.Command {
	cfg := &clientCLIConfig{}
	cmd := &cobra.Command{
		Use:   "client",
		Short: "Interact with a running lockd server",
	}

	flags := cmd.PersistentFlags()
	flags.String("server", "https://127.0.0.1:9341", "lockd server base URL")
	flags.String("bundle", "", "path to client bundle PEM (default auto-discover under $HOME/.lockd)")
	flags.Bool("mtls", true, "enable mutual TLS")
	flags.Duration("timeout", 15*time.Second, "HTTP client timeout")

	mustBindFlag(clientServerKey, "LOCKD_CLIENT_SERVER", flags.Lookup("server"))
	mustBindFlag(clientBundleKey, "LOCKD_CLIENT_BUNDLE", flags.Lookup("bundle"))
	mustBindFlag(clientMTLSKey, "LOCKD_CLIENT_MTLS", flags.Lookup("mtls"))
	mustBindFlag(clientTimeoutKey, "LOCKD_CLIENT_TIMEOUT", flags.Lookup("timeout"))

	cmd.AddCommand(
		newClientAcquireCommand(cfg),
		newClientKeepAliveCommand(cfg),
		newClientReleaseCommand(cfg),
		newClientGetCommand(cfg),
		newClientUpdateCommand(cfg),
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
	bundle           string
	mtls             bool
	timeout          time.Duration
	cachedHTTPClient *http.Client
}

func (c *clientCLIConfig) load() error {
	if c.loaded {
		return nil
	}
	server := viper.GetString(clientServerKey)
	if server == "" {
		server = "https://127.0.0.1:9341"
	}
	normalized, err := normalizeServerURL(server, c.mtls)
	if err != nil {
		return err
	}
	c.server = normalized
	c.bundle = viper.GetString(clientBundleKey)
	c.mtls = viper.GetBool(clientMTLSKey)
	// Ensure default true if flag/env not set.
	if !viper.IsSet(clientMTLSKey) {
		c.mtls = true
	}
	timeout := viper.GetDuration(clientTimeoutKey)
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	c.timeout = timeout
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
	if c.mtls {
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
	return lockdclient.New(c.server, lockdclient.WithHTTPClient(httpClient))
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

func normalizeServerURL(raw string, mtls bool) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("server URL is empty")
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw, nil
	}
	host := raw
	if !strings.Contains(host, ":") {
		if mtls {
			return "https://" + host, nil
		}
		return "http://" + host, nil
	}
	if mtls {
		return "https://" + host, nil
	}
	return "http://" + host, nil
}

func newClientAcquireCommand(cfg *clientCLIConfig) *cobra.Command {
	var ttl time.Duration
	var block time.Duration
	var owner string
	var output string
	var attempts int
	var idempotency string

	cmd := &cobra.Command{
		Use:   "acquire <key>",
		Short: "Acquire a lease for a key",
		Example: `  # Acquire a lease and export environment variables
  eval "$(lockd client acquire --server https://127.0.0.1:9341 --ttl 30s orders)"`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]
			if err := cfg.load(); err != nil {
				return err
			}
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			ownerVal := owner
			if ownerVal == "" {
				ownerVal = defaultOwner()
			}
			req := lockdclient.AcquireRequest{
				Key:         key,
				Owner:       ownerVal,
				TTLSeconds:  mustDurationSeconds(ttl),
				BlockSecs:   mustDurationSeconds(block),
				Idempotency: idempotency,
			}
			if req.TTLSeconds <= 0 {
				return fmt.Errorf("ttl must be > 0")
			}
			ctx := cmd.Context()
			var opts []lockdclient.AcquireOption
			if attempts > 0 {
				opts = append(opts, lockdclient.WithAcquireMaxAttempts(attempts))
			}
			resp, err := cli.Acquire(ctx, req, opts...)
			if err != nil {
				return err
			}
			switch outputMode(strings.ToLower(output)) {
			case outputJSON:
				return writeJSON(cmd.OutOrStdout(), resp)
			default:
				out := cmd.OutOrStdout()
				exports := []struct {
					name  string
					value string
				}{
					{name: envLeaseID, value: resp.LeaseID},
					{name: envKey, value: resp.Key},
					{name: envOwner, value: resp.Owner},
					{name: envServerURL, value: cfg.server},
					{name: envVersion, value: strconv.FormatInt(resp.Version, 10)},
					{name: envETag, value: resp.StateETag},
					{name: envExpires, value: strconv.FormatInt(resp.ExpiresAt, 10)},
				}
				if resp.RetryAfter > 0 {
					exports = append(exports, struct {
						name  string
						value string
					}{name: envRetryAfter, value: strconv.FormatInt(resp.RetryAfter, 10)})
				}
				for _, ex := range exports {
					fmt.Fprintf(out, "export %s=%q\n", ex.name, ex.value)
				}
			}
			return nil
		},
	}
	cmd.Flags().DurationVar(&ttl, "ttl", 30*time.Second, "lease TTL duration")
	cmd.Flags().DurationVar(&block, "block", 60*time.Second, "time to wait on conflicts before retrying")
	cmd.Flags().StringVar(&owner, "owner", "", "owner identity (default hostname + pid)")
	cmd.Flags().StringVar(&output, "output", string(outputText), "output format (text|json)")
	cmd.Flags().IntVar(&attempts, "attempts", 0, "maximum acquire attempts (0=client default)")
	cmd.Flags().StringVar(&idempotency, "idempotency", "", "optional idempotency token")
	return cmd
}

func newClientKeepAliveCommand(cfg *clientCLIConfig) *cobra.Command {
	var ttl time.Duration
	var leaseID string
	var output string
	cmd := &cobra.Command{
		Use:   "keepalive <key>",
		Short: "Keep an existing lease alive",
		Example: `  # Extend the current lease using the exported environment variable
  lockd client keepalive --ttl 1m orders`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			lease, err := resolveLease(leaseID)
			if err != nil {
				return err
			}
			if err := cfg.load(); err != nil {
				return err
			}
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			req := lockdclient.KeepAliveRequest{
				Key:        args[0],
				LeaseID:    lease,
				TTLSeconds: mustDurationSeconds(ttl),
			}
			if req.TTLSeconds <= 0 {
				return fmt.Errorf("ttl must be > 0")
			}
			resp, err := cli.KeepAlive(cmd.Context(), req)
			if err != nil {
				return err
			}
			switch outputMode(strings.ToLower(output)) {
			case outputJSON:
				return writeJSON(cmd.OutOrStdout(), resp)
			default:
				fmt.Fprintf(cmd.OutOrStdout(), "expires_at_unix: %d (%s)\n", resp.ExpiresAt, formatTime(resp.ExpiresAt))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&leaseID, "lease", "", "lease identifier")
	cmd.Flags().DurationVar(&ttl, "ttl", 30*time.Second, "new TTL duration")
	cmd.Flags().StringVar(&output, "output", string(outputText), "output format (text|json)")
	return cmd
}

func newClientReleaseCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var output string
	cmd := &cobra.Command{
		Use:   "release <key>",
		Short: "Release a lease",
		Example: `  # Release the current lease
  lockd client release orders`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			lease, err := resolveLease(leaseID)
			if err != nil {
				return err
			}
			if err := cfg.load(); err != nil {
				return err
			}
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			req := lockdclient.ReleaseRequest{Key: args[0], LeaseID: lease}
			resp, err := cli.Release(cmd.Context(), req)
			if err != nil {
				return err
			}
			switch outputMode(strings.ToLower(output)) {
			case outputJSON:
				return writeJSON(cmd.OutOrStdout(), resp)
			default:
				fmt.Fprintf(cmd.OutOrStdout(), "released: %t\n", resp.Released)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&leaseID, "lease", "", "lease identifier")
	cmd.Flags().StringVar(&output, "output", string(outputText), "output format (text|json)")
	return cmd
}

func newClientGetCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var outputPath string
	var format string
	var showMeta bool
	cmd := &cobra.Command{
		Use:   "get <key>",
		Short: "Fetch state JSON for a key",
		Example: `  # Pretty-print the current state to stdout
  lockd client get orders -o -`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			lease, err := resolveLease(leaseID)
			if err != nil {
				return err
			}
			if err := cfg.load(); err != nil {
				return err
			}
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			data, etag, version, err := cli.GetState(cmd.Context(), args[0], lease)
			if err != nil {
				return err
			}
			fmtChoice := parseStateFormat(stateFormat(format), outputPath)
			payload, err := formatStatePayload(data, fmtChoice)
			if err != nil {
				return err
			}
			if err := writeStateOutput(outputPath, payload); err != nil {
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
	cmd.Flags().StringVar(&leaseID, "lease", "", "active lease identifier")
	cmd.Flags().StringVarP(&outputPath, "output", "o", "-", "output file (use - for stdout)")
	cmd.Flags().StringVar(&format, "type", string(stateFormatAuto), "output type (auto|raw|json|yaml)")
	cmd.Flags().BoolVar(&showMeta, "show-meta", false, "print version/etag metadata to stderr")
	return cmd
}

func newClientUpdateCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var format string
	var ifVersion string
	var ifETag string
	cmd := &cobra.Command{
		Use:   "update <key> [input]",
		Short: "Upload new state from a file",
		Example: `  # Pipe edited state back into the server
  lockd client get orders -o - \
    | lockd client edit status.counter++ \
    | lockd client update orders`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			lease, err := resolveLease(leaseID)
			if err != nil {
				return err
			}
			if err := cfg.load(); err != nil {
				return err
			}
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			path := "-"
			if len(args) == 2 {
				path = args[1]
			}
			fmtChoice := parseStateFormat(stateFormat(format), path)
			payload, err := loadStatePayload(path, fmtChoice)
			if err != nil {
				return err
			}
			opts := lockdclient.UpdateStateOptions{IfVersion: ifVersion, IfETag: ifETag}
			result, err := cli.UpdateState(cmd.Context(), args[0], lease, payload, opts)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "version: %d\nbytes: %d\netag: %s\n", result.NewVersion, result.BytesWritten, result.NewStateETag)
			return nil
		},
	}
	cmd.Flags().StringVar(&leaseID, "lease", "", "active lease identifier")
	cmd.Flags().StringVar(&format, "type", string(stateFormatAuto), "input type (auto|raw|json|yaml)")
	cmd.Flags().StringVar(&ifVersion, "if-version", "", "conditional update on version")
	cmd.Flags().StringVar(&ifETag, "if-etag", "", "conditional update on state etag")
	return cmd
}

func newClientSetCommand(cfg *clientCLIConfig) *cobra.Command {
	var leaseID string
	var ifVersion string
	var ifETag string
	var noCAS bool
	cmd := &cobra.Command{
		Use:   "set <key> mutation [mutation...]",
		Short: "Mutate JSON state fields for an active lease",
		Example: `  # Increment a counter and stamp the current time
  lockd client set orders progress.counter++ time:progress.updated=NOW`,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]
			mutations, err := parseMutations(args[1:], time.Now())
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
			cli, err := cfg.client()
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			stateBytes, etag, version, err := cli.GetState(ctx, key, lease)
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
			opts := lockdclient.UpdateStateOptions{}
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
			result, err := cli.UpdateState(ctx, key, lease, payload, opts)
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "version: %d\nbytes: %d\netag: %s\n", result.NewVersion, result.BytesWritten, result.NewStateETag)
			return nil
		},
	}
	cmd.Flags().StringVar(&leaseID, "lease", "", "active lease identifier")
	cmd.Flags().StringVar(&ifVersion, "if-version", "", "override version used for CAS (default from fetched state)")
	cmd.Flags().StringVar(&ifETag, "if-etag", "", "override ETag used for CAS (default from fetched state)")
	cmd.Flags().BoolVar(&noCAS, "no-cas", false, "skip conditional update using fetched version/etag")
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
