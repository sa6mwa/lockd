package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"pkt.systems/lockd"
	"pkt.systems/lockd/api"
	"pkt.systems/lockd/internal/tcclient"
	"pkt.systems/lockd/internal/tccluster"
	"pkt.systems/lockd/tlsutil"
)

const tcClusterTimeout = 5 * time.Second

func newTCAnnounceCommand() *cobra.Command {
	var serverBundlePath string
	var trustDir string
	var selfEndpoint string
	var endpoints []string
	var disableMTLS bool
	cmd := &cobra.Command{
		Use:     "announce",
		Aliases: []string{"join"},
		Short:   "Announce this node to the TC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			if serverBundlePath == "" {
				if def, err := lockd.DefaultBundlePath(); err == nil {
					serverBundlePath = def
				}
			}
			client, err := tcClusterHTTPClient("", serverBundlePath, trustDir, disableMTLS)
			if err != nil {
				return err
			}
			selfEndpoint = strings.TrimSpace(selfEndpoint)
			if selfEndpoint == "" {
				return fmt.Errorf("--self is required")
			}
			seeds := tccluster.NormalizeEndpoints(endpoints)
			if len(seeds) == 0 {
				return fmt.Errorf("--endpoint is required")
			}
			if disableMTLS {
				for _, endpoint := range seeds {
					if endpoint != selfEndpoint {
						return fmt.Errorf("mTLS is required when contacting non-self TC endpoints")
					}
				}
			}
			merged, err := announceCluster(cmd.Context(), client, selfEndpoint, seeds)
			if err != nil {
				return err
			}
			for _, endpoint := range merged {
				fmt.Fprintln(cmd.OutOrStdout(), endpoint)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&serverBundlePath, "server-bundle", "", "server bundle PEM to read (default $HOME/.lockd/server.pem)")
	cmd.Flags().StringVar(&trustDir, "trust-dir", "", "trust directory (default $HOME/.lockd/tc-trust.d)")
	cmd.Flags().StringVar(&selfEndpoint, "self", "", "this node's endpoint")
	cmd.Flags().StringSliceVar(&endpoints, "endpoint", nil, "TC endpoint to contact (repeatable)")
	cmd.Flags().BoolVar(&disableMTLS, "disable-mtls", false, "disable mTLS for TC cluster requests")
	return cmd
}

func newTCLeaveCommand() *cobra.Command {
	var serverBundlePath string
	var trustDir string
	var endpoints []string
	var disableMTLS bool
	cmd := &cobra.Command{
		Use:   "leave",
		Short: "Leave the TC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			if serverBundlePath == "" {
				if def, err := lockd.DefaultBundlePath(); err == nil {
					serverBundlePath = def
				}
			}
			client, err := tcClusterHTTPClient("", serverBundlePath, trustDir, disableMTLS)
			if err != nil {
				return err
			}
			seeds := tccluster.NormalizeEndpoints(endpoints)
			if len(seeds) == 0 {
				return fmt.Errorf("--endpoint is required")
			}
			if disableMTLS && len(seeds) > 1 {
				return fmt.Errorf("mTLS is required when contacting multiple TC endpoints")
			}
			remaining, err := leaveCluster(cmd.Context(), client, seeds)
			if err != nil {
				return err
			}
			for _, endpoint := range remaining {
				fmt.Fprintln(cmd.OutOrStdout(), endpoint)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&serverBundlePath, "server-bundle", "", "server bundle PEM to read (default $HOME/.lockd/server.pem)")
	cmd.Flags().StringVar(&trustDir, "trust-dir", "", "trust directory (default $HOME/.lockd/tc-trust.d)")
	cmd.Flags().StringSliceVar(&endpoints, "endpoint", nil, "TC endpoint to contact (repeatable)")
	cmd.Flags().BoolVar(&disableMTLS, "disable-mtls", false, "disable mTLS for TC cluster requests")
	return cmd
}

func newTCListCommand() *cobra.Command {
	var bundlePath string
	var trustDir string
	var endpoints []string
	var disableMTLS bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List TC cluster members",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := tcClusterHTTPClient(bundlePath, "", trustDir, disableMTLS)
			if err != nil {
				return err
			}
			seeds := tccluster.NormalizeEndpoints(endpoints)
			if len(seeds) == 0 {
				return fmt.Errorf("--endpoint is required")
			}
			if disableMTLS && len(seeds) > 1 {
				return fmt.Errorf("mTLS is required when contacting multiple TC endpoints")
			}
			members, err := listCluster(cmd.Context(), client, seeds)
			if err != nil {
				return err
			}
			for _, endpoint := range members {
				fmt.Fprintln(cmd.OutOrStdout(), endpoint)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&bundlePath, "bundle", "", "TC client bundle PEM to read (default auto-discover under $HOME/.lockd)")
	cmd.Flags().StringVar(&trustDir, "trust-dir", "", "trust directory (default $HOME/.lockd/tc-trust.d)")
	cmd.Flags().StringSliceVar(&endpoints, "endpoint", nil, "TC endpoint to contact (repeatable)")
	cmd.Flags().BoolVar(&disableMTLS, "disable-mtls", false, "disable mTLS for TC cluster requests")
	return cmd
}

func tcClusterHTTPClient(clientBundlePath, serverBundlePath, trustDir string, disableMTLS bool) (*http.Client, error) {
	var err error
	if clientBundlePath != "" {
		if clientBundlePath, err = expandPath(clientBundlePath); err != nil {
			return nil, err
		}
	}
	if serverBundlePath != "" {
		if serverBundlePath, err = expandPath(serverBundlePath); err != nil {
			return nil, err
		}
	}
	if trustDir == "" {
		if def, derr := lockd.DefaultTCTrustDir(); derr == nil {
			trustDir = def
		}
	}
	if trustDir != "" {
		if trustDir, err = expandPath(trustDir); err != nil {
			return nil, err
		}
	}
	trustPEM, err := loadTrustPEM(trustDir)
	if err != nil {
		return nil, err
	}
	if !disableMTLS && serverBundlePath != "" {
		bundle, err := tlsutil.LoadBundle(serverBundlePath, "")
		if err != nil {
			return nil, fmt.Errorf("load server bundle %s: %w", serverBundlePath, err)
		}
		if len(bundle.CACertPEM) > 0 {
			trustPEM = append(trustPEM, bundle.CACertPEM)
		}
		return tcclient.NewHTTPClient(tcclient.Config{
			DisableMTLS:      disableMTLS,
			ServerBundlePath: serverBundlePath,
			Timeout:          tcClusterTimeout,
			TrustPEM:         trustPEM,
		})
	}
	if !disableMTLS {
		clientBundlePath, err = lockd.ResolveClientBundlePath(lockd.ClientBundleRoleTC, clientBundlePath)
		if err != nil {
			return nil, err
		}
	}
	return tcclient.NewHTTPClient(tcclient.Config{
		DisableMTLS: disableMTLS,
		BundlePath:  clientBundlePath,
		Timeout:     tcClusterTimeout,
		TrustPEM:    trustPEM,
	})
}

func loadTrustPEM(dir string) ([][]byte, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("tc trust: read dir %s: %w", dir, err)
	}
	var blobs [][]byte
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil, fmt.Errorf("tc trust: read %s: %w", path, readErr)
		}
		if len(data) > 0 {
			blobs = append(blobs, data)
		}
	}
	return blobs, nil
}

func announceCluster(ctx context.Context, client *http.Client, selfEndpoint string, seeds []string) ([]string, error) {
	req := api.TCClusterAnnounceRequest{SelfEndpoint: selfEndpoint}
	var lastErr error
	for _, endpoint := range seeds {
		var resp api.TCClusterAnnounceResponse
		if err := doTCClusterRequest(ctx, client, http.MethodPost, endpoint, "/v1/tc/cluster/announce", req, &resp); err != nil {
			lastErr = err
			continue
		}
		return tccluster.NormalizeEndpoints(resp.Endpoints), nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("tc announce failed: %w", lastErr)
	}
	return nil, fmt.Errorf("tc announce failed: no endpoints reachable")
}

func listCluster(ctx context.Context, client *http.Client, seeds []string) ([]string, error) {
	var lastErr error
	for _, endpoint := range seeds {
		var resp api.TCClusterListResponse
		if err := doTCClusterRequest(ctx, client, http.MethodGet, endpoint, "/v1/tc/cluster/list", nil, &resp); err != nil {
			lastErr = err
			continue
		}
		return tccluster.NormalizeEndpoints(resp.Endpoints), nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("tc cluster list failed: %w", lastErr)
	}
	return nil, fmt.Errorf("tc cluster list failed: no endpoints reachable")
}

func leaveCluster(ctx context.Context, client *http.Client, seeds []string) ([]string, error) {
	req := api.TCClusterLeaveRequest{}
	var lastErr error
	for _, endpoint := range seeds {
		var resp api.TCClusterLeaveResponse
		if err := doTCClusterRequest(ctx, client, http.MethodPost, endpoint, "/v1/tc/cluster/leave", req, &resp); err != nil {
			lastErr = err
			continue
		}
		return tccluster.NormalizeEndpoints(resp.Endpoints), nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("tc leave failed: %w", lastErr)
	}
	return nil, fmt.Errorf("tc leave failed: no endpoints reachable")
}

func doTCClusterRequest(ctx context.Context, client *http.Client, method, endpoint, path string, payload any, out any) error {
	if client == nil {
		return errors.New("tc cluster: http client not configured")
	}
	url := strings.TrimSuffix(endpoint, "/") + path
	var body io.Reader
	if payload != nil {
		buf, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if out == nil {
			return nil
		}
		return json.NewDecoder(resp.Body).Decode(out)
	}
	var errResp api.ErrorResponse
	_ = json.NewDecoder(resp.Body).Decode(&errResp)
	if errResp.ErrorCode != "" {
		return fmt.Errorf("status %d: %s: %s", resp.StatusCode, errResp.ErrorCode, errResp.Detail)
	}
	return fmt.Errorf("status %d", resp.StatusCode)
}
