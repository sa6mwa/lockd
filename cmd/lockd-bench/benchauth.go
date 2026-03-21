package main

import (
	"fmt"
	"strings"
	"time"

	lockd "pkt.systems/lockd"
	"pkt.systems/lockd/internal/pathutil"
)

func prepareEmbeddedBenchServerBundle(cfg *benchConfig) error {
	if cfg == nil {
		return fmt.Errorf("bench: nil config")
	}
	if len(cfg.serverBundlePEM) > 0 {
		return nil
	}
	if !cfg.enableCrypto {
		return nil
	}
	caPEM, err := lockd.CreateCABundle(lockd.CreateCABundleRequest{
		CommonName: "lockd-bench-ca",
		ValidFor:   365 * 24 * time.Hour,
	})
	if err != nil {
		return fmt.Errorf("bench: create embedded ca bundle: %w", err)
	}
	serverPEM, err := lockd.CreateServerBundle(lockd.CreateServerBundleRequest{
		CABundlePEM: caPEM,
		CommonName:  "lockd-bench-server",
		ValidFor:    365 * 24 * time.Hour,
		Hosts:       []string{"*", "127.0.0.1", "localhost"},
	})
	if err != nil {
		return fmt.Errorf("bench: create embedded server bundle: %w", err)
	}
	cfg.serverBundlePEM = serverPEM
	return nil
}

func resolveBenchClientBundlePath(cfg benchConfig) (string, error) {
	if cfg.disableMTLS {
		return "", nil
	}
	raw := strings.TrimSpace(cfg.clientBundle)
	if raw == "" {
		return "", fmt.Errorf("bench: -bundle is required when -endpoint is used with mTLS enabled")
	}
	path, err := pathutil.ExpandUserAndEnv(raw)
	if err != nil {
		return "", fmt.Errorf("bench: expand -bundle path: %w", err)
	}
	return path, nil
}
