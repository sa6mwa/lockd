package main

import (
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/nsauth"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/tlsutil"
	"pkt.systems/pslog"
)

const (
	bootstrapStoreDefault = "mem://"
	bootstrapServerCN     = "lockd-anywhere"
	bootstrapClientCN     = "lockd-client"
	bootstrapTCClientCN   = "lockd-tc-client"
)

func bootstrapConfigDir(dir string, logger pslog.Logger) error {
	if strings.TrimSpace(dir) == "" {
		return fmt.Errorf("bootstrap: directory required")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("bootstrap: resolve %s: %w", dir, err)
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return fmt.Errorf("bootstrap: create %s: %w", abs, err)
	}

	paths := map[string]string{
		"ca":       filepath.Join(abs, "ca.pem"),
		"server":   filepath.Join(abs, "server.pem"),
		"client":   filepath.Join(abs, "client.pem"),
		"tcclient": filepath.Join(abs, "tc-client.pem"),
		"config":   filepath.Join(abs, lockd.DefaultConfigFileName),
		"denylist": filepath.Join(abs, "server.denylist"),
	}

	ca, err := ensureBootstrapCA(paths["ca"], logger)
	if err != nil {
		return err
	}
	if err := ensureBootstrapServer(paths["server"], paths["ca"], ca, logger); err != nil {
		return err
	}
	if err := ensureBootstrapClient(paths["client"], ca, logger); err != nil {
		return err
	}
	if err := ensureBootstrapTCClient(paths["tcclient"], ca, logger); err != nil {
		return err
	}
	if err := ensureBootstrapConfig(paths["config"], paths["server"], paths["denylist"], paths["tcclient"], logger); err != nil {
		return err
	}
	if err := ensureBootstrapFile(paths["denylist"], []byte{}); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("bootstrap: ensure denylist %s: %w", paths["denylist"], err)
	}
	return nil
}

func ensureBootstrapCA(path string, logger pslog.Logger) (*tlsutil.CA, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		ca, err := tlsutil.GenerateCA("lockd-root", 10*365*24*time.Hour)
		if err != nil {
			return nil, fmt.Errorf("bootstrap: generate CA: %w", err)
		}
		data, err := tlsutil.EncodeCABundle(ca.CertPEM, ca.KeyPEM)
		if err != nil {
			return nil, fmt.Errorf("bootstrap: encode CA bundle: %w", err)
		}
		if err := writeBootstrapFile(path, data); err != nil {
			return nil, fmt.Errorf("bootstrap: write CA bundle: %w", err)
		}
		logger.Info("bootstrap: generated CA bundle", "path", path)
	}
	ca, err := tlsutil.LoadCA(path)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: load CA bundle %s: %w", path, err)
	}
	return ca, nil
}

func ensureBootstrapServer(path, caPath string, ca *tlsutil.CA, logger pslog.Logger) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("bootstrap: stat server bundle %s: %w", path, err)
	}
	material, err := cryptoutil.EnsureCAMetadataMaterial(caPath, ca.CertPEM)
	if err != nil {
		return fmt.Errorf("bootstrap: prepare metadata material: %w", err)
	}
	nodeID := uuidv7.NewString()
	spiffeURI, err := lockd.SPIFFEURIForServer(nodeID)
	if err != nil {
		return fmt.Errorf("bootstrap: spiffe uri: %w", err)
	}
	allClaim, err := nsauth.ClaimURI("ALL", nsauth.PermissionReadWrite)
	if err != nil {
		return fmt.Errorf("bootstrap: all namespace claim: %w", err)
	}
	issued, err := ca.IssueServerWithRequest(tlsutil.ServerCertRequest{
		CommonName: bootstrapServerCN,
		Validity:   2 * 365 * 24 * time.Hour,
		Hosts:      []string{"*"},
		URIs:       []*url.URL{spiffeURI, allClaim},
	})
	if err != nil {
		return fmt.Errorf("bootstrap: issue server certificate: %w", err)
	}
	bundleBytes, err := tlsutil.EncodeServerBundle(ca.CertPEM, ca.KeyPEM, issued.CertPEM, issued.KeyPEM, nil)
	if err != nil {
		return fmt.Errorf("bootstrap: encode server bundle: %w", err)
	}
	augmented, err := cryptoutil.ApplyMetadataMaterial(bundleBytes, material)
	if err != nil {
		return fmt.Errorf("bootstrap: embed metadata: %w", err)
	}
	if err := writeBootstrapFile(path, augmented); err != nil {
		return fmt.Errorf("bootstrap: write server bundle: %w", err)
	}
	logger.Info("bootstrap: generated server bundle", "path", path)
	return nil
}

func ensureBootstrapClient(path string, ca *tlsutil.CA, logger pslog.Logger) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("bootstrap: stat client bundle %s: %w", path, err)
	}
	spiffeURI, err := lockd.SPIFFEURIForRole(lockd.ClientBundleRoleSDK, bootstrapClientCN)
	if err != nil {
		return fmt.Errorf("bootstrap: spiffe uri: %w", err)
	}
	defaultClaim, err := nsauth.ClaimURI(lockd.DefaultNamespace, nsauth.PermissionReadWrite)
	if err != nil {
		return fmt.Errorf("bootstrap: default namespace claim: %w", err)
	}
	issued, err := ca.IssueClient(tlsutil.ClientCertRequest{
		CommonName: bootstrapClientCN,
		Validity:   365 * 24 * time.Hour,
		URIs:       []*url.URL{spiffeURI, defaultClaim},
	})
	if err != nil {
		return fmt.Errorf("bootstrap: issue client certificate: %w", err)
	}
	clientBundle, err := tlsutil.EncodeClientBundle(ca.CertPEM, issued.CertPEM, issued.KeyPEM)
	if err != nil {
		return fmt.Errorf("bootstrap: encode client bundle: %w", err)
	}
	if err := writeBootstrapFile(path, clientBundle); err != nil {
		return fmt.Errorf("bootstrap: write client bundle: %w", err)
	}
	logger.Info("bootstrap: generated client bundle", "path", path)
	return nil
}

func ensureBootstrapTCClient(path string, ca *tlsutil.CA, logger pslog.Logger) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("bootstrap: stat tc client bundle %s: %w", path, err)
	}
	spiffeURI, err := lockd.SPIFFEURIForRole(lockd.ClientBundleRoleTC, bootstrapTCClientCN)
	if err != nil {
		return fmt.Errorf("bootstrap: spiffe uri: %w", err)
	}
	allClaim, err := nsauth.ClaimURI("ALL", nsauth.PermissionReadWrite)
	if err != nil {
		return fmt.Errorf("bootstrap: tc namespace claim: %w", err)
	}
	issued, err := ca.IssueClient(tlsutil.ClientCertRequest{
		CommonName: bootstrapTCClientCN,
		Validity:   365 * 24 * time.Hour,
		URIs:       []*url.URL{spiffeURI, allClaim},
	})
	if err != nil {
		return fmt.Errorf("bootstrap: issue tc client certificate: %w", err)
	}
	clientBundle, err := tlsutil.EncodeClientBundle(ca.CertPEM, issued.CertPEM, issued.KeyPEM)
	if err != nil {
		return fmt.Errorf("bootstrap: encode tc client bundle: %w", err)
	}
	if err := writeBootstrapFile(path, clientBundle); err != nil {
		return fmt.Errorf("bootstrap: write tc client bundle: %w", err)
	}
	logger.Info("bootstrap: generated tc client bundle", "path", path)
	return nil
}

func ensureBootstrapConfig(path, bundlePath, denylistPath, tcClientPath string, logger pslog.Logger) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("bootstrap: stat config %s: %w", path, err)
	}
	data, err := defaultConfigYAML(func(cfg *configDefaults) {
		cfg.Store = bootstrapStoreDefault
		cfg.Bundle = bundlePath
		cfg.DenylistPath = denylistPath
		cfg.TCClientBundle = tcClientPath
	})
	if err != nil {
		return fmt.Errorf("bootstrap: render default config: %w", err)
	}
	if err := writeBootstrapFile(path, data); err != nil {
		return fmt.Errorf("bootstrap: write config: %w", err)
	}
	logger.Info("bootstrap: generated config", "path", path)
	return nil
}

func ensureBootstrapFile(path string, data []byte) error {
	if _, err := os.Stat(path); err == nil {
		return fs.ErrExist
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	return writeBootstrapFile(path, data)
}

func writeBootstrapFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create dir %s: %w", filepath.Dir(path), err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s: %w", path, err)
	}
	return nil
}
