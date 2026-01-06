package lockd_test

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"pkt.systems/lockd"
	"pkt.systems/lockd/internal/cryptoutil"
	"pkt.systems/lockd/internal/uuidv7"
	"pkt.systems/lockd/tlsutil"
)

type archipelagoTestFixture struct {
	baseDir string
	tcAuth  tcAuthMaterial
	mtls    sharedMTLSAuthority
}

var archipelagoFixture *archipelagoTestFixture

const archipelagoServerCredPoolSize = 32

var archipelagoServerCredHosts = []string{"127.0.0.1", "localhost", "::1"}

func TestMain(m *testing.M) {
	fixture, err := initArchipelagoTestFixture()
	if err != nil {
		fmt.Fprintf(os.Stderr, "archipelago fixture: %v\n", err)
		os.Exit(1)
	}
	archipelagoFixture = fixture
	code := m.Run()
	if fixture != nil {
		_ = os.RemoveAll(fixture.baseDir)
	}
	os.Exit(code)
}

func initArchipelagoTestFixture() (*archipelagoTestFixture, error) {
	baseDir, err := os.MkdirTemp("", "lockd-archipelago-test-")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	tcAuth, err := buildTCAuthority(baseDir)
	if err != nil {
		_ = os.RemoveAll(baseDir)
		return nil, err
	}
	mtls, err := buildSharedMTLSAuthority()
	if err != nil {
		_ = os.RemoveAll(baseDir)
		return nil, err
	}
	return &archipelagoTestFixture{
		baseDir: baseDir,
		tcAuth:  tcAuth,
		mtls:    mtls,
	}, nil
}

func buildTCAuthority(baseDir string) (tcAuthMaterial, error) {
	ca, err := tlsutil.GenerateCA("lockd-test-tc-ca", 365*24*time.Hour)
	if err != nil {
		return tcAuthMaterial{}, fmt.Errorf("tc auth: generate ca: %w", err)
	}
	spiffeURI, err := lockd.SPIFFEURIForRole(lockd.ClientBundleRoleTC, "lockd-test-tc-client")
	if err != nil {
		return tcAuthMaterial{}, fmt.Errorf("tc auth: spiffe uri: %w", err)
	}
	clientIssued, err := ca.IssueClient(tlsutil.ClientCertRequest{
		CommonName: "lockd-test-tc-client",
		Validity:   365 * 24 * time.Hour,
		URIs:       []*url.URL{spiffeURI},
	})
	if err != nil {
		return tcAuthMaterial{}, fmt.Errorf("tc auth: issue client: %w", err)
	}
	bundle, err := tlsutil.EncodeClientBundle(ca.CertPEM, clientIssued.CertPEM, clientIssued.KeyPEM)
	if err != nil {
		return tcAuthMaterial{}, fmt.Errorf("tc auth: encode client bundle: %w", err)
	}
	bundlePath := filepath.Join(baseDir, "tc-client.pem")
	if err := os.WriteFile(bundlePath, bundle, 0o600); err != nil {
		return tcAuthMaterial{}, fmt.Errorf("tc auth: write client bundle: %w", err)
	}
	trustDir := filepath.Join(baseDir, "tc-trust.d")
	if err := os.MkdirAll(trustDir, 0o700); err != nil {
		return tcAuthMaterial{}, fmt.Errorf("tc auth: create trust dir: %w", err)
	}
	if err := os.WriteFile(filepath.Join(trustDir, "tc-ca.pem"), ca.CertPEM, 0o600); err != nil {
		return tcAuthMaterial{}, fmt.Errorf("tc auth: write ca: %w", err)
	}
	return tcAuthMaterial{
		clientBundlePath: bundlePath,
		trustDir:         trustDir,
	}, nil
}

func buildSharedMTLSAuthority() (sharedMTLSAuthority, error) {
	ca, err := tlsutil.GenerateCA("lockd-test-ca", 365*24*time.Hour)
	if err != nil {
		return sharedMTLSAuthority{}, fmt.Errorf("mtls: generate ca: %w", err)
	}
	caBundle, err := tlsutil.EncodeCABundle(ca.CertPEM, ca.KeyPEM)
	if err != nil {
		return sharedMTLSAuthority{}, fmt.Errorf("mtls: encode ca bundle: %w", err)
	}
	material, err := cryptoutil.MetadataMaterialFromBytes(caBundle)
	if err != nil {
		return sharedMTLSAuthority{}, fmt.Errorf("mtls: metadata material: %w", err)
	}
	clientIssued, err := ca.IssueClient(tlsutil.ClientCertRequest{
		CommonName: "lockd-test-client",
		Validity:   365 * 24 * time.Hour,
	})
	if err != nil {
		return sharedMTLSAuthority{}, fmt.Errorf("mtls: issue client: %w", err)
	}
	clientBundle, err := tlsutil.EncodeClientBundle(ca.CertPEM, clientIssued.CertPEM, clientIssued.KeyPEM)
	if err != nil {
		return sharedMTLSAuthority{}, fmt.Errorf("mtls: encode client bundle: %w", err)
	}
	pool, err := buildServerCredPool(ca, material, clientBundle, archipelagoServerCredPoolSize, archipelagoServerCredHosts)
	if err != nil {
		return sharedMTLSAuthority{}, err
	}
	return sharedMTLSAuthority{
		ca:           ca,
		material:     material,
		clientBundle: clientBundle,
		pool:         pool,
	}, nil
}

func buildServerCredPool(ca *tlsutil.CA, material cryptoutil.MetadataMaterial, clientBundle []byte, size int, hosts []string) (*mtlsCredPool, error) {
	if ca == nil {
		return nil, fmt.Errorf("mtls: missing ca")
	}
	if size <= 0 {
		return nil, fmt.Errorf("mtls: invalid pool size")
	}
	creds := make([]lockd.TestMTLSCredentials, 0, size)
	for i := 0; i < size; i++ {
		nodeID := uuidv7.NewString()
		spiffeURI, err := lockd.SPIFFEURIForServer(nodeID)
		if err != nil {
			return nil, fmt.Errorf("mtls: spiffe uri: %w", err)
		}
		serverIssued, err := ca.IssueServerWithRequest(tlsutil.ServerCertRequest{
			CommonName: "lockd-test-server",
			Validity:   365 * 24 * time.Hour,
			Hosts:      hosts,
			URIs:       []*url.URL{spiffeURI},
		})
		if err != nil {
			return nil, fmt.Errorf("mtls: issue server: %w", err)
		}
		serverBundle, err := tlsutil.EncodeServerBundle(ca.CertPEM, nil, serverIssued.CertPEM, serverIssued.KeyPEM, nil)
		if err != nil {
			return nil, fmt.Errorf("mtls: encode server bundle: %w", err)
		}
		serverBundle, err = cryptoutil.ApplyMetadataMaterial(serverBundle, material)
		if err != nil {
			return nil, fmt.Errorf("mtls: apply metadata: %w", err)
		}
		cred, err := lockd.NewTestMTLSCredentialsFromBundles(serverBundle, clientBundle)
		if err != nil {
			return nil, fmt.Errorf("mtls: build credentials: %w", err)
		}
		creds = append(creds, cred)
	}
	return newMTLSCredPool(creds, hosts), nil
}
