package tlsutil

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
)

// CA holds a certificate authority keypair.
type CA struct {
	Cert    *x509.Certificate
	CertPEM []byte
	Key     ed25519.PrivateKey
	KeyPEM  []byte
}

// IssuedCert captures an issued certificate and its private key.
type IssuedCert struct {
	CertPEM []byte
	KeyPEM  []byte
}

// ClientCertRequest describes the inputs used to issue a client certificate.
type ClientCertRequest struct {
	CommonName     string
	Subject        pkix.Name
	NotBefore      time.Time
	NotAfter       time.Time
	Validity       time.Duration
	URIs           []*url.URL
	DNSNames       []string
	IPAddresses    []net.IP
	EmailAddresses []string
}

// ServerCertRequest describes the inputs used to issue a server certificate.
type ServerCertRequest struct {
	CommonName string
	Validity   time.Duration
	Hosts      []string
	URIs       []*url.URL
}

// GenerateCA creates a new self-signed certificate authority.
func GenerateCA(commonName string, validity time.Duration) (*CA, error) {
	if commonName == "" {
		commonName = "lockd-ca"
	}
	if validity <= 0 {
		validity = 10 * 365 * 24 * time.Hour
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ed25519 key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return nil, fmt.Errorf("generate serial: %w", err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkixName(commonName),
		NotBefore:             now.Add(-1 * time.Hour),
		NotAfter:              now.Add(validity),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		return nil, fmt.Errorf("create ca certificate: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("marshal ca key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse ca cert: %w", err)
	}
	return &CA{
		Cert:    ca,
		CertPEM: certPEM,
		Key:     priv,
		KeyPEM:  keyPEM,
	}, nil
}

// IssueServer issues a server certificate for hosts.
func (ca *CA) IssueServer(hosts []string, commonName string, validity time.Duration) (IssuedCert, error) {
	return ca.IssueServerWithRequest(ServerCertRequest{
		CommonName: commonName,
		Validity:   validity,
		Hosts:      hosts,
	})
}

// IssueServerWithRequest issues a server certificate using the supplied request.
func (ca *CA) IssueServerWithRequest(req ServerCertRequest) (IssuedCert, error) {
	if ca == nil {
		return IssuedCert{}, fmt.Errorf("ca is nil")
	}
	validity := req.Validity
	if validity <= 0 {
		validity = 365 * 24 * time.Hour
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return IssuedCert{}, fmt.Errorf("generate server key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return IssuedCert{}, fmt.Errorf("generate serial: %w", err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkixName(defaultString(req.CommonName, "lockd-server")),
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(validity),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	for _, host := range req.Hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		if ip := net.ParseIP(host); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, host)
		}
	}
	if len(template.DNSNames) == 0 && len(template.IPAddresses) == 0 {
		template.DNSNames = append(template.DNSNames, "*")
	}
	for _, uri := range req.URIs {
		if uri == nil {
			continue
		}
		template.URIs = append(template.URIs, uri)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.Cert, pub, ca.Key)
	if err != nil {
		return IssuedCert{}, fmt.Errorf("create server certificate: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return IssuedCert{}, fmt.Errorf("marshal server key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})
	return IssuedCert{CertPEM: certPEM, KeyPEM: keyPEM}, nil
}

// IssueClient issues a mutually-authenticated client certificate.
// IssueClient issues a mutually-authenticated client certificate.
func (ca *CA) IssueClient(req ClientCertRequest) (IssuedCert, error) {
	if ca == nil {
		return IssuedCert{}, fmt.Errorf("ca is nil")
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return IssuedCert{}, fmt.Errorf("generate client key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return IssuedCert{}, fmt.Errorf("generate serial: %w", err)
	}
	now := time.Now().UTC()
	notBefore := req.NotBefore
	if notBefore.IsZero() {
		notBefore = now.Add(-1 * time.Hour)
	}
	notAfter := req.NotAfter
	if notAfter.IsZero() {
		validity := req.Validity
		if validity <= 0 {
			validity = 365 * 24 * time.Hour
		}
		if req.NotBefore.IsZero() {
			notAfter = now.Add(validity)
		} else {
			notAfter = notBefore.Add(validity)
		}
	}
	if notAfter.Before(notBefore) {
		return IssuedCert{}, fmt.Errorf("client certificate notAfter before notBefore")
	}
	subject := req.Subject
	if strings.TrimSpace(subject.CommonName) == "" {
		subject.CommonName = defaultString(req.CommonName, "lockd-client")
	}
	template := &x509.Certificate{
		SerialNumber:   serial,
		Subject:        subject,
		NotBefore:      notBefore,
		NotAfter:       notAfter,
		ExtKeyUsage:    []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:       x509.KeyUsageDigitalSignature,
		URIs:           req.URIs,
		DNSNames:       req.DNSNames,
		IPAddresses:    req.IPAddresses,
		EmailAddresses: req.EmailAddresses,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.Cert, pub, ca.Key)
	if err != nil {
		return IssuedCert{}, fmt.Errorf("create client certificate: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return IssuedCert{}, fmt.Errorf("marshal client key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})
	return IssuedCert{CertPEM: certPEM, KeyPEM: keyPEM}, nil
}

func pkixName(cn string) pkix.Name {
	return pkix.Name{CommonName: cn}
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

// CAFromBundle constructs a CA helper from a parsed bundle.
func CAFromBundle(b *Bundle) (*CA, error) {
	if b == nil || b.CACertificate == nil || b.CAPrivateKey == nil {
		return nil, fmt.Errorf("bundle missing CA material")
	}
	signer, ok := b.CAPrivateKey.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("unsupported CA private key type %T", b.CAPrivateKey)
	}
	return &CA{
		Cert:    b.CACertificate,
		CertPEM: b.CACertPEM,
		Key:     signer,
		KeyPEM:  b.CAPrivateKeyPEM,
	}, nil
}

// LoadCA reads a CA certificate + private key PEM from path.
func LoadCA(path string) (*CA, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ca bundle: %w", err)
	}
	var cert *x509.Certificate
	var certPEM []byte
	var keyPEM []byte
	var key ed25519.PrivateKey
	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		switch block.Type {
		case "CERTIFICATE":
			if cert == nil {
				parsed, err := x509.ParseCertificate(block.Bytes)
				if err != nil {
					return nil, fmt.Errorf("parse ca certificate: %w", err)
				}
				cert = parsed
				certPEM = pem.EncodeToMemory(block)
			}
		case "PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY":
			parsed, err := parsePrivateKey(block)
			if err != nil {
				return nil, fmt.Errorf("parse ca private key: %w", err)
			}
			edKey, ok := parsed.(ed25519.PrivateKey)
			if !ok {
				return nil, fmt.Errorf("ca private key must be ed25519, got %T", parsed)
			}
			key = edKey
			keyPEM = pem.EncodeToMemory(block)
		}
	}
	if cert == nil {
		return nil, fmt.Errorf("ca certificate not found in %s", path)
	}
	if key == nil {
		return nil, fmt.Errorf("ca private key not found in %s", path)
	}
	if !cert.IsCA {
		return nil, fmt.Errorf("certificate in %s is not a CA", path)
	}
	return &CA{
		Cert:    cert,
		CertPEM: certPEM,
		Key:     key,
		KeyPEM:  keyPEM,
	}, nil
}
