package admin

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"pkt.systems/lockd/mcp/oauth"
	mcpstate "pkt.systems/lockd/mcp/state"
)

// ErrNotBootstrapped indicates MCP OAuth state has not been initialized yet.
var ErrNotBootstrapped = mcpstate.ErrNotBootstrapped

// BootstrapRequest proxies mcp/state bootstrap input.
type BootstrapRequest = mcpstate.BootstrapRequest

// BootstrapResponse proxies mcp/state bootstrap output.
type BootstrapResponse = mcpstate.BootstrapResponse

// Client proxies persisted OAuth client metadata.
type Client = mcpstate.Client

// AddClientRequest describes client creation parameters.
type AddClientRequest struct {
	Name   string
	Scopes []string
}

// AddClientResponse returns newly minted client credentials.
type AddClientResponse struct {
	ClientID     string
	ClientSecret string
}

// UpdateClientRequest describes mutable client fields.
type UpdateClientRequest struct {
	ClientID string
	Name     string
	Scopes   []string
}

// CredentialsRequest controls credentials rendering.
type CredentialsRequest struct {
	ClientID     string
	RotateSecret bool
	ResourceURL  string
	MCPPath      string
}

// Credentials contains all values required to configure an MCP client.
type Credentials struct {
	ClientID     string
	ClientSecret string
	SecretIssued bool
	Scopes       []string
	Issuer       string
	AuthorizeURL string
	TokenURL     string
	ResourceURL  string
}

// Config configures administrative access to MCP OAuth state.
type Config struct {
	StatePath    string
	RefreshStore string
}

// Service provides a cohesive SDK admin surface for MCP OAuth lifecycle.
type Service struct {
	cfg Config
}

// New returns a new admin service.
func New(cfg Config) *Service {
	return &Service{cfg: cfg}
}

// Bootstrap initializes MCP OAuth state and emits initial client credentials.
func (s *Service) Bootstrap(req BootstrapRequest) (BootstrapResponse, error) {
	if strings.TrimSpace(req.Path) == "" {
		req.Path = strings.TrimSpace(s.cfg.StatePath)
	}
	return mcpstate.Bootstrap(req)
}

// Issuer returns the configured OAuth issuer.
func (s *Service) Issuer() (string, error) {
	mgr, err := s.manager()
	if err != nil {
		return "", err
	}
	return mgr.Issuer()
}

// SetIssuer updates the OAuth issuer.
func (s *Service) SetIssuer(issuer string) error {
	mgr, err := s.manager()
	if err != nil {
		return err
	}
	return mgr.SetIssuer(issuer)
}

// ListClients returns configured clients.
func (s *Service) ListClients() ([]Client, error) {
	mgr, err := s.manager()
	if err != nil {
		return nil, err
	}
	return mgr.ListClients()
}

// GetClient resolves one client by ID.
func (s *Service) GetClient(clientID string) (Client, error) {
	clients, err := s.ListClients()
	if err != nil {
		return Client{}, err
	}
	target := strings.TrimSpace(clientID)
	for _, client := range clients {
		if client.ID == target {
			return client, nil
		}
	}
	return Client{}, fmt.Errorf("oauth client %s not found", target)
}

// AddClient creates a new confidential OAuth client.
func (s *Service) AddClient(req AddClientRequest) (AddClientResponse, error) {
	mgr, err := s.manager()
	if err != nil {
		return AddClientResponse{}, err
	}
	resp, err := mgr.AddClient(oauth.AddClientRequest{
		Name:   req.Name,
		Scopes: req.Scopes,
	})
	if err != nil {
		return AddClientResponse{}, err
	}
	return AddClientResponse{
		ClientID:     resp.ClientID,
		ClientSecret: resp.ClientSecret,
	}, nil
}

// RemoveClient removes a client from state.
func (s *Service) RemoveClient(clientID string) error {
	mgr, err := s.manager()
	if err != nil {
		return err
	}
	return mgr.RemoveClient(strings.TrimSpace(clientID))
}

// SetClientRevoked toggles revocation state.
func (s *Service) SetClientRevoked(clientID string, revoked bool) error {
	mgr, err := s.manager()
	if err != nil {
		return err
	}
	return mgr.RevokeClient(strings.TrimSpace(clientID), revoked)
}

// RotateClientSecret rotates and returns a new secret.
func (s *Service) RotateClientSecret(clientID string) (string, error) {
	mgr, err := s.manager()
	if err != nil {
		return "", err
	}
	return mgr.RotateClientSecret(strings.TrimSpace(clientID))
}

// UpdateClient updates mutable fields for one client.
func (s *Service) UpdateClient(req UpdateClientRequest) error {
	mgr, err := s.manager()
	if err != nil {
		return err
	}
	return mgr.UpdateClient(oauth.UpdateClientRequest{
		ClientID: strings.TrimSpace(req.ClientID),
		Name:     req.Name,
		Scopes:   req.Scopes,
	})
}

// Credentials returns MCP client configuration values for one OAuth client.
func (s *Service) Credentials(req CredentialsRequest) (Credentials, error) {
	client, err := s.GetClient(req.ClientID)
	if err != nil {
		return Credentials{}, err
	}
	issuer, err := s.Issuer()
	if err != nil {
		return Credentials{}, err
	}
	secret := ""
	if req.RotateSecret {
		secret, err = s.RotateClientSecret(client.ID)
		if err != nil {
			return Credentials{}, err
		}
	}
	resourceURL := strings.TrimSpace(req.ResourceURL)
	if resourceURL == "" {
		resourceURL = joinIssuerPath(issuer, req.MCPPath)
	}
	return Credentials{
		ClientID:     client.ID,
		ClientSecret: secret,
		SecretIssued: req.RotateSecret,
		Scopes:       append([]string(nil), client.Scopes...),
		Issuer:       issuer,
		AuthorizeURL: joinIssuerPath(issuer, "/authorize"),
		TokenURL:     joinIssuerPath(issuer, "/token"),
		ResourceURL:  resourceURL,
	}, nil
}

func (s *Service) manager() (*oauth.Manager, error) {
	mgr, err := oauth.NewManager(oauth.ManagerConfig{
		StatePath:    strings.TrimSpace(s.cfg.StatePath),
		RefreshStore: strings.TrimSpace(s.cfg.RefreshStore),
	})
	if errors.Is(err, mcpstate.ErrNotBootstrapped) {
		return nil, ErrNotBootstrapped
	}
	return mgr, err
}

func joinIssuerPath(issuer, p string) string {
	issuer = strings.TrimSpace(issuer)
	if issuer == "" {
		return normalizePath(p)
	}
	u, err := url.Parse(issuer)
	if err != nil {
		return strings.TrimRight(issuer, "/") + normalizePath(p)
	}
	u.Path = strings.TrimRight(u.Path, "/") + normalizePath(p)
	return u.String()
}

func normalizePath(p string) string {
	value := strings.TrimSpace(p)
	if value == "" {
		return "/"
	}
	if !strings.HasPrefix(value, "/") {
		return "/" + value
	}
	return value
}
