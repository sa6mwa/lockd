package api

// MutateRequest expresses a server-side LQL mutation request for one key.
type MutateRequest struct {
	// Namespace scopes the request to a lockd namespace. Empty uses the server default.
	Namespace string `json:"namespace,omitempty"`
	// Mutations contains one or more LQL mutation expressions in execution order.
	Mutations []string `json:"mutations,omitempty"`
}
