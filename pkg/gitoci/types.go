package gitoci

import "time"

// Protocol identifies which server subsystem handled an audited request.
type Protocol string

const (
	ProtocolGit Protocol = "git"
	ProtocolOCI Protocol = "oci"
)

// Config configures a Git + OCI Distribution v2 Server.
type Config struct {
	// DataDir is the directory on disk storing bare Git repositories and OCI blobs/manifests.
	DataDir string
	// AuthToken is the required Bearer credential for all /repo/* and /v2/* endpoints.
	AuthToken string
	// RepoName overrides DefaultRepoName ("fulfillment-api") when non-empty.
	RepoName string
	// MaxAuditEvents caps in-memory AuditEvent retention (defaults to 2000).
	MaxAuditEvents int
	// Clock overrides time.Now in tests when non-nil.
	Clock func() time.Time
}

// AuditEvent captures a single authenticated or rejected Git/OCI request without
// ever recording raw secret material.
type AuditEvent struct {
	Timestamp       time.Time `json:"timestamp"`
	Protocol        Protocol  `json:"protocol"`
	Method          string    `json:"method"`
	Path            string    `json:"path"`
	StatusCode      int       `json:"statusCode"`
	Authenticated   bool      `json:"authenticated"`
	TokenSourceHint string    `json:"tokenSourceHint"`
	Repository      string    `json:"repository,omitempty"`
	Tag             string    `json:"tag,omitempty"`
	Digest          string    `json:"digest,omitempty"`
}

// AuditSummary aggregates counts across recorded AuditEvents.
type AuditSummary struct {
	Total             int `json:"total"`
	Authenticated     int `json:"authenticated"`
	Rejected          int `json:"rejected"`
	GitRequests       int `json:"gitRequests"`
	OCIRequests       int `json:"ociRequests"`
	OCIManifestPushes int `json:"ociManifestPushes"`
}

// AuditResponse is returned by GET /api/audit.
type AuditResponse struct {
	Events  []AuditEvent `json:"events"`
	Summary AuditSummary `json:"summary"`
}
