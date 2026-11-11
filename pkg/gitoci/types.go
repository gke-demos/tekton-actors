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
	// AuthToken is the required Bearer/Basic credential for all /repo/*, /git/*, and /v2/* endpoints.
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

// RepoStatus describes the current HEAD revision and scenario state of the shared
// fulfillment-api Git repository returned by GET /api/repo/status and POST /api/repo/push.
type RepoStatus struct {
	Repo        string `json:"repo"`
	Branch      string `json:"branch"`
	SHA         string `json:"sha"`
	MainSHA     string `json:"mainSha"`
	BuggySHA    string `json:"buggySha"`
	Release     string `json:"release"`
	Scenario    string `json:"scenario"`
	Tenant      string `json:"tenant,omitempty"`
	Message     string `json:"message"`
	CommittedAt string `json:"committedAt"`
}

// RepoPushRequest is accepted by POST /api/repo/push to commit a normalized
// scenario ("release", "buggy-code", "oom-overlay", or "reset") onto refs/heads/main.
type RepoPushRequest struct {
	Scenario string `json:"scenario"`
	Release  string `json:"release,omitempty"`
	Tenant   string `json:"tenant,omitempty"`
	Message  string `json:"message,omitempty"`
}
