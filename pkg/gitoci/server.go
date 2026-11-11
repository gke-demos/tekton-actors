package gitoci

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const defaultMaxAuditEvents = 2000

// Server hosts an in-cluster Bearer/Basic-authenticated Git Smart-HTTP repository
// and OCI Distribution Spec v2 registry, plus a structured non-secret audit log
// and code-push status/mutation endpoints.
//
// Security notes:
//   - All /repo/*, /git/*, and /v2/* endpoints strictly enforce token authentication
//     using constant-time comparison and never log or return secret values.
//   - TODO(security): /readyz, /healthz, /api/audit*, and /api/repo/* are cluster-internal
//     readiness, telemetry, and demo control endpoints restricted to cluster control-plane / UI
//     pods via Kubernetes NetworkPolicy and excluded from Actor EgressPolicy rules.
//     Add rate limiting and mTLS if exposed beyond cluster-internal networks.
type Server struct {
	dataDir        string
	authToken      string
	repoName       string
	maxAuditEvents int
	now            func() time.Time

	gitRepo *initializedGitRepo
	handler http.Handler

	auditMu sync.RWMutex
	events  []AuditEvent

	ociMu   sync.RWMutex
	uploads map[string]*ociUploadSession
}

type ociUploadSession struct {
	repo     string
	filePath string
	size     int64
}

// NewServer initializes the embedded bare Git repository and OCI storage under cfg.DataDir.
func NewServer(cfg Config) (*Server, error) {
	if strings.TrimSpace(cfg.DataDir) == "" {
		return nil, errors.New("DataDir is required")
	}
	if strings.TrimSpace(cfg.AuthToken) == "" {
		return nil, errors.New("AuthToken is required")
	}
	repoName := strings.TrimSpace(cfg.RepoName)
	if repoName == "" {
		repoName = DefaultRepoName
	}
	maxEvents := cfg.MaxAuditEvents
	if maxEvents <= 0 {
		maxEvents = defaultMaxAuditEvents
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}

	cleanDataDir := filepath.Clean(cfg.DataDir)
	if err := os.MkdirAll(filepath.Join(cleanDataDir, "oci", "blobs", "sha256"), 0o755); err != nil {
		return nil, fmt.Errorf("init oci blobs dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cleanDataDir, "oci", "uploads"), 0o755); err != nil {
		return nil, fmt.Errorf("init oci uploads dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cleanDataDir, "oci", "repos"), 0o755); err != nil {
		return nil, fmt.Errorf("init oci repos dir: %w", err)
	}

	gitRepo, err := initSampleBareRepo(cleanDataDir, repoName)
	if err != nil {
		return nil, fmt.Errorf("init sample git repository: %w", err)
	}

	s := &Server{
		dataDir:        cleanDataDir,
		authToken:      cfg.AuthToken,
		repoName:       repoName,
		maxAuditEvents: maxEvents,
		now:            clock,
		gitRepo:        gitRepo,
		uploads:        make(map[string]*ociUploadSession),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", s.handleProbe)
	mux.HandleFunc("/healthz", s.handleProbe)
	mux.HandleFunc("/api/audit", s.handleGetAudit)
	mux.HandleFunc("/api/audit/reset", s.handleResetAudit)
	mux.HandleFunc("/api/repo/status", s.handleGetRepoStatus)
	mux.HandleFunc("/api/repo/push", s.handlePushRepo)
	mux.HandleFunc("/repo/", s.handleGitRepo)
	mux.HandleFunc("/git/", s.handleGitRepo)
	mux.HandleFunc("/v2/", s.handleOCIV2)
	s.handler = mux
	return s, nil
}

// Handler returns the HTTP handler for the server.
func (s *Server) Handler() http.Handler {
	return s.handler
}

// ServeHTTP implements http.Handler directly for convenience.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// MainCommitSHA returns the commit SHA of the "main" branch.
func (s *Server) MainCommitSHA() string {
	s.gitRepo.mu.RLock()
	defer s.gitRepo.mu.RUnlock()
	return s.gitRepo.mainCommit
}

// BuggyCommitSHA returns the commit SHA of the "buggy" branch.
func (s *Server) BuggyCommitSHA() string {
	s.gitRepo.mu.RLock()
	defer s.gitRepo.mu.RUnlock()
	return s.gitRepo.buggyCommit
}

func (s *Server) handleProbe(w http.ResponseWriter, r *http.Request) {
	setSecurityHeaders(w)
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write([]byte(`{"ok":true}` + "\n"))
	}
}

func (s *Server) handleGetAudit(w http.ResponseWriter, r *http.Request) {
	setSecurityHeaders(w)
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	s.auditMu.RLock()
	snapshot := make([]AuditEvent, len(s.events))
	copy(snapshot, s.events)
	s.auditMu.RUnlock()

	var summary AuditSummary
	summary.Total = len(snapshot)
	for _, ev := range snapshot {
		if ev.Authenticated {
			summary.Authenticated++
		} else {
			summary.Rejected++
		}
		switch ev.Protocol {
		case ProtocolGit:
			summary.GitRequests++
		case ProtocolOCI:
			summary.OCIRequests++
			if ev.Authenticated && ev.Method == http.MethodPut && ev.Digest != "" && strings.Contains(ev.Path, "/manifests/") {
				summary.OCIManifestPushes++
			}
		}
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_ = json.NewEncoder(w).Encode(AuditResponse{
			Events:  snapshot,
			Summary: summary,
		})
	}
}

func (s *Server) handleResetAudit(w http.ResponseWriter, r *http.Request) {
	setSecurityHeaders(w)
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	s.auditMu.Lock()
	s.events = nil
	s.auditMu.Unlock()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}` + "\n"))
}

func (s *Server) verifyBearer(r *http.Request) (bool, string) {
	raw := strings.TrimSpace(r.Header.Get("Authorization"))
	if raw == "" {
		return false, "missing"
	}
	scheme, rest, hasSpace := strings.Cut(raw, " ")
	if !hasSpace {
		return false, "non-bearer"
	}

	var candidate string
	switch {
	case strings.EqualFold(scheme, "Bearer"):
		candidate = strings.TrimSpace(rest)
		if candidate == "" {
			return false, "missing"
		}
	case strings.EqualFold(scheme, "Basic"):
		user, pass, ok := r.BasicAuth()
		if !ok {
			return false, "invalid-basic"
		}
		candidate = strings.TrimSpace(pass)
		if candidate == "" {
			candidate = strings.TrimSpace(user)
		}
		if candidate == "" {
			return false, "missing"
		}
	default:
		return false, "non-bearer"
	}

	if candidate == "ate-placeholder" || candidate == "actor-placeholder" {
		return false, "unrewritten-ate-placeholder"
	}
	if subtle.ConstantTimeCompare([]byte(candidate), []byte(s.authToken)) != 1 {
		return false, "invalid-bearer"
	}

	switch strings.ToLower(strings.TrimSpace(r.Header.Get("X-Tekton-Credential-Mode"))) {
	case "ate-egress":
		return true, "ate-egress"
	case "mounted-secret":
		return true, "mounted-secret"
	default:
		return true, "bearer-verified"
	}
}

func (s *Server) recordAudit(ev AuditEvent) {
	if ev.Timestamp.IsZero() {
		ev.Timestamp = s.now().UTC()
	}
	s.auditMu.Lock()
	defer s.auditMu.Unlock()
	if len(s.events) >= s.maxAuditEvents {
		copy(s.events, s.events[1:])
		s.events[len(s.events)-1] = ev
		return
	}
	s.events = append(s.events, ev)
}

func writeBearerChallenge(w http.ResponseWriter, proto Protocol) {
	setSecurityHeaders(w)
	w.Header().Set("WWW-Authenticate", `Bearer realm="git-oci"`)
	if proto == ProtocolOCI {
		w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"errors":[{"code":"UNAUTHORIZED","message":"authentication required"}]}` + "\n"))
		return
	}
	w.Header().Add("WWW-Authenticate", `Basic realm="git-oci"`)
	writeJSONError(w, http.StatusUnauthorized, "authentication required")
}

func setSecurityHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
	h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	h.Set("Cache-Control", "no-store")
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	setSecurityHeaders(w)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
