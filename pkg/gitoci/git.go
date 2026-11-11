package gitoci

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	maxGitPackBodyBytes = 16 << 20 // 16 MiB
	maxRepoPushBytes    = 1 << 20  // 1 MiB
)

var (
	validRepoNameRe    = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)
	tenantRepoAliasRe  = regexp.MustCompile(`^tenant-(0[1-9]|1[0-9]|2[0-4])$`)
)

type initializedGitRepo struct {
	mu          sync.RWMutex
	bareDir     string
	reposRoot   string
	gitBinary   string
	homeDir     string
	mainCommit  string
	buggyCommit string
	pushSeq     int
	status      RepoStatus
}

func initSampleBareRepo(dataDir, repoName string) (*initializedGitRepo, error) {
	if !validRepoNameRe.MatchString(repoName) {
		return nil, fmt.Errorf("invalid repository name %q", repoName)
	}
	gitBin, err := exec.LookPath("git")
	if err != nil {
		return nil, fmt.Errorf("git executable not found in PATH: %w", err)
	}

	reposRoot := filepath.Join(dataDir, "repos")
	homeDir := filepath.Join(dataDir, "git-home")
	if err := os.MkdirAll(reposRoot, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir repos root: %w", err)
	}
	if err := os.MkdirAll(homeDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir git home: %w", err)
	}

	bareDir := filepath.Join(reposRoot, repoName+".git")
	if err := os.RemoveAll(bareDir); err != nil {
		return nil, fmt.Errorf("clean existing bare repo: %w", err)
	}

	stagingWorkDir, err := os.MkdirTemp(reposRoot, "seed-worktree-*")
	if err != nil {
		return nil, fmt.Errorf("create seed worktree: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(stagingWorkDir)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runGit := func(dir, commitDate string, args ...string) (string, error) {
		cmd := exec.CommandContext(ctx, gitBin, args...)
		cmd.Dir = dir
		cmd.Env = isolatedGitEnv(homeDir, commitDate)
		out, runErr := cmd.CombinedOutput()
		if runErr != nil {
			return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), runErr, strings.TrimSpace(string(out)))
		}
		return strings.TrimSpace(string(out)), nil
	}

	if _, err := runGit(stagingWorkDir, "2026-11-11T00:00:00Z", "init", "-b", "main"); err != nil {
		return nil, err
	}
	if err := WriteSampleAppWithRelease(stagingWorkDir, false, "v1", 1); err != nil {
		return nil, fmt.Errorf("seed clean sample-app: %w", err)
	}
	if _, err := runGit(stagingWorkDir, "2026-11-11T00:00:00Z", "add", "."); err != nil {
		return nil, err
	}
	const initialMsg = "Initial release of fulfillment-api"
	if _, err := runGit(stagingWorkDir, "2026-11-11T00:00:00Z", "commit", "-m", initialMsg); err != nil {
		return nil, err
	}
	mainSHA, err := runGit(stagingWorkDir, "2026-11-11T00:00:00Z", "rev-parse", "HEAD")
	if err != nil {
		return nil, err
	}

	// Create branch "buggy" containing the 1-line discount off-by-one bug.
	if _, err := runGit(stagingWorkDir, "2026-11-11T00:01:00Z", "checkout", "-b", "buggy"); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(stagingWorkDir, "pricing.go"), []byte(SampleAppBuggyPricingGo), 0o644); err != nil {
		return nil, fmt.Errorf("write buggy pricing.go: %w", err)
	}
	if _, err := runGit(stagingWorkDir, "2026-11-11T00:01:00Z", "add", "pricing.go"); err != nil {
		return nil, err
	}
	if _, err := runGit(stagingWorkDir, "2026-11-11T00:01:00Z", "commit", "-m", "Introduce off-by-one cent discount regression"); err != nil {
		return nil, err
	}
	buggySHA, err := runGit(stagingWorkDir, "2026-11-11T00:01:00Z", "rev-parse", "HEAD")
	if err != nil {
		return nil, err
	}

	if _, err := runGit(stagingWorkDir, "2026-11-11T00:00:00Z", "checkout", "main"); err != nil {
		return nil, err
	}
	if _, err := runGit(reposRoot, "2026-11-11T00:00:00Z", "clone", "--bare", stagingWorkDir, bareDir); err != nil {
		return nil, err
	}
	if _, err := runGit(bareDir, "2026-11-11T00:00:00Z", "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		return nil, err
	}
	if _, err := runGit(bareDir, "2026-11-11T00:00:00Z", "config", "http.receivepack", "true"); err != nil {
		return nil, err
	}
	if _, err := runGit(bareDir, "2026-11-11T00:00:00Z", "update-server-info"); err != nil {
		return nil, err
	}

	return &initializedGitRepo{
		bareDir:     bareDir,
		reposRoot:   reposRoot,
		gitBinary:   gitBin,
		homeDir:     homeDir,
		mainCommit:  mainSHA,
		buggyCommit: buggySHA,
		pushSeq:     1,
		status: RepoStatus{
			Repo:        repoName,
			Branch:      "main",
			SHA:         mainSHA,
			MainSHA:     mainSHA,
			BuggySHA:    buggySHA,
			Release:     "v1",
			Scenario:    "clean",
			Tenant:      "all",
			Message:     initialMsg,
			CommittedAt: "2026-11-11T00:00:00Z",
		},
	}, nil
}

func isolatedGitEnv(homeDir, commitDate string) []string {
	env := []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"HOME=" + homeDir,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_AUTHOR_NAME=Tekton Actors CI",
		"GIT_AUTHOR_EMAIL=ci@tekton-actors.local",
		"GIT_COMMITTER_NAME=Tekton Actors CI",
		"GIT_COMMITTER_EMAIL=ci@tekton-actors.local",
		"LC_ALL=C",
	}
	if commitDate != "" {
		env = append(env,
			"GIT_AUTHOR_DATE="+commitDate,
			"GIT_COMMITTER_DATE="+commitDate,
		)
	}
	return env
}

func (s *Server) isAllowedGitRepoName(normalizedRepo string) bool {
	if normalizedRepo == s.repoName {
		return true
	}
	return tenantRepoAliasRe.MatchString(normalizedRepo)
}

func trimGitRoutePrefix(urlPath string) (string, bool) {
	switch {
	case strings.HasPrefix(urlPath, "/repo/"):
		return strings.TrimPrefix(urlPath, "/repo/"), true
	case strings.HasPrefix(urlPath, "/git/"):
		return strings.TrimPrefix(urlPath, "/git/"), true
	default:
		return "", false
	}
}

func (s *Server) handleGitRepo(w http.ResponseWriter, r *http.Request) {
	authOK, hint := s.verifyBearer(r)
	if !authOK {
		s.recordAudit(AuditEvent{
			Protocol:        ProtocolGit,
			Method:          r.Method,
			Path:            r.URL.Path,
			StatusCode:      http.StatusUnauthorized,
			Authenticated:   false,
			TokenSourceHint: hint,
		})
		writeBearerChallenge(w, ProtocolGit)
		return
	}

	trimmed, ok := trimGitRoutePrefix(r.URL.Path)
	repoSeg, subPath, hasSlash := strings.Cut(trimmed, "/")
	normalizedRepo := strings.TrimSuffix(repoSeg, ".git")
	if !ok || !hasSlash || !s.isAllowedGitRepoName(normalizedRepo) || subPath == "" {
		s.recordAudit(AuditEvent{
			Protocol:        ProtocolGit,
			Method:          r.Method,
			Path:            r.URL.Path,
			StatusCode:      http.StatusNotFound,
			Authenticated:   true,
			TokenSourceHint: hint,
			Repository:      normalizedRepo,
		})
		writeJSONError(w, http.StatusNotFound, "repository not found")
		return
	}

	service := r.URL.Query().Get("service")
	switch {
	case subPath == "info/refs" && service != "":
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			s.recordAudit(AuditEvent{
				Protocol:        ProtocolGit,
				Method:          r.Method,
				Path:            r.URL.Path,
				StatusCode:      http.StatusMethodNotAllowed,
				Authenticated:   true,
				TokenSourceHint: hint,
				Repository:      normalizedRepo,
			})
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if service != "git-upload-pack" && service != "git-receive-pack" {
			s.recordAudit(AuditEvent{
				Protocol:        ProtocolGit,
				Method:          r.Method,
				Path:            r.URL.Path,
				StatusCode:      http.StatusForbidden,
				Authenticated:   true,
				TokenSourceHint: hint,
				Repository:      normalizedRepo,
			})
			writeJSONError(w, http.StatusForbidden, "unsupported git smart-http service")
			return
		}
		s.serveGitAdvertiseRefs(w, r, service, normalizedRepo, hint)
		return

	case subPath == "git-upload-pack":
		if r.Method != http.MethodPost {
			s.recordAudit(AuditEvent{
				Protocol:        ProtocolGit,
				Method:          r.Method,
				Path:            r.URL.Path,
				StatusCode:      http.StatusMethodNotAllowed,
				Authenticated:   true,
				TokenSourceHint: hint,
				Repository:      normalizedRepo,
			})
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.serveGitStatelessRPC(w, r, "upload-pack", "application/x-git-upload-pack-result", normalizedRepo, hint)
		return

	case subPath == "git-receive-pack":
		if r.Method != http.MethodPost {
			s.recordAudit(AuditEvent{
				Protocol:        ProtocolGit,
				Method:          r.Method,
				Path:            r.URL.Path,
				StatusCode:      http.StatusMethodNotAllowed,
				Authenticated:   true,
				TokenSourceHint: hint,
				Repository:      normalizedRepo,
			})
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.serveGitStatelessRPC(w, r, "receive-pack", "application/x-git-receive-pack-result", normalizedRepo, hint)
		return

	default:
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			s.recordAudit(AuditEvent{
				Protocol:        ProtocolGit,
				Method:          r.Method,
				Path:            r.URL.Path,
				StatusCode:      http.StatusMethodNotAllowed,
				Authenticated:   true,
				TokenSourceHint: hint,
				Repository:      normalizedRepo,
			})
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.serveDumbGitFile(w, r, subPath, normalizedRepo, hint)
		return
	}
}

func (s *Server) serveGitAdvertiseRefs(w http.ResponseWriter, r *http.Request, service, repoLabel, hint string) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	gitSubcmd := strings.TrimPrefix(service, "git-")
	cmd := exec.CommandContext(ctx, s.gitRepo.gitBinary, gitSubcmd, "--stateless-rpc", "--advertise-refs", s.gitRepo.bareDir)
	cmd.Env = isolatedGitEnv(s.gitRepo.homeDir, "")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		s.recordAudit(AuditEvent{
			Protocol:        ProtocolGit,
			Method:          r.Method,
			Path:            r.URL.Path,
			StatusCode:      http.StatusInternalServerError,
			Authenticated:   true,
			TokenSourceHint: hint,
			Repository:      repoLabel,
		})
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("git %s advertise-refs failed", gitSubcmd))
		return
	}

	setSecurityHeaders(w)
	w.Header().Set("Content-Type", fmt.Sprintf("application/x-%s-advertisement", service))
	w.Header().Set("Cache-Control", "no-cache, max-age=0, must-revalidate")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		preambleLine := fmt.Sprintf("# service=%s\n", service)
		pktHeader := fmt.Sprintf("%04x%s0000", len(preambleLine)+4, preambleLine)
		_, _ = io.WriteString(w, pktHeader)
		_, _ = w.Write(stdout.Bytes())
	}

	s.recordAudit(AuditEvent{
		Protocol:        ProtocolGit,
		Method:          r.Method,
		Path:            r.URL.Path,
		StatusCode:      http.StatusOK,
		Authenticated:   true,
		TokenSourceHint: hint,
		Repository:      repoLabel,
	})
}

func (s *Server) serveGitStatelessRPC(w http.ResponseWriter, r *http.Request, gitSubcmd, contentType, repoLabel, hint string) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if gitSubcmd == "receive-pack" {
		s.gitRepo.mu.Lock()
		defer s.gitRepo.mu.Unlock()
	}

	limitedBody := http.MaxBytesReader(w, r.Body, maxGitPackBodyBytes)
	defer limitedBody.Close()

	var bodyReader io.Reader = limitedBody
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("Content-Encoding")), "gzip") {
		gz, err := gzip.NewReader(limitedBody)
		if err != nil {
			s.recordAudit(AuditEvent{
				Protocol:        ProtocolGit,
				Method:          r.Method,
				Path:            r.URL.Path,
				StatusCode:      http.StatusBadRequest,
				Authenticated:   true,
				TokenSourceHint: hint,
				Repository:      repoLabel,
			})
			writeJSONError(w, http.StatusBadRequest, "invalid gzip request body")
			return
		}
		defer gz.Close()
		bodyReader = io.LimitReader(gz, maxGitPackBodyBytes)
	}

	cmd := exec.CommandContext(ctx, s.gitRepo.gitBinary, gitSubcmd, "--stateless-rpc", s.gitRepo.bareDir)
	cmd.Env = isolatedGitEnv(s.gitRepo.homeDir, "")
	cmd.Stdin = bodyReader
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		s.recordAudit(AuditEvent{
			Protocol:        ProtocolGit,
			Method:          r.Method,
			Path:            r.URL.Path,
			StatusCode:      http.StatusInternalServerError,
			Authenticated:   true,
			TokenSourceHint: hint,
			Repository:      repoLabel,
		})
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("git %s failed", gitSubcmd))
		return
	}

	if gitSubcmd == "receive-pack" {
		updateCmd := exec.CommandContext(ctx, s.gitRepo.gitBinary, "update-server-info")
		updateCmd.Dir = s.gitRepo.bareDir
		updateCmd.Env = isolatedGitEnv(s.gitRepo.homeDir, "")
		_ = updateCmd.Run()
		s.refreshHeadStatusLocked(ctx)
	}

	setSecurityHeaders(w)
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-cache, max-age=0, must-revalidate")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(stdout.Bytes())

	s.recordAudit(AuditEvent{
		Protocol:        ProtocolGit,
		Method:          r.Method,
		Path:            r.URL.Path,
		StatusCode:      http.StatusOK,
		Authenticated:   true,
		TokenSourceHint: hint,
		Repository:      repoLabel,
	})
}

func (s *Server) refreshHeadStatusLocked(ctx context.Context) {
	cmd := exec.CommandContext(ctx, s.gitRepo.gitBinary, "log", "-1", "--format=%H%n%s%n%cI", "refs/heads/main")
	cmd.Dir = s.gitRepo.bareDir
	cmd.Env = isolatedGitEnv(s.gitRepo.homeDir, "")
	out, err := cmd.Output()
	if err != nil {
		return
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) >= 1 && strings.TrimSpace(lines[0]) != "" {
		s.gitRepo.mainCommit = strings.TrimSpace(lines[0])
		s.gitRepo.status.SHA = s.gitRepo.mainCommit
		s.gitRepo.status.MainSHA = s.gitRepo.mainCommit
	}
	if len(lines) >= 2 {
		s.gitRepo.status.Message = strings.TrimSpace(lines[1])
	}
	if len(lines) >= 3 {
		s.gitRepo.status.CommittedAt = strings.TrimSpace(lines[2])
	}
}

func (s *Server) serveDumbGitFile(w http.ResponseWriter, r *http.Request, subPath, repoLabel, hint string) {
	cleaned := path.Clean(subPath)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") || strings.Contains(cleaned, "\\") {
		s.recordAudit(AuditEvent{
			Protocol:        ProtocolGit,
			Method:          r.Method,
			Path:            r.URL.Path,
			StatusCode:      http.StatusBadRequest,
			Authenticated:   true,
			TokenSourceHint: hint,
			Repository:      repoLabel,
		})
		writeJSONError(w, http.StatusBadRequest, "invalid repository path")
		return
	}

	root, err := os.OpenRoot(s.gitRepo.bareDir)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "repository unavailable")
		return
	}
	defer root.Close()

	relOSPath := filepath.FromSlash(cleaned)
	info, err := root.Stat(relOSPath)
	if err != nil || !info.Mode().IsRegular() {
		status := http.StatusNotFound
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			status = http.StatusInternalServerError
		}
		s.recordAudit(AuditEvent{
			Protocol:        ProtocolGit,
			Method:          r.Method,
			Path:            r.URL.Path,
			StatusCode:      status,
			Authenticated:   true,
			TokenSourceHint: hint,
			Repository:      repoLabel,
		})
		writeJSONError(w, status, "object not found")
		return
	}

	data, err := root.ReadFile(relOSPath)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to read object")
		return
	}

	setSecurityHeaders(w)
	if cleaned == "HEAD" || cleaned == "info/refs" || cleaned == "objects/info/packs" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(data)
	}

	s.recordAudit(AuditEvent{
		Protocol:        ProtocolGit,
		Method:          r.Method,
		Path:            r.URL.Path,
		StatusCode:      http.StatusOK,
		Authenticated:   true,
		TokenSourceHint: hint,
		Repository:      repoLabel,
	})
}

// RepoStatus returns a snapshot of the current fulfillment-api repository state.
func (s *Server) RepoStatus() RepoStatus {
	s.gitRepo.mu.RLock()
	defer s.gitRepo.mu.RUnlock()
	return s.gitRepo.status
}

func (s *Server) handleGetRepoStatus(w http.ResponseWriter, r *http.Request) {
	setSecurityHeaders(w)
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_ = json.NewEncoder(w).Encode(s.RepoStatus())
	}
}

func (s *Server) handlePushRepo(w http.ResponseWriter, r *http.Request) {
	setSecurityHeaders(w)
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	limited := http.MaxBytesReader(w, r.Body, maxRepoPushBytes)
	defer limited.Close()

	var req RepoPushRequest
	raw, err := io.ReadAll(limited)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "failed to read push payload")
		return
	}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid push JSON payload")
			return
		}
	}

	status, err := s.PushScenario(r.Context(), req)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(status)
}

// PushScenario commits a normalized demo scenario ("release", "buggy-code", "oom-overlay", or "reset")
// onto refs/heads/main of the embedded bare repository and returns the updated RepoStatus.
func (s *Server) PushScenario(ctx context.Context, req RepoPushRequest) (RepoStatus, error) {
	s.gitRepo.mu.Lock()
	defer s.gitRepo.mu.Unlock()

	rawScenario := strings.ToLower(strings.TrimSpace(req.Scenario))
	if rawScenario == "" {
		rawScenario = "release"
	}

	var canonicalScenario string
	switch rawScenario {
	case "release", "clean":
		canonicalScenario = "clean"
	case "buggy-code", "buggy":
		canonicalScenario = "buggy-code"
	case "oom-overlay", "oom":
		canonicalScenario = "oom-overlay"
	case "reset":
		canonicalScenario = "clean"
	default:
		return RepoStatus{}, fmt.Errorf("unsupported scenario %q (expected release, buggy-code, oom-overlay, or reset)", req.Scenario)
	}

	s.gitRepo.pushSeq++
	seq := s.gitRepo.pushSeq
	if rawScenario == "reset" {
		seq = 1
	}

	release := strings.TrimSpace(req.Release)
	if release == "" {
		if rawScenario == "reset" {
			release = "v1"
		} else {
			release = fmt.Sprintf("v%d", seq)
		}
	}

	tenant := strings.TrimSpace(req.Tenant)
	if tenant == "" {
		if canonicalScenario == "oom-overlay" {
			tenant = "tenant-01"
		} else {
			tenant = "all"
		}
	}

	msg := strings.TrimSpace(req.Message)
	if msg == "" {
		switch {
		case rawScenario == "reset":
			msg = "Reset fulfillment-api to clean v1 baseline"
		case canonicalScenario == "buggy-code":
			msg = fmt.Sprintf("Commit %s with off-by-one cent discount regression in pricing.go", release)
		case canonicalScenario == "oom-overlay":
			msg = fmt.Sprintf("Poison pill: Kustomize memory-bomb amplification overlay on %s (%s)", tenant, release)
		default:
			msg = fmt.Sprintf("Promote fulfillment-api to %s across 24 tenant overlays (seq %d)", release, seq)
		}
	}

	workDir, err := os.MkdirTemp(s.gitRepo.reposRoot, "push-worktree-*")
	if err != nil {
		return RepoStatus{}, fmt.Errorf("create push worktree: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(workDir)
	}()

	commitTime := s.now().UTC().Format(time.RFC3339)
	gitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	runGit := func(dir string, args ...string) (string, error) {
		cmd := exec.CommandContext(gitCtx, s.gitRepo.gitBinary, args...)
		cmd.Dir = dir
		cmd.Env = isolatedGitEnv(s.gitRepo.homeDir, commitTime)
		out, runErr := cmd.CombinedOutput()
		if runErr != nil {
			return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), runErr, strings.TrimSpace(string(out)))
		}
		return strings.TrimSpace(string(out)), nil
	}

	if _, err := runGit(s.gitRepo.reposRoot, "clone", "--branch", "main", s.gitRepo.bareDir, workDir); err != nil {
		return RepoStatus{}, err
	}

	// Clean existing working tree files (except .git) so removed files like payload.txt don't linger after reset.
	entries, err := os.ReadDir(workDir)
	if err != nil {
		return RepoStatus{}, fmt.Errorf("read worktree: %w", err)
	}
	for _, entry := range entries {
		if entry.Name() == ".git" {
			continue
		}
		_ = os.RemoveAll(filepath.Join(workDir, entry.Name()))
	}

	isBuggy := canonicalScenario == "buggy-code"
	if err := WriteSampleAppWithRelease(workDir, isBuggy, release, seq); err != nil {
		return RepoStatus{}, err
	}
	if canonicalScenario == "oom-overlay" {
		if err := WriteOOMOverlay(workDir, tenant); err != nil {
			return RepoStatus{}, err
		}
	}

	if _, err := runGit(workDir, "add", "-A"); err != nil {
		return RepoStatus{}, err
	}
	if _, err := runGit(workDir, "commit", "--allow-empty", "-m", msg); err != nil {
		return RepoStatus{}, err
	}
	newSHA, err := runGit(workDir, "rev-parse", "HEAD")
	if err != nil {
		return RepoStatus{}, err
	}
	if _, err := runGit(workDir, "push", "origin", "HEAD:refs/heads/main"); err != nil {
		return RepoStatus{}, err
	}
	if isBuggy {
		if _, err := runGit(workDir, "push", "--force", "origin", "HEAD:refs/heads/buggy"); err != nil {
			return RepoStatus{}, err
		}
		s.gitRepo.buggyCommit = newSHA
	}
	if _, err := runGit(s.gitRepo.bareDir, "update-server-info"); err != nil {
		return RepoStatus{}, err
	}

	s.gitRepo.mainCommit = newSHA
	s.gitRepo.status = RepoStatus{
		Repo:        s.repoName,
		Branch:      "main",
		SHA:         newSHA,
		MainSHA:     newSHA,
		BuggySHA:    s.gitRepo.buggyCommit,
		Release:     release,
		Scenario:    canonicalScenario,
		Tenant:      tenant,
		Message:     msg,
		CommittedAt: commitTime,
	}

	s.recordAudit(AuditEvent{
		Timestamp:       s.now().UTC(),
		Protocol:        ProtocolGit,
		Method:          http.MethodPost,
		Path:            "/api/repo/push",
		StatusCode:      http.StatusOK,
		Authenticated:   true,
		TokenSourceHint: "control-plane-push",
		Repository:      s.repoName,
		Tag:             release,
		Digest:          newSHA,
	})

	return s.gitRepo.status, nil
}
