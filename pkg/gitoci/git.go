package gitoci

import (
	"bytes"
	"compress/gzip"
	"context"
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
	"time"
)

const maxGitUploadPackBodyBytes = 16 << 20 // 16 MiB

var validRepoNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

type initializedGitRepo struct {
	bareDir     string
	gitBinary   string
	homeDir     string
	mainCommit  string
	buggyCommit string
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
	if err := WriteSampleApp(stagingWorkDir, false); err != nil {
		return nil, fmt.Errorf("seed clean sample-app: %w", err)
	}
	if _, err := runGit(stagingWorkDir, "2026-11-11T00:00:00Z", "add", "."); err != nil {
		return nil, err
	}
	if _, err := runGit(stagingWorkDir, "2026-11-11T00:00:00Z", "commit", "-m", "Initial release of fulfillment-api"); err != nil {
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
	if _, err := runGit(bareDir, "2026-11-11T00:00:00Z", "update-server-info"); err != nil {
		return nil, err
	}

	return &initializedGitRepo{
		bareDir:     bareDir,
		gitBinary:   gitBin,
		homeDir:     homeDir,
		mainCommit:  mainSHA,
		buggyCommit: buggySHA,
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

	trimmed := strings.TrimPrefix(r.URL.Path, "/repo/")
	repoSeg, subPath, hasSlash := strings.Cut(trimmed, "/")
	normalizedRepo := strings.TrimSuffix(repoSeg, ".git")
	if !hasSlash || normalizedRepo != s.repoName || subPath == "" {
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
				Repository:      s.repoName,
			})
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if service != "git-upload-pack" {
			s.recordAudit(AuditEvent{
				Protocol:        ProtocolGit,
				Method:          r.Method,
				Path:            r.URL.Path,
				StatusCode:      http.StatusForbidden,
				Authenticated:   true,
				TokenSourceHint: hint,
				Repository:      s.repoName,
			})
			writeJSONError(w, http.StatusForbidden, "git push is disabled on source repository")
			return
		}
		s.serveGitAdvertiseRefs(w, r, hint)
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
				Repository:      s.repoName,
			})
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.serveGitUploadPack(w, r, hint)
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
				Repository:      s.repoName,
			})
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.serveDumbGitFile(w, r, subPath, hint)
		return
	}
}

func (s *Server) serveGitAdvertiseRefs(w http.ResponseWriter, r *http.Request, hint string) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, s.gitRepo.gitBinary, "upload-pack", "--stateless-rpc", "--advertise-refs", s.gitRepo.bareDir)
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
			Repository:      s.repoName,
		})
		writeJSONError(w, http.StatusInternalServerError, "git upload-pack advertise-refs failed")
		return
	}

	setSecurityHeaders(w)
	w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
	w.Header().Set("Cache-Control", "no-cache, max-age=0, must-revalidate")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		// Smart-HTTP v1 service preamble: pkt-line("service=git-upload-pack") + flush-pkt ("0000").
		_, _ = io.WriteString(w, "001e# service=git-upload-pack\n0000")
		_, _ = w.Write(stdout.Bytes())
	}

	s.recordAudit(AuditEvent{
		Protocol:        ProtocolGit,
		Method:          r.Method,
		Path:            r.URL.Path,
		StatusCode:      http.StatusOK,
		Authenticated:   true,
		TokenSourceHint: hint,
		Repository:      s.repoName,
	})
}

func (s *Server) serveGitUploadPack(w http.ResponseWriter, r *http.Request, hint string) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	limitedBody := http.MaxBytesReader(w, r.Body, maxGitUploadPackBodyBytes)
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
				Repository:      s.repoName,
			})
			writeJSONError(w, http.StatusBadRequest, "invalid gzip request body")
			return
		}
		defer gz.Close()
		bodyReader = io.LimitReader(gz, maxGitUploadPackBodyBytes)
	}

	cmd := exec.CommandContext(ctx, s.gitRepo.gitBinary, "upload-pack", "--stateless-rpc", s.gitRepo.bareDir)
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
			Repository:      s.repoName,
		})
		writeJSONError(w, http.StatusInternalServerError, "git upload-pack failed")
		return
	}

	setSecurityHeaders(w)
	w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
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
		Repository:      s.repoName,
	})
}

func (s *Server) serveDumbGitFile(w http.ResponseWriter, r *http.Request, subPath, hint string) {
	cleaned := path.Clean(subPath)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") || strings.Contains(cleaned, "\\") {
		s.recordAudit(AuditEvent{
			Protocol:        ProtocolGit,
			Method:          r.Method,
			Path:            r.URL.Path,
			StatusCode:      http.StatusBadRequest,
			Authenticated:   true,
			TokenSourceHint: hint,
			Repository:      s.repoName,
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
			Repository:      s.repoName,
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
		Repository:      s.repoName,
	})
}
