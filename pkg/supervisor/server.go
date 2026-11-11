package supervisor

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultMaxRequestBodyBytes int64 = 64 << 20 // 64 MiB

// Server implements the Step Supervisor HTTP API.
//
// Security trust boundary: inside an Agent Substrate Actor sandbox, external
// callers reach coordinator (:8080) strictly over atenet-router :8444
// CONNECT-TLS authenticated by PodCertificateRequest (ate-system/atenet-client),
// while sibling worker instances bind strictly to 127.0.0.1.
// TODO(security): If exposed outside an Actor network namespace in future
// standalone modes, add per-request bearer/mTLS authentication and rate limiting.
type Server struct {
	cfg         Config
	handler     http.Handler
	proxyClient *http.Client

	mu    sync.RWMutex
	steps map[string]*stepRecord
}

// NewServer validates cfg and constructs a Step Supervisor Server.
func NewServer(cfg Config) (*Server, error) {
	if cfg.Role == "" {
		cfg.Role = RoleCoordinator
	}
	if cfg.Role != RoleCoordinator && cfg.Role != RoleWorker {
		return nil, fmt.Errorf("invalid supervisor role %q: must be %q or %q", cfg.Role, RoleCoordinator, RoleWorker)
	}
	if cfg.WorkspaceDir == "" {
		cfg.WorkspaceDir = DefaultWorkspaceDir
	}
	cfg.WorkspaceDir = filepath.Clean(cfg.WorkspaceDir)

	if cfg.ResultsDir == "" {
		cfg.ResultsDir = DefaultResultsDir
	}
	cfg.ResultsDir = filepath.Clean(cfg.ResultsDir)

	if cfg.TektonDir == "" {
		cfg.TektonDir = filepath.Dir(cfg.ResultsDir)
	}
	cfg.TektonDir = filepath.Clean(cfg.TektonDir)

	if cfg.MaxRequestBodyBytes <= 0 {
		cfg.MaxRequestBodyBytes = defaultMaxRequestBodyBytes
	}

	s := &Server{
		cfg:         cfg,
		proxyClient: &http.Client{},
		steps:       make(map[string]*stepRecord),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("POST /v1/steps", s.handleRunStep)
	mux.HandleFunc("GET /v1/steps/{id}", s.handleGetStep)
	mux.HandleFunc("GET /v1/steps/{id}/logs", s.handleStepLogs)
	mux.HandleFunc("POST /v1/files", s.handleUploadFiles)
	mux.HandleFunc("POST /v1/rescue/exec", s.handleRescueExec)
	mux.HandleFunc("POST /v1/rescue/patch", s.handleRescuePatch)
	s.handler = withSecurityHeaders(mux)
	return s, nil
}

// Handler returns the HTTP handler for the supervisor server.
func (s *Server) Handler() http.Handler {
	return s.handler
}

func (s *Server) homeDir() string {
	return filepath.Join(s.cfg.TektonDir, "home")
}

func (s *Server) stepsDir() string {
	return filepath.Join(s.cfg.TektonDir, "steps")
}

func (s *Server) ensureDirs() error {
	for _, dir := range []string{
		s.cfg.WorkspaceDir,
		s.cfg.ResultsDir,
		s.homeDir(),
		s.stepsDir(),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}
	return nil
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, ReadyResponse{OK: true})
}

func (s *Server) handleRunStep(w http.ResponseWriter, r *http.Request) {
	var req RunStepRequest
	if err := s.decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.StepID == "" {
		req.StepID = generateStepID()
	} else if err := validateStepID(req.StepID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if req.Script == "" && len(req.Command) == 0 {
		writeError(w, http.StatusBadRequest, "either script or command must be provided")
		return
	}
	for _, rn := range req.ResultNames {
		if err := validateResultName(rn); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	if strings.TrimSpace(req.TargetAddr) != "" {
		s.forwardRunStep(w, r, req)
		return
	}

	if err := s.ensureDirs(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	envSlice, envMap := buildEnvironment(os.Environ(), s.homeDir(), req.Env)
	cmd, err := prepareExecCommand(
		s.stepsDir(),
		"script-"+req.StepID,
		req.Command,
		req.Args,
		req.Script,
		req.WorkingDir,
		s.cfg.WorkspaceDir,
		envMap,
		envSlice,
	)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	logs := newLogBuffer()
	cmd.Stdout = logs
	cmd.Stderr = logs

	startedAt := time.Now().UTC()
	rec := &stepRecord{
		id:        req.StepID,
		name:      req.Name,
		state:     StepStateRunning,
		startedAt: startedAt,
		logs:      logs,
	}

	s.mu.Lock()
	if _, exists := s.steps[req.StepID]; exists {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, fmt.Sprintf("step %q already exists", req.StepID))
		return
	}
	s.steps[req.StepID] = rec
	s.mu.Unlock()

	initialStatus := StepStatus{
		StepID:    rec.id,
		Name:      rec.name,
		State:     StepStateRunning,
		StartedAt: startedAt,
	}

	if startErr := cmd.Start(); startErr != nil {
		_, _ = fmt.Fprintf(logs, "step-supervisor start error: %v\n", startErr)
		code := 127
		fin := time.Now().UTC()
		rec.mu.Lock()
		rec.state = StepStateFailed
		rec.exitCode = &code
		rec.finishedAt = &fin
		rec.errMsg = startErr.Error()
		rec.mu.Unlock()
		logs.Close(StepStateFailed)
		writeJSON(w, http.StatusAccepted, initialStatus)
		return
	}

	resultNames := append([]string(nil), req.ResultNames...)
	go func() {
		waitErr := cmd.Wait()
		code := exitCodeFromWaitErr(waitErr)
		fin := time.Now().UTC()
		finalState := StepStateCompleted
		if code != 0 {
			finalState = StepStateFailed
		}
		results := readStepResults(s.cfg.ResultsDir, resultNames)

		rec.mu.Lock()
		rec.exitCode = &code
		rec.finishedAt = &fin
		rec.results = results
		rec.state = finalState
		rec.mu.Unlock()
		logs.Close(finalState)
	}()

	writeJSON(w, http.StatusAccepted, initialStatus)
}

func (s *Server) handleGetStep(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := validateStepID(id); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	s.mu.RLock()
	rec, ok := s.steps[id]
	s.mu.RUnlock()
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("step %q not found", id))
		return
	}
	if rec.remoteAddr != "" {
		s.proxyStepRequest(w, r, rec.remoteAddr, escapedStepPath(id, ""))
		return
	}

	if r.URL.Query().Get("wait") == "true" {
		for {
			_, _, _, closed, waitCh := rec.logs.snapshotFrom(0)
			if closed {
				break
			}
			select {
			case <-waitCh:
			case <-r.Context().Done():
				return
			}
		}
	}

	writeJSON(w, http.StatusOK, rec.status())
}

const maxSSEChunkBytes = 64 << 10 // 64 KiB per SSE frame

func (s *Server) handleStepLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := validateStepID(id); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var offset int64
	if rawOff := r.URL.Query().Get("offset"); rawOff != "" {
		parsed, err := strconv.ParseInt(rawOff, 10, 64)
		if err != nil || parsed < 0 {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid offset %q", rawOff))
			return
		}
		offset = parsed
	}
	follow := r.URL.Query().Get("follow") == "true" || r.URL.Query().Get("follow") == "1"
	sse := r.URL.Query().Get("format") == "sse" || strings.Contains(r.Header.Get("Accept"), "text/event-stream")

	s.mu.RLock()
	rec, ok := s.steps[id]
	s.mu.RUnlock()
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Sprintf("step %q not found", id))
		return
	}
	if rec.remoteAddr != "" {
		s.proxyStepRequest(w, r, rec.remoteAddr, escapedStepPath(id, "/logs"))
		return
	}

	if !follow {
		chunk, newOffset, state, _, _ := rec.logs.snapshotFrom(offset)
		w.Header().Set("X-Log-Offset", strconv.FormatInt(newOffset, 10))
		w.Header().Set("X-Step-State", string(state))
		if sse {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			writeSSEChunks(w, offset, chunk, state)
			if state.IsTerminal() {
				writeSSEDone(w, newOffset, state)
			}
			if fl, ok := w.(http.Flusher); ok {
				fl.Flush()
			}
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(chunk)
		return
	}

	flusher, _ := w.(http.Flusher)
	if sse {
		w.Header().Set("Content-Type", "text/event-stream")
	} else {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	_, initTotal, initState, _, _ := rec.logs.snapshotFrom(offset)
	if offset > initTotal {
		offset = initTotal
	}
	w.Header().Set("X-Log-Offset", strconv.FormatInt(offset, 10))
	w.Header().Set("X-Step-State", string(initState))
	w.WriteHeader(http.StatusOK)
	if flusher != nil {
		flusher.Flush()
	}

	curOffset := offset
	for {
		chunk, newOffset, state, closed, waitCh := rec.logs.snapshotFrom(curOffset)
		if len(chunk) > 0 {
			if sse {
				writeSSEChunks(w, curOffset, chunk, state)
			} else {
				if _, err := w.Write(chunk); err != nil {
					return
				}
			}
			curOffset = newOffset
			if flusher != nil {
				flusher.Flush()
			}
		}
		if closed && curOffset >= newOffset {
			if sse {
				writeSSEDone(w, curOffset, state)
				if flusher != nil {
					flusher.Flush()
				}
			}
			return
		}
		select {
		case <-waitCh:
		case <-r.Context().Done():
			return
		}
	}
}

func writeSSEChunks(w http.ResponseWriter, startOffset int64, chunk []byte, state StepState) {
	cur := startOffset
	for len(chunk) > 0 {
		n := len(chunk)
		if n > maxSSEChunkBytes {
			n = maxSSEChunkBytes
		}
		part := chunk[:n]
		chunk = chunk[n:]
		cur += int64(n)
		ev := LogSSEEvent{
			Event:  "log",
			Offset: cur,
			Chunk:  string(part),
			State:  state,
		}
		data, _ := json.Marshal(ev)
		_, _ = fmt.Fprintf(w, "event: log\nid: %d\ndata: %s\n\n", cur, data)
	}
}

func writeSSEDone(w http.ResponseWriter, finalOffset int64, state StepState) {
	ev := LogSSEEvent{
		Event:  "done",
		Offset: finalOffset,
		State:  state,
	}
	data, _ := json.Marshal(ev)
	_, _ = fmt.Fprintf(w, "event: done\nid: %d\ndata: %s\n\n", finalOffset, data)
}

func (s *Server) decodeJSONBody(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxRequestBodyBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("read request body: %w", err)
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("invalid JSON request body: %w", err)
	}
	return nil
}

func withSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		h.Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		data = []byte(`{"error":"internal marshal error"}`)
		status = http.StatusInternalServerError
	}
	data = append(data, '\n')
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}
