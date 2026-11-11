package supervisor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func validateLoopbackTargetAddr(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.New("empty targetAddr")
	}
	if strings.ContainsAny(trimmed, "/\\?#@ \t\r\n") {
		return "", fmt.Errorf("invalid targetAddr %q: must be host:port on loopback", raw)
	}
	host, portStr, err := net.SplitHostPort(trimmed)
	if err != nil {
		return "", fmt.Errorf("invalid targetAddr %q: %w", raw, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("invalid port in targetAddr %q", raw)
	}
	if strings.EqualFold(host, "localhost") {
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return "", fmt.Errorf("targetAddr %q must be a loopback address (127.0.0.1 or ::1)", raw)
	}
	return net.JoinHostPort(ip.String(), strconv.Itoa(port)), nil
}

func (s *Server) forwardRunStep(w http.ResponseWriter, r *http.Request, req RunStepRequest) {
	if s.cfg.Role != RoleCoordinator {
		writeError(w, http.StatusBadRequest, "only coordinator supervisor may forward targetAddr steps")
		return
	}
	targetAddr, err := validateLoopbackTargetAddr(req.TargetAddr)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	rec := &stepRecord{
		id:         req.StepID,
		name:       req.Name,
		remoteAddr: targetAddr,
	}
	s.mu.Lock()
	if _, exists := s.steps[req.StepID]; exists {
		s.mu.Unlock()
		writeError(w, http.StatusConflict, fmt.Sprintf("step %q already exists", req.StepID))
		return
	}
	s.steps[req.StepID] = rec
	s.mu.Unlock()

	forwarded := req
	forwarded.TargetAddr = ""
	payload, err := json.Marshal(forwarded)
	if err != nil {
		s.removeStep(req.StepID)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	upstreamURL := "http://" + targetAddr + "/v1/steps"
	upReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstreamURL, bytes.NewReader(payload))
	if err != nil {
		s.removeStep(req.StepID)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	upReq.Header.Set("Content-Type", "application/json")

	resp, err := s.proxyClient.Do(upReq)
	if err != nil {
		s.removeStep(req.StepID)
		writeError(w, http.StatusBadGateway, fmt.Sprintf("forward step to %s: %v", targetAddr, err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		s.removeStep(req.StepID)
	}
	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) proxyStepRequest(w http.ResponseWriter, r *http.Request, targetAddr string, subPath string) {
	upstreamURL := "http://" + targetAddr + subPath
	if r.URL.RawQuery != "" {
		upstreamURL += "?" + r.URL.RawQuery
	}
	upReq, err := http.NewRequestWithContext(r.Context(), http.MethodGet, upstreamURL, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if accept := r.Header.Get("Accept"); accept != "" {
		upReq.Header.Set("Accept", accept)
	}

	resp, err := s.proxyClient.Do(upReq)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("proxy to worker %s: %v", targetAddr, err))
		return
	}
	defer resp.Body.Close()

	copyResponseHeaders(w.Header(), resp.Header)
	streaming := strings.HasSuffix(subPath, "/logs") && (r.URL.Query().Get("follow") == "true" || r.URL.Query().Get("follow") == "1")
	if !streaming {
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			w.Header().Set("Content-Length", cl)
		}
	}
	w.WriteHeader(resp.StatusCode)

	flusher, _ := w.(http.Flusher)
	if streaming && flusher != nil {
		flusher.Flush()
	}

	buf := make([]byte, 16<<10)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return
			}
			if streaming && flusher != nil {
				flusher.Flush()
			}
		}
		if readErr != nil {
			return
		}
	}
}

func (s *Server) removeStep(id string) {
	s.mu.Lock()
	delete(s.steps, id)
	s.mu.Unlock()
}

func copyResponseHeaders(dst, src http.Header) {
	for _, k := range []string{"Content-Type", "X-Log-Offset", "X-Step-State"} {
		if v := src.Get(k); v != "" {
			dst.Set(k, v)
		}
	}
}

func escapedStepPath(id string, suffix string) string {
	return "/v1/steps/" + url.PathEscape(id) + suffix
}
