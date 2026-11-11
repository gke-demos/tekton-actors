package supervisor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client talks HTTP/1.1 to a Step Supervisor coordinator or worker endpoint.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient constructs a Step Supervisor Client targeting baseURL (for example
// "http://127.0.0.1:8080" or an actor address tunneled via httpClient.Transport).
func NewClient(baseURL string, httpClient *http.Client) *Client {
	trimmed := strings.TrimSpace(baseURL)
	if !strings.HasPrefix(trimmed, "http://") && !strings.HasPrefix(trimmed, "https://") {
		trimmed = "http://" + trimmed
	}
	trimmed = strings.TrimRight(trimmed, "/")
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &Client{
		baseURL:    trimmed,
		httpClient: httpClient,
	}
}

// Ready checks GET /readyz on the target Step Supervisor.
func (c *Client) Ready(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/readyz", nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return readHTTPError("readyz", resp)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read readyz response: %w", err)
	}
	var out ReadyResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return fmt.Errorf("decode readyz response: %w", err)
	}
	if !out.OK {
		return fmt.Errorf("supervisor reported ok=false")
	}
	return nil
}

// RunStep starts a step asynchronously via POST /v1/steps.
func (c *Client) RunStep(ctx context.Context, stepReq RunStepRequest) (*StepStatus, error) {
	var out StepStatus
	if err := c.postJSON(ctx, "/v1/steps", stepReq, &out, http.StatusAccepted, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetStep retrieves the current status of stepID via GET /v1/steps/{id}.
func (c *Client) GetStep(ctx context.Context, stepID string) (*StepStatus, error) {
	return c.getStepWithQuery(ctx, stepID, false)
}

// WaitStep blocks until stepID reaches a terminal state (COMPLETED or FAILED)
// and returns the final StepStatus.
func (c *Client) WaitStep(ctx context.Context, stepID string) (*StepStatus, error) {
	for {
		st, err := c.getStepWithQuery(ctx, stepID, true)
		if err != nil {
			return nil, err
		}
		if st.State.IsTerminal() {
			return st, nil
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *Client) getStepWithQuery(ctx context.Context, stepID string, wait bool) (*StepStatus, error) {
	u := c.baseURL + "/v1/steps/" + url.PathEscape(stepID)
	if wait {
		u += "?wait=true"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, readHTTPError("get step", resp)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read step status: %w", err)
	}
	var out StepStatus
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode step status: %w", err)
	}
	return &out, nil
}

// StreamLogs reads step logs starting at byte offset into dst.
// When follow is false, it reads available bytes up to the current end and
// returns the server's X-Log-Offset. When follow is true, it streams live chunks
// until the step reaches a terminal state and returns the final byte offset.
func (c *Client) StreamLogs(ctx context.Context, stepID string, offset int64, follow bool, dst io.Writer) (int64, error) {
	q := url.Values{}
	q.Set("offset", strconv.FormatInt(offset, 10))
	if follow {
		q.Set("follow", "true")
	} else {
		q.Set("follow", "false")
	}
	u := c.baseURL + "/v1/steps/" + url.PathEscape(stepID) + "/logs?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return offset, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return offset, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return offset, readHTTPError("stream logs", resp)
	}

	var written int64
	if dst != nil {
		written, err = io.Copy(dst, resp.Body)
	} else {
		written, err = io.Copy(io.Discard, resp.Body)
	}
	if err != nil {
		return offset + written, err
	}

	if !follow {
		if hdr := resp.Header.Get("X-Log-Offset"); hdr != "" {
			if parsed, parseErr := strconv.ParseInt(hdr, 10, 64); parseErr == nil {
				return parsed, nil
			}
		}
	}
	return offset + written, nil
}

// StreamLogsSSE streams Server-Sent Events from GET /v1/steps/{id}/logs?follow=true
// starting at offset, invoking onEvent for each "log" and final "done" frame.
func (c *Client) StreamLogsSSE(ctx context.Context, stepID string, offset int64, onEvent func(LogSSEEvent) error) error {
	q := url.Values{}
	q.Set("offset", strconv.FormatInt(offset, 10))
	q.Set("follow", "true")
	u := c.baseURL + "/v1/steps/" + url.PathEscape(stepID) + "/logs?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return readHTTPError("stream logs SSE", resp)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), 4<<20)

	var eventName string
	var dataBuf strings.Builder
	flushFrame := func() (bool, error) {
		if dataBuf.Len() == 0 && eventName == "" {
			return false, nil
		}
		raw := strings.TrimSuffix(dataBuf.String(), "\n")
		curEvent := eventName
		eventName = ""
		dataBuf.Reset()
		if raw == "" {
			return false, nil
		}
		var ev LogSSEEvent
		if err := json.Unmarshal([]byte(raw), &ev); err != nil {
			return false, fmt.Errorf("decode SSE frame: %w", err)
		}
		if ev.Event == "" {
			ev.Event = curEvent
		}
		if onEvent != nil {
			if err := onEvent(ev); err != nil {
				return false, err
			}
		}
		return ev.Event == "done", nil
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			done, frameErr := flushFrame()
			if frameErr != nil {
				return frameErr
			}
			if done {
				return nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		val = strings.TrimPrefix(val, " ")
		switch field {
		case "event":
			eventName = val
		case "data":
			dataBuf.WriteString(val)
			dataBuf.WriteByte('\n')
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	_, err = flushFrame()
	return err
}

// UploadFiles seeds ConfigMap or workspace files into --workspace-dir via POST /v1/files.
func (c *Client) UploadFiles(ctx context.Context, req UploadFilesRequest) (*UploadFilesResponse, error) {
	var out UploadFilesResponse
	if err := c.postJSON(ctx, "/v1/files", req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// RescueExec executes a synchronous diagnostic command inside --workspace-dir
// on a coordinator supervisor via POST /v1/rescue/exec.
func (c *Client) RescueExec(ctx context.Context, req RescueExecRequest) (*RescueExecResponse, error) {
	var out RescueExecResponse
	if err := c.postJSON(ctx, "/v1/rescue/exec", req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

// RescuePatch hot-patches files or applies a unified diff inside --workspace-dir
// on a coordinator supervisor via POST /v1/rescue/patch.
func (c *Client) RescuePatch(ctx context.Context, req RescuePatchRequest) (*RescuePatchResponse, error) {
	var out RescuePatchResponse
	if err := c.postJSON(ctx, "/v1/rescue/patch", req, &out, http.StatusOK); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) postJSON(ctx context.Context, path string, reqBody any, respBody any, expectedStatuses ...int) error {
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("encode request JSON: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	matched := false
	for _, st := range expectedStatuses {
		if resp.StatusCode == st {
			matched = true
			break
		}
	}
	if !matched {
		return readHTTPError(path, resp)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read %s response: %w", path, err)
	}
	if respBody != nil {
		if err := json.Unmarshal(raw, respBody); err != nil {
			return fmt.Errorf("decode %s response: %w", path, err)
		}
	}
	return nil
}

func readHTTPError(op string, resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	var errResp ErrorResponse
	if json.Unmarshal(raw, &errResp) == nil && errResp.Error != "" {
		return fmt.Errorf("%s failed (HTTP %d): %s", op, resp.StatusCode, errResp.Error)
	}
	return fmt.Errorf("%s failed (HTTP %d): %s", op, resp.StatusCode, strings.TrimSpace(string(raw)))
}
