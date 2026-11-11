// Package supervisor provides the HTTP protocol, server, and client for the
// Tekton Actors Step Supervisor daemon mounted inside Agent Substrate Actor
// step containers.
package supervisor

import (
	"fmt"
	"time"
)

// Role identifies whether a Step Supervisor instance serves as the sandbox
// coordinator (container 0 on :8080) or a sibling step worker container
// (127.0.0.1:8090+i).
type Role string

const (
	RoleCoordinator Role = "coordinator"
	RoleWorker      Role = "worker"
)

const (
	DefaultListenAddr          = ":8080"
	DefaultWorkerBasePort      = 8090
	DefaultWorkspaceDir        = "/workspace"
	DefaultTektonDir           = "/tekton"
	DefaultResultsDir          = "/tekton/results"
	DefaultSupervisorMountPath = "/tekton-supervisor"
	DefaultKoBinarySubpath     = "/ko-app/step-supervisor"
	// MountedSupervisorBinaryPath is the read-only executable path inside every
	// ActorTemplate step container when the ko-built step-supervisor image is
	// mounted via ImageVolumeSource at DefaultSupervisorMountPath.
	MountedSupervisorBinaryPath = DefaultSupervisorMountPath + DefaultKoBinarySubpath
)

// WorkerLoopbackAddr returns the loopback listen/target address for sibling
// container index i (1-based: c1 -> "127.0.0.1:8091").
func WorkerLoopbackAddr(containerIndex int) string {
	return fmt.Sprintf("127.0.0.1:%d", DefaultWorkerBasePort+containerIndex)
}

// Config configures a Step Supervisor HTTP server.
type Config struct {
	Role                Role
	WorkspaceDir        string
	ResultsDir          string
	TektonDir           string
	MaxRequestBodyBytes int64
}

// ReadyResponse is the JSON payload returned by GET /readyz.
type ReadyResponse struct {
	OK bool `json:"ok"`
}

// ErrorResponse is the standard JSON error payload returned on 4xx/5xx responses.
type ErrorResponse struct {
	Error string `json:"error"`
}

// StepState describes the execution lifecycle state of a step.
type StepState string

const (
	StepStateRunning   StepState = "RUNNING"
	StepStateCompleted StepState = "COMPLETED"
	StepStateFailed    StepState = "FAILED"
)

// IsTerminal reports whether st is a terminal step state.
func (st StepState) IsTerminal() bool {
	return st == StepStateCompleted || st == StepStateFailed
}

// EnvEntry represents a single environment variable assignment on a step.
type EnvEntry struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// RunStepRequest is the JSON request body for POST /v1/steps.
type RunStepRequest struct {
	StepID      string     `json:"stepId,omitempty"`
	Name        string     `json:"name,omitempty"`
	TargetAddr  string     `json:"targetAddr,omitempty"`
	Command     []string   `json:"command,omitempty"`
	Args        []string   `json:"args,omitempty"`
	Script      string     `json:"script,omitempty"`
	WorkingDir  string     `json:"workingDir,omitempty"`
	Env         []EnvEntry `json:"env,omitempty"`
	ResultNames []string   `json:"resultNames,omitempty"`
}

// StepStatus describes the current or terminal state of a step returned by
// POST /v1/steps and GET /v1/steps/{id}.
type StepStatus struct {
	StepID     string            `json:"stepId"`
	Name       string            `json:"name,omitempty"`
	State      StepState         `json:"state"`
	ExitCode   *int              `json:"exitCode,omitempty"`
	StartedAt  time.Time         `json:"startedAt"`
	FinishedAt *time.Time        `json:"finishedAt,omitempty"`
	Results    map[string]string `json:"results,omitempty"`
	LogSize    int64             `json:"logSize"`
	Error      string            `json:"error,omitempty"`
}

// LogSSEEvent is a Server-Sent Event frame emitted by
// GET /v1/steps/{id}/logs?follow=true when Accept: text/event-stream is set.
type LogSSEEvent struct {
	Event  string    `json:"event,omitempty"`
	Offset int64     `json:"offset"`
	Chunk  string    `json:"chunk,omitempty"`
	State  StepState `json:"state"`
}

// UploadFilesRequest is the JSON request body for POST /v1/files to seed
// ConfigMap or initial workspace files into --workspace-dir.
type UploadFilesRequest struct {
	SubDir string            `json:"subDir,omitempty"`
	Files  map[string][]byte `json:"files"`
	Mode   uint32            `json:"mode,omitempty"`
}

// UploadFilesResponse is the JSON response from POST /v1/files.
type UploadFilesResponse struct {
	WrittenFiles []string `json:"writtenFiles"`
}

// RescueExecRequest is the JSON request body for POST /v1/rescue/exec on a
// coordinator supervisor to inspect /workspace synchronously.
type RescueExecRequest struct {
	Command        []string   `json:"command,omitempty"`
	Args           []string   `json:"args,omitempty"`
	Script         string     `json:"script,omitempty"`
	WorkingDir     string     `json:"workingDir,omitempty"`
	Env            []EnvEntry `json:"env,omitempty"`
	TimeoutSeconds int        `json:"timeoutSeconds,omitempty"`
}

// RescueExecResponse is the JSON response from POST /v1/rescue/exec.
type RescueExecResponse struct {
	ExitCode int    `json:"exitCode"`
	Output   string `json:"output"`
}

// RescuePatchRequest is the JSON request body for POST /v1/rescue/patch on a
// coordinator supervisor to hot-patch files inside --workspace-dir.
type RescuePatchRequest struct {
	Files       map[string]string `json:"files,omitempty"`
	Diff        string            `json:"diff,omitempty"`
	DeleteFiles []string          `json:"deleteFiles,omitempty"`
}

// RescuePatchResponse is the JSON response from POST /v1/rescue/patch.
type RescuePatchResponse struct {
	PatchedFiles []string `json:"patchedFiles"`
}
