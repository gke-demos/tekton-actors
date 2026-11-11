package supervisor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type logBuffer struct {
	mu     sync.Mutex
	buf    []byte
	closed bool
	state  StepState
	notify chan struct{}
}

func newLogBuffer() *logBuffer {
	return &logBuffer{
		state:  StepStateRunning,
		notify: make(chan struct{}),
	}
}

func (b *logBuffer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	b.mu.Lock()
	b.buf = append(b.buf, p...)
	close(b.notify)
	b.notify = make(chan struct{})
	b.mu.Unlock()
	return len(p), nil
}

func (b *logBuffer) Close(finalState StepState) {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		b.state = finalState
		close(b.notify)
		b.notify = make(chan struct{})
	}
	b.mu.Unlock()
}

func (b *logBuffer) Len() int64 {
	b.mu.Lock()
	n := int64(len(b.buf))
	b.mu.Unlock()
	return n
}

func (b *logBuffer) snapshotFrom(offset int64) (chunk []byte, newOffset int64, state StepState, closed bool, waitCh <-chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()

	total := int64(len(b.buf))
	if offset < 0 {
		offset = 0
	}
	if offset > total {
		offset = total
	}
	if offset < total {
		chunk = append([]byte(nil), b.buf[offset:total]...)
	}
	return chunk, total, b.state, b.closed, b.notify
}

type stepRecord struct {
	mu         sync.Mutex
	id         string
	name       string
	remoteAddr string
	state      StepState
	exitCode   *int
	startedAt  time.Time
	finishedAt *time.Time
	results    map[string]string
	errMsg     string
	logs       *logBuffer
}

func (r *stepRecord) status() StepStatus {
	r.mu.Lock()
	defer r.mu.Unlock()

	var codePtr *int
	if r.exitCode != nil {
		c := *r.exitCode
		codePtr = &c
	}
	var finPtr *time.Time
	if r.finishedAt != nil {
		t := *r.finishedAt
		finPtr = &t
	}
	var resCopy map[string]string
	if len(r.results) > 0 {
		resCopy = make(map[string]string, len(r.results))
		for k, v := range r.results {
			resCopy[k] = v
		}
	}
	var logLen int64
	if r.logs != nil {
		logLen = r.logs.Len()
	}
	return StepStatus{
		StepID:     r.id,
		Name:       r.name,
		State:      r.state,
		ExitCode:   codePtr,
		StartedAt:  r.startedAt,
		FinishedAt: finPtr,
		Results:    resCopy,
		LogSize:    logLen,
		Error:      r.errMsg,
	}
}

var stepSeq atomic.Uint64

func generateStepID() string {
	var raw [4]byte
	_, _ = rand.Read(raw[:])
	seq := stepSeq.Add(1)
	return fmt.Sprintf("step-%d-%s", seq, hex.EncodeToString(raw[:]))
}

func validateStepID(id string) error {
	if id == "" {
		return errors.New("stepId must not be empty")
	}
	if len(id) > 128 {
		return errors.New("stepId exceeds 128 characters")
	}
	if id == "." || id == ".." {
		return fmt.Errorf("invalid stepId %q", id)
	}
	for i := 0; i < len(id); i++ {
		ch := id[i]
		isAlphaNum := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')
		if !isAlphaNum && ch != '-' && ch != '_' && ch != '.' {
			return fmt.Errorf("invalid character %q in stepId %q", ch, id)
		}
	}
	return nil
}

// expandKubernetesVars implements Kubernetes $(VAR) expansion semantics:
//   - $$(VAR) escapes to literal $(VAR)
//   - $(VAR) expands to mapping[VAR] if defined, or remains literal $(VAR) if undefined
//   - bare $VAR or ${VAR} are left untouched for shell evaluation
func expandKubernetesVars(input string, mapping map[string]string) string {
	if !strings.ContainsRune(input, '$') {
		return input
	}
	var b strings.Builder
	b.Grow(len(input))

	for i := 0; i < len(input); {
		if input[i] != '$' {
			b.WriteByte(input[i])
			i++
			continue
		}
		if i+1 >= len(input) {
			b.WriteByte('$')
			i++
			continue
		}
		switch input[i+1] {
		case '$':
			b.WriteByte('$')
			i += 2
		case '(':
			closeRel := strings.IndexByte(input[i+2:], ')')
			if closeRel < 0 {
				b.WriteString(input[i:])
				return b.String()
			}
			varName := input[i+2 : i+2+closeRel]
			if val, ok := mapping[varName]; ok && varName != "" {
				b.WriteString(val)
			} else {
				b.WriteString("$(")
				b.WriteString(varName)
				b.WriteByte(')')
			}
			i = i + 2 + closeRel + 1
		default:
			b.WriteByte('$')
			i++
		}
	}
	return b.String()
}

func buildEnvironment(baseEnv []string, defaultHome string, entries []EnvEntry) ([]string, map[string]string) {
	mapping := make(map[string]string, len(baseEnv)+len(entries)+1)
	order := make([]string, 0, len(baseEnv)+len(entries)+1)
	seen := make(map[string]bool, len(baseEnv)+len(entries)+1)

	for _, kv := range baseEnv {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			continue
		}
		if !seen[k] {
			seen[k] = true
			order = append(order, k)
		}
		mapping[k] = v
	}

	// Tekton step contract: default HOME to /tekton/home unless overridden in req.Env.
	if !seen["HOME"] {
		seen["HOME"] = true
		order = append(order, "HOME")
	}
	mapping["HOME"] = defaultHome

	for _, entry := range entries {
		if entry.Name == "" {
			continue
		}
		expanded := expandKubernetesVars(entry.Value, mapping)
		if !seen[entry.Name] {
			seen[entry.Name] = true
			order = append(order, entry.Name)
		}
		mapping[entry.Name] = expanded
	}

	sort.Strings(order)
	envSlice := make([]string, 0, len(order))
	for _, k := range order {
		envSlice = append(envSlice, k+"="+mapping[k])
	}
	return envSlice, mapping
}

func parseShebang(script string) (interpreter string, interpArg string, hasShebang bool) {
	if !strings.HasPrefix(script, "#!") {
		return "", "", false
	}
	firstLine := script[2:]
	if nl := strings.IndexByte(firstLine, '\n'); nl >= 0 {
		firstLine = firstLine[:nl]
	}
	firstLine = strings.TrimRight(firstLine, "\r")
	firstLine = strings.TrimLeft(firstLine, " \t")
	if firstLine == "" {
		return "", "", false
	}
	// Linux binfmt_script splits into interpreter + at most one single argument string.
	idx := strings.IndexAny(firstLine, " \t")
	if idx < 0 {
		return firstLine, "", true
	}
	interp := firstLine[:idx]
	rest := strings.TrimSpace(firstLine[idx+1:])
	return interp, rest, true
}

func prepareExecCommand(
	stepsDir string,
	scriptName string,
	command []string,
	args []string,
	script string,
	workingDir string,
	defaultWorkDir string,
	envMap map[string]string,
	envSlice []string,
) (*exec.Cmd, error) {
	expandedArgs := make([]string, len(args))
	for i, a := range args {
		expandedArgs[i] = expandKubernetesVars(a, envMap)
	}

	var execBin string
	var execArgs []string

	if script != "" {
		scriptPath := filepath.Join(stepsDir, scriptName)
		content := script
		interp, interpArg, ok := parseShebang(script)
		if !ok {
			interp = "/bin/sh"
			interpArg = "-e"
			content = "#!/bin/sh -e\n" + script
		}
		if err := os.WriteFile(scriptPath, []byte(content), 0o700); err != nil {
			return nil, fmt.Errorf("write step script: %w", err)
		}
		execBin = interp
		if interpArg != "" {
			execArgs = append(execArgs, interpArg)
		}
		execArgs = append(execArgs, scriptPath)
		execArgs = append(execArgs, expandedArgs...)
	} else {
		if len(command) == 0 {
			return nil, errors.New("either script or command must be non-empty")
		}
		expandedCmd := make([]string, len(command))
		for i, c := range command {
			expandedCmd[i] = expandKubernetesVars(c, envMap)
		}
		execBin = expandedCmd[0]
		execArgs = append(execArgs, expandedCmd[1:]...)
		execArgs = append(execArgs, expandedArgs...)
	}

	dir := defaultWorkDir
	if strings.TrimSpace(workingDir) != "" {
		expandedDir := expandKubernetesVars(workingDir, envMap)
		if filepath.IsAbs(expandedDir) {
			dir = filepath.Clean(expandedDir)
		} else {
			dir = filepath.Join(defaultWorkDir, expandedDir)
		}
	}

	cmd := exec.Command(execBin, execArgs...)
	cmd.Dir = dir
	cmd.Env = envSlice
	return cmd, nil
}

func execCommandContextFromPrepared(ctx context.Context, prepared *exec.Cmd) *exec.Cmd {
	var args []string
	if len(prepared.Args) > 1 {
		args = prepared.Args[1:]
	}
	cmd := exec.CommandContext(ctx, prepared.Path, args...)
	cmd.Dir = prepared.Dir
	cmd.Env = prepared.Env
	return cmd
}

func exitCodeFromWaitErr(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return 127
}

func validateResultName(name string) error {
	if name == "" || name == "." || name == ".." {
		return fmt.Errorf("invalid result name %q", name)
	}
	if strings.ContainsAny(name, "/\\\x00") {
		return fmt.Errorf("result name %q must not contain path separators", name)
	}
	return nil
}

func readStepResults(resultsDir string, declared []string) map[string]string {
	root, err := os.OpenRoot(resultsDir)
	if err != nil {
		return nil
	}
	defer root.Close()

	out := make(map[string]string)
	if len(declared) > 0 {
		for _, name := range declared {
			if validateResultName(name) != nil {
				continue
			}
			info, statErr := root.Stat(name)
			if statErr != nil || !info.Mode().IsRegular() {
				continue
			}
			data, readErr := root.ReadFile(name)
			if readErr == nil {
				out[name] = string(data)
			}
		}
		return out
	}

	entries, err := os.ReadDir(resultsDir)
	if err != nil {
		return nil
	}
	for _, entry := range entries {
		name := entry.Name()
		if validateResultName(name) != nil {
			continue
		}
		info, statErr := root.Stat(name)
		if statErr != nil || !info.Mode().IsRegular() {
			continue
		}
		data, readErr := root.ReadFile(name)
		if readErr == nil {
			out[name] = string(data)
		}
	}
	return out
}
