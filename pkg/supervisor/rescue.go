package supervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const defaultRescueTimeout = 60 * time.Second

func cleanWorkspaceRelPath(raw string) (string, error) {
	if raw == "" {
		return "", errors.New("file path must not be empty")
	}
	if strings.ContainsRune(raw, '\x00') || strings.ContainsRune(raw, '\\') {
		return "", fmt.Errorf("invalid file path %q", raw)
	}
	if strings.HasPrefix(raw, "/") || filepath.IsAbs(raw) {
		return "", fmt.Errorf("file path %q must be relative to workspace", raw)
	}
	cleaned := path.Clean(raw)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") || strings.Contains(cleaned, "/../") {
		return "", fmt.Errorf("path traversal rejected in %q", raw)
	}
	return filepath.FromSlash(cleaned), nil
}

func ensureNoSymlinkChain(root *os.Root, relPath string) error {
	slashPath := filepath.ToSlash(relPath)
	parts := strings.Split(slashPath, "/")
	cur := ""
	for _, part := range parts {
		if cur == "" {
			cur = part
		} else {
			cur = cur + "/" + part
		}
		info, err := root.Lstat(filepath.FromSlash(cur))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing to traverse symlink at %q", cur)
		}
	}
	return nil
}

func writeRootFile(root *os.Root, relPath string, data []byte, perm os.FileMode) error {
	if err := ensureNoSymlinkChain(root, relPath); err != nil {
		return err
	}
	parent := filepath.Dir(relPath)
	if parent != "." && parent != "" {
		if err := root.MkdirAll(parent, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", parent, err)
		}
	}
	if err := root.WriteFile(relPath, data, perm); err != nil {
		return fmt.Errorf("write %s: %w", relPath, err)
	}
	return nil
}

func (s *Server) handleUploadFiles(w http.ResponseWriter, r *http.Request) {
	var req UploadFilesRequest
	if err := s.decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Files) == 0 {
		writeError(w, http.StatusBadRequest, "files map must not be empty")
		return
	}

	var prefix string
	if strings.TrimSpace(req.SubDir) != "" {
		cleanSub, err := cleanWorkspaceRelPath(req.SubDir)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid subDir: %v", err))
			return
		}
		prefix = cleanSub
	}

	perm := os.FileMode(req.Mode) & 0o777
	if perm == 0 {
		perm = 0o644
	}

	type stagedWrite struct {
		rel  string
		data []byte
	}
	staged := make([]stagedWrite, 0, len(req.Files))
	for rawName, content := range req.Files {
		cleanRel, err := cleanWorkspaceRelPath(rawName)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if prefix != "" {
			cleanRel = filepath.Join(prefix, cleanRel)
		}
		staged = append(staged, stagedWrite{rel: cleanRel, data: content})
	}
	sort.Slice(staged, func(i, j int) bool { return staged[i].rel < staged[j].rel })

	if err := s.ensureDirs(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	root, err := os.OpenRoot(s.cfg.WorkspaceDir)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("open workspace root: %v", err))
		return
	}
	defer root.Close()

	for _, item := range staged {
		if err := ensureNoSymlinkChain(root, item.rel); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	written := make([]string, 0, len(staged))
	for _, item := range staged {
		if err := writeRootFile(root, item.rel, item.data, perm); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		// Ensure exact permission bits even if file previously existed or umask masked bits.
		_ = root.Chmod(item.rel, perm)
		written = append(written, filepath.ToSlash(item.rel))
	}

	writeJSON(w, http.StatusOK, UploadFilesResponse{WrittenFiles: written})
}

func (s *Server) handleRescueExec(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Role != RoleCoordinator {
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 64<<10))
		writeError(w, http.StatusForbidden, "rescue endpoints are only enabled on coordinator supervisor")
		return
	}
	var req RescueExecRequest
	if err := s.decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Script == "" && len(req.Command) == 0 {
		writeError(w, http.StatusBadRequest, "either script or command must be provided")
		return
	}
	if err := s.ensureDirs(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	timeout := defaultRescueTimeout
	if req.TimeoutSeconds > 0 {
		timeout = time.Duration(req.TimeoutSeconds) * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	scriptName := "rescue-" + generateStepID()
	if req.Script != "" {
		defer func() {
			_ = os.Remove(filepath.Join(s.stepsDir(), scriptName))
		}()
	}

	envSlice, envMap := buildEnvironment(os.Environ(), s.homeDir(), req.Env)
	cmd, err := prepareExecCommand(
		s.stepsDir(),
		scriptName,
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

	// Bind cmd lifecycle to ctx so timeouts terminate hung diagnostic commands.
	execCmd := execCommandContextFromPrepared(ctx, cmd)
	var out bytes.Buffer
	execCmd.Stdout = &out
	execCmd.Stderr = &out

	runErr := execCmd.Run()
	code := exitCodeFromWaitErr(runErr)
	writeJSON(w, http.StatusOK, RescueExecResponse{
		ExitCode: code,
		Output:   out.String(),
	})
}

func (s *Server) handleRescuePatch(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Role != RoleCoordinator {
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 64<<10))
		writeError(w, http.StatusForbidden, "rescue endpoints are only enabled on coordinator supervisor")
		return
	}
	var req RescuePatchRequest
	if err := s.decodeJSONBody(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Files) == 0 && strings.TrimSpace(req.Diff) == "" && len(req.DeleteFiles) == 0 {
		writeError(w, http.StatusBadRequest, "rescue patch requires at least one of files, diff, or deleteFiles")
		return
	}

	if err := s.ensureDirs(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	root, err := os.OpenRoot(s.cfg.WorkspaceDir)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("open workspace root: %v", err))
		return
	}
	defer root.Close()

	type mutation struct {
		delete bool
		data   []byte
		perm   os.FileMode
	}
	staged := make(map[string]mutation)

	readExisting := func(rel string) ([]byte, os.FileMode, bool, error) {
		if m, ok := staged[rel]; ok {
			if m.delete {
				return nil, 0o644, false, nil
			}
			return m.data, m.perm, true, nil
		}
		if err := ensureNoSymlinkChain(root, rel); err != nil {
			return nil, 0, false, err
		}
		info, statErr := root.Lstat(rel)
		if statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				return nil, 0o644, false, nil
			}
			return nil, 0, false, statErr
		}
		if !info.Mode().IsRegular() {
			return nil, 0, false, fmt.Errorf("%q is not a regular file", rel)
		}
		data, readErr := root.ReadFile(rel)
		if readErr != nil {
			return nil, 0, false, readErr
		}
		return data, info.Mode().Perm(), true, nil
	}

	// 1. Validate and compute unified diff mutations first (in-memory, atomic).
	if strings.TrimSpace(req.Diff) != "" {
		fileDiffs, parseErr := parseUnifiedDiff(req.Diff)
		if parseErr != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid unified diff: %v", parseErr))
			return
		}
		for _, fd := range fileDiffs {
			var targetRel string
			if !fd.newIsDevNull {
				cleanNew, pathErr := cleanWorkspaceRelPath(fd.newPath)
				if pathErr != nil {
					writeError(w, http.StatusBadRequest, fmt.Sprintf("diff target path: %v", pathErr))
					return
				}
				targetRel = cleanNew
			}
			if !fd.oldIsDevNull {
				cleanOld, pathErr := cleanWorkspaceRelPath(fd.oldPath)
				if pathErr != nil {
					writeError(w, http.StatusBadRequest, fmt.Sprintf("diff source path: %v", pathErr))
					return
				}
				if targetRel == "" {
					targetRel = cleanOld
				}
			}
			if targetRel == "" {
				writeError(w, http.StatusBadRequest, "diff entry missing file path")
				return
			}

			origBytes, perm, exists, readErr := readExisting(targetRel)
			if readErr != nil {
				writeError(w, http.StatusBadRequest, readErr.Error())
				return
			}
			if !fd.oldIsDevNull && !exists {
				writeError(w, http.StatusConflict, fmt.Sprintf("cannot patch non-existent file %q", filepath.ToSlash(targetRel)))
				return
			}
			if fd.newIsDevNull {
				staged[targetRel] = mutation{delete: true}
				continue
			}

			patchedBytes, applyErr := applyFileDiff(origBytes, fd)
			if applyErr != nil {
				writeError(w, http.StatusConflict, fmt.Sprintf("patch %s: %v", filepath.ToSlash(targetRel), applyErr))
				return
			}
			staged[targetRel] = mutation{data: patchedBytes, perm: perm}
		}
	}

	// 2. Validate and stage direct file replacements.
	for rawName, content := range req.Files {
		cleanRel, pathErr := cleanWorkspaceRelPath(rawName)
		if pathErr != nil {
			writeError(w, http.StatusBadRequest, pathErr.Error())
			return
		}
		_, perm, _, readErr := readExisting(cleanRel)
		if readErr != nil {
			writeError(w, http.StatusBadRequest, readErr.Error())
			return
		}
		staged[cleanRel] = mutation{data: []byte(content), perm: perm}
	}

	// 3. Validate and stage explicit deletions.
	for _, rawDel := range req.DeleteFiles {
		cleanRel, pathErr := cleanWorkspaceRelPath(rawDel)
		if pathErr != nil {
			writeError(w, http.StatusBadRequest, pathErr.Error())
			return
		}
		if err := ensureNoSymlinkChain(root, cleanRel); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		staged[cleanRel] = mutation{delete: true}
	}

	// 4. Commit all staged mutations deterministically.
	keys := make([]string, 0, len(staged))
	for k := range staged {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	patchedFiles := make([]string, 0, len(keys))
	for _, rel := range keys {
		m := staged[rel]
		if m.delete {
			if rmErr := root.Remove(rel); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
				writeError(w, http.StatusInternalServerError, fmt.Sprintf("remove %s: %v", rel, rmErr))
				return
			}
		} else {
			if wErr := writeRootFile(root, rel, m.data, m.perm); wErr != nil {
				writeError(w, http.StatusBadRequest, wErr.Error())
				return
			}
		}
		patchedFiles = append(patchedFiles, filepath.ToSlash(rel))
	}

	writeJSON(w, http.StatusOK, RescuePatchResponse{PatchedFiles: patchedFiles})
}

// --- Pure-Go Unified Diff Parser & Applier ---

type diffOpKind byte

const (
	diffContext diffOpKind = ' '
	diffDelete  diffOpKind = '-'
	diffInsert  diffOpKind = '+'
)

type diffOp struct {
	kind      diffOpKind
	text      string
	noNewline bool
}

type diffHunk struct {
	oldStart int
	oldCount int
	newStart int
	newCount int
	ops      []diffOp
}

type unifiedFileDiff struct {
	oldPath      string
	newPath      string
	oldIsDevNull bool
	newIsDevNull bool
	hunks        []diffHunk
}

func parseUnifiedDiff(raw string) ([]unifiedFileDiff, error) {
	normalized := strings.ReplaceAll(raw, "\r\n", "\n")
	lines := strings.Split(normalized, "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	var files []unifiedFileDiff
	for i := 0; i < len(lines); {
		line := lines[i]
		if !strings.HasPrefix(line, "--- ") {
			i++
			continue
		}
		if i+1 >= len(lines) || !strings.HasPrefix(lines[i+1], "+++ ") {
			return nil, fmt.Errorf("missing +++ header after %q", line)
		}
		oldPath, oldDevNull := parseDiffHeaderPath(strings.TrimPrefix(line, "--- "), "a/")
		newPath, newDevNull := parseDiffHeaderPath(strings.TrimPrefix(lines[i+1], "+++ "), "b/")
		i += 2

		fd := unifiedFileDiff{
			oldPath:      oldPath,
			newPath:      newPath,
			oldIsDevNull: oldDevNull,
			newIsDevNull: newDevNull,
		}

		for i < len(lines) && strings.HasPrefix(lines[i], "@@ ") {
			hunk, nextIdx, err := parseHunk(lines, i)
			if err != nil {
				return nil, err
			}
			fd.hunks = append(fd.hunks, hunk)
			i = nextIdx
		}
		if len(fd.hunks) == 0 {
			return nil, fmt.Errorf("diff for %q contains no hunks", newPath)
		}
		files = append(files, fd)
	}
	if len(files) == 0 {
		return nil, errors.New("no unified diff file headers found")
	}
	return files, nil
}

func parseDiffHeaderPath(raw string, stripPrefix string) (string, bool) {
	if tab := strings.IndexByte(raw, '\t'); tab >= 0 {
		raw = raw[:tab]
	}
	raw = strings.TrimSpace(raw)
	if raw == "/dev/null" {
		return "", true
	}
	raw = strings.TrimPrefix(raw, stripPrefix)
	return raw, false
}

func parseHunk(lines []string, startIdx int) (diffHunk, int, error) {
	hdr := lines[startIdx]
	endAt := strings.Index(hdr[3:], " @@")
	if endAt < 0 {
		return diffHunk{}, startIdx, fmt.Errorf("malformed hunk header %q", hdr)
	}
	rangeSpec := strings.TrimSpace(hdr[3 : 3+endAt])
	oldSpec, newSpec, ok := strings.Cut(rangeSpec, " ")
	if !ok || !strings.HasPrefix(oldSpec, "-") || !strings.HasPrefix(newSpec, "+") {
		return diffHunk{}, startIdx, fmt.Errorf("malformed hunk range %q", hdr)
	}
	oldStart, oldCount, err := parseRangePair(oldSpec[1:])
	if err != nil {
		return diffHunk{}, startIdx, fmt.Errorf("invalid old hunk range %q: %w", oldSpec, err)
	}
	newStart, newCount, err := parseRangePair(newSpec[1:])
	if err != nil {
		return diffHunk{}, startIdx, fmt.Errorf("invalid new hunk range %q: %w", newSpec, err)
	}

	h := diffHunk{
		oldStart: oldStart,
		oldCount: oldCount,
		newStart: newStart,
		newCount: newCount,
	}

	i := startIdx + 1
	remOld := oldCount
	remNew := newCount
	for i < len(lines) && (remOld > 0 || remNew > 0) {
		l := lines[i]
		if strings.HasPrefix(l, `\ `) {
			if len(h.ops) > 0 {
				h.ops[len(h.ops)-1].noNewline = true
			}
			i++
			continue
		}
		if l == "" {
			// Treat bare empty line inside hunk as empty context line.
			h.ops = append(h.ops, diffOp{kind: diffContext, text: ""})
			remOld--
			remNew--
			i++
			continue
		}
		switch l[0] {
		case ' ':
			h.ops = append(h.ops, diffOp{kind: diffContext, text: l[1:]})
			remOld--
			remNew--
		case '-':
			h.ops = append(h.ops, diffOp{kind: diffDelete, text: l[1:]})
			remOld--
		case '+':
			h.ops = append(h.ops, diffOp{kind: diffInsert, text: l[1:]})
			remNew--
		default:
			return diffHunk{}, startIdx, fmt.Errorf("unexpected line %q inside hunk", l)
		}
		i++
	}
	if i < len(lines) && strings.HasPrefix(lines[i], `\ `) {
		if len(h.ops) > 0 {
			h.ops[len(h.ops)-1].noNewline = true
		}
		i++
	}
	if remOld != 0 || remNew != 0 {
		return diffHunk{}, startIdx, fmt.Errorf("incomplete hunk %q (remOld=%d remNew=%d)", hdr, remOld, remNew)
	}
	return h, i, nil
}

func parseRangePair(spec string) (start int, count int, err error) {
	sStr, cStr, hasComma := strings.Cut(spec, ",")
	start, err = strconv.Atoi(sStr)
	if err != nil || start < 0 {
		return 0, 0, fmt.Errorf("invalid start %q", sStr)
	}
	if !hasComma {
		return start, 1, nil
	}
	count, err = strconv.Atoi(cStr)
	if err != nil || count < 0 {
		return 0, 0, fmt.Errorf("invalid count %q", cStr)
	}
	return start, count, nil
}

func applyFileDiff(orig []byte, fd unifiedFileDiff) ([]byte, error) {
	origStr := strings.ReplaceAll(string(orig), "\r\n", "\n")
	var srcLines []string
	trailingNL := true
	if len(origStr) > 0 {
		if strings.HasSuffix(origStr, "\n") {
			srcLines = strings.Split(origStr[:len(origStr)-1], "\n")
			trailingNL = true
		} else {
			srcLines = strings.Split(origStr, "\n")
			trailingNL = false
		}
	}

	var outLines []string
	srcIdx := 0

	for _, h := range fd.hunks {
		targetIdx := 0
		if h.oldStart > 0 {
			targetIdx = h.oldStart - 1
		}
		if targetIdx < srcIdx || targetIdx > len(srcLines) {
			return nil, fmt.Errorf("hunk start %d out of bounds (len=%d)", h.oldStart, len(srcLines))
		}
		outLines = append(outLines, srcLines[srcIdx:targetIdx]...)
		srcIdx = targetIdx

		for _, op := range h.ops {
			switch op.kind {
			case diffContext:
				if srcIdx >= len(srcLines) || srcLines[srcIdx] != op.text {
					got := "<EOF>"
					if srcIdx < len(srcLines) {
						got = fmt.Sprintf("%q", srcLines[srcIdx])
					}
					return nil, fmt.Errorf("context mismatch at line %d: got %s, want %q", srcIdx+1, got, op.text)
				}
				outLines = append(outLines, op.text)
				if srcIdx == len(srcLines)-1 {
					trailingNL = !op.noNewline
				}
				srcIdx++
			case diffDelete:
				if srcIdx >= len(srcLines) || srcLines[srcIdx] != op.text {
					got := "<EOF>"
					if srcIdx < len(srcLines) {
						got = fmt.Sprintf("%q", srcLines[srcIdx])
					}
					return nil, fmt.Errorf("delete mismatch at line %d: got %s, want %q", srcIdx+1, got, op.text)
				}
				srcIdx++
			case diffInsert:
				outLines = append(outLines, op.text)
				trailingNL = !op.noNewline
			}
		}
	}

	if srcIdx < len(srcLines) {
		outLines = append(outLines, srcLines[srcIdx:]...)
	}
	if len(outLines) == 0 {
		return nil, nil
	}
	joined := strings.Join(outLines, "\n")
	if trailingNL {
		joined += "\n"
	}
	return []byte(joined), nil
}
