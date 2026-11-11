#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-tekton-actors}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-$(kubectl config current-context)}"
LOCAL_PORT="${LOCAL_PORT:-18085}"
GATEWAY_URL="${GATEWAY_URL:-http://127.0.0.1:${LOCAL_PORT}}"

PF_PID=""
cleanup() {
  if [[ -n "${PF_PID}" ]] && kill -0 "${PF_PID}" 2>/dev/null; then
    kill "${PF_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

if [[ -z "${USE_EXISTING_GATEWAY:-}" ]]; then
  echo "==> Starting port-forward to svc/tekton-actors-gateway on 127.0.0.1:${LOCAL_PORT}..."
  kubectl --context="${KUBECTL_CONTEXT}" -n "${NAMESPACE}" port-forward "svc/tekton-actors-gateway" "${LOCAL_PORT}:80" >/dev/null 2>&1 &
  PF_PID=$!
  for _ in $(seq 1 30); do
    if curl -fsS "${GATEWAY_URL}/healthz" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
fi

INIT_STATE="$(curl -fsS "${GATEWAY_URL}/api/state")"
for running_actor in $(python3 -c '
import json, sys
s = json.load(sys.stdin)
for a in s.get("substrate", {}).get("actors", []):
    if a.get("state") == "ACTOR_STATE_RUNNING":
        print(a["name"])
' <<<"${INIT_STATE}"); do
  echo "==> Suspending leftover running Actor ${running_actor} before E2E verification..."
  curl -fsS -X POST "${GATEWAY_URL}/api/actors/${running_actor}/suspend" >/dev/null || true
done

echo "==> [1/5] Verifying Mission-Control UI routes (/, /compare, /rescue, /about)..."
for route in "/" "/compare" "/rescue" "/about"; do
  html="$(curl -fsS "${GATEWAY_URL}${route}")"
  if [[ "${html}" != *"theme-dark-btn"* || "${html}" != *"theme-light-btn"* ]]; then
    echo "ERROR: Route ${route} missing Projector Light Mode toggle" >&2
    exit 1
  fi
  echo "    GET ${route} -> 200 OK (Projector Light Mode toggle + SVG icons verified)"
done

echo "==> [2/5] Launching Actor-backed PipelineRun with Zero-Slot ApprovalGate (requireApproval=true)..."
ACTOR_LAUNCH_JSON="$(curl -fsS -X POST "${GATEWAY_URL}/api/pipelineruns" \
  -H "Content-Type: application/json" \
  -d '{"mode":"actor","injectBug":false,"requireApproval":true}')"
ACTOR_PR="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["created"][0])' <<<"${ACTOR_LAUNCH_JSON}")"
echo "    Created Actor PipelineRun: ${ACTOR_PR}"

echo "==> Waiting for Actor PipelineRun ${ACTOR_PR} to reach PendingApproval on release-gate..."
GATE_NAME=""
for _ in $(seq 1 180); do
  STATE_JSON="$(curl -fsS "${GATEWAY_URL}/api/state")"
  GATE_NAME="$(STATE_JSON="${STATE_JSON}" python3 - "${ACTOR_PR}" <<'PY'
import json, os, sys
pr_name = sys.argv[1]
state = json.loads(os.environ["STATE_JSON"])
for g in state.get("approvalGates", []):
    if g.get("pipelineRunName") == pr_name and g.get("status") == "PendingApproval":
        print(g["name"])
        sys.exit(0)
PY
)"
  if [[ -n "${GATE_NAME}" ]]; then
    break
  fi
  PR_STATUS="$(STATE_JSON="${STATE_JSON}" python3 - "${ACTOR_PR}" <<'PY'
import json, os, sys
pr_name = sys.argv[1]
state = json.loads(os.environ["STATE_JSON"])
for pr in state.get("pipelineRuns", []):
    if pr.get("name") == pr_name:
        print(pr.get("status", ""))
        sys.exit(0)
PY
)"
  if [[ "${PR_STATUS}" == "Failed" ]]; then
    echo "ERROR: Actor PipelineRun ${ACTOR_PR} failed before reaching ApprovalGate" >&2
    kubectl --context="${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get pipelineruns,taskruns,customruns -o wide
    exit 1
  fi
  sleep 2
done

if [[ -z "${GATE_NAME}" ]]; then
  echo "ERROR: Timed out waiting for ApprovalGate on ${ACTOR_PR}" >&2
  exit 1
fi
echo "    ApprovalGate ${GATE_NAME} is PendingApproval!"

STATE_JSON="$(curl -fsS "${GATEWAY_URL}/api/state")"
STATE_JSON="${STATE_JSON}" python3 - "${ACTOR_PR}" <<'PY'
import json, os, sys
pr_name = sys.argv[1]
state = json.loads(os.environ["STATE_JSON"])
pr = next(p for p in state["pipelineRuns"] if p["name"] == pr_name)
ws_actor = pr.get("workspaceActor") or ("ws-" + pr_name)
ms_tag = pr.get("milestoneTag", "")
actors = {a["name"]: a for a in state["substrate"]["actors"]}
ws_state = actors.get(ws_actor, {}).get("state", "UNKNOWN")
print(f"    Trunk Workspace Actor: {ws_actor} -> state={ws_state}")
print(f"    Milestone Snapshot Tag: {ms_tag}")
for t in pr.get("tasks", []):
    if t.get("taskName") == "vuln-scan":
        posture = (t.get("results") or {}).get("secret-posture", "")
        print(f"    vuln-scan secret-posture result: {posture}")
        assert posture == "ZERO_AMBIENT_SECRETS", f"expected ZERO_AMBIENT_SECRETS, got {posture}"
assert ws_state == "ACTOR_STATE_SUSPENDED", f"expected trunk actor SUSPENDED during approval gate, got {ws_state}"
PY

echo "==> Approving ApprovalGate ${GATE_NAME} via Mission-Control REST API..."
curl -fsS -X POST "${GATEWAY_URL}/api/approvals/${GATE_NAME}" \
  -H "Content-Type: application/json" \
  -d '{"decision":"approved","approver":"e2e-verifier","note":"Verified zero-slot suspended Actor pause"}' >/dev/null

echo "==> Waiting for Actor PipelineRun ${ACTOR_PR} to complete package-and-publish..."
PR_STATUS=""
for _ in $(seq 1 120); do
  STATE_JSON="$(curl -fsS "${GATEWAY_URL}/api/state")"
  PR_STATUS="$(STATE_JSON="${STATE_JSON}" python3 - "${ACTOR_PR}" <<'PY'
import json, os, sys
pr_name = sys.argv[1]
state = json.loads(os.environ["STATE_JSON"])
for pr in state.get("pipelineRuns", []):
    if pr.get("name") == pr_name:
        print(pr.get("status", ""))
        sys.exit(0)
PY
)"
  if [[ "${PR_STATUS}" == "Succeeded" ]]; then
    echo "    Actor PipelineRun ${ACTOR_PR} Succeeded!"
    break
  fi
  if [[ "${PR_STATUS}" == "Failed" ]]; then
    echo "ERROR: Actor PipelineRun ${ACTOR_PR} Failed after approval" >&2
    exit 1
  fi
  sleep 2
done
if [[ "${PR_STATUS}" != "Succeeded" ]]; then
  echo "ERROR: Timed out waiting for ${ACTOR_PR} to succeed" >&2
  exit 1
fi

echo "==> [3/5] Launching Stock Pod+PVC PipelineRun on the exact same release-pipeline..."
POD_LAUNCH_JSON="$(curl -fsS -X POST "${GATEWAY_URL}/api/pipelineruns" \
  -H "Content-Type: application/json" \
  -d '{"mode":"pod-pvc","injectBug":false,"requireApproval":false}')"
POD_PR="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["created"][0])' <<<"${POD_LAUNCH_JSON}")"
echo "    Created Pod+PVC PipelineRun: ${POD_PR}"

POD_STATUS=""
for _ in $(seq 1 180); do
  STATE_JSON="$(curl -fsS "${GATEWAY_URL}/api/state")"
  POD_STATUS="$(STATE_JSON="${STATE_JSON}" python3 - "${POD_PR}" <<'PY'
import json, os, sys
pr_name = sys.argv[1]
state = json.loads(os.environ["STATE_JSON"])
for pr in state.get("pipelineRuns", []):
    if pr.get("name") == pr_name:
        print(pr.get("status", ""))
        sys.exit(0)
PY
)"
  if [[ "${POD_STATUS}" == "Succeeded" ]]; then
    echo "    Stock Pod+PVC PipelineRun ${POD_PR} Succeeded!"
    break
  fi
  if [[ "${POD_STATUS}" == "Failed" ]]; then
    echo "ERROR: Stock Pod+PVC PipelineRun ${POD_PR} Failed" >&2
    exit 1
  fi
  sleep 2
done
if [[ "${POD_STATUS}" != "Succeeded" ]]; then
  echo "ERROR: Timed out waiting for ${POD_PR} to succeed" >&2
  exit 1
fi

echo "==> [4/5] Launching Buggy Actor PipelineRun (injectBug=true) to verify Failure Snapshot Tag..."
BUG_LAUNCH_JSON="$(curl -fsS -X POST "${GATEWAY_URL}/api/pipelineruns" \
  -H "Content-Type: application/json" \
  -d '{"mode":"actor","injectBug":true,"requireApproval":false}')"
BUG_PR="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["created"][0])' <<<"${BUG_LAUNCH_JSON}")"
echo "    Created Buggy Actor PipelineRun: ${BUG_PR}"

FAILURE_TAG=""
for _ in $(seq 1 180); do
  STATE_JSON="$(curl -fsS "${GATEWAY_URL}/api/state")"
  FAILURE_TAG="$(STATE_JSON="${STATE_JSON}" python3 - "${BUG_PR}" <<'PY'
import json, os, sys
pr_name = sys.argv[1]
state = json.loads(os.environ["STATE_JSON"])
for pr in state.get("pipelineRuns", []):
    if pr.get("name") == pr_name:
        for t in pr.get("tasks", []):
            if t.get("failureTag"):
                print(t["failureTag"])
                sys.exit(0)
PY
)"
  if [[ -n "${FAILURE_TAG}" ]]; then
    break
  fi
  sleep 2
done

if [[ -z "${FAILURE_TAG}" ]]; then
  echo "ERROR: Timed out waiting for failure Snapshot Tag on ${BUG_PR}" >&2
  exit 1
fi
echo "    Captured immutable Failure Snapshot Tag: ${FAILURE_TAG}"

echo "==> [5/5] Forking Rescue Actor from ${FAILURE_TAG}, hot-patching pricing.go, and resuming mid-DAG..."
FORK_JSON="$(curl -fsS -X POST "${GATEWAY_URL}/api/rescue/fork" \
  -H "Content-Type: application/json" \
  -d "{\"failureTag\":\"${FAILURE_TAG}\"}")"
RESCUE_ACTOR="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["rescueActorName"])' <<<"${FORK_JSON}")"
echo "    Forked & resumed Rescue Actor: ${RESCUE_ACTOR}"

PATCH_JSON="$(curl -fsS -X POST "${GATEWAY_URL}/api/rescue/patch" \
  -H "Content-Type: application/json" \
  -d "{\"rescueActorName\":\"${RESCUE_ACTOR}\"}")"
echo "    Applied hot-patch in Rescue Actor: ${PATCH_JSON}"

EXEC_JSON="$(curl -fsS -X POST "${GATEWAY_URL}/api/rescue/exec" \
  -H "Content-Type: application/json" \
  -d "{\"rescueActorName\":\"${RESCUE_ACTOR}\",\"workingDir\":\"/workspace/source/app\",\"script\":\"export GOCACHE=/workspace/source/.gocache GOPROXY=off && go test -v ./...\"}")"
EXEC_EXIT="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["exitCode"])' <<<"${EXEC_JSON}")"
if [[ "${EXEC_EXIT}" != "0" ]]; then
  echo "ERROR: go test inside Rescue Actor exited ${EXEC_EXIT}: ${EXEC_JSON}" >&2
  exit 1
fi
echo "    Verified go test passes inside Rescue Actor (exitCode=0, warm GOCACHE)"

RESUME_JSON="$(curl -fsS -X POST "${GATEWAY_URL}/api/rescue/commit-and-resume" \
  -H "Content-Type: application/json" \
  -d "{\"rescueActorName\":\"${RESCUE_ACTOR}\",\"originalPipelineRunName\":\"${BUG_PR}\",\"skipTasks\":[\"clone-and-build\"]}")"
FIXED_TAG="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["fixedTag"])' <<<"${RESUME_JSON}")"
RESUMED_PR="$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["resumedPipelineRun"])' <<<"${RESUME_JSON}")"
echo "    Minted Fixed Snapshot Tag: ${FIXED_TAG}"
echo "    Launched Mid-DAG Resumed PipelineRun: ${RESUMED_PR} (skipTasks=[\"clone-and-build\"])"

RESUMED_STATUS=""
for _ in $(seq 1 180); do
  STATE_JSON="$(curl -fsS "${GATEWAY_URL}/api/state")"
  RESUMED_STATUS="$(STATE_JSON="${STATE_JSON}" python3 - "${RESUMED_PR}" <<'PY'
import json, os, sys
pr_name = sys.argv[1]
state = json.loads(os.environ["STATE_JSON"])
for pr in state.get("pipelineRuns", []):
    if pr.get("name") == pr_name:
        print(pr.get("status", ""))
        sys.exit(0)
PY
)"
  if [[ "${RESUMED_STATUS}" == "Succeeded" ]]; then
    echo "    Mid-DAG Resumed PipelineRun ${RESUMED_PR} Succeeded!"
    break
  fi
  if [[ "${RESUMED_STATUS}" == "Failed" ]]; then
    echo "ERROR: Mid-DAG Resumed PipelineRun ${RESUMED_PR} Failed" >&2
    exit 1
  fi
  sleep 2
done
if [[ "${RESUMED_STATUS}" != "Succeeded" ]]; then
  echo "ERROR: Timed out waiting for ${RESUMED_PR} to succeed" >&2
  exit 1
fi

echo "==> All E2E checks passed!"
