#!/usr/bin/env bash
set -euo pipefail

export PATH="/usr/bin/go/bin:${HOME}/go/bin:${PATH}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEKTON_PIPELINE_DIR="${TEKTON_PIPELINE_DIR:-${REPO_ROOT}/../tekton-pipeline}"
NAMESPACE="${NAMESPACE:-tekton-actors}"
ATESPACE="${ATESPACE:-tekton-actors}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-$(kubectl config current-context)}"

if [[ -z "${KO_DOCKER_REPO:-}" ]]; then
  echo "ERROR: KO_DOCKER_REPO must be set to your container registry path (e.g. us-central1-docker.pkg.dev/<project>/<repo>)" >&2
  exit 1
fi

if [[ ! -d "${TEKTON_PIPELINE_DIR}/config" ]]; then
  echo "ERROR: forked tekton-pipeline directory not found at ${TEKTON_PIPELINE_DIR}" >&2
  exit 1
fi

echo "==> Deploying Tekton on Agent Substrate to context: ${KUBECTL_CONTEXT}"
echo "==> Using KO_DOCKER_REPO: ${KO_DOCKER_REPO}"

# 1. Build and push digest-pinned Step Supervisor OCI image (mounted into every ActorTemplate).
echo "==> [1/6] Building digest-pinned Step Supervisor OCI image..."
STEP_SUPERVISOR_IMAGE="$(cd "${REPO_ROOT}" && ko build ./cmd/step-supervisor --sbom=none)"
echo "    STEP_SUPERVISOR_IMAGE=${STEP_SUPERVISOR_IMAGE}"

# 2. Deploy forked Tekton Pipelines (actor-runtime) and inject ATE_SUPERVISOR_IMAGE.
echo "==> [2/6] Deploying forked Tekton Pipelines from ${TEKTON_PIPELINE_DIR}/config..."
(
  cd "${TEKTON_PIPELINE_DIR}"
  kubectl --context="${KUBECTL_CONTEXT}" apply -f config/100-namespace/ -f config/300-crds/
  ko resolve --sbom=none -f config/ | kubectl --context="${KUBECTL_CONTEXT}" apply -f -
)
kubectl --context="${KUBECTL_CONTEXT}" -n tekton-pipelines set env deployment/tekton-pipelines-controller \
  "ATE_SUPERVISOR_IMAGE=${STEP_SUPERVISOR_IMAGE}" \
  "ATE_SYSTEM_NAMESPACE=ate-system"
kubectl --context="${KUBECTL_CONTEXT}" -n tekton-pipelines rollout status deployment/tekton-pipelines-controller --timeout=300s
kubectl --context="${KUBECTL_CONTEXT}" -n tekton-pipelines rollout status deployment/tekton-pipelines-webhook --timeout=300s

# 3. Create namespace tekton-actors with ate.dev/api-access="true" and WorkerPool.
echo "==> [3/6] Provisioning namespace ${NAMESPACE} and Agent Substrate WorkerPool..."
ATE_WORKER_IMAGE="${ATE_WORKER_IMAGE:-}"
if [[ -z "${ATE_WORKER_IMAGE}" ]]; then
  ATE_WORKER_IMAGE="$(kubectl --context="${KUBECTL_CONTEXT}" get workerpools.ate.dev -A -o jsonpath='{.items[0].spec.workerImage}' 2>/dev/null || true)"
fi
if [[ -z "${ATE_WORKER_IMAGE}" ]]; then
  ATE_WORKER_IMAGE="ghcr.io/agent-substrate/substrate/ateworker-gvisor:v1.0.0"
fi
echo "    ATE_WORKER_IMAGE=${ATE_WORKER_IMAGE}"

kubectl --context="${KUBECTL_CONTEXT}" apply -f "${REPO_ROOT}/manifests/workerpool.yaml"
kubectl --context="${KUBECTL_CONTEXT}" -n "${NAMESPACE}" patch workerpool.ate.dev/tekton-actors-pool \
  --type=merge -p "{\"spec\":{\"workerImage\":\"${ATE_WORKER_IMAGE}\"}}"

# 4. Provision Git + OCI auth Secret and k8s-credential-provider namespace policy.
echo "==> [4/6] Configuring k8s-credential-provider and deploying Git + OCI server..."
EXISTING_TOKEN="$(kubectl --context="${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get secret git-oci-auth -o jsonpath='{.data.token}' 2>/dev/null | base64 -d || true)"
if [[ -z "${EXISTING_TOKEN}" || "${EXISTING_TOKEN}" == "REPLACE_WITH_RANDOM_CLUSTER_TOKEN" ]]; then
  RANDOM_TOKEN="$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')"
else
  RANDOM_TOKEN="${EXISTING_TOKEN}"
fi

(
  cd "${REPO_ROOT}"
  ko resolve --sbom=none -f manifests/git-oci-server.yaml | kubectl --context="${KUBECTL_CONTEXT}" apply -f -
)
kubectl --context="${KUBECTL_CONTEXT}" -n "${NAMESPACE}" create secret generic git-oci-auth \
  --from-literal="token=${RANDOM_TOKEN}" \
  --dry-run=client -o yaml | kubectl --context="${KUBECTL_CONTEXT}" apply -f -

kubectl --context="${KUBECTL_CONTEXT}" -n ate-system rollout restart deployment/k8s-credential-provider
kubectl --context="${KUBECTL_CONTEXT}" -n ate-system rollout status deployment/k8s-credential-provider --timeout=180s
kubectl --context="${KUBECTL_CONTEXT}" -n "${NAMESPACE}" rollout restart deployment/git-oci
kubectl --context="${KUBECTL_CONTEXT}" -n "${NAMESPACE}" rollout status deployment/git-oci --timeout=180s

# 5. Deploy ApprovalGate controller, Tekton Tasks/Pipeline, and Mission-Control Gateway.
echo "==> [5/6] Deploying ApprovalGate controller, Tekton Tasks/Pipeline, and Mission-Control Gateway..."
(
  cd "${REPO_ROOT}"
  ko resolve --sbom=none -f manifests/approvalgate-controller.yaml | kubectl --context="${KUBECTL_CONTEXT}" apply -f -
  kubectl --context="${KUBECTL_CONTEXT}" -n "${NAMESPACE}" apply -f manifests/pipeline/
  ko resolve --sbom=none -f manifests/gateway.yaml | kubectl --context="${KUBECTL_CONTEXT}" apply -f -
)
kubectl --context="${KUBECTL_CONTEXT}" -n "${NAMESPACE}" set env deployment/tekton-actors-gateway \
  "ATE_SUPERVISOR_IMAGE=${STEP_SUPERVISOR_IMAGE}"
kubectl --context="${KUBECTL_CONTEXT}" -n "${NAMESPACE}" rollout status deployment/approvalgate-controller --timeout=180s
kubectl --context="${KUBECTL_CONTEXT}" -n "${NAMESPACE}" rollout status deployment/tekton-actors-gateway --timeout=180s

# 6. Wait for WorkerPool warm pods to become Ready.
echo "==> [6/6] Waiting for Agent Substrate WorkerPool warm pods..."
for i in $(seq 1 60); do
  READY="$(kubectl --context="${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get workerpool.ate.dev/tekton-actors-pool -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")"
  DESIRED="$(kubectl --context="${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get workerpool.ate.dev/tekton-actors-pool -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "4")"
  if [[ "${READY:-0}" -ge 1 && "${READY:-0}" == "${DESIRED:-4}" ]]; then
    echo "    WorkerPool tekton-actors-pool is Ready (${READY}/${DESIRED} warm worker pods)"
    break
  fi
  echo "    Waiting for WorkerPool tekton-actors-pool (${READY:-0}/${DESIRED:-4} ready)..."
  sleep 3
done

kubectl --context="${KUBECTL_CONTEXT}" -n "${NAMESPACE}" get pods,workerpools.ate.dev,pipelines.tekton.dev,tasks.tekton.dev
echo "==> Deployment complete!"
