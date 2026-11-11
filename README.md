# Tekton on Agent Substrate (`tekton-actors`)

Run unmodified `tekton.dev/v1` `Pipeline` and `Task` workflows using **Agent Substrate `Actor` sandboxes (`v1.0.0`)** as the stateful unit of operation instead of ephemeral Kubernetes `Pod`s and `PersistentVolumeClaim`s (`PVC`s).

## Why Agent Substrate Actors as Tekton's Unit of Operation?

In stock Tekton Pipelines (`tektoncd/pipeline`), every `TaskRun` allocates a Kubernetes `Pod` and shares filesystem state across tasks by provisioning a `ReadWriteOnce` `PersistentVolumeClaim` (`PVC`) plus an `AffinityAssistant` placeholder Pod that pins the entire `PipelineRun` to a single node—even while waiting on human release approvals.

By binding a Tekton workspace to an Agent Substrate `Actor` (`workspaces[].actor`), `tekton-actors` and our forked Tekton controller ([`gke-demos/tekton-pipeline`](https://github.com/gke-demos/tekton-pipeline) on branch `actor-runtime`) replace the `Pod + PVC + AffinityAssistant` model with four Actor primitives:

1. **Sequential Toolchain Morphing (`Workspace Actor`)**: A trunk `Workspace Actor` (`ws-<pipelinerun>`) carries `/workspace` and `/tekton` (`durable_dir` volumes) across sequential tasks via `SuspendActor` → `UpdateActor(actor_template)` → `ResumeActor` (`SNAPSHOT_FIDELITY_VOLUMES`).
2. **Copy-on-Write DAG Fan-Out (`Milestone Snapshot Tag` + `Branch Actor`s)**: When multiple tasks fan out in parallel (`runAfter`), the controller suspends the trunk `Workspace Actor`, mints an immutable milestone `Snapshot Tag` (`ms-<pipelinerun>-<wave>`), and spawns isolated copy-on-write `Branch Actor`s (`br-<taskrun>`) seeded from `source_tag`. Branch outputs flow back strictly via Tekton `TaskRun` `results`, and fan-in tasks resume the clean trunk `Workspace Actor`.
3. **Zero-Slot Approval Pause (`ApprovalGate` `CustomRun`)**: While waiting on `tekton.ate.dev/v1alpha1` `ApprovalGate` sign-off, the trunk `Workspace Actor` sits `ACTOR_STATE_SUSPENDED` in object storage consuming **`0` active `WorkerPool` slots, `0` PVCs, and `0` `AffinityAssistant` pods**.
4. **Zero Ambient Secrets (`EgressPolicy` + `k8s-credential-provider`)**: No Git or OCI credentials are mounted into the Actor sandbox (`ZERO_AMBIENT_SECRETS`). Instead, the controller attaches a per-Actor `EgressPolicy` referencing `ate-secret://k8s.io/default/tekton-actors/git-oci-auth/token`, and `atenet-egress` injects `Authorization: Bearer <token>` on the wire outside the gVisor boundary.
5. **Time-Travel Failure Recovery (`Rescue Actor` + Mid-DAG Resume)**: When a step exits non-zero, the controller freezes an immutable `failure-<taskrun>` `Snapshot Tag`. Operators fork a live `Rescue Actor`, apply a hot-patch with warm `/workspace/.cache/go-build` intact, mint `fixed-<taskrun>`, and launch a resumed `PipelineRun` (`sourceTag: fixed-...`, `skipTasks: ["clone-and-build"]`) that skips completed upstream tasks and resumes mid-DAG.

## Dual-Mode Execution on the Same `tekton.dev/v1` Pipeline

The `release-pipeline` (`manifests/pipeline/release-pipeline.yaml`) runs in either mode simply by switching the `PipelineRun` `workspaces` binding:

- **Substrate Actor Mode** (`manifests/examples/pipelinerun-actor.yaml`):
  ```yaml
  workspaces:
    - name: source
      actor:
        atespace: tekton-actors
        workerSelector:
          workload: tekton-actors
        egressSecret: git-oci-auth
  ```
- **Stock Tekton `Pod + PVC` Mode** (`manifests/examples/pipelinerun-pod-pvc.yaml`):
  ```yaml
  workspaces:
    - name: source
      volumeClaimTemplate:
        spec:
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: 1Gi
    - name: creds
      secret:
        secretName: git-oci-auth
  ```

## Mission-Control Web UI

The Mission-Control Gateway (`cmd/gateway`, `pkg/gateway`) serves four path-based views with a top-right icon-only Projector Light Mode toggle (Moon/Sun):

- **`/` (Live Pipeline Studio)**: Trigger Actor-mode or Pod+PVC `PipelineRun`s, inspect the 6-node DAG (`clone-and-build` → `unit-test` / `vuln-scan` / `integration-smoke` → `release-gate` → `package-and-publish`), stream live Step Supervisor logs, and approve or reject zero-slot `ApprovalGate` pauses.
- **`/compare` (Actor vs. Pod+PVC Comparison)**: Launch both modes side-by-side on the exact same `release-pipeline` and compare PVC count, `AffinityAssistant` pods, active worker slots during approval pause, and secret posture (`ZERO_AMBIENT_SECRETS` vs. `AMBIENT_SECRET_MOUNTED`).
- **`/rescue` (Time-Travel Failure Rescue)**: Inspect `failure-*` Snapshot Tags, fork a live `Rescue Actor`, apply the 1-line `pricing.go` hot-patch, verify `go test ./...` against the warm Go build cache, and commit `fixed-*` to resume mid-DAG with `skipTasks: ["clone-and-build"]`.
- **`/about` (How It Works & Architecture)**: Interactive SVG topology diagrams covering trunk/branch Actor orchestration, Step Supervisor PID 1 coordination, zero-secret egress injection, and links to [GLOSSARY.md](GLOSSARY.md) and ADRs ([0001](docs/adr/0001-substrate-actor-execution-engine-for-tekton.md), [0002](docs/adr/0002-step-supervisor-and-egress-credential-injection.md)).

## Deploying & Verifying on GKE

Ensure your active `kubectl config current-context` points to a GKE cluster with Agent Substrate `v1.0.0` installed in `ate-system`, and set `KO_DOCKER_REPO` to your Artifact Registry repository:

```bash
export KO_DOCKER_REPO="us-central1-docker.pkg.dev/<your-gcp-project>/<your-repo>"

# 1. Deploy forked Tekton (../tekton-pipeline), WorkerPool, Git+OCI server, ApprovalGate controller, and Gateway
./hack/deploy.sh

# 2. Run the automated end-to-end verification suite (Actor run + Zero-Slot Approval + Pod+PVC baseline + Rescue mid-DAG resume)
./hack/verify-e2e.sh
```
