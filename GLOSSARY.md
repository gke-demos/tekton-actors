# Tekton Actors

Running `tekton.dev/v1` `PipelineRun` and `TaskRun` workflows with Agent Substrate `Actor` sandboxes as the stateful unit of operation instead of Kubernetes `Pod`s and `PersistentVolumeClaim`s.

## Language

### Execution & State

**Actor Workspace Binding**:
The `actor` source on `PipelineRun.spec.workspaces[]` in our forked Tekton API that routes a `PipelineRun`'s tasks onto Substrate `Actor`s instead of Kubernetes `Pod`s and `PVC`s, allowing the exact same `tekton.dev/v1` `Pipeline` and `Task` definitions to run in either mode.
_Avoid_: Custom task spec, actor annotation, runtime flag

**Workspace Actor**:
The primary trunk Substrate `Actor` backing a `PipelineRun` workspace (`durable_dir`), carrying `/workspace` across sequential tasks and across fan-in boundaries via `Suspend` → `UpdateActor(actor_template)` → `Resume` without Kubernetes `PVC`s or `AffinityAssistant` pods.
_Avoid_: Pipeline pod, shared runner, volume container

**Branch Actor**:
An independent Substrate `Actor` cloned from a `Snapshot Tag` (`CreateActor` with `source_tag`) to execute a parallel DAG branch or `matrix` leg on an isolated copy-on-write view of `/workspace`, forwarding outputs solely as Tekton `TaskRun` `results`.
_Avoid_: Forked pod, replica container, merged worker

**Rescue Actor**:
A child Substrate `Actor` forked from a failed `TaskRun`'s `Snapshot Tag` (`failure-<taskrun>`) with the broken `/workspace` and warm build cache intact so a human or agent can inspect, patch, mint a `fixed-<taskrun>` `Snapshot Tag`, and resume the pipeline mid-DAG without re-cloning or rebuilding.
_Avoid_: Debug pod, retry container, ephemeral shell

**Snapshot Tag**:
An immutable Substrate `Tag` (`CreateTag`) capturing a suspended `Actor`'s state at a DAG milestone (post-build fan-out, step failure, or hot-patch) so downstream `Branch Actor`s, `Rescue Actor`s, or resumed `PipelineRun`s can seed from it.
_Avoid_: Backup, volume snapshot, image commit

**Step Supervisor**:
The long-lived PID 1 daemon mounted into each `ActorTemplate` step container via read-only `ImageVolumeSource` that executes Tekton steps on demand over `atenet-router`, streams logs, reads `/tekton/results/*`, and keeps the sandbox alive so exiting steps do not trigger `ACTOR_STATE_CRASHED`.
_Avoid_: Entrypoint wrapper, init shim, sidecar agent

### Pipeline Control & Security

**Approval Gate**:
A `CustomRun` (`tekton.ate.dev/v1alpha1`, `kind: ApprovalGate`) DAG node that pauses release progression until explicitly approved—leaving the `Workspace Actor` `SUSPENDED` in object storage at zero `WorkerPool` slots while the stock Pod+PVC lane keeps its `AffinityAssistant` pod and `ReadWriteOnce` `PVC` pinned to a node.
_Avoid_: Sleep step, manual webhook pod, pause container
