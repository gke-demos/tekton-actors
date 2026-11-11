# Tekton Actors

End-to-end demo and reference architecture running `tekton.dev/v1` `PipelineRun` and `TaskRun` workflows on [Agent Substrate](https://github.com/agent-substrate/substrate) (`v1.0.0`) `Actor` sandboxes as the stateful unit of operation.

## Conventions

- Use `kubectl config current-context` defaults in scripts and generic GKE cluster/node descriptions in docs; never hardcode personal cluster, project, or node names.
- UI uses SVG icons (Lucide-style inline SVGs, zero emojis), path-based routing (`/`, `/compare`, `/rescue`, `/about`), and a zero-layout-shift icon-only Moon/Sun Projector Light Mode toggle at the top-right corner.
- Keep `/about` updated whenever architecture or pipeline mechanics change.

## Agent skills

### Issue tracker

Issues and specs are tracked in GitHub Issues on `gke-demos/tekton-actors`. See `docs/agents/issue-tracker.md`.

### Triage labels

Uses the default five canonical triage labels (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout (`GLOSSARY.md` + `docs/adr/` at the repo root). See `docs/agents/domain.md`.
