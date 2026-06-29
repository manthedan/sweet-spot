# SweetSpot agent instructions

SweetSpot is intended to be agent-first: a coding agent should be able to open this repository, read the local agent guidance, and operate the project through safe, reviewable commands.

## Start here

For normal SweetSpot use, read and follow:

- `agent-skills/sweetspot-run.md` — preferred planner/controller workflow for new runs.

Use these only when explicitly doing advanced operator/debugging work:

- `agent-skills/sweetspot-reference.md` — full CLI reference.
- `agent-skills/sweetspot-ops.md` — AWS/SQS/S3/Batch diagnostics.
- `agent-skills/sweetspot-enqueue.md` — manual task enqueueing.
- `agent-skills/sweetspot-workers.md` — manual worker submission/supervision.
- `agent-skills/sweetspot-finalize.md` — manual closeout/finalization.
- `agent-skills/sweetspot-scout.md` — Spot lane/cost investigation.

Prefer the high-level controller workflow over lower-level admin commands:

```bash
sweetspot init
sweetspot doctor project --format json
sweetspot bootstrap plan --format json
sweetspot plan .sweetspot/job.json
sweetspot run .sweetspot/job.json --artifact-dir artifacts/RUN_ID
sweetspot monitor RUN_ID --artifact-dir artifacts/RUN_ID --emit-command
sweetspot status RUN_ID --artifact-dir artifacts/RUN_ID --from-state
sweetspot finish RUN_ID --artifact-dir artifacts/RUN_ID --from-state --publish-ready
sweetspot postmortem RUN_ID --artifact-dir artifacts/RUN_ID --from-state --format markdown
sweetspot cleanup RUN_ID --artifact-dir artifacts/RUN_ID --from-state --write-plan
```

## Safety rules

- Treat all cloud mutations as explicit approval points. Review JSON plans/reports before adding `--apply`, `--publish-ready`, or any destructive operator flag.
- Keep first success local and non-mutating: `init`, `doctor project`, `bootstrap plan`, `plan`, and dry-run `run` are the expected first steps.
- Do not foreground long polling in an interactive agent unless actively diagnosing. Prefer `--kickoff-only`, then schedule or emit checkpoint commands with `sweetspot monitor --emit-command` and `sweetspot status --from-state`.
- Bind lifecycle commands to persisted state with `--from-state` whenever possible. Do not override recorded output prefixes, task manifests, queues, or deployment bindings unless a human has reviewed the drift report.
- SweetSpot task commands are trusted code. Do not enqueue untrusted user-submitted commands.
- Generated run outputs under `artifacts/`, `.sweetspot/`, and workload data directories are usually local state, not source code. Do not commit them unless the user explicitly asks.

## Repository work

For code changes, run the relevant local checks before proposing or pushing:

```bash
ruff check .
mypy sweetspot
python -m unittest discover
```

If a change affects first-run UX, setup, lifecycle state, or cloud safety, also update the matching agent skill and README/docs references so future agents see the new workflow.
