---
name: sweetspot-run
description: 'Use the simplified SweetSpot planner/controller workflow: JobSpec, plan, run, status, finish, explain/postmortem, cleanup, repair, and cancel. Prefer this over lower-level phase commands for new runs.'
---

# Skill: sweetspot-run

Thin guide for the preferred SweetSpot agent workflow. Use this skill for new manifest-based Spot workloads unless you are explicitly debugging lower-level operator phases.

## Preferred workflow

```bash
sweetspot plan job.json
sweetspot run job.json --artifact-dir artifacts/RUN_ID
sweetspot run job.json --artifact-dir artifacts/RUN_ID \
  --deployment deployment.json \
  --input-manifest-jsonl manifest.jsonl \
  --apply
sweetspot monitor RUN_ID --artifact-dir artifacts/RUN_ID --emit-command
sweetspot status RUN_ID --artifact-dir artifacts/RUN_ID --from-state
sweetspot finish RUN_ID --artifact-dir artifacts/RUN_ID --from-state --publish-ready
sweetspot explain RUN_ID --artifact-dir artifacts/RUN_ID --from-state --format text
sweetspot postmortem RUN_ID --artifact-dir artifacts/RUN_ID --from-state --format markdown
sweetspot cleanup RUN_ID --artifact-dir artifacts/RUN_ID --from-state --dry-run
sweetspot repair RUN_ID --tasks-jsonl artifacts/RUN_ID/production_tasks.jsonl \
  --task-status-jsonl artifacts/RUN_ID/finalizer/task_status.jsonl \
  --job-queue batch-queue
sweetspot cancel RUN_ID --job-queue batch-queue
```

Mutating commands are dry-run by default. Add `--apply` only after reviewing the emitted JSON plan/report.

## JobSpec contract

Provide intent and constraints, not AWS sizing knobs:

```json
{
  "schema": "sweetspot.job.v1",
  "run_id": "example-run",
  "image": "123456789012.dkr.ecr.us-west-2.amazonaws.com/worker@sha256:...",
  "command": ["python", "/app/process.py"],
  "input_manifest": "s3://bucket/inputs/items.jsonl",
  "output_prefix": "s3://bucket/runs/example-run",
  "constraints": {
    "max_cost_usd": 50,
    "deadline_hours": 6,
    "completion_fraction": 1.0,
    "architectures": ["x86_64"]
  },
  "validation": {"output_check": "done_marker"}
}
```

Do not put worker count, vCPUs, memory, shard size, messages per worker, visibility timeout, or retry policy in the primary JobSpec. Those are advanced overrides/operator controls.

## Adaptive canary flow

If you have a local JSONL copy of the logical input manifest but no measured canaries yet:

```bash
sweetspot run job.json \
  --input-manifest-jsonl manifest.jsonl \
  --artifact-dir artifacts/RUN_ID
```

This dry-run materializes `artifacts/RUN_ID/canary_tasks.jsonl` from tiny controller-owned shards. The artifact includes the built-in 1/2/4 vCPU resource lattice and, when `arm64` is allowed in the JobSpec, paired x86/ARM candidates. If `deployment.json` declares isolated per-candidate `canary_routes` (for example `x86_64-1vcpu-2048mib`), the controller can launch those canaries safely:

```bash
sweetspot run job.json \
  --input-manifest-jsonl manifest.jsonl \
  --artifact-dir artifacts/RUN_ID \
  --deployment deployment.json \
  --apply
```

Canary apply fails closed when any candidate route is missing or shares the production queue. After canary workers finish, the controller can collect each task's S3 summary and write `canary_summaries.jsonl`, `production_plan.json`, and (when calibration is ready) `production_tasks.jsonl`:

```bash
sweetspot run job.json \
  --input-manifest-jsonl manifest.jsonl \
  --artifact-dir artifacts/RUN_ID \
  --deployment deployment.json \
  --apply \
  --collect-canary-summaries
```

You can also collect worker summaries externally, then rerun with measured telemetry:

```bash
sweetspot run job.json \
  --canary-summary-jsonl canary_summaries.jsonl \
  --input-manifest-jsonl manifest.jsonl \
  --artifact-dir artifacts/RUN_ID
```

If the measured canaries are still too tiny to calibrate the target replay-safe duration, the next dry-run writes a larger canary generation instead of production tasks. Once shard sizing and resource telemetry are calibrated, the planner selects the measured architecture/resource shape and produces `production_tasks.jsonl` for review. Production kickoff should use a deployment registry so the Plan-selected region/architecture resolves to a digest-pinned image, revision-pinned job definition, and verified S3 manifest identity:

```bash
sweetspot run job.json \
  --canary-summary-jsonl canary_summaries.jsonl \
  --input-manifest-jsonl manifest.jsonl \
  --artifact-dir artifacts/RUN_ID \
  --deployment deployment.json \
  --apply
```

Legacy `--queue-url`/`--batch-job-queue`/`--job-definition` production targets remain compatibility fallbacks for operator debugging, but they bypass deployment-registry image/job-definition binding and should not be the primary agent path. Rerunning the same apply command resumes from `run_state.json` and refuses unsafe config drift. With `--deployment`, the local `--input-manifest-jsonl` must verify against the S3 `input_manifest` by size plus SHA256 metadata/checksum or single-part ETag before any mutation.

For production, prefer a run-owned queue: pass `--dedicated-run-queue --create-run-queue` to let the controller create/verify a tagged per-run SQS queue and bind production workers to that run-owned URL, so SQS depth is a valid run-specific backlog signal. Preflight operator IAM with `sweetspot admin doctor --check-run-queue-create --run-queue-name NAME` for `sqs:CreateQueue`, queue tagging, redrive-policy updates, and any DLQ redrive-allow-policy changes. If the operator cannot create queues, stop and use a pre-provisioned empty run-scoped queue; document the fallback explicitly, set the visibility timeout from the Plan, and do not silently reuse a canary/shared queue with stale messages.

Dedicated-queue top-up submits are persisted as in-flight before each Batch mutation; shared queues use conservative observation only. For interactive agent sessions, use `--kickoff-only` for production launch after the initial Plan-sized worker wave, then hand monitoring to `sweetspot monitor RUN_ID --emit-command` / `sweetspot status RUN_ID --from-state` plus a scheduled/CI checkpoint. Do not keep the foreground agent blocked in a long poll unless actively diagnosing. For unattended operators, `--reconcile-until-drained --reconcile-rounds N --reconcile-interval-seconds S` turns bounded reconciliation into a watch loop that stops early only after run-scoped backlog and active workers both drain; rerun with a larger round limit to extend an already-completed watch. After workers have had time to finish, use `sweetspot finish RUN_ID --from-state --publish-ready` to run the drain checks, finalizer, manifest upload, and READY publish from persisted `run_state.json`; use `sweetspot explain RUN_ID --from-state` / `sweetspot postmortem RUN_ID --from-state` for closeout reporting.

## Status, closeout, repair, and cancel

- `sweetspot monitor RUN_ID --artifact-dir artifacts/RUN_ID --emit-command` is the preferred way to generate non-blocking scheduler/CI checkpoint commands.
- `sweetspot status RUN_ID --artifact-dir artifacts/RUN_ID --from-state` reconstructs queue, DLQ, Batch queue, job-name prefix, output prefix, and task artifacts from `run_state.json`; it calls AWS only for the recovered queue/Batch/S3 checks.
- `sweetspot finalize RUN_ID --artifact-dir artifacts/RUN_ID --from-state` is the explicit state-driven finalizer path when you do not want the full finish checklist.
- `sweetspot finish RUN_ID --artifact-dir artifacts/RUN_ID --from-state --publish-ready` runs the production drain checks before finalization/READY and writes `finish_report.json`.
- `sweetspot explain RUN_ID --artifact-dir artifacts/RUN_ID --from-state` explains the reconstructed lifecycle state and next actions without mutating AWS.
- `sweetspot postmortem RUN_ID --artifact-dir artifacts/RUN_ID --from-state` writes a JSON or Markdown closeout report from state/finalizer/finish artifacts.
- `sweetspot cleanup RUN_ID --artifact-dir artifacts/RUN_ID --from-state --dry-run` writes `cleanup_report.json`; it is conservative/report-only and leaves destructive SQS/DLQ/S3/Batch capacity mutations as explicit admin/operator actions.
- `sweetspot repair RUN_ID ...` builds a run-scoped repair plan. Add `--apply` only after reviewing the repair JSON.
- `sweetspot cancel RUN_ID ...` is run-scoped. Broad regex cancellation belongs to the advanced `cancel-jobs` command.

## ARM/Graviton policy

Keep x86 as the safe default, but do not assume larger instances are cheaper. Before production, run or inspect small-lane canaries/scout results for 1 vCPU shapes such as `c7a.medium`, `c7g.medium`, and `c6g.medium`. Include `arm64` in `constraints.architectures` only when the image is multi-arch and you are prepared to canary ARM compatibility. ARM is selected only from successful measured canaries and is rejected on validation/runtime failure or materially worse measured cost per useful unit. Use separate x86/ARM queues and job definitions for mixed-architecture operation. For 2 GiB medium instances, reserve less than 2048 MiB in Batch (start around 1536 MiB) so ECS/Batch can schedule the worker. Do not downshift managed Batch users to `t3*`/`t4g*` small or micro lanes: AWS Batch rejects these burstable instance types at compute-environment creation, before an OOM/failure canary can run. Legacy `small` types such as `m1.small` may run but are diagnostic-only unless canaries prove they beat modern medium lanes.

## Advanced commands

`enqueue-jsonl`, `submit-workers`, `finalize`, `repair-plan`, `cleanup-stale-messages`, `scout`, and `lane-manager` remain available for debugging/admin workflows, but they are not the primary interface for new agents. Prefer the explicit aliases such as `sweetspot admin enqueue-jsonl`, `sweetspot admin finalize`, and `sweetspot admin scout` when intentionally leaving the primary controller workflow.
