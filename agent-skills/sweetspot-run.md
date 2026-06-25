---
name: sweetspot-run
description: Use the simplified SweetSpot planner/controller workflow: JobSpec, plan, run, status, repair, and cancel. Prefer this over lower-level phase commands for new runs.
---

# Skill: sweetspot-run

Thin guide for the preferred SweetSpot agent workflow. Use this skill for new manifest-based Spot workloads unless you are explicitly debugging lower-level operator phases.

## Preferred workflow

```bash
sweetspot plan job.json
sweetspot run job.json --artifact-dir artifacts/RUN_ID
sweetspot run job.json --artifact-dir artifacts/RUN_ID \
  --queue-url https://sqs... \
  --batch-job-queue batch-queue \
  --job-definition worker-jobdef \
  --apply
sweetspot status RUN_ID --artifact-dir artifacts/RUN_ID
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

Canary apply fails closed when any candidate route is missing or shares the production queue. Collect worker summaries, then rerun with measured telemetry:

```bash
sweetspot run job.json \
  --canary-summary-jsonl canary_summaries.jsonl \
  --input-manifest-jsonl manifest.jsonl \
  --artifact-dir artifacts/RUN_ID
```

If the measured canaries are still too tiny to calibrate the target replay-safe duration, the next dry-run writes a larger canary generation instead of production tasks. Once shard sizing and resource telemetry are calibrated, the planner selects the measured architecture/resource shape and produces `production_tasks.jsonl` for review. Production kickoff requires the calibrated artifact and explicit AWS targets:

```bash
sweetspot run job.json \
  --canary-summary-jsonl canary_summaries.jsonl \
  --input-manifest-jsonl manifest.jsonl \
  --artifact-dir artifacts/RUN_ID \
  --queue-url https://sqs... \
  --batch-job-queue batch-queue \
  --job-definition worker-jobdef \
  --apply
```

Rerunning the same apply command resumes from `run_state.json` and refuses unsafe config drift. With `--deployment`, the local `--input-manifest-jsonl` must verify against the S3 `input_manifest` by size plus SHA256 metadata/checksum or single-part ETag before any mutation. Reconciliation is bounded; pass `--dedicated-run-queue` only for a fresh queue dedicated to this run, where SQS depth is a valid run-specific backlog signal. Dedicated-queue top-up submits are persisted as in-flight before each Batch mutation; shared queues use conservative observation only.

## Status, repair, and cancel

- `sweetspot status RUN_ID --artifact-dir artifacts/RUN_ID` is safe and local by default. It only calls AWS when AWS flags are provided.
- `sweetspot repair RUN_ID ...` builds a run-scoped repair plan. Add `--apply` only after reviewing the repair JSON.
- `sweetspot cancel RUN_ID ...` is run-scoped. Broad regex cancellation belongs to the advanced `cancel-jobs` command.

## ARM/Graviton policy

Keep x86 as the safe default. Include `arm64` in `constraints.architectures` only when the image is multi-arch and you are prepared to canary ARM compatibility. ARM is selected only from successful measured canaries and is rejected on validation/runtime failure or materially worse measured vCPU-seconds per useful unit. Use separate x86/ARM queues and job definitions for mixed-architecture operation.

## Advanced commands

`enqueue-jsonl`, `submit-workers`, `finalize`, `repair-plan`, `cleanup-stale-messages`, `scout`, and `lane-manager` remain available for debugging/admin workflows, but they are not the primary interface for new agents.
