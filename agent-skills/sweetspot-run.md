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

This dry-run materializes `artifacts/RUN_ID/canary_tasks.jsonl` from tiny controller-owned shards. Review/run those canaries through the normal SweetSpot worker path, collect worker summaries, then rerun with measured telemetry:

```bash
sweetspot run job.json \
  --canary-summary-jsonl canary_summaries.jsonl \
  --input-manifest-jsonl manifest.jsonl \
  --artifact-dir artifacts/RUN_ID
```

That produces calibrated `production_tasks.jsonl` for review. Production kickoff requires the calibrated artifact and explicit AWS targets:

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

Rerunning the same apply command resumes from `run_state.json` and refuses unsafe config drift.

## Status, repair, and cancel

- `sweetspot status RUN_ID --artifact-dir artifacts/RUN_ID` is safe and local by default. It only calls AWS when AWS flags are provided.
- `sweetspot repair RUN_ID ...` builds a run-scoped repair plan. Add `--apply` only after reviewing the repair JSON.
- `sweetspot cancel RUN_ID ...` is run-scoped. Broad regex cancellation belongs to the advanced `cancel-jobs` command.

## ARM/Graviton policy

Keep x86 as the safe default. Include `arm64` in `constraints.architectures` only when the image is multi-arch and you are prepared to canary ARM compatibility. Use separate x86/ARM queues and job definitions for mixed-architecture operation.

## Advanced commands

`enqueue-jsonl`, `submit-workers`, `finalize`, `repair-plan`, `cleanup-stale-messages`, `scout`, and `lane-manager` remain available for debugging/admin workflows, but they are not the primary interface for new agents.
