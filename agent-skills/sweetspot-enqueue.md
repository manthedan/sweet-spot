---
name: sweetspot-enqueue
description: 'Advanced/admin workflow for manually creating, validating, canarying, estimating, and enqueueing SweetSpot task JSONL workloads to SQS. For new runs, prefer sweetspot-run.'
---

# Skill: sweetspot-enqueue

Advanced/admin guide for creating, validating, and enqueuing SweetSpot tasks to SQS.

## When to use

Invoke this skill for advanced/operator workflows. For new manifest-based runs, prefer `sweetspot-run` and let `sweetspot plan`/`sweetspot run` materialize canary and production tasks.

Use this skill when an agent explicitly needs to:
- Create a tasks.jsonl file for a batch workload
- Validate task payloads before submission
- Enqueue tasks to an SQS queue
- Derive a canary subset before a large run
- Estimate runtime/cost from telemetry samples

## Cost-first task design

Before preparing a large Spot run, design tasks to be **small, idempotent, and cheap to replay**:

1. Split large inputs into bounded shards with stable `task_id`s and deterministic output/done-marker paths.
2. Keep per-task runtime comfortably below `timeout_seconds`; if the estimate is close to the timeout, split smaller or add checkpointing.
3. Run a canary subset before the full enqueue.
4. Have the worker command write `SWEETSPOT_METRICS_PATH` with `completed_units`, `useful_compute_seconds`, `input_bytes`, and `output_bytes` so `sweetspot estimate-runtime` and `sweetspot scout` can rank pools by useful work, not just hourly price.
5. After the canary, run `sweetspot scout --preset mixed --observed-summaries ...` to surface potential ARM/Graviton savings. Use ARM only after an ARM canary proves the workload and image are compatible.

## Task schema

Every task is a JSON object on its own line in a `.jsonl` file:

```json
{
  "schema": "sweetspot.task.v1",
  "run_id": "my-run-001",
  "task_id": "task-000001",
  "command": ["python", "/app/my_worker.py"],
  "timeout_seconds": 3600,
  "output_s3": "s3://my-bucket/runs/my-run-001/shards/task-000001.json",
  "summary_s3": "s3://my-bucket/runs/my-run-001/summaries/task-000001.summary.json",
  "done_s3": "s3://my-bucket/runs/my-run-001/done/task-000001.done.json"
}
```

### Required fields
- `schema`: Must be `"sweetspot.task.v1"`
- `run_id`: Non-empty string, identifies the run
- `task_id`: Non-empty string, unique within the run
- `command`: Non-empty list of strings (the worker command)

### Optional fields
- `timeout_seconds`: Positive number, capped at 39600s (11h). Default: 39600
- `output_s3`: S3 URI where worker output is uploaded
- `summary_s3`: S3 URI where worker summary is uploaded
- `done_s3`: S3 URI for the done marker. Prefer setting it explicitly as `done/<task_id>.done.json` for clean run layouts.
- `env`: Object mapping string keys to scalar values (not `SWEETSPOT_*`, `AWS_*`, `ECS_*`)
- `payload`: Arbitrary JSON passed through to the worker via `SWEETSPOT_TASK_JSON`
- `job_type`: Optional metadata string for future workload profiles

### Validation rules
- `run_id` and `task_id` must match `^[A-Za-z0-9][A-Za-z0-9_.:@/+\-]{0,255}$`
- All `s3://` URIs in the task (including command arguments) must be under allowed prefixes when configured
- If `done_s3` is omitted, SweetSpot derives it exactly as `output_s3.replace("/shards/", "/done/") + ".done.json"`; for output keys with extensions, this yields names like `task-000001.json.done.json`. Set `done_s3` explicitly when you want the cleaner `task-000001.done.json` convention.
- Task-provided env keys must not start with `SWEETSPOT_`, `AWS_`, or `ECS_`

## S3 path conventions

SweetSpot uses a consistent S3 layout per run:

```
s3://bucket/runs/<run_id>/
  shards/<task_id>.json          # task output
  summaries/<task_id>.summary.json  # task summary with telemetry
  done/<task_id>.done.json       # done marker (source of truth)
```

## Generating tasks.jsonl

When creating a tasks.jsonl for a workload, follow this pattern:

```python
import json

tasks = []
for i in range(1, 1001):
    task_id = f"task-{i:06d}"
    tasks.append({
        "schema": "sweetspot.task.v1",
        "run_id": "my-run-001",
        "task_id": task_id,
        "command": ["python", "/app/my_worker.py"],
        "timeout_seconds": 3600,
        "output_s3": f"s3://my-bucket/runs/my-run-001/shards/{task_id}.json",
        "summary_s3": f"s3://my-bucket/runs/my-run-001/summaries/{task_id}.summary.json",
        "done_s3": f"s3://my-bucket/runs/my-run-001/done/{task_id}.done.json",
        "payload": {"start": i * 1000, "count": 1000},
    })

with open("tasks.jsonl", "w") as f:
    for t in tasks:
        f.write(json.dumps(t, sort_keys=True) + "\n")
```

## CLI commands

### Validate and enqueue
```bash
# Dry-run: validates tasks and writes artifacts without sending to SQS
sweetspot enqueue-jsonl \
  --tasks-jsonl tasks.jsonl \
  --run-id my-run-001 \
  --artifact-dir artifacts/my-run-001 \
  --allowed-s3-prefix s3://my-bucket/runs/my-run-001

# Submit: validates, writes artifacts, and sends to SQS
sweetspot enqueue-jsonl \
  --tasks-jsonl tasks.jsonl \
  --queue-url https://sqs.us-west-2.amazonaws.com/123456789012/my-work-queue \
  --run-id my-run-001 \
  --artifact-dir artifacts/my-run-001 \
  --allowed-s3-prefix s3://my-bucket/runs/my-run-001 \
  --submit
```

### Derive a canary subset
```bash
sweetspot derive-canary \
  --tasks-jsonl artifacts/my-run-001/tasks.jsonl \
  --out-dir artifacts/my-run-001/canary \
  --task-count 4 \
  --include-dlq-probe \
  --dlq-probe-prefix s3://my-bucket/runs/my-run-001/dlq-probes
```

### Estimate runtime from canary telemetry
```bash
sweetspot estimate-runtime \
  --sample-jsonl artifacts/my-run-001/canary/summaries.jsonl \
  --target-units 1000000 \
  --active-workers 64 \
  --vcpus-per-worker 2 \
  --price-per-vcpu-hour 0.034 \
  --spot \
  --task-timeout-seconds 3600
```

### Atomic enqueue + submit
```bash
sweetspot enqueue-and-submit \
  --tasks-jsonl tasks.jsonl \
  --queue-url https://sqs.us-west-2.amazonaws.com/123456789012/my-work-queue \
  --batch-job-queue my-batch-spot-queue \
  --job-definition my-worker-jobdef:1 \
  --job-name-prefix my-run-001-worker \
  --messages-per-worker 4 \
  --max-workers 64 \
  --allowed-s3-prefix s3://my-bucket/runs/my-run-001 \
  --wait-for-visible-seconds 30 \
  --submit
```

## Output interpretation

### enqueue-jsonl output
```json
{
  "schema": "sweetspot.enqueue_summary.v1",
  "task_count": 1000,
  "sent": 1000,
  "submitted": true,
  "tasks_jsonl": "artifacts/my-run-001/tasks.jsonl"
}
```

### derive-canary output
```json
{
  "schema": "sweetspot.derive_canary_summary.v1",
  "task_count": 4,
  "selected_indices": [0, 333, 666, 999],
  "canary_tasks_jsonl": "artifacts/my-run-001/canary/canary_tasks.jsonl"
}
```

### estimate-runtime output
```json
{
  "schema": "sweetspot.runtime_estimate.v1",
  "median_units_per_second_per_worker": 277.8,
  "predicted_wall_seconds": 56.0,
  "predicted_seconds_per_task": 3600.0,
  "warnings": []
}
```

Check the `warnings` array. If it contains messages about timeout safety or Spot task length, the tasks need to be split smaller before the full enqueue. The same canary summaries should be passed to `sweetspot scout --observed-summaries` so pool choice accounts for real throughput and retry/discard overhead.

## Common pitfalls

1. **Missing S3 paths**: If `output_s3` is set, the worker command must write to `SWEETSPOT_OUTPUT_PATH` or the task will be treated as failed.
2. **Duplicate task_ids**: The CLI rejects duplicate task_ids during enqueue and finalize.
3. **S3 prefix mismatch**: When `--allowed-s3-prefix` is set, every `s3://` URI must be under one of those prefixes. Exact-key equality to a non-root prefix is rejected.
4. **Reserved env keys**: Task env keys starting with `SWEETSPOT_`, `AWS_`, or `ECS_` are rejected.
5. **Timeout too long**: Timeouts above 39600s (11h) are rejected to stay below SQS's 12-hour visibility ceiling.
