---
name: sweetspot-workers
description: 'Advanced/admin workflow for manual SweetSpot AWS Batch worker sizing, submission, supervision, active-worker checks, and log inspection. For new runs, prefer sweetspot-run.'
---

# Skill: sweetspot-workers

Advanced/admin guide for submitting, supervising, and monitoring SweetSpot Batch workers.

## When to use

Invoke this skill for advanced/operator workflows. For new manifest-based runs, prefer `sweetspot-run`; the controller should choose shard size, resource shape, and initial worker submission from canary evidence.

Use this skill when an agent explicitly needs to:
- Size and submit AWS Batch workers for an SQS queue
- Run a multi-loop supervisor to maintain a target worker pool
- Monitor active jobs, logs, and job status
- Estimate worker count from queue depth

## Worker lifecycle

```
1. Tasks are enqueued to SQS (see sweetspot-enqueue skill)
2. Workers are submitted to AWS Batch with submit-workers or supervise-workers
3. Each worker:
   a. Polls SQS for messages
   b. Checks S3 done marker (skip if already done)
   c. Runs the task command
   d. Uploads output/summary to attempt-scoped S3 paths
   e. Writes canonical done marker with conditional PutObject
   f. Deletes SQS message only after done marker is committed
4. Finalization checks done markers (see sweetspot-finalize skill)
```

## CLI commands

### Config file support

All worker-submission commands support `--config <path.json>` (or `SWEETSPOT_CONFIG` env var) to pre-populate common flags like `--queue-url`, `--batch-job-queue`, `--job-definition`, `--allowed-s3-prefix`, etc. Explicit CLI flags override config values. Example config:

```json
{
  "defaults": {"profile": "prod", "region": "us-west-2"},
  "submit-workers": {"queue_url": "https://sqs...", "batch_job_queue": "jq", "job_definition": "jd:1"}
}
```

### Submit workers (one-shot)

Dry-run (default) shows sizing without submitting:
```bash
sweetspot submit-workers \
  --sqs-queue-url https://sqs.us-west-2.amazonaws.com/123456789012/my-work-queue \
  --batch-job-queue my-batch-spot-queue \
  --job-definition my-worker-jobdef:1 \
  --job-name-prefix my-run-001-worker \
  --messages-per-worker 4 \
  --max-workers 64 \
  --subtract-active \
  --allowed-s3-prefix s3://my-bucket/runs/my-run-001
```

Add `--submit` to actually submit jobs. Always review the dry-run output first.

Key arguments:
- `--messages-per-worker`: How many SQS messages each worker processes (default 1)
- `--max-workers`: Cap on workers to submit (default 64)
- `--min-workers`: Floor when backlog exists (default 0)
- `--subtract-active`: Subtract currently active matching jobs from the desired count
- `--include-not-visible`: Count in-flight messages in backlog
- `--vcpus` / `--memory`: Override Batch container resources
- `--retry-attempts`: Batch retry strategy attempts
- `--env KEY=VALUE`: Extra environment variables for workers (repeatable)

### Supervise workers (multi-loop)

Keeps a bounded pool topped across multiple polling loops:
```bash
sweetspot supervise-workers \
  --sqs-queue-url https://sqs.us-west-2.amazonaws.com/123456789012/my-work-queue \
  --batch-job-queue my-batch-spot-queue \
  --job-definition my-worker-jobdef:1 \
  --job-name-prefix my-run-001-worker \
  --target-active-workers 64 \
  --max-active-workers 64 \
  --max-submit-per-loop 16 \
  --loops 10 \
  --interval-seconds 60 \
  --stop-on-dlq \
  --dlq-url https://sqs.us-west-2.amazonaws.com/123456789012/my-dlq \
  --allowed-s3-prefix s3://my-bucket/runs/my-run-001 \
  --submit
```

Key arguments:
- `--target-active-workers`: Desired steady-state worker count (default 64)
- `--max-active-workers`: Hard cap on concurrent workers (default 64)
- `--max-submit-per-loop`: Max new workers per loop (default 64)
- `--loops`: Number of supervision loops (default 1)
- `--interval-seconds`: Sleep between loops (default 60)
- `--keep-full-pool`: Maintain target workers even when backlog is low
- `--stop-on-dlq`: Stop submitting if DLQ has any messages
- `--fail-on-stop`: Exit code 2 when stopping due to DLQ
- `--include-terminal-counts`: Include SUCCEEDED/FAILED counts per loop

### Watch a job to completion

```bash
sweetspot watch-job --job-id <id> --max-seconds 3600
```

Polls every 30 seconds. Exit code 0 = SUCCEEDED, 2 = FAILED, 3 = timeout.

### List jobs

```bash
sweetspot jobs --job-queue my-batch-spot-queue \
  --status RUNNING --name-regex 'my-run-001'
```

### Describe a job

```bash
sweetspot describe-job --job-id <id>
```

### View logs

```bash
# Last 50 events from a job's logs
sweetspot logs --job-id <id> --last 50

# Filter for errors or progress
sweetspot logs --job-id <id> --filter-regex 'progress|ERROR' --max-events 200
```

## Output interpretation

### submit-workers / supervise-workers summary
```json
{
  "schema": "sweetspot.worker_submitter_summary.v1",
  "queue_depth": {"visible": 500, "not_visible": 20, "delayed": 0},
  "messages_per_worker": 4,
  "raw_desired_workers": 125,
  "to_submit": 64,
  "submitted_count": 64,
  "submitted": [{"jobName": "...", "jobId": "...", "jobArn": "..."}]
}
```

Key fields to check:
- `to_submit`: How many workers will be/were submitted
- `submitted_count`: How many were actually submitted (0 without `--submit`)
- `queue_depth.visible`: Current SQS visible message count

### supervise-workers loop records
Each loop writes a `supervisor_status.jsonl` entry:
```json
{
  "schema": "sweetspot.supervisor_loop.v1",
  "loop_index": 0,
  "queue_depth": {"visible": 480, "not_visible": 40},
  "desired_active_workers": 64,
  "active_count": 60,
  "to_submit": 4,
  "submitted_count": 4,
  "stop_reason": null
}
```

If `stop_reason` is `"dlq_not_empty"`, the supervisor stopped because the DLQ received messages.

## Sizing guidance

1. **messages_per_worker**: Set to 1 for long tasks (minutes+), 4+ for short tasks (seconds). Higher values mean fewer Batch jobs because each worker processes more messages sequentially, but the worker receives and commits one SQS message at a time; on Spot interruption, only the currently executing task is exposed to replay.
2. **max_workers**: Start with SQS visible depth / messages_per_worker. Use `--subtract-active` to avoid oversubscription.
3. **Spot vs On-Demand**: Use Spot queues for cheap retryable work. Use On-Demand queues for repair lanes.
4. **timeout_seconds**: Must fit within SQS visibility timeout. Default 11h cap. Prefer much shorter tasks.
5. **Architecture**: x86 is the safe default. ARM/Graviton can be cheaper, but only submit ARM workers to Batch queues/job definitions whose image and native dependencies passed an ARM canary. Do not mix x86 and ARM in one queue unless the image is verified multi-arch.
6. **Always dry-run first**: Review `to_submit` and `raw_desired_workers` before adding `--submit`.

## Worker environment

Workers receive these environment variables from the framework:

| Variable | Purpose |
|---|---|
| `SWEETSPOT_SQS_QUEUE_URL` | Queue to poll |
| `SWEETSPOT_MAX_MESSAGES` | Max SQS messages per poll |
| `SWEETSPOT_VISIBILITY_TIMEOUT` | SQS visibility timeout |
| `SWEETSPOT_HEARTBEAT_SECONDS` | Heartbeat interval |
| `SWEETSPOT_TASK_TIMEOUT_SECONDS` | Per-task command timeout |
| `SWEETSPOT_ALLOWED_S3_PREFIXES` | Comma-separated allowed S3 prefixes |
| `SWEETSPOT_LOG_TAIL_BYTES` | Redacted log tail size for summaries |
| `SWEETSPOT_MAX_LOG_BYTES` | Max redacted bytes per stream uploaded to S3 |
| `SWEETSPOT_REDACT_REGEXES` | Newline-separated redaction patterns |

Worker commands receive at runtime:

| Variable | Purpose |
|---|---|
| `SWEETSPOT_TASK_JSON` | Path to local task JSON |
| `SWEETSPOT_TASK_ID` | Task ID |
| `SWEETSPOT_RUN_ID` | Run ID |
| `SWEETSPOT_OUTPUT_PATH` | Local path for output (if output_s3 set) |
| `SWEETSPOT_METRICS_PATH` | Optional metrics file for telemetry |
| `SWEETSPOT_ATTEMPT_ID` | Immutable attempt ID |
| `SWEETSPOT_TASK_HASH` | Stable hash of task fields |

## Common pitfalls

1. **Forgetting `--submit`**: Without it, the command is a dry-run and submits nothing. This is by design.
2. **Not subtracting active workers**: If workers are already running, `submit-workers` will oversubscribe without `--subtract-active`.
3. **No `--stop-on-dlq`**: Without this, the supervisor keeps submitting even when tasks are failing to the DLQ.
4. **messages_per_worker too high for long tasks**: Very high values can keep a worker running longer than intended and delay fleet scale-down. It does not make already completed messages replay; the worker commits and deletes each SQS message before receiving the next.
5. **Architecture mismatch**: ARM instance queues need ARM-compatible or multi-arch worker images. An x86-only image can fail immediately on Graviton even if the Spot price is attractive.
6. **Queue URL mismatch**: The queue URL must match the one the workers poll. Double-check region and account.
