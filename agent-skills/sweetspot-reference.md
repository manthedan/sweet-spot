---
name: sweetspot-reference
description: 'Advanced/admin master SweetSpot CLI reference. Use for lower-level commands, task schemas, worker environments, output schemas, or safety rules; for new runs, prefer sweetspot-run.'
---

# Skill: sweetspot-reference

Master CLI reference for SweetSpot, a cost-aware AWS Batch Spot work runner for trusted, idempotent, embarrassingly parallel workloads.

## When to use

Invoke this skill when an agent needs to understand, reference, or execute any SweetSpot CLI command. This is the comprehensive reference covering all current `sweetspot` subcommands plus the standalone `sweetspot-scout` and `sweetspot-lane-manager` compatibility entry points.

For new runs, prefer the thin `sweetspot-run` skill and the controller workflow: `plan`, `run`, `status`, `repair`, and `cancel`. The lower-level phase commands documented here remain advanced/operator controls for debugging, migration, and manual recovery.

## Architecture

SweetSpot implements a reliability pattern for large retryable AWS Batch jobs:

```
SQS task message
-> worker checks deterministic S3 done marker
-> if done exists: delete/ack message
-> else process task
-> upload output/summary
-> upload done marker last (conditional write)
-> only then delete/ack message
```

Three console-script entry points:
- `sweetspot` - main CLI, including nested `sweetspot scout` and `sweetspot lane-manager`
- `sweetspot-scout` - standalone compatibility entry point for the read-only Spot pool ranking tool
- `sweetspot-lane-manager` - standalone compatibility entry point for the multi-region lane allocator

## Common arguments (shared across many commands)

- `--queue-url` / `--sqs-queue-url`: SQS queue URL (also from `SWEETSPOT_SQS_QUEUE_URL`)
- `--allowed-s3-prefix`: S3 prefix allow-list, repeatable (also from `SWEETSPOT_ALLOWED_S3_PREFIXES`)
- `--profile` / `--region`: AWS profile/region (available on all AWS-calling commands)
- `--config <path>`: JSON config file with `defaults` and per-command sections; also from `SWEETSPOT_CONFIG` env var. Explicit CLI flags override config values.
- `--submit` / `--apply` / `--delete`: Mutation flags (always dry-run by default)
- `--env KEY=VALUE`: Environment overrides for workers, repeatable
- `--visibility-timeout`: SQS visibility timeout (default 1800s)
- `--heartbeat-seconds`: SQS heartbeat interval (default 300s)
- `--task-timeout-seconds`: Per-task timeout cap (default 39600s / 11h)
- `--vcpus` / `--memory`: Batch container overrides

## Complete command reference

### Version and status
```bash
sweetspot version
sweetspot status --queue-url <url> --job-queue <queue> [--dlq-url <dlq-url>] [--format json|table]
```
`status` reports AWS identity, queue depth, DLQ depth, and active Batch worker summary. JSON is the default; table output is opt-in.

### Worker
```bash
sweetspot worker --queue-url <url> [--max-messages N] [--visibility-timeout S]
```
Runs an SQS worker inside AWS Batch. Usually invoked by the Batch job definition, not manually.

### Enqueue
```bash
# Validate and optionally submit tasks
sweetspot enqueue-jsonl --tasks-jsonl tasks.jsonl [--queue-url <url>] [--submit]
                        [--run-id <id>] [--artifact-dir <dir>]
                        [--allowed-s3-prefix s3://bucket/prefix]

# Atomic enqueue + wait + submit workers
sweetspot enqueue-and-submit --tasks-jsonl tasks.jsonl --queue-url <url>
                             --batch-job-queue <queue> --job-definition <def>
                             [--wait-for-visible-seconds 30] --submit
```

### Canary, telemetry, and estimation
```bash
# Derive a deterministic canary subset
sweetspot derive-canary --tasks-jsonl tasks.jsonl --out-dir ./canary
                        [--task-count 4] [--include-dlq-probe]

# Estimate runtime/cost from telemetry
sweetspot estimate-runtime --sample-jsonl summaries.jsonl
                           [--target-units N | --task-count N --units-per-task M]
                           [--active-workers W] [--price-per-vcpu-hour P]
```

Use small, idempotent tasks that are cheap to replay after Spot interruption. Have canary task commands write `SWEETSPOT_METRICS_PATH` so runtime estimates and Spot scouting use observed useful throughput rather than guesswork.

### Worker submission
```bash
# Dry-run worker sizing (add --submit to actually submit)
sweetspot submit-workers --sqs-queue-url <url> --batch-job-queue <queue>
                         --job-definition <def> --job-name-prefix <prefix>
                         [--messages-per-worker 4] [--max-workers 64]
                         [--subtract-active] --submit

# Multi-loop supervisor
sweetspot supervise-workers --sqs-queue-url <url> --batch-job-queue <queue>
                            --job-definition <def> --job-name-prefix <prefix>
                            --target-active-workers 64 [--loops 10]
                            [--interval-seconds 60] [--stop-on-dlq --dlq-url <url>]
                            --submit
```

### Finalization
```bash
# Stream tasks, check done markers, write manifests
sweetspot finalize --run-id <id> --output-prefix s3://bucket/runs/<id>
                   --tasks-jsonl tasks.jsonl [--workers 32]
                   [--use-listing-index] [--upload] [--publish-ready]
                   [--write-repair-jsonl repair.jsonl] [--require-complete]

# Build repair plan excluding active-worker tasks
sweetspot repair-plan --tasks-jsonl tasks.jsonl
                      --task-status-jsonl task_status.jsonl
                      --out-jsonl repair.jsonl
                      [--job-queue <queue> --job-name-regex <pattern>]

# Clean up stale SQS messages
sweetspot cleanup-stale-messages --queue-url <url> [--run-id <id>] [--apply]
```

### Job and log inspection
```bash
sweetspot jobs --job-queue <queue> [--status RUNNING] [--name-regex <pattern>]
sweetspot describe-job --job-id <id>
sweetspot logs --job-id <id> [--last 50] [--max-events 500] [--filter-regex <pattern>]
sweetspot watch-job --job-id <id> [--max-seconds 3600]
```

### DLQ
```bash
# Inspect DLQ
sweetspot dlq --dlq-url <url> [--run-id <id>]

# Manual filtered redrive
sweetspot dlq --dlq-url <url> --queue-url <main-url> --run-id <id> --apply

# Native whole-DLQ redrive
sweetspot dlq --dlq-url <url> --queue-url <main-url> --native-redrive --apply
```

### S3 cleanup
```bash
# Dry-run prefix inspection (add --delete --confirm-prefix to mutate)
sweetspot s3-delete-prefix --prefix s3://bucket/runs/old-run/
                           [--include-versions] [--artifact-dir <dir>]
```

### Doctor
```bash
sweetspot doctor --queue-url <url> --dlq-url <dlq-url>
                 --job-queue <queue> --job-definition <def>
                 --s3-prefix s3://bucket/prefix [--write-probe]
                 [--validate-batch-metrics]
```

### Spot scouting
```bash
sweetspot scout --preset mixed --regions us-west-2 us-east-2
               --target-vcpus 256 512 --bucket my-data-bucket
               [--observed-summaries summaries/]
               [--json-out scout.json]
# Standalone compatibility entry point also works: sweetspot-scout ...
```

`--preset mixed` surfaces ARM/Graviton savings, but ARM is opt-in. Deploy ARM lanes only after an ARM canary proves the workload, native dependencies, and worker image are compatible; otherwise keep x86 as the safe default.

### Lane management
```bash
sweetspot lane-manager --config lanes.json
# Standalone compatibility entry point also works: sweetspot-lane-manager --config lanes.json
```

Cost-annotated lanes are allocated cheapest-first among placement-score-eligible lanes. For mixed x86/ARM configs, use separate Batch queues/job definitions and set per-lane `instance_types` so placement-score checks match each architecture.

## Task schema (sweetspot.task.v1)

```json
{
  "schema": "sweetspot.task.v1",
  "run_id": "hello-001",
  "task_id": "task-000001",
  "command": ["python", "/app/hello_worker.py"],
  "timeout_seconds": 3600,
  "output_s3": "s3://bucket/runs/hello-001/shards/task-000001.txt",
  "summary_s3": "s3://bucket/runs/hello-001/summaries/task-000001.summary.json",
  "done_s3": "s3://bucket/runs/hello-001/done/task-000001.done.json"
}
```

## Worker environment variables

| Variable | Purpose |
|---|---|
| `SWEETSPOT_TASK_JSON` | Path to local task JSON file |
| `SWEETSPOT_TASK_ID` | Task ID |
| `SWEETSPOT_RUN_ID` | Run ID |
| `SWEETSPOT_TASK_HASH` | Stable hash of task fields |
| `SWEETSPOT_ATTEMPT_ID` | Immutable execution attempt ID |
| `SWEETSPOT_OUTPUT_PATH` | Local path to write output (if output_s3 is set) |
| `SWEETSPOT_METRICS_PATH` | Optional JSON metrics file for cost telemetry |
| `SWEETSPOT_OUTPUT_S3` | Attempt-scoped S3 output URI |
| `SWEETSPOT_SUMMARY_S3` | Attempt-scoped S3 summary URI |
| `SWEETSPOT_DONE_S3` | Canonical done marker S3 URI |
| `SWEETSPOT_TASK_TIMEOUT_SECONDS` | Task timeout (default 39600 / 11h) |

## Output schemas

SweetSpot JSON outputs include a `schema` field. Common top-level schemas include:
- `sweetspot.version.v1`
- `sweetspot.status.v1`
- `sweetspot.enqueue_summary.v1`
- `sweetspot.enqueue_and_submit_summary.v1`
- `sweetspot.derive_canary_summary.v1`
- `sweetspot.worker_submitter_summary.v1`
- `sweetspot.supervisor_summary.v1`
- `sweetspot.final_manifest.v1`
- `sweetspot.repair_plan.v1`
- `sweetspot.stale_message_cleanup.v1`
- `sweetspot.runtime_estimate.v1`
- `sweetspot.job_description.v1`
- `sweetspot.jobs.v1`
- `sweetspot.logs.v1`
- `sweetspot.watch_job.v1`
- `sweetspot.s3_delete_prefix_summary.v1`
- `sweetspot.dlq_summary.v1`
- `sweetspot.dlq_redrive_summary.v1`
- `sweetspot.doctor.v1`

## Safety rules

1. **Dry-run by default**: `submit-workers`, `supervise-workers`, `s3-delete-prefix`, `dlq`, `cleanup-stale-messages` require explicit mutation flags.
2. **S3 prefix allow-list**: When `--allowed-s3-prefix` is set, every `s3://` URI in task payloads must be under one of those prefixes.
3. **Reserved env namespaces**: Task-provided env keys may not start with `SWEETSPOT_`, `AWS_`, or `ECS_`.
4. **Timeout cap**: Task timeouts are capped below SQS's 12-hour visibility ceiling (default 11h).
5. **Done marker is source of truth**: S3 object existence alone does not mean a task is complete. The done marker is written last with a conditional write.
6. **Trusted workload**: The SQS queue is a trusted control plane. Anyone who can enqueue a task can choose the command executed by the worker.
