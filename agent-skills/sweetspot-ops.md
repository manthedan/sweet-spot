---
name: sweetspot-ops
description: 'Advanced/admin diagnostics for SweetSpot AWS/SQS/S3/Batch/CloudWatch operations, status/jobs/logs/DLQs, safe redrive, and stalled-run troubleshooting. For normal visibility, prefer sweetspot-run.'
---

# Skill: sweetspot-ops

Guide for operational diagnostics, DLQ management, and troubleshooting SweetSpot runs.

## When to use

Invoke this skill for advanced/operator workflows. For normal run visibility, prefer `sweetspot-run` plus `sweetspot status RUN_ID`; use this skill when diagnosis goes beyond the simplified workflow.

Use this skill when an agent explicitly needs to:
- Diagnose SweetSpot or AWS infrastructure issues
- Inspect or redrive DLQ messages
- Run the doctor preflight check
- Troubleshoot failed or stalled jobs
- Inspect logs and job details
- Understand task states and failure modes

## Diagnostic workflow

When something goes wrong, follow this diagnostic sequence:

1. **Run status** for a quick identity, queue depth, DLQ depth, and active-worker snapshot
2. **Run doctor** to validate AWS prerequisites
3. **Check DLQ** for failed tasks
4. **Inspect jobs** for FAILED status or RUNNABLE stalls
5. **Read logs** for error details
6. **Finalize** to get a complete picture of task states
7. **Plan repairs** for incomplete tasks

## CLI commands

### Doctor (preflight validation)

Validates SQS, S3, Batch, CloudWatch configuration:
```bash
sweetspot doctor \
  --queue-url https://sqs.us-west-2.amazonaws.com/123456789012/my-work-queue \
  --dlq-url https://sqs.us-west-2.amazonaws.com/123456789012/my-dlq \
  --job-queue my-batch-spot-queue \
  --job-definition my-worker-jobdef:1 \
  --s3-prefix s3://my-bucket/runs/my-run-001 \
  --write-probe \
  --validate-batch-metrics
```

Doctor checks:
- `sqs_work_queue`: Queue exists, attributes are sane (visibility, redrive policy)
- `sqs_dlq`: DLQ exists, retention period
- `batch_job_queue`: Queue exists, ENABLED state, VALID status
- `batch_job_definition`: Definition exists, ACTIVE status, container image, log group
- `s3_prefix`: Bucket/prefix listable, optionally write/delete probe
- `cloudwatch_log_group`: Log group exists, retention setting
- `batch_metrics`: CloudWatch metric dimensions for the job queue
- `service_quotas`: Advisory note about Batch vCPU quotas

Each check returns:
```json
{
  "name": "sqs_work_queue",
  "ok": true,
  "elapsed_sec": 0.234,
  "details": {"queue_url": "...", "attributes": {...}}
}
```

If `ok` is false, check `error_type` and `error`.

### Quick status

```bash
sweetspot status \
  --queue-url https://sqs.us-west-2.amazonaws.com/123456789012/my-work-queue \
  --dlq-url https://sqs.us-west-2.amazonaws.com/123456789012/my-dlq \
  --job-queue my-batch-spot-queue \
  --format table
```

Use JSON output for automation and `--format table` for operator snapshots.

### Inspect DLQ

Read-only inspection:
```bash
sweetspot dlq \
  --dlq-url https://sqs.us-west-2.amazonaws.com/123456789012/my-dlq \
  --run-id my-run-001
```

Output shows message breakdown by run_id, schema, receive counts, and examples.

### Redrive from DLQ

Manual filtered redrive (small targeted repairs):
```bash
sweetspot dlq \
  --dlq-url https://sqs.us-west-2.amazonaws.com/123456789012/my-dlq \
  --queue-url https://sqs.us-west-2.amazonaws.com/123456789012/my-work-queue \
  --run-id my-run-001 \
  --apply
```

Native whole-DLQ redrive (uses SQS StartMessageMoveTask):
```bash
sweetspot dlq \
  --dlq-url https://sqs.us-west-2.amazonaws.com/123456789012/my-dlq \
  --queue-url https://sqs.us-west-2.amazonaws.com/123456789012/my-work-queue \
  --native-redrive \
  --apply
```

Native redrive moves the entire DLQ. Use manual redrive with `--run-id` for filtered repairs.

### Job inspection

List jobs by status:
```bash
sweetspot jobs --job-queue my-batch-spot-queue \
  --status RUNNING --name-regex 'my-run-001'
```

Describe a specific job:
```bash
sweetspot describe-job --job-id <id>
```

Watch a job to completion:
```bash
sweetspot watch-job --job-id <id> --max-seconds 3600
```

### Log inspection

Fetch logs for a job:
```bash
# Last 50 log events
sweetspot logs --job-id <id> --last 50

# Filtered log events
sweetspot logs --job-id <id> --filter-regex 'ERROR|WARN|traceback' --max-events 200

# From a specific log stream
sweetspot logs --log-stream <stream-name> --log-group /aws/batch/job
```

If `--job-id` is given and `--log-group` is omitted, the CLI auto-detects the log group from the job definition.

## Interpreting job states

### AWS Batch job statuses
| Status | Meaning | Action |
|---|---|---|
| `SUBMITTED` | Accepted by Batch | Wait |
| `PENDING` | Awaiting scheduling | Wait |
| `RUNNABLE` | Scheduled but no capacity | Check Spot capacity, vCPU quota |
| `STARTING` | Container pulling | Wait |
| `RUNNING` | Executing | Monitor logs |
| `SUCCEEDED` | Completed normally | None |
| `FAILED` | Container exited non-zero or host died | Check logs, exit code, retries |

### Task states (from finalize)
| State | Meaning | Action |
|---|---|---|
| `done` | Valid done marker, output exists | None |
| `incomplete` | No done marker found | Enqueue repair task |
| `missing_output` | Done marker exists, output gone | Re-run task (output was deleted?) |
| `output_without_done` | Output exists, no/invalid done marker | Repair task will fix marker |
| `invalid_done_marker` | Done marker failed validation | Repair task with `.repair-` suffix |

### Worker events

Workers emit structured JSON events to stdout (visible in CloudWatch):
- `message_received`: SQS message received
- `skip_existing_done`: Done marker already exists, skipping
- `task_started`: Command execution began
- `lease_renewed`: SQS visibility extended by heartbeat
- `task_timeout`: Task exceeded timeout
- `command_finished`: Task subprocess exited or timed out
- `output_uploaded`: Output uploaded to attempt-scoped S3
- `telemetry`: Cost/retry/throughput telemetry emitted
- `summary_uploaded`: Summary uploaded, sometimes corrected after a lost commit race
- `log_uploaded`: Redacted stdout/stderr log uploaded to attempt-scoped S3
- `commit_succeeded`: Canonical done marker written successfully
- `commit_lost`: Another attempt won the done-marker race
- `message_deleted`: SQS message deleted after success/skip
- `task_failed`: Task command or framework validation failed

## Common failure scenarios and resolution

### 1. Jobs stuck in RUNNABLE

**Cause**: No Spot capacity available, or vCPU quota exceeded.

**Diagnosis**:
```bash
sweetspot jobs --job-queue my-batch-spot-queue --status RUNNABLE
sweetspot describe-job --job-id <stalled-job-id>
```

Check `statusReason` for quota or capacity messages.

**Resolution**: Use `sweetspot scout` to find better pools, or add an On-Demand repair queue.

### 2. Tasks going to DLQ

**Cause**: Repeated task failures (poison messages) triggering SQS redrive policy.

**Diagnosis**:
```bash
sweetspot dlq --dlq-url <dlq-url> --run-id my-run-001
```

Check `by_run` and `by_schema` in the output. Look at receive counts in examples.

**Resolution**: Inspect the task command in the DLQ message. Fix the command or data, then redrive.

### 3. Done marker validation failures

**Cause**: Done marker exists but its task hash, schema, or output checksum don't match.

**Diagnosis**: Run finalize and check for `invalid_marker_count` in the output.

**Resolution**: Repair tasks automatically use a `.repair-<timestamp>` suffix for the done marker, avoiding collision with the invalid canonical marker.

### 4. Output exists but no done marker

**Cause**: Worker uploaded output but was interrupted before writing the done marker.

**Diagnosis**: Run finalize and check `output_without_done_count`.

**Resolution**: Repair tasks re-run the work. The existing orphan output is safe to garbage-collect after retention.

### 5. Queue depth not draining

**Cause**: Workers not processing fast enough, or no active workers.

**Diagnosis**:
```bash
sweetspot jobs --job-queue my-batch-spot-queue --status RUNNING --name-regex 'my-run'
```

Check active worker count. If zero, submit more workers.

**Resolution**: Use `sweetspot supervise-workers` to maintain a target pool size.

### 6. Supervisor stopped due to DLQ

**Cause**: `--stop-on-dlq` triggered because DLQ received messages.

**Diagnosis**: Check the supervisor summary's `stop_reason` field.

**Resolution**: Inspect DLQ, fix root cause, redrive, and restart supervisor without `--stop-on-dlq` or with the issue resolved.

## DLQ redrive decision matrix

| Scenario | Command |
|---|---|
| Need to redrive specific run's messages | `sweetspot dlq --dlq-url <url> --queue-url <main> --run-id <id> --apply` |
| Need to redrive entire DLQ | `sweetspot dlq --dlq-url <url> --queue-url <main> --native-redrive --apply` |
| Need to inspect before redriving | `sweetspot dlq --dlq-url <url> --run-id <id>` (no --apply) |
| Need rate-limited native redrive | `sweetspot dlq --dlq-url <url> --native-redrive --max-messages-per-second 100 --apply` |

## Common pitfalls

1. **Not running doctor before a big run**: Doctor catches misconfigurations (wrong log group, missing IAM permissions, wrong queue attributes) before they cause silent failures.
2. **Ignoring RUNNABLE stalls**: Jobs in RUNNABLE for more than a few minutes usually means capacity or quota issues. Don't wait hoping they'll start.
3. **Manual redrive of large DLQ**: Manual receive/send/delete is slow and can hit API rate limits. Use `--native-redrive` for large DLQs.
4. **Not checking `statusReason`**: When jobs fail, `describe-job` shows `statusReason` and container `exitCode`. These are the primary diagnostic signals.
5. **Forgetting that done marker is source of truth**: S3 output existing does not mean a task is complete. Always finalize to verify done markers.
