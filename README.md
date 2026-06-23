# Miser

Miser is a cost-aware AWS Batch Spot work runner for trusted, idempotent, embarrassingly parallel workloads.

The installed CLI is `spotbatch`. Older docs and package metadata may still refer to the original package name, `aws-batch-job-runner`.

This project packages a reliability pattern for large retryable AWS Batch jobs:

```text
SQS task message
→ worker checks deterministic S3 done marker
→ if done exists: delete/ack message
→ else process task
→ upload output/summary
→ upload done marker last
→ only then delete/ack message
```

If a Spot host dies before ack, SQS visibility timeout returns the task. If a task repeatedly fails, SQS redrives it to the DLQ.

Miser is an at-least-once runner, not an exactly-once transaction system. The SQS queue is a trusted control plane: anyone who can enqueue a task can choose the command executed by the worker task role. Commands must therefore be trusted and idempotent, or use `task_id` as their own idempotency key for external side effects. Do not expose the queue to untrusted producers.

## What it is

AWS-specific, OpenTofu-compatible framework for embarrassingly parallel jobs:

- batch inference / annotation
- dataset conversion
- web scraping
- simulation sweeps
- self-play generation
- CPU-heavy ETL

## What it is not

- not cloud agnostic
- not a scheduler replacing AWS Batch
- not tied to chess or any single project

Chess/Stockfish workflows live under `examples/`.

## Install for development

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
```

## Minimal task schema

Each SQS message is a JSON object:

```json
{
  "schema": "spotbatch.task.v1",
  "run_id": "hello-001",
  "task_id": "task-000001",
  "command": ["python", "/app/hello_worker.py"],
  "timeout_seconds": 3600,
  "output_s3": "s3://my-bucket/runs/hello-001/shards/task-000001.txt",
  "summary_s3": "s3://my-bucket/runs/hello-001/summaries/task-000001.summary.json",
  "done_s3": "s3://my-bucket/runs/hello-001/done/task-000001.done.json"
}
```

The worker sets environment variables for the command:

```text
SPOTBATCH_TASK_JSON       path to local task JSON
SPOTBATCH_TASK_ID
SPOTBATCH_RUN_ID
SPOTBATCH_TASK_HASH       stable hash of the task fields committed by the worker
SPOTBATCH_ATTEMPT_ID      immutable execution attempt id
SPOTBATCH_OUTPUT_PATH     local path to write if output_s3 should be uploaded by framework
SPOTBATCH_OUTPUT_S3       attempt-scoped S3 URI used by this execution
SPOTBATCH_SUMMARY_S3      attempt-scoped S3 URI used by this execution
SPOTBATCH_DONE_S3         canonical conditional done marker URI
SPOTBATCH_TASK_TIMEOUT_SECONDS default task timeout used by the worker (default: 39600 / 11h)
```

If `output_s3` is present, the command must create `SPOTBATCH_OUTPUT_PATH` before exiting successfully; otherwise the task is treated as failed and no done marker is written. Successful workers upload output, summaries, and stdout/stderr under attempt-scoped S3 paths, then publish the canonical done marker with a conditional `If-None-Match: *` write. If another duplicate attempt won first, the worker validates the winning marker before deleting the SQS message.

For v2 markers, `output_s3` in the task is the logical output URI used for task hashing; the actual immutable object URI is recorded in the done marker's `output.uri` and in the final manifest `outputs` list.

Task payloads are validated as `spotbatch.task.v1` before execution: `run_id`, `task_id`, `command`, timeout, env, marker URIs, and S3 URI syntax must pass bounded checks. Task-provided `env` keys may not start with `SPOTBATCH_`, `AWS_`, or `ECS_`; those namespaces are reserved for the framework and runtime.

For production, set `SPOTBATCH_ALLOWED_S3_PREFIXES` or pass `--allowed-s3-prefix` to `spotbatch worker` / `submit-workers` / `supervise-workers`. When configured, every `s3://...` URI found in the task payload, including command arguments and derived done markers, must be inside one of those prefixes.

Worker observability is on by default: child stdout/stderr are streamed to container logs for CloudWatch, a bounded redacted tail is stored in the task summary, and capped redacted attempt logs are uploaded to S3. Use `--log-tail-bytes`, `--max-log-bytes`, and repeatable `--redact-regex` on `worker`, `submit-workers`, or `supervise-workers` for sensitive workloads. Redaction is applied per newline-terminated log record; overlong unterminated records are suppressed with a placeholder rather than risk leaking a partial secret.

Task timeouts are capped below SQS's 12-hour visibility ceiling. Prefer much shorter shards, and checkpoint/split work that cannot fit safely under the default 11-hour cap.

## CLI quickstart

```bash
# enqueue JSONL task messages
spotbatch enqueue-jsonl \
  --queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue \
  --tasks-jsonl examples/hello_world/tasks.jsonl \
  --artifact-dir artifacts/hello-001 \
  --allowed-s3-prefix s3://my-bucket/runs/hello-001 \
  --submit

# derive a deterministic canary subset before large launches
spotbatch derive-canary \
  --tasks-jsonl artifacts/hello-001/tasks.jsonl \
  --out-dir artifacts/hello-001/canary \
  --task-count 4 \
  --include-dlq-probe

# submit AWS Batch workers, dry-run by default
spotbatch submit-workers \
  --sqs-queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue \
  --batch-job-queue my-batch-spot-queue \
  --job-definition my-worker-jobdef:1 \
  --job-name-prefix hello-001-worker \
  --messages-per-worker 4 \
  --max-workers 64 \
  --allowed-s3-prefix s3://my-bucket/runs/hello-001 \
  --subtract-active

# add --submit after reviewing the dry-run

# keep a bounded worker pool topped up across one or more loops
spotbatch supervise-workers \
  --sqs-queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue \
  --batch-job-queue my-batch-spot-queue \
  --job-definition my-worker-jobdef:1 \
  --job-name-prefix hello-001-worker \
  --target-active-workers 64 \
  --max-active-workers 64 \
  --max-submit-per-loop 16

# add --submit after reviewing the dry-run

# preflight AWS/SQS/S3/Batch/CloudWatch permissions and configuration
spotbatch doctor \
  --queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue \
  --dlq-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-dlq \
  --job-queue my-batch-spot-queue \
  --job-definition my-worker-jobdef:1 \
  --s3-prefix s3://my-bucket/runs/hello-001

# finalize by checking S3 done markers
spotbatch finalize \
  --run-id hello-001 \
  --output-prefix s3://my-bucket/runs/hello-001 \
  --tasks-jsonl artifacts/hello-001/tasks.jsonl \
  --workers 32 \
  --write-repair-jsonl artifacts/hello-001/repair_tasks.jsonl \
  --require-complete

# optionally upload final_manifest.json and publish READY only when complete
spotbatch finalize \
  --run-id hello-001 \
  --output-prefix s3://my-bucket/runs/hello-001 \
  --tasks-jsonl artifacts/hello-001/tasks.jsonl \
  --upload \
  --publish-ready \
  --require-complete

# inspect AWS Batch jobs and logs
spotbatch jobs --job-queue my-batch-spot-queue --status RUNNING --name-regex hello-001
spotbatch describe-job --job-id AWS_BATCH_JOB_ID
spotbatch logs --job-id AWS_BATCH_JOB_ID --tail 50 --filter-regex 'progress|ERROR'
# If --job-id is provided and --log-group is omitted, spotbatch uses the job's awslogs-group when AWS Batch reports it.
spotbatch watch-job --job-id AWS_BATCH_JOB_ID --max-seconds 3600

# dry-run a guarded S3 prefix cleanup; add --delete and exact --confirm-prefix to mutate
spotbatch s3-delete-prefix \
  --prefix s3://my-bucket/runs/old-run/ \
  --artifact-dir artifacts/old-run/delete-dryrun

# inspect DLQ
spotbatch dlq \
  --dlq-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-dlq \
  --run-id hello-001

# read-only Spot scout
spotbatch-spot-scout \
  --preset x86 \
  --regions us-west-2 us-east-2 eu-north-1 \
  --target-vcpus 256 512 \
  --bucket my-data-bucket

# multi-lane dry-run submitter
spotbatch-lane-manager --config lanes.json
```

## OpenTofu

`infra/opentofu/` creates:

- SQS work queue + DLQ
- AWS Batch Spot compute environment and queue
- optional On-Demand repair queue
- least-privilege IAM roles scoped to the work queue and configured S3 bucket/prefixes
- generic worker job definition with runtime S3-prefix validation
- CloudWatch dashboard and baseline alarms for queue age, DLQ depth, Batch failures, and runnable-job stalls

See `infra/opentofu/README.md`.

## License

Apache-2.0.
