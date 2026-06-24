# SweetSpot

SweetSpot is a cost-aware AWS Batch Spot work runner for trusted, idempotent, embarrassingly parallel workloads.

Install the Python package and use the `sweetspot` CLI for enqueueing, worker submission, finalization, diagnostics, Spot scouting, and lane management.

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

SweetSpot is an at-least-once runner, not an exactly-once transaction system. The SQS queue is a trusted control plane: anyone who can enqueue a task can choose the command executed by the worker task role. Commands must therefore be trusted and idempotent, or use `task_id` as their own idempotency key for external side effects. Do not expose the queue to untrusted producers.

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

See `CONTRIBUTING.md` for contributor workflow, trust-boundary expectations, and release hygiene.

```bash
python -m venv .venv
. .venv/bin/activate
pip install --constraint requirements.lock -e .

# optional local closeout checks used by CI
pip install --constraint requirements-dev.lock -e '.[dev]'
ruff format --check .
ruff check .
mypy sweetspot
python -m unittest discover -s tests -v

# full local release closeout (also runs OpenTofu checks when tofu is installed)
scripts/verify_release.sh
```

## Minimal task schema

Each SQS message is a JSON object:

```json
{
  "schema": "sweetspot.task.v1",
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
SWEETSPOT_TASK_JSON       path to local task JSON
SWEETSPOT_TASK_ID
SWEETSPOT_RUN_ID
SWEETSPOT_TASK_HASH       stable hash of the task fields committed by the worker
SWEETSPOT_ATTEMPT_ID      immutable execution attempt id
SWEETSPOT_OUTPUT_PATH     local path to write if output_s3 should be uploaded by framework
SWEETSPOT_METRICS_PATH    optional JSON metrics file written by the command for cost telemetry
SWEETSPOT_OUTPUT_S3       attempt-scoped S3 URI used by this execution
SWEETSPOT_SUMMARY_S3      attempt-scoped S3 URI used by this execution
SWEETSPOT_DONE_S3         canonical conditional done marker URI
SWEETSPOT_TASK_TIMEOUT_SECONDS default task timeout used by the worker (default: 39600 / 11h)
```

If `output_s3` is present, the command must create `SWEETSPOT_OUTPUT_PATH` before exiting successfully; otherwise the task is treated as failed and no done marker is written. Successful workers upload output, summaries, and stdout/stderr under attempt-scoped S3 paths, then publish the canonical done marker with a conditional `If-None-Match: *` write (412 means an existing marker won; transient 409 conditional conflicts are retried). If another duplicate attempt won first, the worker validates the winning marker before deleting the SQS message.

For v2 markers, `output_s3` in the task is the logical output URI used for task hashing; the actual immutable object URI is recorded in the done marker's `output.uri` and in finalizer outputs manifests.

Task payloads are validated as `sweetspot.task.v1` before execution: `run_id`, `task_id`, `command`, timeout, env, marker URIs, and S3 URI syntax must pass bounded checks. Task-provided `env` keys may not start with `SWEETSPOT_`, `AWS_`, or `ECS_`; those namespaces are reserved for the framework and runtime.

For production, set `SWEETSPOT_ALLOWED_S3_PREFIXES` or pass `--allowed-s3-prefix` to `sweetspot worker` / `submit-workers` / `supervise-workers`. When configured, every `s3://...` URI found in the task payload, including command arguments and derived done markers, must be under one of those prefixes. Exact-key equality to a non-root prefix is rejected so runtime validation matches the OpenTofu IAM policy's `${prefix}/*` object scope.

Legacy v1 done markers are not accepted by default because they do not bind the full task hash or immutable attempt output. Use `--allow-legacy-done-markers` / `SWEETSPOT_ALLOW_LEGACY_DONE_MARKERS=1` only for an explicit migration pass, including `sweetspot finalize --allow-legacy-done-markers` for old runs.

Finalization is streaming and scale-oriented: `sweetspot finalize` reads task JSONL line-by-line, writes complete `task_status.jsonl`, `repair_tasks.jsonl`, and `outputs.jsonl` artifacts, and keeps only bounded samples in `final_manifest.json`. Use `--dry-run` to preview upload/READY targets while still writing local artifacts but skipping S3 uploads, READY deletion, and READY publishing. Use `--use-listing-index` or repeat `--preload-s3-prefix` for large runs to trade S3 LIST calls for fewer per-task HEAD requests.

Worker observability is on by default: child stdout/stderr are streamed to container logs for CloudWatch, a bounded redacted tail is stored in the task summary, and capped redacted attempt logs are uploaded to S3. Use `--log-tail-bytes`, `--max-log-bytes`, and repeatable `--redact-regex` on `worker`, `submit-workers`, or `supervise-workers` for sensitive workloads. Redaction is applied per newline-terminated log record; overlong unterminated records are suppressed with a placeholder rather than risk leaking a partial secret.

For measured cost optimization, tasks may write a JSON object to `SWEETSPOT_METRICS_PATH`, for example `{"completed_units": 100000, "useful_compute_seconds": 3600, "input_bytes": 1048576, "output_bytes": 524288}`. The worker includes this under `summary.telemetry` together with instance/architecture/Region/AZ/image best-effort metadata, SQS receive count/retry status, startup delay from SQS sent timestamp, bytes transferred, interruption/failure status, and discarded compute seconds. `sweetspot-scout --observed-summaries ...` consumes these summaries to rank pools by expected total cost, not only Spot price.

Task timeouts are capped below SQS's 12-hour visibility ceiling. Prefer much shorter shards, and checkpoint/split work that cannot fit safely under the default 11-hour cap.

## Sizing and repair safety

SweetSpot works best when each task is small, trusted, and idempotent. Do **not** launch huge uncheckpointed tasks on Spot just because your input files are large. First run a representative canary, estimate throughput, then choose a chunk size whose predicted per-task runtime is comfortably below your timeout and cheap to replay after interruption.

A safe production loop is:

1. derive and run a canary;
2. estimate runtime/cost from canary telemetry with `sweetspot estimate-runtime`;
3. enqueue and submit in one command with `sweetspot enqueue-and-submit --wait-for-visible-seconds ...` to avoid SQS approximate-depth races;
4. finalize to produce `task_status.jsonl` and `repair_tasks.jsonl`;
5. build repairs with `sweetspot repair-plan`, excluding tasks already owned by active workers;
6. dry-run then apply `sweetspot cleanup-stale-messages` for visible duplicate messages whose done markers already exist.

For Spot, prefer many short tasks over a few long tasks. If a task cannot checkpoint or finish quickly, use an On-Demand repair lane or split it further.

## CLI quickstart

```bash
# enqueue JSONL task messages
sweetspot enqueue-jsonl \
  --queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue \
  --tasks-jsonl examples/hello_world/tasks.jsonl \
  --artifact-dir artifacts/hello-001 \
  --allowed-s3-prefix s3://my-bucket/runs/hello-001 \
  --submit

# derive a deterministic canary subset before large launches
sweetspot derive-canary \
  --tasks-jsonl artifacts/hello-001/tasks.jsonl \
  --out-dir artifacts/hello-001/canary \
  --task-count 4 \
  --include-dlq-probe \
  --dlq-probe-prefix s3://my-bucket/runs/hello-001/dlq-probes

# submit AWS Batch workers, dry-run by default
sweetspot submit-workers \
  --sqs-queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue \
  --batch-job-queue my-batch-spot-queue \
  --job-definition my-worker-jobdef:1 \
  --job-name-prefix hello-001-worker \
  --messages-per-worker 4 \
  --max-workers 64 \
  --allowed-s3-prefix s3://my-bucket/runs/hello-001 \
  --subtract-active

# add --submit after reviewing the dry-run

# enqueue and submit in one step, waiting for SQS's approximate visible count
# to catch up before sizing the worker wave
sweetspot enqueue-and-submit \
  --queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue \
  --tasks-jsonl artifacts/hello-001/tasks.jsonl \
  --artifact-dir artifacts/hello-001/enqueue-submit \
  --batch-job-queue my-batch-spot-queue \
  --job-definition my-worker-jobdef:1 \
  --job-name-prefix hello-001-worker \
  --messages-per-worker 4 \
  --max-workers 64 \
  --wait-for-visible-seconds 60 \
  --allowed-s3-prefix s3://my-bucket/runs/hello-001

# add --submit after reviewing the dry-run

# estimate full-run wall time/cost from canary or task summary telemetry
sweetspot estimate-runtime \
  --sample-jsonl artifacts/hello-001/canary_summaries.jsonl \
  --task-count 1000 \
  --units-per-task 25000 \
  --active-workers 64 \
  --vcpus-per-worker 2 \
  --price-per-vcpu-hour 0.02 \
  --task-timeout-seconds 3600 \
  --spot

# keep a bounded worker pool topped up across one or more loops
sweetspot supervise-workers \
  --sqs-queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue \
  --batch-job-queue my-batch-spot-queue \
  --job-definition my-worker-jobdef:1 \
  --job-name-prefix hello-001-worker \
  --target-active-workers 64 \
  --max-active-workers 64 \
  --max-submit-per-loop 16

# add --submit after reviewing the dry-run

# optional: put repeated defaults in JSON and pass --config (or SWEETSPOT_CONFIG)
cat > sweetspot.json <<'JSON'
{
  "defaults": {
    "profile": "prod",
    "region": "us-west-2",
    "queue_url": "https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue"
  },
  "submit-workers": {
    "batch_job_queue": "my-batch-spot-queue",
    "job_definition": "my-worker-jobdef:1",
    "messages_per_worker": 4
  }
}
JSON
sweetspot --config sweetspot.json submit-workers --job-name-prefix hello-001-worker

# quick operator overview; JSON remains the default, table is opt-in
sweetspot status \
  --queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue \
  --job-queue my-batch-spot-queue \
  --job-name-prefix hello-001-worker \
  --format table

# preflight AWS/SQS/S3/Batch/CloudWatch permissions and configuration
sweetspot doctor \
  --queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue \
  --dlq-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-dlq \
  --job-queue my-batch-spot-queue \
  --job-definition my-worker-jobdef:1 \
  --s3-prefix s3://my-bucket/runs/hello-001 \
  --validate-batch-metrics

# finalize by streaming tasks, checking S3 done markers, and writing manifests
sweetspot finalize \
  --run-id hello-001 \
  --output-prefix s3://my-bucket/runs/hello-001 \
  --tasks-jsonl artifacts/hello-001/tasks.jsonl \
  --workers 32 \
  --use-listing-index \
  --write-repair-jsonl artifacts/hello-001/repair_tasks.jsonl \
  --require-complete

# preview upload/READY targets without mutating S3
sweetspot finalize \
  --run-id hello-001 \
  --output-prefix s3://my-bucket/runs/hello-001 \
  --tasks-jsonl artifacts/hello-001/tasks.jsonl \
  --upload \
  --publish-ready \
  --dry-run \
  --require-complete

# optionally upload final_manifest.json and publish READY only when complete
sweetspot finalize \
  --run-id hello-001 \
  --output-prefix s3://my-bucket/runs/hello-001 \
  --tasks-jsonl artifacts/hello-001/tasks.jsonl \
  --upload \
  --publish-ready \
  --require-complete

# build a repair JSONL while excluding missing tasks currently owned by active jobs
sweetspot repair-plan \
  --tasks-jsonl artifacts/hello-001/tasks.jsonl \
  --task-status-jsonl artifacts/hello-001/finalizer/task_status.jsonl \
  --out-jsonl artifacts/hello-001/repair_safe.jsonl \
  --job-queue my-batch-spot-queue \
  --job-name-regex hello-001-worker

# dry-run stale duplicate cleanup; add --apply only after reviewing counts/examples
sweetspot cleanup-stale-messages \
  --queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue \
  --run-id hello-001 \
  --max-messages 100

# inspect AWS Batch jobs and logs; JSON remains the default, table is opt-in
sweetspot jobs --job-queue my-batch-spot-queue --status RUNNING --name-regex hello-001 --format table
sweetspot describe-job --job-id AWS_BATCH_JOB_ID --format table
sweetspot logs --job-id AWS_BATCH_JOB_ID --max-events 500 --last 50 --filter-regex 'progress|ERROR' --format table
# If --job-id is provided and --log-group is omitted, sweetspot uses the job's awslogs-group when AWS Batch reports it.
sweetspot watch-job --job-id AWS_BATCH_JOB_ID --max-seconds 3600 --format table

# dry-run a guarded S3 prefix cleanup; add --delete and exact --confirm-prefix to mutate.
# Add --include-versions for versioned buckets so old versions and delete markers are included.
sweetspot s3-delete-prefix \
  --prefix s3://my-bucket/runs/old-run/ \
  --include-versions \
  --artifact-dir artifacts/old-run/delete-dryrun

# inspect DLQ; filtered manual redrive is available for small repairs
sweetspot dlq \
  --dlq-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-dlq \
  --run-id hello-001 \
  --format table

# whole-DLQ redrive should use native SQS StartMessageMoveTask where possible
sweetspot dlq \
  --dlq-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-dlq \
  --queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue \
  --native-redrive \
  --apply

# read-only Spot scout (also available as standalone sweetspot-scout)
sweetspot scout \
  --preset x86 \
  --regions us-west-2 us-east-2 eu-north-1 \
  --target-vcpus 256 512 \
  --bucket my-data-bucket \
  --observed-summaries artifacts/hello-001/summaries \
  --startup-overhead-seconds 90 \
  --cross-region-gb-per-1m-units 2 \
  --nat-gb-per-1m-units 0 \
  --json-out artifacts/hello-001/scout.json

# multi-lane dry-run submitter; lanes with expected_total_cost_per_1m_units are allocated cheapest-first among eligible placement scores
# If min_placement_score is set and AWS cannot return a score, the lane is ineligible unless allow_unknown_placement_score=true.
sweetspot lane-manager --config lanes.json
```

## OpenTofu

`infra/opentofu/` creates:

- SQS work queue + DLQ with SSE enabled, longer DLQ retention, and a by-source-queue redrive allow policy
- AWS Batch Spot compute environment and queue
- optional On-Demand repair queue
- least-privilege IAM roles scoped to the work queue and configured S3 bucket/prefixes
- a no-ingress Batch security group by default, plus an IMDSv2-required encrypted-root launch template
- generic worker job definition with runtime S3-prefix validation
- CloudWatch dashboard and baseline alarms for queue age, DLQ depth, Batch failures, and runnable-job stalls
- optional monthly AWS Budget alerts

Runtime and dev dependency lock files (`requirements.lock`, `requirements-dev.lock`) pin the Python dependency graph used by Docker and CI. The worker Dockerfile pins its Python base image by digest and drops to an unprivileged `sweetspot` user. CI runs unit tests, Ruff formatting/linting, typing, OpenTofu lock/fmt/validate checks, and a container build with SBOM/provenance attestations plus Trivy scanning.

See `infra/opentofu/README.md`.

## Cost case studies and release hygiene

- `docs/cost_model.md` explains worker telemetry, expected-total-cost pool ranking, and cost-aware lane allocation.
- `examples/run_manifest.example.json` is the machine-readable shape for anonymized run/cost evidence.
- `docs/case_study_template.md` is the companion prose template for public Spot vs On-Demand comparisons when Cost Explorer data is unavailable or private.
- `scripts/verify_release.sh` runs the local closeout checks that mirror CI-critical gates.
- `docs/release_checklist.md` covers branch protection, action pin updates, and release/tag hygiene.
- `SECURITY.md` documents the trusted-workload threat model and reporting process.
- `CHANGELOG.md` tracks unreleased production-hardening changes.

## License

Apache-2.0.
