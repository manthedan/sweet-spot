# aws-batch-job-runner

Durable AWS Batch + SQS + S3 job runner for cheap Spot compute.

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
SPOTBATCH_OUTPUT_PATH     local path to write if output_s3 should be uploaded by framework
SPOTBATCH_OUTPUT_S3
SPOTBATCH_SUMMARY_S3
SPOTBATCH_DONE_S3
```

If `output_s3` is present and `SPOTBATCH_OUTPUT_PATH` exists after the command exits successfully, the framework uploads it. The done marker is uploaded last.

## CLI quickstart

```bash
# enqueue JSONL task messages
spotbatch enqueue-jsonl \
  --queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue \
  --tasks-jsonl examples/hello_world/tasks.jsonl \
  --artifact-dir artifacts/hello-001 \
  --submit

# submit AWS Batch workers, dry-run by default
spotbatch submit-workers \
  --sqs-queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/my-work-queue \
  --batch-job-queue my-batch-spot-queue \
  --job-definition my-worker-jobdef:1 \
  --job-name-prefix hello-001-worker \
  --messages-per-worker 4 \
  --max-workers 64 \
  --subtract-active

# add --submit after reviewing the dry-run

# finalize by checking S3 done markers
spotbatch finalize \
  --run-id hello-001 \
  --output-prefix s3://my-bucket/runs/hello-001 \
  --tasks-jsonl artifacts/hello-001/tasks.jsonl \
  --require-complete

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
- IAM roles needed by workers
- generic worker job definition

See `infra/opentofu/README.md`.

## License

Apache-2.0.
