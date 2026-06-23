# Reliability contract

Miser / `spotbatch` is designed for trusted, idempotent AWS Batch Spot work.

The SQS queue is a trusted control plane. A queued task chooses the worker command, so queue producers must be trusted and commands must be safe to run at least once. Miser does not make arbitrary external side effects exactly-once.

Worker algorithm:

```text
receive SQS message
parse task JSON
validate heartbeat/visibility/timeouts below SQS hard limits
compute stable task hash and generated attempt id
if done_s3 exists:
  validate marker schema/run/task/hash/output/checksum
  delete message only if the marker is valid
  exit/sleep for next message
start heartbeat to extend visibility timeout
run task command with a bounded timeout
if output_s3 exists and command did not write SPOTBATCH_OUTPUT_PATH:
  upload attempt-scoped failure summary_s3 if configured
  do not upload done_s3
if command wrote SPOTBATCH_OUTPUT_PATH and output_s3 exists:
  upload output to an immutable attempt-scoped URI with sha256 metadata
upload attempt-scoped summary/logs
conditionally upload canonical done_s3 with If-None-Match: *
if another attempt already wrote done_s3, validate the winning marker
only then delete SQS message
```

Failure behavior:

- Spot host terminated before delete: SQS visibility timeout expires; task is retried.
- The worker caps task timeouts below SQS's 12-hour maximum visibility window; longer work must be checkpointed or split.
- Heartbeat/lease-renewal failures are emitted as structured stderr JSON events.
- Command fails or times out: worker does not delete message; task is retried.
- Expected output missing for a task with `output_s3`: worker writes no done marker, does not delete the message, and the task is retried.
- Attempt output exists without done marker: task is considered incomplete and will be reprocessed; orphan attempt objects are safe to garbage-collect after retention.
- Repeated poison task: SQS redrive policy moves message to DLQ.
- Task-provided environment variables may not override reserved `SPOTBATCH_*`, `AWS_*`, or `ECS_*` names.

Why done markers are the source of truth:

- S3 object uploads are not a transaction.
- A partial task may leave output or summary behind.
- Uploading a small deterministic done marker last gives finalizers and workers a stable idempotency check.
- Done marker v2 records the task hash, attempt id, logical output URI, actual immutable output URI, size, and SHA-256.
- The canonical marker is written conditionally; duplicate attempts either win exactly one marker or validate the winner before acknowledging SQS.
