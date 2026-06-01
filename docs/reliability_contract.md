# Reliability contract

`aws-batch-job-runner` / `spotbatch` is designed for AWS Spot interruption.

Worker algorithm:

```text
receive SQS message
parse task JSON
if done_s3 exists:
  delete message
  exit/sleep for next message
start heartbeat to extend visibility timeout
run task command with a bounded timeout
if output_s3 exists and command did not write SPOTBATCH_OUTPUT_PATH:
  upload failure summary_s3 if configured
  do not upload done_s3
if command wrote SPOTBATCH_OUTPUT_PATH and output_s3 exists:
  upload output
upload summary_s3
upload done_s3 last
only then delete SQS message
```

Failure behavior:

- Spot host terminated before delete: SQS visibility timeout expires; task is retried.
- Command fails or times out: worker does not delete message; task is retried.
- Expected output missing for a task with `output_s3`: worker writes no done marker, does not delete the message, and the task is retried.
- Output exists without done marker: task is considered incomplete and will be reprocessed.
- Repeated poison task: SQS redrive policy moves message to DLQ.

Why done markers are the source of truth:

- S3 object uploads are not a transaction.
- A partial task may leave output or summary behind.
- Uploading a small deterministic done marker last gives finalizers and workers a stable idempotency check.
