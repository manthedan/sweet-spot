# Reliability contract

`aws-spot-batch-runner` is designed for AWS Spot interruption.

Worker algorithm:

```text
receive SQS message
parse task JSON
if done_s3 exists:
  delete message
  exit/sleep for next message
start heartbeat to extend visibility timeout
run task command
if command wrote SPOTBATCH_OUTPUT_PATH and output_s3 exists:
  upload output
upload summary_s3
upload done_s3 last
only then delete SQS message
```

Failure behavior:

- Spot host terminated before delete: SQS visibility timeout expires; task is retried.
- Command fails: worker does not delete message; task is retried.
- Output exists without done marker: task is considered incomplete and will be reprocessed.
- Repeated poison task: SQS redrive policy moves message to DLQ.

Why done markers are the source of truth:

- S3 object uploads are not a transaction.
- A partial task may leave output or summary behind.
- Uploading a small deterministic done marker last gives finalizers and workers a stable idempotency check.
