# Reliability contract

SweetSpot / `sweetspot` is designed for trusted, idempotent AWS Batch Spot work.

The SQS queue is a trusted control plane. A queued task chooses the worker command, so queue producers must be trusted and commands must be safe to run at least once. SweetSpot does not make arbitrary external side effects exactly-once.

Worker algorithm:

```text
receive SQS message
parse task JSON
validate heartbeat/visibility/timeouts below SQS hard limits
validate task schema, identifiers, command shape, env, timeout, marker URIs, and allowed S3 prefixes
compute stable task hash and generated attempt id
if done_s3 exists:
  validate marker schema/run/task/hash/output/checksum
  delete message only if the marker is valid
  exit/sleep for next message
start heartbeat to extend visibility timeout
run task command with a bounded timeout while streaming redacted stdout/stderr to container logs
if output_s3 exists and command did not write SWEETSPOT_OUTPUT_PATH:
  upload attempt-scoped failure summary_s3 if configured
  do not upload done_s3
if command wrote SWEETSPOT_OUTPUT_PATH and output_s3 exists:
  upload output to an immutable attempt-scoped URI with sha256 metadata
upload attempt-scoped summary plus capped/redacted stdout and stderr logs
conditionally upload canonical done_s3 with If-None-Match: *
if another attempt already wrote done_s3, validate the winning marker
only then delete SQS message
```

Failure behavior:

- Spot host terminated before delete: SQS visibility timeout expires; task is retried.
- The worker caps task timeouts below SQS's 12-hour maximum visibility window; longer work must be checkpointed or split.
- Heartbeat/lease-renewal successes and major task decisions are emitted as structured `sweetspot.worker_event.v1` JSON events; heartbeat failures keep the `sweetspot.heartbeat_error.v1` stderr schema.
- Command fails or times out: worker does not delete message; task is retried.
- Expected output missing for a task with `output_s3`: worker writes no done marker, does not delete the message, and the task is retried.
- Attempt output exists without done marker: task is considered incomplete and will be reprocessed; orphan attempt objects are safe to garbage-collect after retention.
- Repeated poison task: SQS redrive policy moves message to DLQ.
- Task-provided environment variables may not override reserved `SWEETSPOT_*`, `AWS_*`, or `ECS_*` names.
- If allowed S3 prefixes are configured, every `s3://...` URI in the task payload must stay inside those prefixes; this complements the worker task role's bucket/prefix IAM scope.
- Task stdout/stderr summaries are bounded tails, not whole-file reads. Uploaded attempt logs are capped by `SWEETSPOT_MAX_LOG_BYTES` / `--max-log-bytes` and redacted by configured regexes before streaming/upload. Redaction fails closed for overlong unterminated records by suppressing the record with a placeholder.

Why done markers are the source of truth:

- S3 object uploads are not a transaction.
- A partial task may leave output or summary behind.
- Uploading a small deterministic done marker last gives finalizers and workers a stable idempotency check.
- Done marker v2 records the task hash, attempt id, logical output URI, actual immutable output URI, size, and SHA-256.
- The canonical marker is written conditionally; duplicate attempts either win exactly one marker or validate the winner before acknowledging SQS.

Cost telemetry and optimization:

- Commands may write a JSON object to `SWEETSPOT_METRICS_PATH` with `completed_units`, `useful_compute_seconds`, `input_bytes`, `output_bytes`, or `bytes_transferred`.
- Task summaries include best-effort runtime metadata plus startup delay, retry/receive count, interruption/failure status, transferred bytes, useful throughput, and discarded compute seconds.
- `sweetspot-scout` consumes summary telemetry and ranks pools by expected total cost, including compute, replay, startup overhead, transfer, NAT/endpoints, CloudWatch logs, and S3 storage/request assumptions.
- `sweetspot-lane-manager` allocates cost-annotated lanes cheapest-first among placement-score-eligible lanes.

Finalization and cleanup:

- `sweetspot finalize` streams task JSONL and writes `task_status.jsonl`, `repair_tasks.jsonl`, and `outputs.jsonl` instead of retaining every task/status record in memory.
- The finalizer rejects duplicate task IDs while streaming, validates marker contents, and verifies v2 output size/SHA metadata before counting a task complete.
- `--use-listing-index` preloads default run prefixes (`done/`, `shards/`, `summaries/`) with S3 listings to avoid many per-task existence HEAD requests; extra prefixes can be added with `--preload-s3-prefix`.
- The final manifest inlines only a bounded `outputs` sample (`--max-inline-outputs`) and points to the complete outputs manifest.
- For versioned buckets, cleanup must use `sweetspot s3-delete-prefix --include-versions` or an S3 lifecycle policy that expires noncurrent versions/delete markers; deleting only current keys does not reclaim all storage.
- Whole-DLQ redrive should use native SQS `StartMessageMoveTask` via `sweetspot dlq --native-redrive --apply` when unfiltered redrive is acceptable. Filtered/manual receive-send-delete redrive remains available for small targeted repairs.

Deployment reproducibility and safety:

- CI is expected to run unit tests, Ruff formatting/linting, typing, OpenTofu formatting/validation with the committed provider lock, and a worker-image build that emits SBOM/provenance attestations and runs vulnerability scanning.
- The worker image pins its base image by digest and runs as an unprivileged `sweetspot` user.
- The OpenTofu module defaults to a no-ingress Batch security group, SQS SSE, IMDSv2-required Batch instances, encrypted root volumes, longer DLQ retention than source retention, and a redrive allow policy restricted to the module's source queue.
