# Case study: Tiny Leela Stockfish 18 labeling run

This is a real operator case study from the Tiny Leela Stockfish 18 root-labeling workload.  It captures the evidence that drove SweetSpot's high-level controller design: `plan → run --apply → status → repair/cancel`.

## Workload

- Project: Tiny Leela chess training data pipeline.
- Job: root-position Stockfish 18 labels for a 10M-position supervised dataset sample.
- Run ID: `sweetspot-stockfish-root-10m-d10-mpv2-20260624`.
- Output prefix: `s3://example-sweetspot-bucket/sweetspot_stockfish_root_10m_d10_mpv2_20260624`.
- Worker image: digest-pinned SweetSpot/Tiny-Leela Stockfish worker image in ECR.
- Worker command: SQS task worker invoking the Tiny Leela Stockfish labeling script.
- AWS region: `us-west-2`.

## Observed outcome

The first large production attempt did useful work but did not safely reach `READY`:

- 29 of 40 output shards completed.
- 11 shards were still missing.
- The run could not publish a `READY` marker.
- Existing shared work queue depth made a clean follow-up measurement hard without a fresh queue/DLQ.
- Tail shards at 250k rows/task, depth 10, MultiPV 2 exceeded the 7200s timeout.

## What SweetSpot learned

1. **Plan must be authoritative for apply.**  The submitted worker vCPU, memory, target architecture, region, worker count, task timeout, SQS visibility timeout, and heartbeat interval now come from Plan JSON.  Legacy sizing/timing flags are rejected for controller-owned apply.
2. **Deployment identity must be bound before mutation.**  `run --apply --deployment` validates a deployment registry, requires digest-pinned images, checks the Batch job definition image, and verifies the local JSONL against the S3 manifest identity before enqueueing or submitting workers.
3. **One-shot submission is not enough, but extra mutations must fail closed.**  Production apply now persists run state, enqueues idempotently, submits the initial Plan-sized worker wave, then performs bounded reconciliation observations. Queue-depth backlog accounting is allowed only for dedicated queues, and automatic top-up mutation remains blocked until each extra submit can be durably recorded before the Batch call.
4. **Canaries must be controller-owned but fail closed until routing is safe.**  Initial canary Plans materialize reviewed canary task artifacts, and automatic mutation is blocked until each candidate can be routed without shared-queue receive-count/DLQ contamination.
5. **Telemetry drives timing.**  Planner output includes task timeout, visibility timeout, and heartbeat settings derived from target task runtime instead of leaving these as independent operator guesses.
6. **ARM is opt-in, not default.**  ARM/Graviton candidates may be cheaper, but the workload and image must pass canary evidence first; mixed architecture deployments should use separate queue/job-definition targets.

## Recommended replay workflow

```bash
sweetspot plan job.json \
  --input-manifest-jsonl manifest.jsonl \
  --out-canary-tasks-jsonl artifacts/run/canary_tasks.jsonl

sweetspot run job.json \
  --input-manifest-jsonl manifest.jsonl \
  --deployment deployment.json \
  --artifact-dir artifacts/run \
  --apply

sweetspot status RUN_ID \
  --artifact-dir artifacts/run \
  --queue-url QUEUE_URL \
  --dlq-url DLQ_URL \
  --job-queue JOB_QUEUE
```

After canary summaries are collected, rerun `sweetspot plan`/`sweetspot run --apply` with `--canary-summary-jsonl` to produce and launch calibrated production tasks.  Use `sweetspot repair` only from persisted task/status artifacts; do not purge shared SQS queues to recover a run.
