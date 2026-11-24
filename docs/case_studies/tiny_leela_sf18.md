# Case study: Tiny Leela Stockfish 18 labeling run

This is a real operator case study from the Tiny Leela Stockfish 18 root-labeling workload.  It captures the evidence that drove SweetSpot's high-level controller design: `plan → run --apply → status --from-state → finish --from-state → explain/postmortem → cleanup --from-state → repair/cancel`.

## Workload

- Project: Tiny Leela chess training data pipeline.
- Job: root-position Stockfish 18 labels for a 10M-position supervised dataset sample.
- Run ID: `sweetspot-stockfish-root-10m-d10-mpv2-20260624`.
- Output prefix: `s3://<results-bucket>/sweetspot_stockfish_root_10m_d10_mpv2_20260624`.
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
3. **One-shot submission is not enough, but extra mutations must be durable.**  Production apply now persists run state, enqueues idempotently, submits the initial Plan-sized worker wave, then performs bounded reconciliation. Queue-depth backlog accounting is allowed only for dedicated queues; every top-up worker is recorded as in-flight before the Batch submit call, while shared queues remain conservative/observational.
4. **Canaries must be controller-owned and isolated.**  Initial canary Plans materialize reviewed canary task artifacts; `run --apply --deployment` can launch them only when the deployment registry provides isolated per-candidate `canary_routes`, with durable in-flight state before enqueue and Batch mutations. Missing routes still fail closed rather than mixing candidates on a shared queue.
5. **Telemetry drives timing.**  Planner output includes task timeout, visibility timeout, and heartbeat settings derived from target task runtime instead of leaving these as independent operator guesses.
6. **ARM is opt-in, not default.**  ARM/Graviton candidates may be cheaper, but the workload and image must pass canary evidence first; mixed architecture deployments should use separate queue/job-definition targets.
7. **Smallest supported lanes need measured ranking, not family assumptions.**  Follow-up 1-vCPU medium canaries for the depth-10/MultiPV-1 workload found `c7g.medium` cheapest in the sample (~64.4 rows/s, ~$0.26 per 10M rows), `c7a.medium` fastest (~75.1 rows/s, ~$0.30 per 10M rows), and `c6g.medium` supported but worse (~42.9 rows/s, ~$0.67 per 10M rows). Modern `t3*`/`t4g*` small/micro lanes were rejected by managed AWS Batch before workers could run; legacy `m1.small` ran but was slower/costlier than modern medium lanes.
8. **Production launch should not monopolize an interactive agent.**  The first production attempt used a foreground `--reconcile-until-drained` watch and tied up the coding agent. For reviewed production launches, use `--kickoff-only` to enqueue tasks and submit the Plan-sized worker wave, then move progress checks to `sweetspot monitor` / `sweetspot status --from-state` from a scheduled/CI monitor. Keep foreground polling for active diagnosis only.
9. **Closeout must be state-driven.**  After drain, `sweetspot finish RUN_ID --from-state --publish-ready` reconstructs the output prefix, task JSONL, queue/DLQ, Batch queue, and job-name prefix from `run_state.json`; it blocks on non-empty queues, DLQ messages, active Batch jobs, incomplete finalization, or unscoped worker prefixes before publishing READY. `sweetspot explain` and `sweetspot postmortem` turn the same state and finalizer artifacts into operator-facing closeout evidence. `sweetspot cleanup --from-state --dry-run` adds a conservative cleanup report without purging queues or resizing compute unless a future reviewed explicit action is added.
10. **Dedicated run queues need IAM preflight or an explicit fallback.**  In this account, the operator could update existing resources but lacked `sqs:CreateQueue`, so a reviewed empty pre-provisioned queue was used as the run queue and the fallback was recorded in the deployment artifact. `sweetspot admin doctor --check-run-queue-create --run-queue-name NAME` now preflights `sqs:CreateQueue`, queue tagging, visibility timeout/redrive policy updates, and DLQ redrive-allow-policy permissions before production mutation.

## Recommended replay workflow

```bash
sweetspot plan job.json \
  --input-manifest-jsonl manifest.jsonl \
  --out-canary-tasks-jsonl artifacts/run/canary_tasks.jsonl

sweetspot run job.json \
  --input-manifest-jsonl manifest.jsonl \
  --deployment deployment.json \
  --artifact-dir artifacts/run \
  --apply \
  --kickoff-only

# Generate a scheduler/CI checkpoint command rather than blocking the launch agent.
sweetspot monitor RUN_ID \
  --artifact-dir artifacts/run \
  --queue-url QUEUE_URL \
  --dlq-url DLQ_URL \
  --job-queue JOB_QUEUE \
  --output-prefix s3://bucket/run \
  --emit-command

sweetspot status RUN_ID \
  --artifact-dir artifacts/run \
  --from-state

sweetspot finish RUN_ID \
  --artifact-dir artifacts/run \
  --from-state \
  --publish-ready

sweetspot explain RUN_ID \
  --artifact-dir artifacts/run \
  --from-state \
  --format text

sweetspot postmortem RUN_ID \
  --artifact-dir artifacts/run \
  --from-state \
  --format markdown

sweetspot cleanup RUN_ID \
  --artifact-dir artifacts/run \
  --from-state \
  --dry-run
```

The canary apply step requires `deployment.json` to map every candidate shape (for example `x86_64-1vcpu-2048mib`) to its own SQS queue and architecture-compatible Batch job definition. After canary summaries are collected, rerun `sweetspot plan`/`sweetspot run --apply` with `--canary-summary-jsonl` to produce and launch calibrated production tasks.  Use `sweetspot repair` only from persisted task/status artifacts; do not purge shared SQS queues to recover a run.
