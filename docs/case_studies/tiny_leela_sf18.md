# Case study: Tiny Leela Stockfish 18 labeling (June 2026, SweetSpot 0.1)

The workload SweetSpot was built around, and what its first large run taught.

## Workload

Root-position Stockfish 18 labels (depth 10, MultiPV 2) for a 10M-position supervised chess dataset, us-west-2,
digest-pinned worker image, inputs as zstd JSONL shards in S3.

## What happened

- 29 of 40 output shards completed; 11 were missing, so the run could not be published.
- Tail shards of 250k rows exceeded the 7200 s task timeout.
- The run shared a work queue with earlier experiments, so queue depth no longer meant anything for this run.
- The launch command watched the run in the foreground and tied up the operator's agent session for hours.
- 1-vCPU canaries (depth 10, MultiPV 1) found large differences between lanes:

| lane | rows/s per worker | $ per 10M rows (Spot, at the time) |
|---|---|---|
| c7g.medium | ~64.4 | ~$0.26 |
| c7a.medium | ~75.1 | ~$0.30 |
| c6g.medium | ~42.9 | ~$0.67 |

  Burstable `t3`/`t4g` small/micro types were rejected by managed AWS Batch before any worker ran.

## What changed in 0.2 because of it

- **Small tasks.** `sweetspot tasks --chunks K` splits each shard into row ranges, so no task comes near its timeout,
  and a lost Spot host costs minutes, not hours.
- **A queue per run.** Depth, DLQ and cancellation belong to one run. `status` reports done/failing from S3 markers,
  not from queue depth.
- **No foreground watching.** `submit` returns as soon as workers are launched; `status` is a quick one-shot check
  that says what to do next.
- **Measured lane choice.** Workers record instance type, units and elapsed time in every done marker;
  `sweetspot scout --run` turns a small run into $ per million units per instance type.
- **Liveness.** The first version let every worker exit while failed tasks were still backing off. Now one worker
  (array child 0) stays while anything is in flight, failed tasks go to a DLQ after 5 deliveries, and
  `submit --retry` re-enqueues them after a fix.
