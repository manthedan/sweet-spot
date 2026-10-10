# Design

How SweetSpot keeps runs correct and cheap, and why it is shaped this way. README.md covers usage.

## Shape

A **run** is a fixed list of tasks plus everything needed to execute and audit it:

- S3, under `<prefix>/<run>/`: the ledger, done markers, outputs, attempt logs, failure records, launch and
  enqueue records, and the published manifest. S3 is the source of truth, so any machine with credentials can
  inspect, resume, or finish a run; nothing important lives on the machine that submitted it.
- SQS: one work queue and one DLQ per run (`<project>-<run>`, `<project>-<run>_dlq`). A shared queue made one
  run's depth, retries and cancellation meaningless for another (see the case study).
- Batch: one job definition per run, registered at each launch, and one array job per launch.

Long-lived infrastructure (compute environments, job queues, roles) comes from `infra/opentofu`; per-run
resources are created by the CLI and tagged `sweetspot-project`/`sweetspot-run`. The CLI never adopts, counts or
deletes a queue, job or job definition whose tags name another run.

Run names are lowercase letters, digits and single hyphens. That keeps the DLQ suffix (`_dlq`) and the job-name
separator (`--`) unambiguous, and Batch's case-insensitive name filter safe.

## Tasks

`{"id", "command", "inputs"?, "env"?, "timeout"?, "cwd"?, "outputs"?, "units"?}`. The worker downloads `inputs`
into `{in}`, runs `command` with `{in}`/`{out}`/`{id}` substituted, and treats the files in `{out}` (filtered by
the `outputs` globs) as the result. There is no escaping in substitution: escaping would mangle JSON arguments
containing `}}`. Existing scripts that take `--input X --out Y` run unchanged; the earlier version needed a
per-workload adapter.

A run's ledger is immutable: `tasks.jsonl` and `run.json` (which holds the ledger's sha256) are created with
conditional writes, and submitting a different list under the same name is an error before any AWS mutation.
The worker image may change between launches (to fix a crash), but every done marker records its image, the
manifest carries it per task, and `submit`/`finish` warn when a run's outputs come from more than one image. A fix
that changes results belongs in a new run.

## Completion protocol

1. A worker receives one message with a 5-minute visibility lease and renews it every minute from receipt until
   the message is acknowledged or released. If the host dies, the task reappears within the lease.
2. If `done/<id>.json` exists and its `task_hash` matches, the message is a duplicate: acknowledge it. A marker
   for a different task under the same id is an error, left for the DLQ.
3. Run the command in its own process group, reaping it with `wait4` (which also yields peak RSS and CPU time).
   Timeouts and interruptions kill the whole group.
4. On failure: write logs, a summary and a small `failures/<id>/<attempt>.json` (best effort), then release the
   message with exponential backoff. SQS moves it to the DLQ after `--max-attempts` deliveries (default 5).
5. On success: upload outputs under `attempts/<id>/<attempt>/out/`, then create `done/<id>.json` with
   `If-None-Match: *`. Exactly one attempt wins. The winner copies its files to `out/<id>/`; a loser deletes its
   staged files, so duplicate deliveries never mix two attempts' outputs. A 412 after a retried PUT is checked
   against the marker's attempt id: if the marker is ours, we won. Only then is the message deleted.
6. Diagnostics (logs, summaries, failure records) never fail an attempt; outputs and the marker do.

Crash at any step is safe: before the marker, the message reappears and the attempt is an orphan; after the marker
but before the copy, `finish` (or a `local` resume) repeats the copy from the marker's attempt; after the copy but
before the acknowledgement, the next delivery sees the marker and acknowledges.

Execution is at-least-once. Task commands should be safe to repeat.

## Worker lifecycle

- **Failure accounting.** A command that fails, times out, writes nothing, or can't find its input is the task's
  problem: the worker moves on. Only infrastructure errors (storage or network exceptions, failed staging other than
  missing inputs) count toward `max_infra_errors` (5 in a row), after which the worker exits 3 and is not replaced.
- **Interruption.** A Spot interruption notice (polled from IMDS) or SIGTERM kills the running task, releases its
  message with visibility 0, records an "interrupted" attempt and exits 75. The job definition retries exit 75 and
  `Host EC2*` up to 5 times, so lost workers are replaced. To fit ECS's 30 s stop grace: the task gets 5 s after
  SIGTERM, the release waits at most 5 s for an in-flight renewal, and lease calls use a client with short timeouts
  and two attempts. Diagnostics come last and may be cut off.
- **Liveness.** Ordinary workers leave after two empty polls. Array child 0 (or a single-worker job) is the
  sweeper: it stays while the queue reports anything visible, in flight or delayed, and leaves only when every
  ledger task has a marker, or after receiving nothing for longer than any message can stay invisible (the longest
  failure backoff plus a lease, ~22 minutes). SQS's counts can lag, so they only ever keep the sweeper longer.
  Messages held by dead workers and failed tasks in backoff therefore always get picked up.

## Submit, status, finish

`submit` is the only way to change a run: create it, add workers, resume it after `cancel`, or (with `--retry`)
move DLQ messages back into the queue with a fresh retry budget. Enqueueing happens when no enqueue wave has
completed yet (including one interrupted mid-way) or when the queue and DLQ are both empty while tasks remain.
SQS batches are packed under 256 KiB (an implementation cap below SQS's current 1 MiB limit).
`cancel` writes a fence record under `control/`; a `submit` that was already running checks it (by S3's clock)
before creating queues and before launching workers, so a cancel always wins over an in-flight submit.

`status` reads markers, failure records, queue counts and worker jobs, derives a state (`running`, `stalled`,
`failed`, `complete`, `finished`) and prints the next command.

`finish` checks every marker against the ledger. Before publication it copies each winner's staged files to `out/`
again while they exist (a size check cannot see same-length damage) and verifies sizes; once READY exists it never
writes to `out/`, only checks it. `--verify-hashes` reads every output back against its marker's sha256. It then publishes `manifest.jsonl`
(deterministic: ledger order, built from immutable markers) and `READY` (ledger and manifest sha256, file/byte/unit
totals, images), both with conditional writes. A repeated finish verifies instead of overwriting (and restores
`manifest.jsonl` if it no longer matches READY); a conflicting one fails without writing. It then terminates leftover workers, deletes the queues,
deregisters job definitions and deletes staged attempt outputs (summaries and logs stay for `scout` and `logs`).

## Choosing capacity

Markers record instance type, architecture, worker shape, image, units and elapsed time. `scout --run` groups them
by (instance type, arch, shape, image), takes median units per second per worker, adds replay from failed,
duplicate and interrupted attempts, and prices it with current Spot prices and memory-aware packing. Types without
measurements get $/worker-hour only; the tool never invents throughput. Lanes in the stack restrict which instance
types a run can land on; there is no planner.

## Known limits

- Control commands are not fully serialized. The cancel fence and READY re-check cover the cases that matter; a
  submit that passes its last check in the instant before a cancel can still launch workers that find no queue
  and exit.
- The sweeper trusts SQS's approximate counts not to read zero for longer than its ~22 minute grace while a
  message is held elsewhere. If they did, and the holder then died, that task would wait for the next `submit`.
- On interruption, the message release is bounded (about 25 s worst case); diagnostics written after it are best
  effort and may be cut off by the container's stop timeout.
- IAM can't stop an operator from terminating untagged Batch jobs in the same queues (Batch has no create-only
  tagging condition); see infra/opentofu/README.md.
- Without `--verify-hashes`, same-length damage to `out/` after the staged copies are deleted goes unnoticed.
- Not built: multi-region lanes, automatic shard sizing from measured runtimes.
