# SweetSpot lifecycle report schemas

SweetSpot lifecycle commands are designed for unattended agents and CI jobs. JSON reports are stable enough for automation to inspect top-level status fields, while remaining additive: consumers should ignore unknown fields.

## Common conventions

- Timestamps are UTC ISO-8601 strings ending in `Z`.
- Local artifact paths are written as strings.
- S3 locations are full `s3://bucket/prefix/...` URIs.
- Commands that read `run_state.json` include a `run_context` object with reconstructed bindings such as `run_id`, `artifact_dir`, `queue_url`, `dlq_url`, `batch_job_queue`, `job_name_prefix`, `output_prefix`, and task/finalizer artifact paths.
- Non-zero lifecycle blockers are reported as structured objects with a `code` and `details` field when possible.

## `sweetspot status RUN_ID --from-state`

Schema: `sweetspot.status.v1`

Purpose: non-mutating checkpoint for run artifacts, queues, Batch workers, and S3 done-marker progress.

Key fields:

- `schema`, `checked_at`
- `run`: run id, artifact directory, task count, artifact warnings
- `run_context`: reconstructed state bindings
- `queue`: source queue URL/depth when known
- `dlq`: DLQ URL/depth when known
- `batch`: job queue, job-name prefix, active job count/status/examples
- `output_s3`: output prefix and done-marker progress when known

## `sweetspot finish RUN_ID --from-state`

Schema: `sweetspot.finish_report.v1`

Purpose: enforce the closeout checklist before finalization and optional READY publish.

Key fields:

- `ok`: true only when drain checks and finalizer completed successfully
- `checked_at`, `run_id`
- `checks`: queue, DLQ, Batch-active, output-prefix, and finalizer readiness checks
- `blockers`: structured reasons preventing finalization/READY
- `finalizer`: embedded finalizer summary when run
- `artifacts.finish_report`: local `finish_report.json` path
- `cleanup_recommendation`: next cleanup command

## `sweetspot explain RUN_ID --from-state`

Schema: `sweetspot.lifecycle_explain.v1`

Purpose: human/agent reconstruction of the lifecycle without mutating AWS.

Key fields:

- `run_id`, `checked_at`, `outcome`
- `run_context`
- `artifacts`: presence and paths for run state, production tasks, finalizer outputs, final manifest, finish report, cleanup report
- `finalizer`: complete/task/done/output/missing counts when available
- `ready`: READY marker status/location when known
- `next_actions`: recommended commands or operator steps

Use `--format text` for an operator-readable summary and default JSON for automation.

## `sweetspot postmortem RUN_ID --from-state`

Schema: `sweetspot.lifecycle_postmortem.v1`

Purpose: durable closeout record for the run.

Key fields:

- `run_id`, `generated_at`, `outcome`
- `summary`: task/output/missing counts and READY/finalizer status
- `timeline`: recovered timestamps from run state, finalizer, finish, and artifacts
- `artifacts`: important local and S3 artifact locations
- `recommendations`: next operational/product follow-ups

Use `--format markdown` for a shareable narrative report or JSON for tooling.

## `sweetspot cleanup RUN_ID --from-state`

Schema: `sweetspot.cleanup_report.v1`

Purpose: conservative report-only cleanup plan. Destructive SQS, DLQ, S3, and Batch-capacity actions remain explicit admin/operator actions.

Key fields:

- `ok`: true when cleanup is unblocked; false when operator review is required
- `mode`: `dry_run` or `apply` (current apply remains conservative/report-only)
- `observations`: source queue, DLQ, Batch active jobs, scoped worker prefix, finalizer/READY state
- `blockers`: non-empty queues/DLQ, active Batch jobs, unsafe prefix, or incomplete finalizer evidence
- `recommendations`: exact conservative follow-up guidance
- `artifacts.cleanup_report`: local `cleanup_report.json` path

Exit code is `2` when blockers are present.

## Binding-drift errors

Schema: `sweetspot.lifecycle_error.v1`, `reason=binding_drift`

Purpose: refuse unsafe lifecycle overrides that conflict with `run_state.json`.

Key fields:

- `run_id`, `field`
- `expected`: controller-recorded value
- `actual`: override/current value that conflicts
- `diagnostic.recorded.source/value`
- `diagnostic.override.source/value`
- `diagnostic.unsafe_reason`
- `recovery.command`: exact state-bound command to rerun without the conflicting override
- `recovery.explain_command`: command to inspect reconstructed state

## Run queue creation denial

When `sweetspot run --dedicated-run-queue --create-run-queue` receives an SQS authorization denial, it exits with a `run_queue_create_denied` message. The message includes:

- the queue name SweetSpot attempted to create
- the AWS error code/message
- a `sweetspot admin doctor --check-run-queue-create ...` preflight command
- required fallback queue settings, including visibility timeout and DLQ/redrive binding
- a safe pre-provisioned-queue rerun pattern

Do not silently reuse a shared/canary queue with stale messages as the fallback.
