# SweetSpot run lifecycle inventory

This inventory supports M007 S01. It maps the expert-proposed lifecycle state machine to the current SweetSpot runtime artifacts and commands before the formal state contract is implemented.

## Current lifecycle surfaces

| Surface | Current role | Evidence | State-machine implication |
|---|---|---|---|
| `sweetspot run` without `--apply` | Builds a local plan/report and can materialize canary or production task files depending on options. | `sweetspot/cli.py` `cmd_run`, `_materialize_run_tasks`, `_write_canary_tasks_from_plan`, `_write_production_tasks_from_plan`; `sweetspot/planner.py` plan schema. | Supports `PLANNING`, `CANARY_MATERIALIZED`, and `PLAN_READY`, but does not name them as lifecycle states yet. |
| `run_state.json` | Controller state persisted before SQS or Batch mutation. It stores run identity, bindings, controller metadata, and a `phases` array. | `sweetspot/run_state.py`; `sweetspot/cli.py` controller apply paths. | Existing phases should be input facts for state evaluation, not replaced by a second source of truth. |
| `phases` inside `run_state.json` | Tracks low-level controller phases such as `plan`, `enqueue_tasks`, and `submit_workers` with status/progress fields. | `sweetspot/run_state.py` exposes `phase_by_name`, `phase_completed`, `APPLY_PROGRESS_PHASES = ("enqueue_tasks", "submit_workers")`. | Directly supports `PRODUCTION_ENQUEUED` and `WORKERS_RUNNING`; cannot alone distinguish collection, draining, finalizing, or review-required states. |
| Dedicated run queue metadata | Captures run-scoped SQS queue binding when configured. | `RunContext.run_queue`; CLI controller queue helpers. | Useful fact for deciding whether `cleanup` can safely inspect or delete run-scoped infrastructure. |
| Worker output and status files | Workers emit task status, done markers, logs, and output manifests. | `sweetspot/worker.py`; `RunContext.task_status_jsonl`, `outputs_manifest_jsonl`; tests around worker behavior. | Supports `CANARY_RUNNING`, `CANARY_COLLECTING`, `WORKERS_RUNNING`, and `DRAINING` by inference from task progress and queue depth, but current state names are not explicit. |
| Finalizer service | Produces final manifest and can identify incomplete or invalid artifacts. | `sweetspot/finalize_service.py`; `RunContext.final_manifest_json`; `cmd_finalize`; `cmd_finish`. | Supports `FINALIZING`, `COMPLETE`, `NEEDS_REPAIR`, and `FAILED_REVIEW_REQUIRED` style outcomes, but current CLI names use `ready_to_finish`, `repair_needed`, `blocked`, and `finished`. |
| Lifecycle report docs | Document schemas for `status`, `finish`, `explain`, `postmortem`, and `cleanup`. | `docs/lifecycle_reports.md`. | Existing report schemas are the compatibility base. M007 should add canonical state fields additively rather than breaking these reports. |
| `sweetspot status RUN_ID --from-state` | Reconstructs a `RunContext`, reads local state, and combines it with runtime queue/Batch/S3 observations when available. | `cmd_status`; `load_run_context`. | Already supports state-derived status, but reports operational observations rather than canonical state machine position. |
| `sweetspot explain RUN_ID --from-state` | Builds a lifecycle explanation from `RunContext`, finalizer artifacts, finish reports, and run status. | `_lifecycle_outcome`, `_lifecycle_next_actions`, `_build_lifecycle_report`, `cmd_explain`. | Closest current precursor to the state machine; should become the first consumer of canonical evaluator output. |
| `sweetspot finish RUN_ID --from-state` | Reconstructs finalizer arguments from local state and blocks when source or DLQ queues are not drained. | `cmd_finish`, `_finish_finalizer_args`, `_finalize_args_from_state`. | Already encodes safety gates for `DRAINING` to `FINALIZING`; needs explicit state-aware refusal reasons. |
| `sweetspot cleanup RUN_ID --from-state` | Requires `--from-state`, inspects queues, and reports blockers before cleanup. | `cmd_cleanup`. | Already cautious; should become dry-run or confirmation-driven cleanup from canonical states. |
| Repair commands | Generate repair plans or enqueue repair work from failed/incomplete outputs. | `cmd_repair_plan`, `cmd_repair`, `RunContext.repair_tasks_jsonl`. | Supports `NEEDS_REPAIR` and `REPAIR_RUNNING`, but those are currently inferred from finalizer/report artifacts. |

## Proposed state support map

| Proposed state | Current support | Primary current signals | Notes and gaps |
|---|---|---|---|
| `NEW` | Directly inferable | No artifact directory or no `run_state.json`; job spec exists. | Needs clear distinction between never-started and malformed/missing run ID. |
| `PLANNING` | Partially inferable | Plan command running or local plan report exists before task materialization. | No durable in-progress marker for planning today. Usually only observed after completion as plan artifacts. |
| `CANARY_MATERIALIZED` | Directly inferable | Canary task JSONL exists; `run_state.json` has canary binding or artifacts include canary task hash. | Should capture task count and manifest binding as known facts. |
| `CANARY_RUNNING` | Partially inferable | Canary tasks enqueued/submitted or worker status exists for canary task set. | Current controller phases focus on apply/enqueue/submit; canary-specific running state may need clearer markers. |
| `CANARY_COLLECTING` | Partially inferable | Canary summary or task status/output markers exist but production plan not yet ready. | Current adaptive planning consumes summaries but does not name collection as a state. |
| `PLAN_READY` | Directly inferable | Production plan exists, plan status ready, production tasks can be rendered or already exist without apply. | Existing plan schema has `status` values such as `ready` and `blocked`. |
| `PRODUCTION_ENQUEUED` | Directly inferable | `run_state.json` phase `enqueue_tasks` completed or in progress. | Existing phase progress can identify completed, in-progress, and resume positions. |
| `WORKERS_RUNNING` | Directly inferable | `run_state.json` phase `submit_workers` completed or active Batch workers observed. | Queue depth and active worker count make this stronger when live AWS is available; local artifacts can still infer submitted intent. |
| `DRAINING` | Partially inferable | Source queue empty or draining, outputs/status manifests incomplete, final manifest absent. | Needs a local-only approximation and optional live queue facts. |
| `FINALIZING` | Partially inferable | Finalizer command in progress or finish report/final manifest being written. | No durable start marker unless finalizer writes partial report. S02 may need a conservative state based on finalizer artifacts. |
| `COMPLETE` | Directly inferable | Final manifest `complete: true` or finish report `ok: true`. | Current outcome names include `finalized_complete` and `finished`. |
| `NEEDS_REPAIR` | Directly inferable | Final manifest exists with `complete` false or current lifecycle outcome `repair_needed`. | Should include repair task path and missing/failed outputs as facts. |
| `REPAIR_RUNNING` | Partially inferable | Repair task JSONL exists and repair enqueue/worker activity is visible. | Existing repair commands do not yet appear to persist a dedicated repair-running phase. |
| `BLOCKED` | Directly inferable | Finish report has `blocked`; plan/report blockers; queue blockers for finish/cleanup. | Should separate temporary blockers from failed-review states. |
| `CANCELLED` | Partially inferable | Cancel commands exist for Batch jobs, but run-level cancellation marker is not obvious. | Needs explicit local marker or documented unsupported gap. |
| `FAILED_REVIEW_REQUIRED` | Partially inferable | Invalid artifacts, malformed state, finalizer validation failures, or unsafe drift detected. | Current outcomes include `invalid_artifacts`; contract should define review-required criteria. |

## Existing outcome vocabulary to preserve

Current `explain` and lifecycle helpers use these outcome-style values:

- `finished`
- `blocked`
- `finalized_complete`
- `repair_needed`
- `ready_to_finish`
- `invalid_artifacts`
- `in_progress`
- `unknown`

M007 should not remove those values from existing reports without a compatibility plan. The safest approach is to add a canonical `state` field and keep legacy outcome fields additive for at least one milestone.

## Known facts available today

A state evaluator can derive these facts from existing local artifacts:

- `run_id`
- `artifact_dir`
- `job_spec_sha256`
- deployment binding hash
- region/profile-derived runtime bindings when recorded
- source queue URL and DLQ URL when recorded
- Batch job queue and job name prefix when recorded
- plan presence and plan status
- production task file path and task count
- canary task file path and task count when materialized
- task status JSONL path and count when present
- repair task JSONL path and count when present
- outputs manifest path and count when present
- final manifest path and completeness when present
- finish report path, `ok`, and blockers when present
- controller phases and phase progress from `run_state.json`
- run-scoped queue metadata when present
- warnings emitted while reconstructing `RunContext`

## Missing or ambiguous facts

These facts are not reliably persisted today and should either be added later or treated conservatively:

- durable planning started timestamp
- durable canary running versus canary collecting marker
- durable finalizer started marker
- explicit run-level cancellation marker
- explicit repair-running marker
- distinction between temporarily blocked and failed-review-required in all cases
- local-only proof that AWS Batch workers are still running after submission without live AWS reads
- cleanup confirmation history

## Recommended S02 evaluator shape

S02 should expose a pure local evaluator first, with optional live observations layered later by existing commands:

```json
{
  "schema": "sweetspot.lifecycle_state.v1",
  "run_id": "RUN_ID",
  "state": "PLAN_READY",
  "legacy_outcome": "ready_to_finish",
  "terminal": false,
  "known_facts": {},
  "missing_facts": [],
  "safe_actions": [],
  "unsafe_actions": [],
  "recommended_commands": [],
  "evidence": [],
  "warnings": []
}
```

The evaluator should be deterministic over local files. Commands such as `status`, `finish`, and `cleanup` can add live queue or Batch observations without making the core state contract depend on AWS.

## Open design decisions for S01 contract

1. Whether state values should be uppercase (`PLAN_READY`) or lower snake case (`plan_ready`) in JSON. Uppercase matches the expert model; lower snake case matches existing SweetSpot JSON style better.
2. Whether `COMPLETE` should mean final manifest complete, finish report ok, or either. Current behavior distinguishes `finalized_complete` and `finished`.
3. Whether `CANCELLED` requires a new local marker or remains unsupported until cancellation writes run-level state.
4. Whether `FAILED_REVIEW_REQUIRED` should be a terminal state or a blocked side path that can transition to repair.
