# SweetSpot run lifecycle state machine

Status: draft contract for M007 S01  
Schema: `sweetspot.lifecycle_state.v1`

SweetSpot run recovery is modeled as an explicit state machine over local run artifacts. The state machine does not replace `run_state.json`; it evaluates existing artifacts and phases into a canonical, agent-readable lifecycle report.

## Goals

The state machine answers five questions for a run:

1. Where is this run?
2. What facts are known?
3. What facts are missing or ambiguous?
4. What is safe to do next?
5. What would be unsafe, and why?

This is intentionally more constrained than a DAG. DAG-style orchestration is deferred until SweetSpot can reliably explain and recover one run lifecycle.

## Compatibility rules

- Canonical state values are uppercase strings such as `PLAN_READY`.
- Existing report schemas and lower-case statuses remain additive compatibility surfaces.
- Existing lifecycle outcomes such as `ready_to_finish`, `repair_needed`, `blocked`, and `finished` may appear as `legacy_outcome` while consumers migrate.
- The evaluator is local-first and deterministic over artifacts. Commands may add live AWS observations, but the core state is not allowed to require live AWS reads.
- The evaluator must not log, persist, or infer secret values.
- Unsafe actions must be explicit. Missing evidence defaults to refusal or explanation, not mutation.

## Canonical states

### Main path

| State | Meaning | Required or typical evidence | Safe next actions | Unsafe actions |
|---|---|---|---|---|
| `NEW` | No durable run artifacts exist yet for the requested run. | Missing artifact directory or missing `run_state.json`; job spec may exist. | Create or inspect plan; run `sweetspot plan` or `sweetspot run` without mutation. | Finish, cleanup as completed run, repair, or assume AWS resources exist. |
| `PLANNING` | A plan is being produced or a planning artifact exists but is not yet ready for canary or production. | Job spec present; plan report in progress or plan status not ready. | Continue planning; inspect plan errors. | Enqueue production work or finalize. |
| `CANARY_MATERIALIZED` | Canary tasks have been rendered locally but are not proven running. | Canary task JSONL or canary binding/hash. | Enqueue or run canary path; inspect canary task count and manifest binding. | Promote production plan without canary observations when required. |
| `CANARY_RUNNING` | Canary work has been submitted or worker status indicates active canary processing. | Canary enqueue/submission evidence, worker status, live worker observation, or task status progress. | Monitor canary, collect outputs. | Finalize production or cleanup active canary resources. |
| `CANARY_COLLECTING` | Canary work has produced outputs/status but production plan is not ready. | Canary summary/status/output artifacts exist; production plan missing or blocked. | Build adaptive production plan; inspect failed canary tasks. | Submit production workers before plan readiness. |
| `PLAN_READY` | Production plan and local task artifacts are ready for controlled execution. | Plan status `ready`; production task JSONL exists or can be deterministically rendered; no apply progress. | Enqueue production tasks or run status/explain. | Finalize before production work completes; cleanup as complete. |
| `PRODUCTION_ENQUEUED` | Production tasks have been enqueued or enqueue is resumable. | `run_state.json` phase `enqueue_tasks` in progress or completed. | Resume enqueue if incomplete; submit or supervise workers if enqueue complete. | Re-plan with different bindings; finalize; destructive cleanup. |
| `WORKERS_RUNNING` | Production workers have been submitted or are expected to be processing queued tasks. | `run_state.json` phase `submit_workers` in progress/completed, active Batch workers, or worker status progress. | Monitor, supervise, or wait for drain. | Finish while source queue or DLQ still has work; delete queues. |
| `DRAINING` | Submission is complete and remaining work is draining toward finalization. | Source queue empty or shrinking, no active workers or workers nearing zero, outputs/status incomplete, final manifest absent. | Run status; when queues are empty, run finish/finalize. | Cleanup before finalization; assume complete without manifest. |
| `FINALIZING` | Finalizer is validating done markers and writing final outputs. | Finalizer invocation in progress, partial finalizer report, or final manifest being produced. | Let finalizer complete; inspect finalizer report. | Enqueue new work or cleanup finalizer inputs. |
| `COMPLETE` | Run has finished successfully. | Finish report `ok: true` or final manifest `complete: true`, with compatible run bindings. | Read final outputs; optional safe cleanup dry-run; postmortem/report. | Repair as failed run; mutate final artifacts without explicit operator intent. |

### Failure and side path states

| State | Meaning | Required or typical evidence | Safe next actions | Unsafe actions |
|---|---|---|---|---|
| `NEEDS_REPAIR` | Finalization found missing, failed, or invalid task outputs and repair work is possible. | Final manifest `complete: false`, finalizer report blockers, repair task JSONL, or legacy outcome `repair_needed`. | Run repair plan; enqueue repair work; inspect failed outputs. | Mark complete; cleanup required inputs. |
| `REPAIR_RUNNING` | Repair tasks have been generated and submitted or are expected to be processing. | Repair task JSONL plus enqueue/submission/status evidence. | Monitor repair; rerun finalizer after drain. | Run normal production enqueue with conflicting bindings; cleanup. |
| `BLOCKED` | A known blocker prevents the requested transition, but the run may still be recoverable. | Finish report `blocked`, queue blockers, missing permissions, incomplete artifacts, or safety refusal. | Follow recommended command; resolve blocker; rerun status/explain. | Force the blocked action without new evidence. |
| `CANCELLED` | The run has been explicitly cancelled at the run level. | Future explicit cancellation marker or compatible cancellation report. | Explain cancellation; cleanup only through guarded path. | Resume workers or mark complete without operator decision. |
| `FAILED_REVIEW_REQUIRED` | Artifact drift, malformed state, invalid bindings, or unsafe ambiguity requires human or agent review before repair. | Invalid JSON, binding mismatch, hash drift, invalid artifacts, untrusted state, or unresolved contradictory facts. | Run explain/postmortem; inspect artifacts; create repair or remediation plan. | Apply, finish, cleanup, or repair automatically. |

## Transition model

Transitions are event-driven and conservative. A command may only transition when required evidence is present.

```text
NEW
  -> PLANNING
  -> CANARY_MATERIALIZED
  -> CANARY_RUNNING
  -> CANARY_COLLECTING
  -> PLAN_READY
  -> PRODUCTION_ENQUEUED
  -> WORKERS_RUNNING
  -> DRAINING
  -> FINALIZING
  -> COMPLETE
```

Side paths:

```text
ANY non-terminal state -> BLOCKED
ANY ambiguous or drifted state -> FAILED_REVIEW_REQUIRED
FINALIZING -> NEEDS_REPAIR -> REPAIR_RUNNING -> DRAINING
ANY active state with explicit run-level cancellation -> CANCELLED
```

Terminal states:

- `COMPLETE`
- `CANCELLED`

Review-gated states:

- `FAILED_REVIEW_REQUIRED`
- `BLOCKED` when the blocker is safety-related or the next action would mutate external state

## Lifecycle report schema

A lifecycle state report is a JSON object with this required shape:

```json
{
  "schema": "sweetspot.lifecycle_state.v1",
  "run_id": "example-run",
  "artifact_dir": "artifacts/example-run",
  "state": "PLAN_READY",
  "legacy_outcome": "ready_to_finish",
  "terminal": false,
  "review_required": false,
  "generated_at": "2026-06-27T00:00:00Z",
  "known_facts": {},
  "missing_facts": [],
  "safe_actions": [],
  "unsafe_actions": [],
  "recommended_commands": [],
  "evidence": [],
  "warnings": []
}
```

### Required fields

| Field | Type | Meaning |
|---|---|---|
| `schema` | string | Must be `sweetspot.lifecycle_state.v1`. |
| `run_id` | string or null | Run identifier when known. |
| `artifact_dir` | string or null | Local artifact directory used for evaluation. |
| `state` | string | One canonical state from this document. |
| `legacy_outcome` | string or null | Existing lifecycle outcome, when available. |
| `terminal` | boolean | Whether no normal run progress should continue. |
| `review_required` | boolean | Whether a human or higher-level agent should inspect before mutation. |
| `generated_at` | string | UTC ISO-8601 timestamp ending in `Z`. |
| `known_facts` | object | Stable facts derived from local artifacts. |
| `missing_facts` | array | Facts needed for safer or more precise classification. |
| `safe_actions` | array | Actions that are safe from this state. |
| `unsafe_actions` | array | Actions that are unsafe, each with a reason. |
| `recommended_commands` | array | Concrete CLI commands to run next. |
| `evidence` | array | Artifact paths, phase names, or report fields used for classification. |
| `warnings` | array | Non-fatal reconstruction or compatibility warnings. |

### Action item shape

`safe_actions` entries should be objects:

```json
{
  "action": "finish",
  "command": ["sweetspot", "finish", "example-run", "--from-state"],
  "reason": "source and DLQ queues are drained and final manifest is absent"
}
```

`unsafe_actions` entries should be objects:

```json
{
  "action": "cleanup",
  "reason": "production workers may still be running",
  "required_state": "COMPLETE"
}
```

### Evidence item shape

Evidence entries should be small and non-secret:

```json
{
  "kind": "artifact",
  "path": "artifacts/example-run/run_state.json",
  "field": "phases.enqueue_tasks.status",
  "value": "completed"
}
```

Allowed evidence kinds:

- `artifact`
- `phase`
- `report`
- `derived`
- `live_observation`

The core evaluator should emit only local `artifact`, `phase`, `report`, and `derived` evidence. CLI commands that inspect AWS may append `live_observation` evidence.

## Known fact keys

The evaluator should use stable lower snake case keys inside `known_facts`:

- `job_spec_sha256`
- `deployment_sha256`
- `plan_status`
- `plan_task_count`
- `canary_task_count`
- `production_task_count`
- `repair_task_count`
- `task_status_count`
- `outputs_manifest_count`
- `final_manifest_complete`
- `finish_report_ok`
- `finish_blocker_count`
- `controller_phases`
- `enqueue_tasks_status`
- `submit_workers_status`
- `source_queue_url_recorded`
- `dlq_url_recorded`
- `run_queue_recorded`
- `batch_job_queue_recorded`
- `job_name_prefix_recorded`

Values that may contain AWS account IDs, ARNs, queue URLs, or profile names must follow the existing redaction rules when report output is intended for broad logs. Local artifact paths are allowed.

## Recommended command policy

Recommended commands should be concrete and copyable. They should prefer local, non-mutating commands first.

Examples:

| State | Recommended command examples |
|---|---|
| `NEW` | `sweetspot run JOB_SPEC --artifact-dir artifacts/RUN_ID` without `--apply`; `sweetspot plan JOB_SPEC` |
| `PLAN_READY` | `sweetspot status RUN_ID --from-state --artifact-dir artifacts/RUN_ID`; production enqueue command when configured |
| `WORKERS_RUNNING` | `sweetspot status RUN_ID --from-state --artifact-dir artifacts/RUN_ID`; monitor/supervise command |
| `DRAINING` | `sweetspot finish RUN_ID --from-state --artifact-dir artifacts/RUN_ID --dry-run` |
| `NEEDS_REPAIR` | `sweetspot repair RUN_ID --from-state --artifact-dir artifacts/RUN_ID` |
| `BLOCKED` | `sweetspot explain RUN_ID --from-state --artifact-dir artifacts/RUN_ID` |
| `COMPLETE` | `sweetspot cleanup RUN_ID --from-state --artifact-dir artifacts/RUN_ID --write-plan` |

## Guard policy for finish and cleanup

`finish --from-state` may proceed only when the evaluator state is one of:

- `DRAINING`
- `FINALIZING`
- `NEEDS_REPAIR` when rerunning finalization after repair is explicitly requested and evidence supports it

It must refuse from:

- `NEW`
- `PLANNING`
- `CANARY_MATERIALIZED`
- `CANARY_RUNNING`
- `CANARY_COLLECTING`
- `PLAN_READY`
- `PRODUCTION_ENQUEUED`
- `WORKERS_RUNNING`
- `BLOCKED`
- `CANCELLED`
- `FAILED_REVIEW_REQUIRED`

`cleanup --from-state` defaults to dry-run style reporting. Destructive cleanup may proceed only from:

- `COMPLETE`
- `CANCELLED` with explicit confirmation
- `FAILED_REVIEW_REQUIRED` only for local diagnostic cleanup, never external resources, unless explicitly confirmed by a future remediation flow

## Gaps carried into S02

S02 should implement the evaluator conservatively around these gaps:

1. No durable planning-started marker: classify as `PLANNING` only from existing plan artifacts or explicit command context; otherwise prefer `NEW` or `FAILED_REVIEW_REQUIRED`.
2. No durable finalizer-started marker: classify finalizer activity from existing finalizer report or manifest evidence only.
3. No explicit run-level cancellation marker: `CANCELLED` remains contract-defined but likely unsupported until cancellation writes local state.
4. No explicit repair-running phase: infer `REPAIR_RUNNING` only when repair tasks and enqueue/submission evidence exist.
5. Live AWS worker status is optional: local evaluator can infer submitted intent, while `status --from-state` may improve confidence with live observations.

## S02 implementation boundary

S02 should add a pure evaluator API, likely in `sweetspot/lifecycle.py` or a sibling module, without changing command behavior first. CLI integration belongs to S03 and guarded finish/cleanup behavior belongs to S04.
