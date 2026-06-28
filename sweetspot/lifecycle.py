from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LIFECYCLE_STATE_SCHEMA_V1 = "sweetspot.lifecycle_state.v1"

LIFECYCLE_STATES: tuple[str, ...] = (
    "NEW",
    "PLANNING",
    "CANARY_MATERIALIZED",
    "CANARY_RUNNING",
    "CANARY_COLLECTING",
    "PLAN_READY",
    "PRODUCTION_ENQUEUED",
    "WORKERS_RUNNING",
    "DRAINING",
    "FINALIZING",
    "COMPLETE",
    "NEEDS_REPAIR",
    "REPAIR_RUNNING",
    "BLOCKED",
    "CANCELLED",
    "FAILED_REVIEW_REQUIRED",
)

TERMINAL_LIFECYCLE_STATES = frozenset({"COMPLETE", "CANCELLED"})
REVIEW_REQUIRED_LIFECYCLE_STATES = frozenset({"FAILED_REVIEW_REQUIRED"})

LIFECYCLE_STATE_REPORT_REQUIRED_FIELDS: tuple[str, ...] = (
    "schema",
    "run_id",
    "artifact_dir",
    "state",
    "legacy_outcome",
    "terminal",
    "review_required",
    "generated_at",
    "known_facts",
    "missing_facts",
    "safe_actions",
    "unsafe_actions",
    "recommended_commands",
    "evidence",
    "warnings",
)


def validate_lifecycle_state_report(report: dict[str, Any]) -> list[str]:
    """Return contract violations for a lifecycle state report.

    This pins the M007 S01 contract without implementing state evaluation yet.
    S02 should call this from tests when it starts producing real reports.
    """

    errors: list[str] = []
    for field in LIFECYCLE_STATE_REPORT_REQUIRED_FIELDS:
        if field not in report:
            errors.append(f"missing required field: {field}")
    if report.get("schema") != LIFECYCLE_STATE_SCHEMA_V1:
        errors.append(f"schema must be {LIFECYCLE_STATE_SCHEMA_V1}")
    state = report.get("state")
    if state not in LIFECYCLE_STATES:
        errors.append(f"state must be one of {', '.join(LIFECYCLE_STATES)}")
    if "terminal" in report and not isinstance(report.get("terminal"), bool):
        errors.append("terminal must be a boolean")
    if "review_required" in report and not isinstance(report.get("review_required"), bool):
        errors.append("review_required must be a boolean")
    for field in ("missing_facts", "safe_actions", "unsafe_actions", "recommended_commands", "evidence", "warnings"):
        if field in report and not isinstance(report.get(field), list):
            errors.append(f"{field} must be a list")
    if "known_facts" in report and not isinstance(report.get("known_facts"), dict):
        errors.append("known_facts must be an object")
    return errors


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class RunContext:
    run_id: str
    artifact_dir: Path
    run_state: dict[str, Any]
    job_spec_sha256: str | None
    job: dict[str, Any]
    plan: dict[str, Any]
    output_prefix: str | None
    production_tasks_jsonl: Path | None
    task_status_jsonl: Path | None
    repair_tasks_jsonl: Path | None
    outputs_manifest_jsonl: Path | None
    final_manifest_json: Path | None
    finish_report_json: Path | None
    deployment_sha256: str | None
    region: str | None
    queue_url: str | None
    dlq_url: str | None
    batch_job_queue: str | None
    job_name_prefix: str | None
    run_queue: dict[str, Any] | None
    warnings: list[dict[str, Any]]

    def as_report(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "artifact_dir": str(self.artifact_dir),
            "job_spec_sha256": self.job_spec_sha256,
            "deployment_sha256": self.deployment_sha256,
            "output_prefix": self.output_prefix,
            "production_tasks_jsonl": str(self.production_tasks_jsonl) if self.production_tasks_jsonl else None,
            "task_status_jsonl": str(self.task_status_jsonl) if self.task_status_jsonl else None,
            "repair_tasks_jsonl": str(self.repair_tasks_jsonl) if self.repair_tasks_jsonl else None,
            "outputs_manifest_jsonl": str(self.outputs_manifest_jsonl) if self.outputs_manifest_jsonl else None,
            "final_manifest_json": str(self.final_manifest_json) if self.final_manifest_json else None,
            "finish_report_json": str(self.finish_report_json) if self.finish_report_json else None,
            "region": self.region,
            "queue_url": self.queue_url,
            "dlq_url": self.dlq_url,
            "batch_job_queue": self.batch_job_queue,
            "job_name_prefix": self.job_name_prefix,
            "run_queue": self.run_queue,
            "warnings": self.warnings,
        }


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _string_or_none(value: Any) -> str | None:
    return str(value) if value not in (None, "") else None


def _first_str(*values: Any) -> str | None:
    for value in values:
        out = _string_or_none(value)
        if out is not None:
            return out
    return None


def _nested(mapping: dict[str, Any], *keys: str) -> Any:
    cur: Any = mapping
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _phase_by_name(state: dict[str, Any], name: str) -> dict[str, Any]:
    phases = state.get("phases")
    if not isinstance(phases, list):
        return {}
    for phase in phases:
        if isinstance(phase, dict) and phase.get("name") == name:
            return phase
    return {}


def _resolve_existing_path(raw: Any, *, artifact_dir: Path) -> Path | None:
    if not isinstance(raw, str) or not raw:
        return None
    path = Path(raw)
    candidates: list[Path] = []
    if path.is_absolute():
        candidates.append(path)
    else:
        candidates.extend([artifact_dir / path, artifact_dir.parent / path])
        cwd_path = Path.cwd() / path
        artifact_root = artifact_dir.resolve()
        parent_root = artifact_dir.parent.resolve()
        try:
            resolved_cwd_path = cwd_path.resolve()
            if resolved_cwd_path.is_relative_to(artifact_root) or resolved_cwd_path.is_relative_to(parent_root):
                candidates.append(cwd_path)
        except OSError:
            pass
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _first_existing_path(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def load_run_context(run_id: str | None, artifact_dir: Path | None) -> RunContext:
    """Load the local state needed to reconstruct lifecycle commands for a run."""

    if artifact_dir is None:
        if not run_id:
            raise SystemExit("--from-state requires RUN_ID or --artifact-dir")
        artifact_dir = Path("artifacts") / run_id
    run_state_path = artifact_dir / "run_state.json"
    if not run_state_path.exists():
        raise SystemExit(f"--from-state could not find run state: {run_state_path}")
    try:
        loaded = json.loads(run_state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"--from-state found invalid JSON at {run_state_path}: {exc}") from exc
    if not isinstance(loaded, dict):
        raise SystemExit(f"--from-state run state at {run_state_path} is not a JSON object")
    state: dict[str, Any] = loaded
    state_run_id = _string_or_none(state.get("run_id"))
    if run_id and state_run_id and run_id != state_run_id:
        raise SystemExit(f"--from-state expected run_id={run_id!r}, found {state_run_id!r} in {run_state_path}")
    effective_run_id = run_id or state_run_id
    if not effective_run_id:
        raise SystemExit(f"--from-state run state at {run_state_path} does not record run_id")

    plan = _dict_or_empty(state.get("plan"))
    job = _dict_or_empty(state.get("job")) or _dict_or_empty(plan.get("job"))
    controller = _dict_or_empty(state.get("controller"))
    production_binding = _dict_or_empty(controller.get("production_binding")) or _dict_or_empty(controller.get("plan_binding")) or _dict_or_empty(controller.get("job_binding"))
    target = _dict_or_empty(production_binding.get("target"))
    run_queue = _dict_or_empty(controller.get("run_queue")) or _dict_or_empty(state.get("run_queue"))
    enqueue_phase = _phase_by_name(state, "enqueue_tasks")
    submit_phase = _phase_by_name(state, "submit_workers")
    artifacts = _dict_or_empty(state.get("artifacts"))

    production_tasks = _resolve_existing_path(artifacts.get("production_tasks_jsonl"), artifact_dir=artifact_dir)
    if production_tasks is None:
        production_tasks = _first_existing_path([artifact_dir / "production_tasks.jsonl"])
    task_status = _resolve_existing_path(artifacts.get("task_status_jsonl"), artifact_dir=artifact_dir)
    if task_status is None:
        task_status = _first_existing_path([artifact_dir / "task_status.jsonl", artifact_dir / "finalizer" / "task_status.jsonl"])
    repair_tasks = _resolve_existing_path(artifacts.get("repair_tasks_jsonl"), artifact_dir=artifact_dir)
    if repair_tasks is None:
        repair_tasks = _first_existing_path([artifact_dir / "repair_tasks.jsonl", artifact_dir / "finalizer" / "repair_tasks.jsonl", artifact_dir / "repair" / "repair_tasks.jsonl"])
    outputs_manifest = _resolve_existing_path(artifacts.get("outputs_manifest"), artifact_dir=artifact_dir)
    if outputs_manifest is None:
        outputs_manifest = _first_existing_path([artifact_dir / "outputs.jsonl", artifact_dir / "finalizer" / "outputs.jsonl"])
    final_manifest = _resolve_existing_path(artifacts.get("final_manifest"), artifact_dir=artifact_dir)
    if final_manifest is None:
        final_manifest = _first_existing_path([artifact_dir / "final_manifest.json", artifact_dir / "finalizer" / "final_manifest.json"])
    finish_report = _first_existing_path([artifact_dir / "finish_report.json"])

    warnings: list[dict[str, Any]] = []
    if production_tasks is None:
        warnings.append({"code": "missing_production_tasks_jsonl", "message": "run_state.json does not identify an existing production task JSONL"})

    return RunContext(
        run_id=effective_run_id,
        artifact_dir=artifact_dir,
        run_state=state,
        job_spec_sha256=_string_or_none(state.get("job_spec_sha256")),
        job=job,
        plan=plan,
        output_prefix=_first_str(job.get("output_prefix"), _nested(plan, "job", "output_prefix")),
        production_tasks_jsonl=production_tasks,
        task_status_jsonl=task_status,
        repair_tasks_jsonl=repair_tasks,
        outputs_manifest_jsonl=outputs_manifest,
        final_manifest_json=final_manifest,
        finish_report_json=finish_report,
        deployment_sha256=_first_str(production_binding.get("deployment_sha256"), controller.get("deployment_sha256")),
        region=_first_str(target.get("region"), _nested(production_binding, "selected", "region"), submit_phase.get("region"), job.get("region")),
        queue_url=_first_str(run_queue.get("queue_url"), target.get("sqs_queue_url"), enqueue_phase.get("queue_url"), submit_phase.get("queue_url")),
        dlq_url=_first_str(run_queue.get("dlq_url"), target.get("dlq_url")),
        batch_job_queue=_first_str(target.get("batch_job_queue"), submit_phase.get("batch_job_queue")),
        job_name_prefix=_first_str(submit_phase.get("job_name_prefix"), controller.get("job_name_prefix")),
        run_queue=run_queue or None,
        warnings=warnings,
    )


def _jsonl_count(path: Path | None) -> int | None:
    if path is None or not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def _read_json_object(path: Path | None) -> tuple[dict[str, Any] | None, str | None]:
    if path is None or not path.exists():
        return None, None
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return None, f"invalid JSON at {path}: {exc}"
    if not isinstance(loaded, dict):
        return None, f"JSON at {path} is not an object"
    return loaded, None


def _phase_status(context: RunContext, name: str) -> str | None:
    return _string_or_none(_phase_by_name(context.run_state, name).get("status"))


def _first_phase_status(context: RunContext, *names: str) -> str | None:
    for name in names:
        status = _phase_status(context, name)
        if status is not None:
            return status
    return None


def _evidence(kind: str, *, path: Path | None = None, field: str | None = None, value: Any = None) -> dict[str, Any]:
    item: dict[str, Any] = {"kind": kind}
    if path is not None:
        item["path"] = str(path)
    if field is not None:
        item["field"] = field
    if value is not None:
        item["value"] = value
    return item


def _action(action: str, command: list[str], reason: str) -> dict[str, Any]:
    return {"action": action, "command": command, "reason": reason}


def _unsafe(action: str, reason: str, required_state: str | None = None) -> dict[str, Any]:
    item: dict[str, Any] = {"action": action, "reason": reason}
    if required_state is not None:
        item["required_state"] = required_state
    return item


def _commands_for(run_id: str | None, artifact_dir: Path | None) -> dict[str, list[str]]:
    rid = run_id or "RUN_ID"
    artifact_arg = ["--artifact-dir", str(artifact_dir)] if artifact_dir is not None else []
    return {
        "status": ["sweetspot", "status", rid, "--from-state", *artifact_arg],
        "explain": ["sweetspot", "explain", rid, "--from-state", *artifact_arg],
        "finish": ["sweetspot", "finish", rid, "--from-state", *artifact_arg],
        "finish_dry_run": ["sweetspot", "finish", rid, "--from-state", *artifact_arg, "--dry-run"],
        "cleanup_dry_run": ["sweetspot", "cleanup", rid, "--from-state", *artifact_arg],
        "cleanup": ["sweetspot", "cleanup", rid, "--from-state", *artifact_arg, "--apply"],
        "repair_plan": ["sweetspot", "repair-plan", rid, "--from-state", *artifact_arg],
    }


def _base_state_actions(state: str, run_id: str | None, artifact_dir: Path | None) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[list[str]]]:
    commands = _commands_for(run_id, artifact_dir)
    safe: list[dict[str, Any]] = []
    unsafe: list[dict[str, Any]] = []
    recommended: list[list[str]] = []

    if state == "NEW":
        safe.append(_action("plan", ["sweetspot", "run", "JOB_SPEC", "--artifact-dir", str(artifact_dir or "artifacts/RUN_ID")], "no durable run state exists yet"))
        unsafe.extend(
            [
                _unsafe("finish", "no durable run artifacts prove work was submitted or drained", "DRAINING"),
                _unsafe("cleanup", "no completed run state exists to clean up", "COMPLETE"),
                _unsafe("repair", "no finalizer evidence identifies repairable outputs", "NEEDS_REPAIR"),
            ]
        )
        recommended.append(safe[0]["command"])
    elif state in {"PLANNING", "CANARY_MATERIALIZED", "CANARY_RUNNING", "CANARY_COLLECTING", "PLAN_READY"}:
        safe.append(_action("status", commands["status"], "inspect local state before mutation"))
        unsafe.extend(
            [
                _unsafe("finish", "production work is not proven drained", "DRAINING"),
                _unsafe("cleanup", "run is not complete", "COMPLETE"),
            ]
        )
        recommended.append(commands["status"])
    elif state == "PRODUCTION_ENQUEUED":
        safe.append(_action("status", commands["status"], "enqueue progress can be inspected or resumed"))
        unsafe.extend(
            [
                _unsafe("replan", "production bindings may already have been used for enqueue"),
                _unsafe("finish", "workers are not proven drained", "DRAINING"),
                _unsafe("cleanup", "queued work may still exist", "COMPLETE"),
            ]
        )
        recommended.append(commands["status"])
    elif state == "WORKERS_RUNNING":
        safe.append(_action("status", commands["status"], "workers are expected to be processing queued work"))
        unsafe.extend(
            [
                _unsafe("finish", "local-only evidence cannot prove queues are drained", "DRAINING"),
                _unsafe("cleanup", "workers may still be running", "COMPLETE"),
            ]
        )
        recommended.append(commands["status"])
    elif state == "DRAINING":
        safe.extend(
            [
                _action("status", commands["status"], "inspect drain progress"),
                _action("finish_dry_run", commands["finish_dry_run"], "finalization is the next guarded transition after drain evidence"),
                _action("finish", commands["finish"], "mutating finish is allowed after live drain checks pass"),
            ]
        )
        unsafe.extend(
            [
                _unsafe("cleanup", "final manifest is absent", "COMPLETE"),
            ]
        )
        recommended.append(commands["finish_dry_run"])
    elif state == "FINALIZING":
        safe.extend(
            [
                _action("status", commands["status"], "finalizer artifacts should be inspected until completion"),
                _action("finish_dry_run", commands["finish_dry_run"], "finish is guarded from finalizing state and should remain dry-run until evidence is reviewed"),
                _action("finish", commands["finish"], "resume interrupted finalization after live drain checks pass"),
            ]
        )
        unsafe.extend(
            [
                _unsafe("enqueue", "finalization is already in progress"),
                _unsafe("cleanup", "finalization has not completed", "COMPLETE"),
            ]
        )
        recommended.append(commands["status"])
    elif state == "COMPLETE":
        safe.extend(
            [
                _action("status", commands["status"], "read final run state"),
                _action("cleanup_dry_run", commands["cleanup_dry_run"], "completed runs may be inspected for guarded cleanup"),
                _action("cleanup", commands["cleanup"], "completed runs may run guarded cleanup after live safety checks pass"),
            ]
        )
        unsafe.append(_unsafe("repair", "successful completion evidence is present", "NEEDS_REPAIR"))
        recommended.append(commands["cleanup_dry_run"])
    elif state == "NEEDS_REPAIR":
        safe.extend(
            [
                _action("repair_plan", commands["repair_plan"], "finalizer evidence indicates repairable missing or failed outputs"),
                _action("finish_dry_run", commands["finish_dry_run"], "rerun finalization only as a guarded dry-run after repair evidence is reviewed"),
            ]
        )
        unsafe.extend(
            [
                _unsafe("finish", "repair evidence must be reviewed before mutating finalization", "FINALIZING"),
                _unsafe("mark_complete", "final manifest or report is incomplete", "COMPLETE"),
                _unsafe("cleanup", "repair inputs may still be required", "COMPLETE"),
            ]
        )
        recommended.append(commands["repair_plan"])
    elif state == "REPAIR_RUNNING":
        safe.append(_action("status", commands["status"], "repair work should be monitored before finalization"))
        unsafe.append(_unsafe("cleanup", "repair work may still be running", "COMPLETE"))
        recommended.append(commands["status"])
    elif state in {"BLOCKED", "FAILED_REVIEW_REQUIRED"}:
        safe.append(_action("explain", commands["explain"], "review the blocker or unsafe ambiguity before mutation"))
        unsafe.extend(
            [
                _unsafe("apply", "state requires review before mutation"),
                _unsafe("finish", "state requires review before finalization", "DRAINING"),
                _unsafe("cleanup", "state requires review before cleanup", "COMPLETE"),
            ]
        )
        recommended.append(commands["explain"])
    elif state == "CANCELLED":
        safe.append(_action("cleanup_dry_run", commands["cleanup_dry_run"], "cancelled runs require guarded cleanup inspection"))
        unsafe.append(_unsafe("resume_workers", "run is explicitly cancelled"))
        recommended.append(commands["cleanup_dry_run"])
    return safe, unsafe, recommended


def evaluate_lifecycle_state(
    context: RunContext | None = None,
    *,
    run_id: str | None = None,
    artifact_dir: Path | str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Evaluate local run artifacts into a lifecycle state report.

    The evaluator is deterministic over local files and performs no live AWS reads.
    Missing or malformed state is reported conservatively as NEW or
    FAILED_REVIEW_REQUIRED rather than mutating or probing external resources.
    """

    resolved_artifact_dir = Path(artifact_dir) if artifact_dir is not None else (Path("artifacts") / run_id if run_id else None)
    warnings: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    missing_facts: list[str] = []
    known_facts: dict[str, Any] = {}
    legacy_outcome: str | None = None

    if context is None:
        if resolved_artifact_dir is not None and not (resolved_artifact_dir / "run_state.json").exists():
            state = "NEW"
            evidence.append(_evidence("derived", path=resolved_artifact_dir / "run_state.json", field="exists", value=False))
            missing_facts.append("run_state_json")
            safe_actions, unsafe_actions, recommended_commands = _base_state_actions(state, run_id, resolved_artifact_dir)
            report = {
                "schema": LIFECYCLE_STATE_SCHEMA_V1,
                "run_id": run_id,
                "artifact_dir": str(resolved_artifact_dir),
                "state": state,
                "legacy_outcome": legacy_outcome,
                "terminal": False,
                "review_required": False,
                "generated_at": generated_at or _utc_now_iso(),
                "known_facts": known_facts,
                "missing_facts": missing_facts,
                "safe_actions": safe_actions,
                "unsafe_actions": unsafe_actions,
                "recommended_commands": recommended_commands,
                "evidence": evidence,
                "warnings": warnings,
            }
            return report
        try:
            context = load_run_context(run_id, resolved_artifact_dir)
        except SystemExit as exc:
            state = "FAILED_REVIEW_REQUIRED"
            message = str(exc)
            warnings.append({"code": "run_context_load_failed", "message": message})
            evidence.append(_evidence("derived", path=resolved_artifact_dir / "run_state.json" if resolved_artifact_dir is not None else None, field="load_run_context", value="failed"))
            safe_actions, unsafe_actions, recommended_commands = _base_state_actions(state, run_id, resolved_artifact_dir)
            return {
                "schema": LIFECYCLE_STATE_SCHEMA_V1,
                "run_id": run_id,
                "artifact_dir": str(resolved_artifact_dir) if resolved_artifact_dir is not None else None,
                "state": state,
                "legacy_outcome": legacy_outcome,
                "terminal": False,
                "review_required": True,
                "generated_at": generated_at or _utc_now_iso(),
                "known_facts": known_facts,
                "missing_facts": ["valid_run_state_json"],
                "safe_actions": safe_actions,
                "unsafe_actions": unsafe_actions,
                "recommended_commands": recommended_commands,
                "evidence": evidence,
                "warnings": warnings,
            }

    artifact_root = context.artifact_dir
    run_id = context.run_id
    warnings.extend(context.warnings)
    evidence.append(_evidence("artifact", path=artifact_root / "run_state.json", field="run_id", value=run_id))

    enqueue_status = _phase_status(context, "enqueue_tasks")
    canary_enqueue_status = _phase_status(context, "enqueue_canary_tasks")
    canary_submit_status = _phase_status(context, "submit_canary_workers")
    canary_collect_status = _phase_status(context, "collect_canary_summaries")
    submit_phase = _phase_by_name(context.run_state, "submit_workers")
    submit_phase_present = bool(submit_phase)
    submit_status = _string_or_none(submit_phase.get("status"))
    plan_status = _string_or_none(context.plan.get("status"))
    artifacts_map = _dict_or_empty(context.run_state.get("artifacts"))
    canary_tasks_jsonl = _resolve_existing_path(artifacts_map.get("canary_tasks_jsonl"), artifact_dir=artifact_root)
    if canary_tasks_jsonl is None:
        canary_tasks_jsonl = _first_existing_path([artifact_root / "canary_tasks.jsonl"])
    canary_count = _jsonl_count(canary_tasks_jsonl)
    production_count = _jsonl_count(context.production_tasks_jsonl)
    task_status_count = _jsonl_count(context.task_status_jsonl)
    outputs_count = _jsonl_count(context.outputs_manifest_jsonl)
    repair_count = _jsonl_count(context.repair_tasks_jsonl)
    repair_enqueue_status = _first_phase_status(context, "enqueue_repair_tasks", "repair_enqueue", "enqueue_repairs")
    repair_submit_status = _first_phase_status(context, "submit_repair_workers", "repair_submit", "submit_repairs")
    final_manifest, final_manifest_error = _read_json_object(context.final_manifest_json)
    finish_report, finish_report_error = _read_json_object(context.finish_report_json)
    cancellation_report_path = _first_existing_path([artifact_root / "cancelled.json", artifact_root / "cancellation.json", artifact_root / "cancel_report.json"])
    blocked_report_path = _first_existing_path([artifact_root / "blocked.json", artifact_root / "blocker_report.json"])
    cancellation_report, cancellation_report_error = _read_json_object(cancellation_report_path)
    blocked_report, blocked_report_error = _read_json_object(blocked_report_path)

    known_facts.update(
        {
            "controller_phases": [phase.get("name") for phase in context.run_state.get("phases", []) if isinstance(phase, dict) and phase.get("name")],
            "source_queue_url_recorded": bool(context.queue_url),
            "dlq_url_recorded": bool(context.dlq_url),
            "run_queue_recorded": bool(context.run_queue),
            "batch_job_queue_recorded": bool(context.batch_job_queue),
            "job_name_prefix_recorded": bool(context.job_name_prefix),
        }
    )
    plan_tasks = context.plan.get("tasks")
    plan_task_count = len(plan_tasks) if isinstance(plan_tasks, list) else None
    optional_fact_values = {
        "job_spec_sha256": context.job_spec_sha256,
        "deployment_sha256": context.deployment_sha256,
        "plan_status": plan_status,
        "plan_task_count": plan_task_count,
        "canary_task_count": canary_count,
        "production_task_count": production_count,
        "repair_task_count": repair_count,
        "task_status_count": task_status_count,
        "outputs_manifest_count": outputs_count,
        "enqueue_tasks_status": enqueue_status,
        "submit_workers_status": submit_status,
        "enqueue_canary_tasks_status": canary_enqueue_status,
        "submit_canary_workers_status": canary_submit_status,
        "collect_canary_summaries_status": canary_collect_status,
        "repair_enqueue_status": repair_enqueue_status,
        "repair_submit_status": repair_submit_status,
    }
    known_facts.update({key: value for key, value in optional_fact_values.items() if value is not None})

    if canary_tasks_jsonl is not None:
        evidence.append(_evidence("artifact", path=canary_tasks_jsonl, field="canary_task_count", value=canary_count))
    if context.production_tasks_jsonl is not None:
        evidence.append(_evidence("artifact", path=context.production_tasks_jsonl, field="production_task_count", value=production_count))
    if context.task_status_jsonl is not None:
        evidence.append(_evidence("artifact", path=context.task_status_jsonl, field="task_status_count", value=task_status_count))
    if context.outputs_manifest_jsonl is not None:
        evidence.append(_evidence("artifact", path=context.outputs_manifest_jsonl, field="outputs_manifest_count", value=outputs_count))
    if context.repair_tasks_jsonl is not None:
        evidence.append(_evidence("artifact", path=context.repair_tasks_jsonl, field="repair_task_count", value=repair_count))
    if enqueue_status is not None:
        evidence.append(_evidence("phase", path=artifact_root / "run_state.json", field="phases.enqueue_tasks.status", value=enqueue_status))
    if submit_status is not None:
        evidence.append(_evidence("phase", path=artifact_root / "run_state.json", field="phases.submit_workers.status", value=submit_status))
    if canary_enqueue_status is not None:
        evidence.append(_evidence("phase", path=artifact_root / "run_state.json", field="phases.enqueue_canary_tasks.status", value=canary_enqueue_status))
    if canary_submit_status is not None:
        evidence.append(_evidence("phase", path=artifact_root / "run_state.json", field="phases.submit_canary_workers.status", value=canary_submit_status))
    if canary_collect_status is not None:
        evidence.append(_evidence("phase", path=artifact_root / "run_state.json", field="phases.collect_canary_summaries.status", value=canary_collect_status))
    if repair_enqueue_status is not None:
        evidence.append(_evidence("phase", path=artifact_root / "run_state.json", field="phases.repair_enqueue.status", value=repair_enqueue_status))
    if repair_submit_status is not None:
        evidence.append(_evidence("phase", path=artifact_root / "run_state.json", field="phases.repair_submit.status", value=repair_submit_status))
    if cancellation_report_path is not None:
        evidence.append(_evidence("artifact", path=cancellation_report_path, field="exists", value=True))
    if blocked_report_path is not None:
        evidence.append(_evidence("artifact", path=blocked_report_path, field="exists", value=True))

    if final_manifest_error:
        warnings.append({"code": "invalid_final_manifest", "message": final_manifest_error})
    if finish_report_error:
        warnings.append({"code": "invalid_finish_report", "message": finish_report_error})
    if cancellation_report_error:
        warnings.append({"code": "invalid_cancellation_report", "message": cancellation_report_error})
    if blocked_report_error:
        warnings.append({"code": "invalid_blocked_report", "message": blocked_report_error})

    final_complete = final_manifest.get("complete") if final_manifest is not None else None
    finish_ok = finish_report.get("ok") if finish_report is not None else None
    finish_confirmed_mutating = finish_report.get("dry_run") is not True if finish_report is not None else False
    finish_blockers = finish_report.get("blockers") if finish_report is not None else None
    finish_blocker_count = len(finish_blockers) if isinstance(finish_blockers, list) else None
    finish_blocker_codes = {str(blocker.get("code")) for blocker in finish_blockers if isinstance(blocker, dict) and blocker.get("code")} if isinstance(finish_blockers, list) else set()
    if isinstance(final_complete, bool):
        known_facts["final_manifest_complete"] = final_complete
        evidence.append(_evidence("report", path=context.final_manifest_json, field="complete", value=final_complete))
    elif final_manifest is not None:
        evidence.append(_evidence("report", path=context.final_manifest_json, field="exists", value=True))
    if isinstance(finish_ok, bool):
        known_facts["finish_report_ok"] = finish_ok
        evidence.append(_evidence("report", path=context.finish_report_json, field="ok", value=finish_ok))
    elif finish_report is not None:
        evidence.append(_evidence("report", path=context.finish_report_json, field="exists", value=True))
    if finish_blocker_count is not None:
        known_facts["finish_blocker_count"] = finish_blocker_count
        evidence.append(_evidence("report", path=context.finish_report_json, field="blockers", value=finish_blocker_count))

    invalid_local_artifact = bool(final_manifest_error or finish_report_error or cancellation_report_error or blocked_report_error)
    terminal_success = (finish_ok is True and finish_confirmed_mutating) or final_complete is True
    repair_evidence = final_complete is False or (repair_count or 0) > 0
    finish_blocked_evidence = finish_ok is False or (finish_blocker_count or 0) > 0
    transient_finish_blocker_codes = {"source_queue_not_empty", "dlq_not_empty", "batch_jobs_active"}
    retryable_finish_blocker_codes = {"finalizer_failed", "finalizer_incomplete"}
    finish_blocked_allows_retry = bool(finish_blocker_codes) and finish_blocker_codes <= (transient_finish_blocker_codes | retryable_finish_blocker_codes)
    durable_blocked_evidence = blocked_report_path is not None or (finish_blocked_evidence and not finish_blocked_allows_retry)
    blocked_evidence = finish_blocked_evidence or durable_blocked_evidence
    repair_work_started = (repair_count or 0) > 0 and repair_enqueue_status in {"in_progress", "running"}
    repair_workers_started = (repair_count or 0) > 0 and repair_submit_status in {"in_progress", "running"}
    repair_work_completed = (repair_count or 0) > 0 and (repair_enqueue_status in {"completed", "complete"} or repair_submit_status in {"completed", "complete"})
    active_repair_work = repair_work_started or repair_workers_started
    terminal_repair_conflict = final_complete is False or active_repair_work
    active_production_phase = enqueue_status in {"in_progress", "running"} or submit_status in {"in_progress", "running"}
    failed_production_phase = enqueue_status in {"failed", "error", "cancelled", "canceled"} or submit_status in {"failed", "error", "cancelled", "canceled"}
    contradictory_terminal_evidence = terminal_success and (terminal_repair_conflict or blocked_evidence or active_production_phase or failed_production_phase)
    if invalid_local_artifact:
        state = "FAILED_REVIEW_REQUIRED"
        if final_manifest_error or finish_report_error:
            missing_facts.append("valid_finalizer_artifacts")
        if cancellation_report_error or blocked_report_error:
            missing_facts.append("valid_local_side_path_artifacts")
    elif contradictory_terminal_evidence:
        state = "FAILED_REVIEW_REQUIRED"
        missing_facts.append("consistent_terminal_artifacts")
        warnings.append({"code": "contradictory_lifecycle_artifacts", "message": "terminal success evidence conflicts with repair or blocked evidence"})
    elif cancellation_report_path is not None:
        state = "CANCELLED"
        legacy_outcome = "cancelled"
    elif terminal_success:
        state = "COMPLETE"
        legacy_outcome = "finished" if finish_ok is True else "finalized_complete"
    elif repair_workers_started or repair_work_started:
        state = "REPAIR_RUNNING"
        legacy_outcome = "repair_running"
        missing_facts.extend(["repair_queue_depth", "active_repair_worker_count"])
    elif repair_work_completed:
        state = "DRAINING"
        legacy_outcome = "in_progress"
        missing_facts.extend(["repair_queue_depth", "active_repair_worker_count", "final_manifest_complete"])
    elif repair_evidence:
        state = "NEEDS_REPAIR"
        legacy_outcome = "repair_needed"
    elif durable_blocked_evidence:
        state = "BLOCKED"
        legacy_outcome = "blocked"
    elif finish_blocked_evidence:
        state = "DRAINING"
        legacy_outcome = "in_progress"
        warnings.append({"code": "previous_finish_blocked", "message": "A prior finish attempt was blocked; rerun finish to refresh live drain checks."})
    elif finish_ok is True and final_complete is not True:
        state = "DRAINING"
        legacy_outcome = "in_progress"
        missing_facts.append("final_manifest_complete")
        warnings.append({"code": "finish_report_not_terminal", "message": "A successful finish report without durable terminal evidence still requires mutating finish/final manifest confirmation."})
    elif final_manifest is not None or finish_report is not None:
        state = "FINALIZING"
        legacy_outcome = "in_progress"
        if not isinstance(final_complete, bool):
            missing_facts.append("final_manifest_complete")
        if not isinstance(finish_ok, bool):
            missing_facts.append("finish_report_ok")
    elif failed_production_phase:
        state = "BLOCKED"
        legacy_outcome = "blocked"
        warnings.append({"code": "production_phase_failed", "message": "A production enqueue or worker-submit phase failed or was cancelled."})
    elif production_count is None and enqueue_status is None and not submit_phase_present and (canary_collect_status in {"in_progress", "running"} or canary_submit_status in {"completed", "complete"}):
        state = "CANARY_COLLECTING"
        legacy_outcome = "in_progress"
        missing_facts.append("canary_summary_count")
    elif (
        production_count is None
        and enqueue_status is None
        and not submit_phase_present
        and (canary_enqueue_status in {"in_progress", "running", "completed", "complete"} or canary_submit_status in {"in_progress", "running"})
    ):
        state = "CANARY_RUNNING"
        legacy_outcome = "in_progress"
        missing_facts.extend(["canary_queue_depth", "active_canary_worker_count"])
    elif canary_count is not None and production_count is None:
        state = "CANARY_MATERIALIZED"
        legacy_outcome = "canary_materialized"
        missing_facts.append("enqueue_canary_tasks_status")
    elif submit_status in {"completed", "complete"} or (submit_status is None and submit_phase_present):
        state = "DRAINING"
        legacy_outcome = "in_progress"
        missing_facts.extend(["source_queue_depth", "dlq_queue_depth", "active_worker_count", "final_manifest_complete"])
        if submit_status is None:
            missing_facts.append("submit_workers_status")
        if task_status_count is None and outputs_count is None:
            missing_facts.append("task_status_or_output_progress")
    elif submit_status in {"in_progress", "running", "completed", "complete"}:
        state = "WORKERS_RUNNING"
        legacy_outcome = "in_progress"
        missing_facts.extend(["source_queue_depth", "dlq_queue_depth", "active_worker_count"])
    elif enqueue_status in {"in_progress", "running", "completed", "complete"}:
        state = "PRODUCTION_ENQUEUED"
        legacy_outcome = "in_progress"
        missing_facts.append("submit_workers_status")
    elif plan_status == "ready" or production_count is not None:
        state = "PLAN_READY"
        legacy_outcome = "plan_ready"
        if production_count is None:
            missing_facts.append("production_task_count")
    elif context.plan:
        state = "PLANNING"
        legacy_outcome = "unknown"
        missing_facts.append("plan_status")
    else:
        state = "FAILED_REVIEW_REQUIRED"
        legacy_outcome = "unknown"
        missing_facts.append("plan_status")
        warnings.append({"code": "insufficient_local_artifacts", "message": "run_state.json exists but does not contain plan or progress evidence"})

    if context.production_tasks_jsonl is None and state not in {"NEW", "FAILED_REVIEW_REQUIRED", "COMPLETE", "CANARY_MATERIALIZED", "CANARY_RUNNING", "CANARY_COLLECTING"}:
        missing_facts.append("production_tasks_jsonl")

    safe_actions, unsafe_actions, recommended_commands = _base_state_actions(state, run_id, artifact_root)
    report = {
        "schema": LIFECYCLE_STATE_SCHEMA_V1,
        "run_id": run_id,
        "artifact_dir": str(artifact_root),
        "state": state,
        "legacy_outcome": legacy_outcome,
        "terminal": state in TERMINAL_LIFECYCLE_STATES,
        "review_required": state in REVIEW_REQUIRED_LIFECYCLE_STATES,
        "generated_at": generated_at or _utc_now_iso(),
        "known_facts": known_facts,
        "missing_facts": sorted(set(missing_facts)),
        "safe_actions": safe_actions,
        "unsafe_actions": unsafe_actions,
        "recommended_commands": recommended_commands,
        "evidence": evidence,
        "warnings": warnings,
    }
    return report
