from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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
