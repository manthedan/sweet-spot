from __future__ import annotations

import json
from pathlib import Path
from typing import Any


APPLY_PROGRESS_PHASES = ("enqueue_tasks", "submit_workers")


def load_run_state(path: Path, *, run_id: str, job_spec_sha256: str | None = None, require_job_spec_sha256: bool = False) -> dict[str, Any]:
    """Load and validate a controller run_state.json file.

    The controller persists this file before mutating SQS/Batch so retries can
    refuse drift and resume only from unambiguous phases.
    """

    if not path.exists():
        return {}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"existing run state at {path} is not valid JSON: {exc}") from exc
    if not isinstance(state, dict):
        raise SystemExit(f"existing run state at {path} is not a JSON object")
    existing_run_id = state.get("run_id")
    if existing_run_id and existing_run_id != run_id:
        raise SystemExit(f"existing run state at {path} is for run_id={existing_run_id!r}, not {run_id!r}")
    existing_hash = state.get("job_spec_sha256")
    if require_job_spec_sha256 and job_spec_sha256 and not existing_hash:
        raise SystemExit(f"existing run state at {path} does not record job_spec_sha256; rerun dry-run in a new artifact directory before applying")
    if job_spec_sha256 and existing_hash and existing_hash != job_spec_sha256:
        raise SystemExit(f"existing run state at {path} was created for a different JobSpec")
    return state


def phase_by_name(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    phases = state.get("phases")
    if not isinstance(phases, list):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for phase in phases:
        if isinstance(phase, dict) and phase.get("name"):
            out[str(phase["name"])] = phase
    return out


def phase_completed(state: dict[str, Any], name: str) -> bool:
    return phase_by_name(state).get(name, {}).get("status") == "completed"


def run_state_has_apply_progress(state: dict[str, Any]) -> bool:
    if not state:
        return False
    controller_obj = state.get("controller")
    controller: dict[str, Any] = controller_obj if isinstance(controller_obj, dict) else {}
    if state.get("applied") is True or state.get("mode") == "apply" or controller.get("mutations_allowed") is True:
        return True
    phases = phase_by_name(state)
    for name in APPLY_PROGRESS_PHASES:
        status = phases.get(name, {}).get("status")
        if status and status != "not_started":
            return True
    return False


def write_run_state(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def replace_or_append_phase(phases: list[dict[str, Any]], phase: dict[str, Any]) -> list[dict[str, Any]]:
    name = phase.get("name")
    if not name:
        return [*phases, phase]
    out: list[dict[str, Any]] = []
    replaced = False
    for existing in phases:
        if existing.get("name") == name:
            out.append(phase)
            replaced = True
        else:
            out.append(existing)
    if not replaced:
        out.append(phase)
    return out
