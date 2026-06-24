from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Iterator

from .adaptive import choose_next_shard_units, logical_shard_plan
from .s3util import parse_s3_uri, s3_join
from .task_model import SAFE_ID_RE, TASK_SCHEMA_V1


JOB_SPEC_SCHEMA_V1 = "sweetspot.job.v1"
PLAN_SCHEMA_V1 = "sweetspot.plan.v1"
PLAN_STATUSES = {"ready", "blocked"}
ARCHITECTURES = {"x86_64", "arm64"}
OUTPUT_CHECKS = {"done_marker"}
FORBIDDEN_PRIMARY_JOB_SPEC_KEYS = {
    "instance_types",
    "vcpus",
    "memory",
    "memory_mib",
    "worker_count",
    "max_workers",
    "messages_per_worker",
    "shard_size",
    "task_timeout_seconds",
    "visibility_timeout",
    "retry_attempts",
}

PLAN_REASON_CODES: dict[str, str] = {
    "arm_canary_failed": "ARM was requested but rejected after a failed compatibility or validation canary.",
    "arm_not_requested": "ARM was not included in the requested architecture set.",
    "budget_caps_parallelism": "The requested budget limits the safe worker count below the deadline-driven target.",
    "canary_validation_failed": "A canary failed framework or output validation, so production shards cannot be generated safely.",
    "deadline_unachievable": "The available throughput and limits cannot satisfy the requested deadline.",
    "insufficient_telemetry": "Planner telemetry is missing or too sparse for a measured decision.",
    "memory_shape_rejected_oom": "A candidate resource shape was rejected after an out-of-memory signal or validation failure.",
    "placement_score_low": "Capacity placement evidence is below the configured safety threshold.",
    "using_conservative_defaults": "The plan uses conservative defaults instead of measured workload-specific values.",
}


class PlannerSpecError(ValueError):
    """Raised when a JobSpec or Plan violates the SweetSpot planner contract."""


def load_json_object(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise PlannerSpecError(f"failed to read {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise PlannerSpecError(f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise PlannerSpecError(f"{path} must contain a JSON object")
    return data


def load_job_spec(path: Path) -> dict[str, Any]:
    return validate_job_spec(load_json_object(path))


def load_plan(path: Path) -> dict[str, Any]:
    return validate_plan(load_json_object(path))


def initial_blocked_plan(job_spec: dict[str, Any]) -> dict[str, Any]:
    """Return a machine-readable placeholder plan until calibration exists.

    This deliberately does not invent worker count, shard size, vCPU, memory, or architecture choices. It gives agents stable reason codes and a validated Plan envelope while future planner phases add canary-backed execution settings.
    """

    plan = _base_blocked_plan(job_spec)
    plan["reasons"] = [
        {
            "code": "insufficient_telemetry",
            "severity": "warning",
            "message": "The planner contract is valid, but canary telemetry and adaptive sizing are not available yet.",
        }
    ]
    return validate_plan(plan)


def plan_with_adaptive_canaries(
    job_spec: dict[str, Any],
    canary_observations: list[dict[str, Any]],
    *,
    target_task_seconds: float = 300.0,
    logical_unit_count: int | None = None,
) -> dict[str, Any]:
    """Return a Plan envelope with an adaptive shard-sizing canary decision.

    The planner still refuses to invent full execution settings, but it can expose
    canary-backed shard-size evidence in a stable machine-readable location for
    the future run controller.
    """

    plan = _base_blocked_plan(job_spec)
    shard_decision = choose_next_shard_units(canary_observations, target_task_seconds=target_task_seconds)
    if shard_decision["status"] == "blocked":
        decision_reasons = shard_decision.get("reasons") if isinstance(shard_decision.get("reasons"), list) else []
        raw_decision_code = decision_reasons[0].get("code") if decision_reasons and isinstance(decision_reasons[0], dict) else "memory_shape_rejected_oom"
        decision_code = str(raw_decision_code)
        plan_code = decision_code if decision_code in PLAN_REASON_CODES else "memory_shape_rejected_oom"
        plan["reasons"] = [
            {
                "code": plan_code,
                "severity": "error",
                "message": PLAN_REASON_CODES[plan_code],
            }
        ]
    else:
        plan["reasons"] = [
            {
                "code": "insufficient_telemetry",
                "severity": "warning",
                "message": "Adaptive shard telemetry is available, but resource, architecture, and cost calibration are not complete enough to select full execution settings.",
            }
        ]
    canary_entry: dict[str, Any] = {
        "purpose": "adaptive_shard_sizing",
        "decision": shard_decision,
    }
    selected_units = shard_decision.get("selected_units_per_task")
    if logical_unit_count is not None and isinstance(selected_units, int):
        canary_entry["production_shards"] = logical_shard_plan(logical_unit_count, selected_units, max_inline_ranges=0)
    plan["canaries"] = [canary_entry]
    return validate_plan(plan)


def iter_production_tasks_from_logical_unit_count(job_spec: dict[str, Any], total_units: int, units_per_task: int) -> Iterator[dict[str, Any]]:
    """Yield deterministic production task payloads without materializing the full run."""

    spec = validate_job_spec(job_spec)
    total = _non_negative_int(total_units, "total_units")
    units = _positive_int(units_per_task, "units_per_task")
    task_count = math.ceil(total / units)
    for shard_index in range(task_count):
        unit_start = shard_index * units
        unit_count = min(units, total - unit_start)
        yield _production_task_for_range(spec, shard_index=shard_index, unit_start=unit_start, unit_count=unit_count)


def production_tasks_from_logical_shard_plan(job_spec: dict[str, Any], shard_plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Materialize deterministic SweetSpot task payloads from a logical shard plan.

    Commands receive the manifest URI and contiguous logical range in
    SWEETSPOT_TASK_JSON. The helper is pure and writes nothing; CLI/controller
    code decides whether to persist or enqueue the generated JSONL.
    """

    spec = validate_job_spec(job_spec)
    ranges = shard_plan.get("ranges")
    if not isinstance(ranges, list):
        raise PlannerSpecError("logical shard plan must include ranges to materialize production tasks")
    task_count = _non_negative_int(shard_plan.get("task_count"), "task_count")
    ranges_omitted = _non_negative_int(shard_plan.get("ranges_omitted", 0), "ranges_omitted")
    if ranges_omitted:
        raise PlannerSpecError("logical shard plan omitted ranges and cannot be materialized safely")
    if len(ranges) != task_count:
        raise PlannerSpecError("logical shard plan range count does not match task_count")
    tasks: list[dict[str, Any]] = []
    for range_obj in ranges:
        if not isinstance(range_obj, dict):
            raise PlannerSpecError("logical shard ranges must be objects")
        shard_index = _non_negative_int(range_obj.get("shard_index"), "shard_index")
        unit_start = _non_negative_int(range_obj.get("unit_start"), "unit_start")
        unit_count = _positive_int(range_obj.get("unit_count"), "unit_count")
        tasks.append(_production_task_for_range(spec, shard_index=shard_index, unit_start=unit_start, unit_count=unit_count))
    return tasks


def _production_task_for_range(spec: dict[str, Any], *, shard_index: int, unit_start: int, unit_count: int) -> dict[str, Any]:
    task_id = f"shard-{shard_index:06d}"
    output_s3 = s3_join(spec["output_prefix"], "shards", task_id)
    return {
        "schema": TASK_SCHEMA_V1,
        "run_id": spec["run_id"],
        "task_id": task_id,
        "job_type": "production",
        "command": list(spec["command"]),
        "input_s3": spec["input_manifest"],
        "input": {
            "manifest_s3": spec["input_manifest"],
            "logical_unit_start": unit_start,
            "logical_unit_count": unit_count,
        },
        "logical_unit_start": unit_start,
        "logical_unit_count": unit_count,
        "output_s3": output_s3,
        "summary_s3": s3_join(spec["output_prefix"], "summaries", f"{task_id}.summary.json"),
        "done_s3": s3_join(spec["output_prefix"], "done", f"{task_id}.done.json"),
    }


def _base_blocked_plan(job_spec: dict[str, Any]) -> dict[str, Any]:
    spec = validate_job_spec(job_spec)
    constraints = spec["constraints"]
    plan: dict[str, Any] = {
        "schema": PLAN_SCHEMA_V1,
        "run_id": spec["run_id"],
        "status": "blocked",
        "job": {
            "image": spec["image"],
            "command": spec["command"],
            "input_manifest": spec["input_manifest"],
            "output_prefix": spec["output_prefix"],
        },
        "constraints": {
            "max_cost_usd": constraints["max_cost_usd"],
            "completion_fraction": constraints.get("completion_fraction", 1.0),
            "architectures": constraints.get("architectures", ["x86_64"]),
        },
        "reasons": [],
    }
    if "deadline_hours" in constraints:
        plan["constraints"]["deadline_seconds"] = constraints["deadline_hours"] * 3600
    if constraints.get("low_urgency") is True:
        plan["constraints"]["low_urgency"] = True
    return plan


def validate_job_spec(spec: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(spec, dict):
        raise PlannerSpecError("JobSpec must be a JSON object")
    if spec.get("schema") != JOB_SPEC_SCHEMA_V1:
        raise PlannerSpecError(f"JobSpec schema must be {JOB_SPEC_SCHEMA_V1!r}")
    unknown_primary_controls = sorted(FORBIDDEN_PRIMARY_JOB_SPEC_KEYS.intersection(spec))
    if unknown_primary_controls:
        raise PlannerSpecError(f"JobSpec primary contract must not set sizing controls directly: {', '.join(unknown_primary_controls)}")

    _require_id(spec.get("run_id"), "run_id")
    _require_non_empty_string(spec.get("image"), "image")
    _require_command(spec.get("command"))
    _require_s3_uri(spec.get("input_manifest"), "input_manifest")
    _require_s3_uri(spec.get("output_prefix"), "output_prefix")
    _validate_constraints(spec.get("constraints"))
    _validate_validation(spec.get("validation", {"output_check": "done_marker"}))
    if "overrides" in spec and not isinstance(spec["overrides"], dict):
        raise PlannerSpecError("JobSpec overrides must be an object when present")
    if "metadata" in spec and not isinstance(spec["metadata"], dict):
        raise PlannerSpecError("JobSpec metadata must be an object when present")
    return spec


def validate_plan(plan: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(plan, dict):
        raise PlannerSpecError("Plan must be a JSON object")
    if plan.get("schema") != PLAN_SCHEMA_V1:
        raise PlannerSpecError(f"Plan schema must be {PLAN_SCHEMA_V1!r}")
    _require_id(plan.get("run_id"), "run_id")
    status = plan.get("status")
    if status not in PLAN_STATUSES:
        raise PlannerSpecError(f"Plan status must be one of: {', '.join(sorted(PLAN_STATUSES))}")
    reasons = plan.get("reasons", [])
    if not isinstance(reasons, list):
        raise PlannerSpecError("Plan reasons must be a list")
    for reason in reasons:
        if not isinstance(reason, dict):
            raise PlannerSpecError("Plan reasons must be objects")
        code = reason.get("code")
        if code not in PLAN_REASON_CODES:
            raise PlannerSpecError(f"unknown Plan reason code: {code!r}")
        severity = reason.get("severity", "info")
        if severity not in {"info", "warning", "error"}:
            raise PlannerSpecError("Plan reason severity must be info, warning, or error")
    for object_field in ("selected", "constraints", "estimates"):
        if object_field in plan and not isinstance(plan[object_field], dict):
            raise PlannerSpecError(f"Plan {object_field} must be an object when present")
    if "canaries" in plan and not isinstance(plan["canaries"], list):
        raise PlannerSpecError("Plan canaries must be a list when present")
    if status == "ready":
        _validate_ready_plan(plan)
    return plan


def _validate_ready_plan(plan: dict[str, Any]) -> None:
    selected = plan.get("selected")
    if not isinstance(selected, dict):
        raise PlannerSpecError("ready Plan requires selected execution settings")
    _require_non_empty_string(selected.get("region"), "selected.region")
    architecture = selected.get("architecture")
    if architecture not in ARCHITECTURES:
        raise PlannerSpecError("ready Plan selected.architecture must be x86_64 or arm64")
    _positive_number(selected.get("vcpus"), "selected.vcpus")
    _positive_number(selected.get("memory_mib"), "selected.memory_mib")
    _positive_number(selected.get("target_task_seconds"), "selected.target_task_seconds")
    _positive_number(selected.get("estimated_workers"), "selected.estimated_workers")

    constraints = plan.get("constraints")
    if not isinstance(constraints, dict):
        raise PlannerSpecError("ready Plan requires constraints")
    _positive_number(constraints.get("max_cost_usd"), "constraints.max_cost_usd")
    if "deadline_seconds" in constraints:
        _positive_number(constraints.get("deadline_seconds"), "constraints.deadline_seconds")
    elif constraints.get("low_urgency") is not True:
        raise PlannerSpecError("ready Plan constraints require deadline_seconds or low_urgency: true")
    completion_fraction = _positive_number(constraints.get("completion_fraction", 1.0), "constraints.completion_fraction")
    if completion_fraction > 1.0:
        raise PlannerSpecError("constraints.completion_fraction must be <= 1.0")

    estimates = plan.get("estimates")
    if not isinstance(estimates, dict):
        raise PlannerSpecError("ready Plan requires estimates")
    _non_negative_number(estimates.get("expected_cost_usd"), "estimates.expected_cost_usd")
    _positive_number(estimates.get("expected_wall_seconds"), "estimates.expected_wall_seconds")


def _require_id(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise PlannerSpecError(f"JobSpec requires non-empty string {field}")
    if any(ord(ch) < 32 for ch in value) or not SAFE_ID_RE.fullmatch(value):
        raise PlannerSpecError(f"JobSpec {field} contains unsupported characters or is too long")
    return value


def _require_non_empty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise PlannerSpecError(f"JobSpec requires non-empty string {field}")
    if "\x00" in value:
        raise PlannerSpecError(f"JobSpec {field} must not contain NUL bytes")
    return value


def _require_command(value: Any) -> list[str]:
    if not isinstance(value, list) or not value:
        raise PlannerSpecError("JobSpec command must be a non-empty list of strings")
    out: list[str] = []
    for arg in value:
        if not isinstance(arg, str) or "\x00" in arg:
            raise PlannerSpecError("JobSpec command must be a non-empty list of strings without NUL bytes")
        out.append(arg)
    return out


def _require_s3_uri(value: Any, field: str) -> str:
    uri = _require_non_empty_string(value, field)
    try:
        bucket, _key = parse_s3_uri(uri)
    except ValueError as exc:
        raise PlannerSpecError(f"JobSpec {field} must be an S3 URI") from exc
    if not bucket:
        raise PlannerSpecError(f"JobSpec {field} must include an S3 bucket")
    return uri


def _validate_constraints(value: Any) -> None:
    if not isinstance(value, dict):
        raise PlannerSpecError("JobSpec constraints must be an object")
    _positive_number(value.get("max_cost_usd"), "constraints.max_cost_usd")
    deadline = value.get("deadline_hours")
    low_urgency = value.get("low_urgency", False)
    if deadline is None and low_urgency is not True:
        raise PlannerSpecError("JobSpec constraints require deadline_hours or low_urgency: true")
    if deadline is not None:
        _positive_number(deadline, "constraints.deadline_hours")
    completion_fraction = value.get("completion_fraction", 1.0)
    fraction = _positive_number(completion_fraction, "constraints.completion_fraction")
    if fraction > 1.0:
        raise PlannerSpecError("constraints.completion_fraction must be <= 1.0")
    architectures = value.get("architectures", ["x86_64"])
    if not isinstance(architectures, list) or not architectures:
        raise PlannerSpecError("constraints.architectures must be a non-empty list")
    invalid = sorted({repr(arch) for arch in architectures if not isinstance(arch, str) or arch not in ARCHITECTURES})
    if invalid:
        raise PlannerSpecError(f"unsupported architecture(s): {', '.join(invalid)}")


def _validate_validation(value: Any) -> None:
    if not isinstance(value, dict):
        raise PlannerSpecError("JobSpec validation must be an object when present")
    output_check = value.get("output_check", "done_marker")
    if output_check not in OUTPUT_CHECKS:
        raise PlannerSpecError(f"JobSpec validation.output_check must be one of: {', '.join(sorted(OUTPUT_CHECKS))}")


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise PlannerSpecError(f"{field} must be a positive integer")
    return value


def _non_negative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PlannerSpecError(f"{field} must be a non-negative integer")
    return value


def _positive_number(value: Any, field: str) -> float:
    number = _finite_number(value, field)
    if number <= 0:
        raise PlannerSpecError(f"{field} must be a positive finite number")
    return number


def _non_negative_number(value: Any, field: str) -> float:
    number = _finite_number(value, field)
    if number < 0:
        raise PlannerSpecError(f"{field} must be a non-negative finite number")
    return number


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PlannerSpecError(f"{field} must be a finite JSON number")
    number = float(value)
    if not math.isfinite(number):
        raise PlannerSpecError(f"{field} must be a finite JSON number")
    return number
