from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from .s3util import parse_s3_uri
from .task_model import SAFE_ID_RE


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
        "reasons": [
            {
                "code": "insufficient_telemetry",
                "severity": "warning",
                "message": "The planner contract is valid, but canary telemetry and adaptive sizing are not available yet.",
            }
        ],
    }
    if "deadline_hours" in constraints:
        plan["constraints"]["deadline_seconds"] = constraints["deadline_hours"] * 3600
    if constraints.get("low_urgency") is True:
        plan["constraints"]["low_urgency"] = True
    return validate_plan(plan)


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
