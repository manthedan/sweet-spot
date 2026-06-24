from __future__ import annotations

import math
import re
import statistics
from typing import Any

ADAPTIVE_SHARD_SCHEMA_V1 = "sweetspot.adaptive_shard_decision.v1"
LOGICAL_SHARD_PLAN_SCHEMA_V1 = "sweetspot.logical_shard_plan.v1"
ADAPTIVE_SHARD_REASON_CODES: dict[str, str] = {
    "canary_required": "No measurable canary summary was available, so the next canary should use the minimum shard size.",
    "geometric_growth_cap": "The measured rate supports a larger shard, but growth was capped to keep canaries replay-safe.",
    "max_units_cap": "The selected shard size was capped by the configured maximum unit count.",
    "memory_shape_rejected_oom": "A canary reported an out-of-memory signal; increase memory or reject this resource shape before growing shards.",
    "target_duration_selected": "The selected shard size targets the configured replay-safe task duration from measured canary throughput.",
}


def _positive_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0 else None


def _positive_int(value: Any) -> int | None:
    parsed = _positive_float(value)
    if parsed is None:
        return None
    return max(1, int(parsed))


def _looks_like_oom(summary: dict[str, Any], telemetry: dict[str, Any]) -> bool:
    text = " ".join(str(summary.get(key) or "") for key in ("framework_error", "stderr_tail", "error"))
    text = " ".join([text, str(telemetry.get("interruption_status") or ""), str(telemetry.get("metrics_error") or "")]).lower()
    specific_markers = ("out of memory", "oomkilled", "memoryerror", "cannot allocate memory")
    return any(marker in text for marker in specific_markers) or re.search(r"\boom\b", text) is not None


def canary_observation_from_summary(summary: dict[str, Any]) -> dict[str, Any]:
    """Extract adaptive-shard telemetry from a SweetSpot task summary.

    The returned object is intentionally small and stable so future run-controller
    code can consume either raw worker summaries or normalized observations.
    """

    telemetry_raw = summary.get("telemetry")
    telemetry = telemetry_raw if isinstance(telemetry_raw, dict) else {}
    units = _positive_float(telemetry.get("completed_units") or summary.get("completed_units"))
    seconds = _positive_float(telemetry.get("useful_compute_seconds") or summary.get("elapsed_sec"))
    success = summary.get("returncode") in (None, 0) and not summary.get("timed_out") and not summary.get("framework_error") and summary.get("commit_status") != "lost"
    observation: dict[str, Any] = {
        "task_id": summary.get("task_id"),
        "success": bool(success),
        "completed_units": units,
        "useful_compute_seconds": seconds,
        "oom": _looks_like_oom(summary, telemetry),
    }
    if units and seconds:
        observation["units_per_second"] = units / seconds
    return {k: v for k, v in observation.items() if v is not None}


def logical_shard_plan(total_units: int, units_per_task: int, *, max_inline_ranges: int = 1000) -> dict[str, Any]:
    """Plan deterministic contiguous shards for a logical-unit manifest.

    This is intentionally side-effect-free: it only describes how a future run
    controller should split a manifest after canary-backed shard sizing. The
    controller can choose not to inline ranges in large Plan JSON outputs.
    """

    if isinstance(total_units, bool) or not isinstance(total_units, int) or total_units < 0:
        raise ValueError("total_units must be a non-negative integer")
    if isinstance(units_per_task, bool) or not isinstance(units_per_task, int) or units_per_task <= 0:
        raise ValueError("units_per_task must be a positive integer")
    if isinstance(max_inline_ranges, bool) or not isinstance(max_inline_ranges, int) or max_inline_ranges < 0:
        raise ValueError("max_inline_ranges must be non-negative")

    task_count = math.ceil(total_units / units_per_task)
    out: dict[str, Any] = {
        "schema": LOGICAL_SHARD_PLAN_SCHEMA_V1,
        "logical_unit_count": total_units,
        "units_per_task": units_per_task,
        "task_count": task_count,
    }
    if task_count <= max_inline_ranges:
        ranges: list[dict[str, int]] = []
        for shard_index in range(task_count):
            unit_start = shard_index * units_per_task
            ranges.append(
                {
                    "shard_index": shard_index,
                    "unit_start": unit_start,
                    "unit_count": min(units_per_task, total_units - unit_start),
                }
            )
        out["ranges"] = ranges
    else:
        out["ranges_omitted"] = task_count
    return out


def choose_next_shard_units(
    observations: list[dict[str, Any]],
    *,
    target_task_seconds: float,
    min_units: int = 1,
    max_units: int | None = None,
    growth_factor: float = 4.0,
) -> dict[str, Any]:
    """Choose the next replay-safe shard size from canary observations.

    Agents should not supply shard sizes. This helper lets the controller grow
    from tiny canaries toward a target task duration while capping geometric
    growth so failed/interrupted canaries remain cheap to replay.
    """

    target = _positive_float(target_task_seconds)
    if target is None:
        raise ValueError("target_task_seconds must be positive")
    if min_units <= 0:
        raise ValueError("min_units must be positive")
    if max_units is not None and max_units < min_units:
        raise ValueError("max_units must be >= min_units")
    if growth_factor <= 1 or not math.isfinite(growth_factor):
        raise ValueError("growth_factor must be finite and > 1")

    raw_summary_keys = {"telemetry", "elapsed_sec", "returncode", "timed_out", "framework_error", "stderr_tail", "commit_status"}
    normalized = [canary_observation_from_summary(obs) if raw_summary_keys.intersection(obs) else dict(obs) for obs in observations]
    if any(obs.get("oom") for obs in normalized):
        return {
            "schema": ADAPTIVE_SHARD_SCHEMA_V1,
            "status": "blocked",
            "selected_units_per_task": None,
            "target_task_seconds": target,
            "observations_used": 0,
            "reasons": [
                {
                    "code": "memory_shape_rejected_oom",
                    "severity": "error",
                    "message": ADAPTIVE_SHARD_REASON_CODES["memory_shape_rejected_oom"],
                }
            ],
        }

    rates: list[float] = []
    max_observed_units = 0
    for obs in normalized:
        if obs.get("success") is False:
            continue
        units = _positive_float(obs.get("completed_units"))
        seconds = _positive_float(obs.get("useful_compute_seconds"))
        rate = _positive_float(obs.get("units_per_second"))
        if rate is None and units and seconds:
            rate = units / seconds
        if rate is None:
            continue
        rates.append(rate)
        if units is not None:
            max_observed_units = max(max_observed_units, int(units))

    reasons: list[dict[str, Any]] = []
    if not rates:
        selected = min_units
        reasons.append({"code": "canary_required", "severity": "warning", "message": ADAPTIVE_SHARD_REASON_CODES["canary_required"]})
        median_rate = None
    else:
        median_rate = statistics.median(rates)
        selected = max(min_units, math.ceil(median_rate * target))
        reasons.append({"code": "target_duration_selected", "severity": "info", "message": ADAPTIVE_SHARD_REASON_CODES["target_duration_selected"]})
        growth_cap = max(min_units, math.ceil(max_observed_units * growth_factor)) if max_observed_units else selected
        if selected > growth_cap:
            selected = growth_cap
            reasons.append({"code": "geometric_growth_cap", "severity": "info", "message": ADAPTIVE_SHARD_REASON_CODES["geometric_growth_cap"]})

    if max_units is not None and selected > max_units:
        selected = max_units
        reasons.append({"code": "max_units_cap", "severity": "info", "message": ADAPTIVE_SHARD_REASON_CODES["max_units_cap"]})

    return {
        "schema": ADAPTIVE_SHARD_SCHEMA_V1,
        "status": "ready",
        "selected_units_per_task": selected,
        "target_task_seconds": target,
        "observations_used": len(rates),
        "median_units_per_second": median_rate,
        "reasons": reasons,
    }
