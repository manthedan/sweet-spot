from __future__ import annotations

import math
import re
import statistics
from typing import Any

ADAPTIVE_SHARD_SCHEMA_V1 = "sweetspot.adaptive_shard_decision.v1"
RESOURCE_SELECTION_SCHEMA_V1 = "sweetspot.resource_selection.v1"
LOGICAL_SHARD_PLAN_SCHEMA_V1 = "sweetspot.logical_shard_plan.v1"
SUCCESS_COMMIT_STATUSES = {"", "succeeded", "success", "committed"}
NON_BLOCKING_UNSUCCESSFUL_COMMIT_STATUSES = {"lost"}
ARCHITECTURE_ALIASES = {"amd64": "x86_64", "x64": "x86_64", "x86_64": "x86_64", "aarch64": "arm64", "arm64": "arm64"}
DEFAULT_RESOURCE_LATTICE: tuple[dict[str, int], ...] = (
    {"vcpus": 1, "memory_mib": 2048},
    {"vcpus": 2, "memory_mib": 4096},
    {"vcpus": 4, "memory_mib": 8192},
)

ADAPTIVE_SHARD_REASON_CODES: dict[str, str] = {
    "canary_required": "No measurable canary summary was available, so the next canary should use the minimum shard size.",
    "geometric_growth_cap": "The measured rate supports a larger shard, but growth was capped to keep canaries replay-safe.",
    "max_units_cap": "The selected shard size was capped by the configured maximum unit count.",
    "memory_shape_rejected_oom": "A canary reported an out-of-memory signal; increase memory or reject this resource shape before growing shards.",
    "canary_validation_failed": "A canary failed framework or output validation; fix the workload contract before producing production shards.",
    "target_duration_selected": "The selected shard size targets the configured replay-safe task duration from measured canary throughput.",
}

RESOURCE_SELECTION_REASON_CODES: dict[str, str] = {
    "arm_canary_failed": "ARM was requested but rejected after a failed compatibility, runtime, or validation canary.",
    "arm_cost_rejected": "ARM completed canaries but was materially worse than the best x86 candidate by measured useful-unit cost.",
    "arm_not_requested": "ARM was not included in the requested architecture set.",
    "canary_validation_failed": "A canary failed framework or output validation; reject this candidate before production.",
    "memory_shape_rejected_oom": "A resource shape reported an out-of-memory signal; retry a larger memory shape or reject it.",
    "resource_canary_required": "Architecture/resource telemetry is missing; run paired resource canaries before selecting a shape.",
    "resource_shape_selected": "The resource shape was selected by measured useful-unit cost among successful canaries.",
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


def _non_negative_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed >= 0 else None


def _commit_status(summary: dict[str, Any]) -> str:
    return str(summary.get("commit_status") or "").strip().lower()


def _normalize_architecture(value: Any) -> str | None:
    raw = str(value or "").strip().lower().replace("-", "_")
    return ARCHITECTURE_ALIASES.get(raw)


def _summary_text(summary: dict[str, Any], telemetry: dict[str, Any]) -> str:
    text = " ".join(str(summary.get(key) or "") for key in ("framework_error", "stderr_tail", "error", "commit_status", "commit_error"))
    return " ".join([text, str(telemetry.get("interruption_status") or ""), str(telemetry.get("metrics_error") or "")]).lower()


def _looks_like_oom(summary: dict[str, Any], telemetry: dict[str, Any]) -> bool:
    text = _summary_text(summary, telemetry)
    specific_markers = ("out of memory", "oomkilled", "memoryerror", "cannot allocate memory")
    return any(marker in text for marker in specific_markers) or re.search(r"\boom\b", text) is not None


def _looks_like_validation_failure(summary: dict[str, Any], telemetry: dict[str, Any]) -> bool:
    if summary.get("timed_out"):
        return False
    commit_status = _commit_status(summary)
    if commit_status and commit_status not in SUCCESS_COMMIT_STATUSES | NON_BLOCKING_UNSUCCESSFUL_COMMIT_STATUSES:
        return True
    text = _summary_text(summary, telemetry)
    if not text or _looks_like_oom(summary, telemetry):
        return False
    validation_markers = (
        "expected output file was not produced",
        "framework validation",
        "done marker",
        "output logical_uri mismatch",
        "output uri does not match",
        "output validation",
        "validation failed",
    )
    return any(marker in text for marker in validation_markers)


def canary_observation_from_summary(summary: dict[str, Any]) -> dict[str, Any]:
    """Extract adaptive-shard telemetry from a SweetSpot task summary.

    The returned object is intentionally small and stable so future run-controller
    code can consume either raw worker summaries or normalized observations.
    """

    telemetry_raw = summary.get("telemetry")
    telemetry = telemetry_raw if isinstance(telemetry_raw, dict) else {}
    units = _positive_float(telemetry.get("completed_units") or summary.get("completed_units"))
    seconds = _positive_float(telemetry.get("useful_compute_seconds") or summary.get("elapsed_sec"))
    commit_status = _commit_status(summary)
    success = summary.get("returncode") in (None, 0) and not summary.get("timed_out") and not summary.get("framework_error") and commit_status in SUCCESS_COMMIT_STATUSES
    architecture = _normalize_architecture(telemetry.get("architecture") or summary.get("architecture"))
    worker_vcpus = _positive_float(telemetry.get("worker_vcpus") or summary.get("worker_vcpus") or summary.get("vcpus"))
    hourly_price_usd = _positive_float(telemetry.get("hourly_price_usd") or telemetry.get("spot_hourly_price_usd") or summary.get("hourly_price_usd"))
    vcpu_hour_usd = _positive_float(telemetry.get("vcpu_hour_usd") or telemetry.get("vcpu_hour_price_usd") or summary.get("vcpu_hour_usd"))
    if vcpu_hour_usd is None and hourly_price_usd is not None and worker_vcpus:
        vcpu_hour_usd = hourly_price_usd / worker_vcpus
    replay_fraction = _non_negative_float(telemetry.get("replay_fraction") or summary.get("replay_fraction"))
    discarded_compute_seconds = _non_negative_float(telemetry.get("discarded_compute_seconds") or summary.get("discarded_compute_seconds"))
    if replay_fraction is None and discarded_compute_seconds is not None and seconds and seconds > 0:
        replay_fraction = discarded_compute_seconds / seconds
    observation: dict[str, Any] = {
        "task_id": summary.get("task_id"),
        "success": bool(success),
        "completed_units": units,
        "useful_compute_seconds": seconds,
        "oom": _looks_like_oom(summary, telemetry),
        "validation_failed": _looks_like_validation_failure(summary, telemetry),
        "architecture": architecture,
        "region": telemetry.get("region") or summary.get("region"),
        "instance_type": telemetry.get("instance_type") or summary.get("instance_type"),
        "worker_vcpus": worker_vcpus,
        "worker_memory_mib": _positive_float(telemetry.get("worker_memory_mib") or summary.get("worker_memory_mib") or summary.get("memory_mib")),
        "peak_memory_mib": _positive_float(telemetry.get("peak_memory_mib") or summary.get("peak_memory_mib")),
        "hourly_price_usd": hourly_price_usd,
        "vcpu_hour_usd": vcpu_hour_usd,
        "replay_fraction": replay_fraction,
        "startup_overhead_seconds": _non_negative_float(telemetry.get("startup_overhead_seconds") or telemetry.get("startup_delay_seconds") or summary.get("startup_overhead_seconds")),
        "placement_score": _non_negative_float(telemetry.get("placement_score") or summary.get("placement_score")),
    }
    if units and seconds:
        observation["units_per_second"] = units / seconds
    return {k: v for k, v in observation.items() if v is not None}


def _resource_shape_from_observation(obs: dict[str, Any]) -> tuple[str, float, float, str | None] | None:
    architecture = _normalize_architecture(obs.get("architecture"))
    vcpus = _positive_float(obs.get("worker_vcpus") or obs.get("vcpus"))
    memory_mib = _positive_float(obs.get("worker_memory_mib") or obs.get("memory_mib"))
    region = obs.get("region") if isinstance(obs.get("region"), str) and obs.get("region") else None
    if architecture is None or vcpus is None or memory_mib is None:
        return None
    return architecture, vcpus, memory_mib, region


def choose_resource_candidate(
    observations: list[dict[str, Any]],
    *,
    allowed_architectures: list[str] | tuple[str, ...] = ("x86_64",),
    arm_cost_penalty_threshold: float = 1.10,
) -> dict[str, Any]:
    """Select an architecture/resource shape from measured canary summaries.

    The selector is deliberately conservative: ARM is considered only when the
    JobSpec allowed it, failed validation/OOM candidates are rejected, and ARM
    must beat (or be within the configured tolerance of) the best successful x86
    useful-unit cost.  Cost is represented as vCPU-seconds per useful unit when
    no account-specific price telemetry is available.
    """

    allowed = {_normalize_architecture(arch) for arch in allowed_architectures}
    allowed.discard(None)
    normalized = [
        canary_observation_from_summary(obs) if {"telemetry", "elapsed_sec", "returncode", "timed_out", "framework_error", "stderr_tail", "commit_status"}.intersection(obs) else dict(obs) for obs in observations
    ]
    reasons: list[dict[str, Any]] = []
    if "arm64" not in allowed:
        reasons.append({"code": "arm_not_requested", "severity": "info", "message": RESOURCE_SELECTION_REASON_CODES["arm_not_requested"]})

    grouped: dict[tuple[str, float, float, str | None], list[dict[str, Any]]] = {}
    for obs in normalized:
        shape = _resource_shape_from_observation(obs)
        if shape is None or shape[0] not in allowed:
            continue
        grouped.setdefault(shape, []).append(obs)

    candidates: list[dict[str, Any]] = []
    for (architecture, vcpus, memory_mib, region), rows in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1], item[0][2], item[0][3] or "")):
        candidate: dict[str, Any] = {
            "architecture": architecture,
            "vcpus": vcpus,
            "memory_mib": memory_mib,
            "region": region,
            "observations": len(rows),
        }
        if any(row.get("oom") for row in rows):
            candidate.update({"status": "rejected", "reason_code": "memory_shape_rejected_oom", "message": RESOURCE_SELECTION_REASON_CODES["memory_shape_rejected_oom"]})
        elif any(row.get("validation_failed") for row in rows):
            candidate.update({"status": "rejected", "reason_code": "canary_validation_failed", "message": RESOURCE_SELECTION_REASON_CODES["canary_validation_failed"]})
        elif any(row.get("success") is False for row in rows):
            reason_code = "arm_canary_failed" if architecture == "arm64" else "resource_canary_required"
            candidate.update({"status": "rejected", "reason_code": reason_code, "message": RESOURCE_SELECTION_REASON_CODES[reason_code]})
        else:
            rates: list[float] = []
            for row in rows:
                if row.get("success") is False:
                    continue
                rate = _positive_float(row.get("units_per_second"))
                if rate is None:
                    units = _positive_float(row.get("completed_units"))
                    seconds = _positive_float(row.get("useful_compute_seconds"))
                    rate = units / seconds if units and seconds else None
                if rate is not None:
                    rates.append(rate)
            if rates:
                median_rate = statistics.median(rates)
                price_scores: list[float] = []
                vcpu_prices: list[float] = []
                replay_fractions: list[float] = []
                startup_overheads: list[float] = []
                placement_scores: list[float] = []
                for row in rows:
                    if row.get("success") is False:
                        continue
                    rate = _positive_float(row.get("units_per_second"))
                    if rate is None:
                        units = _positive_float(row.get("completed_units"))
                        seconds = _positive_float(row.get("useful_compute_seconds"))
                        rate = units / seconds if units and seconds else None
                    vcpu_price = _positive_float(row.get("vcpu_hour_usd"))
                    if rate is not None and vcpu_price is not None:
                        replay = _non_negative_float(row.get("replay_fraction")) or 0.0
                        startup = _non_negative_float(row.get("startup_overhead_seconds")) or 0.0
                        useful = _positive_float(row.get("useful_compute_seconds")) or 3600.0
                        price_scores.append(((vcpus * vcpu_price) / (rate * 3600.0)) * 1_000_000.0 * (1.0 + replay + startup / max(1.0, useful)))
                        vcpu_prices.append(vcpu_price)
                        replay_fractions.append(replay)
                        startup_overheads.append(startup)
                    placement = _non_negative_float(row.get("placement_score"))
                    if placement is not None:
                        placement_scores.append(placement)
                candidate_payload: dict[str, Any] = {
                    "status": "ready",
                    "successful_observations": len(rates),
                    "median_units_per_second": median_rate,
                    "vcpu_seconds_per_unit": vcpus / median_rate,
                }
                if price_scores:
                    median_price = statistics.median(price_scores)
                    median_vcpu_price = statistics.median(vcpu_prices)
                    candidate_payload.update(
                        {
                            "expected_cost_per_1m_units": median_price,
                            "vcpu_hour_usd": median_vcpu_price,
                            "hourly_price_usd": median_vcpu_price * vcpus,
                            "pricing_observations": len(price_scores),
                            "replay_fraction": statistics.median(replay_fractions),
                            "startup_overhead_seconds": statistics.median(startup_overheads),
                            "cost_basis": "telemetry_price_replay_startup",
                        }
                    )
                if placement_scores:
                    candidate_payload["placement_score"] = statistics.median(placement_scores)
                    candidate_payload["placement_observations"] = len(placement_scores)
                candidate.update(candidate_payload)
            else:
                candidate.update(
                    {
                        "status": "rejected",
                        "reason_code": "arm_canary_failed" if architecture == "arm64" else "resource_canary_required",
                        "message": RESOURCE_SELECTION_REASON_CODES.get("arm_canary_failed" if architecture == "arm64" else "resource_canary_required"),
                    }
                )
        candidates.append({k: v for k, v in candidate.items() if v is not None})

    ready_candidates = [candidate for candidate in candidates if candidate.get("status") == "ready"]
    if not candidates:
        reasons.append({"code": "resource_canary_required", "severity": "warning", "message": RESOURCE_SELECTION_REASON_CODES["resource_canary_required"]})
        return {"schema": RESOURCE_SELECTION_SCHEMA_V1, "status": "needs_canary", "selected": None, "candidates": [], "reasons": reasons}
    if not ready_candidates:
        if any(candidate.get("architecture") == "arm64" for candidate in candidates):
            reasons.append({"code": "arm_canary_failed", "severity": "warning", "message": RESOURCE_SELECTION_REASON_CODES["arm_canary_failed"]})
        return {"schema": RESOURCE_SELECTION_SCHEMA_V1, "status": "blocked", "selected": None, "candidates": candidates, "reasons": reasons}

    use_priced_selection = all(candidate.get("expected_cost_per_1m_units") is not None for candidate in ready_candidates)

    def selection_cost(candidate: dict[str, Any]) -> float:
        if use_priced_selection:
            return float(candidate["expected_cost_per_1m_units"])
        return float(candidate["vcpu_seconds_per_unit"])

    selected = min(ready_candidates, key=selection_cost)
    best_x86 = min((candidate for candidate in ready_candidates if candidate.get("architecture") == "x86_64"), key=selection_cost, default=None)
    if selected.get("architecture") == "arm64" and "x86_64" in allowed and best_x86 is None:
        reasons.append({"code": "resource_canary_required", "severity": "warning", "message": RESOURCE_SELECTION_REASON_CODES["resource_canary_required"]})
        return {"schema": RESOURCE_SELECTION_SCHEMA_V1, "status": "needs_canary", "selected": None, "candidates": candidates, "reasons": reasons}
    if best_x86 is not None:
        x86_cost = selection_cost(best_x86)
        rejected_arm = False
        for candidate in candidates:
            if candidate.get("architecture") != "arm64" or candidate.get("status") != "ready":
                continue
            arm_cost = selection_cost(candidate)
            if arm_cost > x86_cost * arm_cost_penalty_threshold:
                candidate["status"] = "rejected"
                candidate["reason_code"] = "arm_cost_rejected"
                candidate["message"] = RESOURCE_SELECTION_REASON_CODES["arm_cost_rejected"]
                rejected_arm = True
                if selected is candidate:
                    selected = best_x86
        if rejected_arm:
            reasons.append({"code": "arm_cost_rejected", "severity": "info", "message": RESOURCE_SELECTION_REASON_CODES["arm_cost_rejected"]})
    reasons.append({"code": "resource_shape_selected", "severity": "info", "message": RESOURCE_SELECTION_REASON_CODES["resource_shape_selected"]})
    return {"schema": RESOURCE_SELECTION_SCHEMA_V1, "status": "ready", "selected": dict(selected), "candidates": candidates, "reasons": reasons}


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
        return _blocked_adaptive_decision("memory_shape_rejected_oom", target)
    if any(obs.get("validation_failed") for obs in normalized):
        return _blocked_adaptive_decision("canary_validation_failed", target)

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
    calibrated = False
    next_action = "run_canary"
    median_observed_seconds = None
    if not rates:
        selected = min_units
        reasons.append({"code": "canary_required", "severity": "warning", "message": ADAPTIVE_SHARD_REASON_CODES["canary_required"]})
        median_rate = None
    else:
        median_rate = statistics.median(rates)
        observed_seconds: list[float] = []
        for obs in normalized:
            seconds = _positive_float(obs.get("useful_compute_seconds"))
            if seconds is not None and obs.get("success") is not False:
                observed_seconds.append(seconds)
        median_observed_seconds = statistics.median(observed_seconds) if observed_seconds else None
        selected = max(min_units, math.ceil(median_rate * target))
        reasons.append({"code": "target_duration_selected", "severity": "info", "message": ADAPTIVE_SHARD_REASON_CODES["target_duration_selected"]})
        growth_cap = max(min_units, math.ceil(max_observed_units * growth_factor)) if max_observed_units else selected
        if selected > growth_cap:
            selected = growth_cap
            reasons.append({"code": "geometric_growth_cap", "severity": "info", "message": ADAPTIVE_SHARD_REASON_CODES["geometric_growth_cap"]})
        else:
            calibrated = True
            next_action = "produce_production"

    if max_units is not None and selected > max_units:
        selected = max_units
        calibrated = bool(rates)
        next_action = "produce_production" if calibrated else "run_canary"
        reasons.append({"code": "max_units_cap", "severity": "info", "message": ADAPTIVE_SHARD_REASON_CODES["max_units_cap"]})

    return {
        "schema": ADAPTIVE_SHARD_SCHEMA_V1,
        "status": "ready",
        "selected_units_per_task": selected,
        "target_task_seconds": target,
        "observations_used": len(rates),
        "median_units_per_second": median_rate,
        "median_observed_task_seconds": median_observed_seconds,
        "calibrated": calibrated,
        "next_action": next_action,
        "reasons": reasons,
    }


def _blocked_adaptive_decision(code: str, target_task_seconds: float) -> dict[str, Any]:
    return {
        "schema": ADAPTIVE_SHARD_SCHEMA_V1,
        "status": "blocked",
        "selected_units_per_task": None,
        "target_task_seconds": target_task_seconds,
        "observations_used": 0,
        "calibrated": False,
        "next_action": "blocked",
        "reasons": [
            {
                "code": code,
                "severity": "error",
                "message": ADAPTIVE_SHARD_REASON_CODES[code],
            }
        ],
    }
