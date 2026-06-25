from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CostEstimate:
    expected_cost_usd: float
    expected_cost_per_1m_units: float
    compute_cost_per_1m_units: float
    confidence: str
    assumptions: dict[str, Any]


def expected_cost_per_1m_units(
    *,
    hourly_price: float,
    units_per_hour: float,
    replay_fraction: float = 0.0,
    startup_overhead_seconds: float = 0.0,
    useful_task_seconds: float = 3600.0,
    noncompute_per_1m: float = 0.0,
) -> float:
    """Shared Scout/Planner expected total cost per 1M useful units.

    The model is intentionally simple and explicit: compute price is adjusted by
    replay risk and startup amortization, then optional non-compute costs are
    added.  Callers must label defaulted prices/omitted components in their JSON
    so agents do not confuse this with exact billing.
    """

    if units_per_hour <= 0:
        return math.nan
    compute = (max(0.0, hourly_price) / units_per_hour) * 1_000_000.0
    startup_fraction = max(0.0, startup_overhead_seconds) / max(1.0, useful_task_seconds)
    return compute * (1.0 + max(0.0, replay_fraction) + startup_fraction) + max(0.0, noncompute_per_1m)


def estimate_worker_shape_cost(
    *,
    total_units: float,
    units_per_second_per_worker: float,
    worker_vcpus: float,
    vcpu_hour_usd: float,
    replay_fraction: float = 0.0,
    startup_overhead_seconds: float = 0.0,
    useful_task_seconds: float = 3600.0,
    noncompute_per_1m_units: float = 0.0,
    confidence: str = "price_defaulted",
) -> CostEstimate:
    """Estimate dollars for a worker shape using the same formula as Scout.

    Planner can call this with a conservative per-vCPU default; Scout can call
    the lower-level function with observed instance Spot prices.  The returned
    assumptions are designed to be embedded in Plan JSON.
    """

    units_per_hour = max(0.0, units_per_second_per_worker) * 3600.0
    hourly_price = max(0.0, worker_vcpus) * max(0.0, vcpu_hour_usd)
    per_1m = expected_cost_per_1m_units(
        hourly_price=hourly_price,
        units_per_hour=units_per_hour,
        replay_fraction=replay_fraction,
        startup_overhead_seconds=startup_overhead_seconds,
        useful_task_seconds=useful_task_seconds,
        noncompute_per_1m=noncompute_per_1m_units,
    )
    compute_only = expected_cost_per_1m_units(
        hourly_price=hourly_price,
        units_per_hour=units_per_hour,
        replay_fraction=0.0,
        startup_overhead_seconds=0.0,
        useful_task_seconds=useful_task_seconds,
        noncompute_per_1m=0.0,
    )
    expected_total = per_1m * max(0.0, total_units) / 1_000_000.0 if math.isfinite(per_1m) else math.nan
    return CostEstimate(
        expected_cost_usd=expected_total,
        expected_cost_per_1m_units=per_1m,
        compute_cost_per_1m_units=compute_only,
        confidence=confidence,
        assumptions={
            "hourly_price_usd": hourly_price,
            "worker_vcpus": worker_vcpus,
            "vcpu_hour_usd": vcpu_hour_usd,
            "units_per_second_per_worker": units_per_second_per_worker,
            "expected_replay_fraction": replay_fraction,
            "startup_overhead_seconds": startup_overhead_seconds,
            "useful_task_seconds": useful_task_seconds,
            "noncompute_cost_per_1m_units": noncompute_per_1m_units,
            "confidence": confidence,
        },
    )
