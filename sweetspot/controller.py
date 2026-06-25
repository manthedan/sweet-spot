from __future__ import annotations

from typing import Any


def choose_worker_top_up(*, backlog: int, active_workers: int, target_workers: int) -> int:
    """Return a bounded reconciliation top-up count.

    `backlog` is run-relevant visible/not-visible work, `active_workers` is the
    currently observed run-scoped Batch capacity, and `target_workers` comes from
    the authoritative Plan.  The controller never submits more than the Plan's
    target in one decision.
    """

    if backlog <= 0 or target_workers <= 0:
        return 0
    return max(0, min(target_workers, target_workers - max(0, active_workers)))


def canary_candidate_key(task: dict[str, Any]) -> str | None:
    input_obj = task.get("input")
    if not isinstance(input_obj, dict):
        return None
    arch = input_obj.get("candidate_architecture")
    vcpus = input_obj.get("candidate_vcpus")
    memory = input_obj.get("candidate_memory_mib")
    units = input_obj.get("canary_units_per_task")
    if arch is None or vcpus is None or memory is None:
        return None
    return f"{arch}-{vcpus}vcpu-{memory}mib-u{units}"


def group_canary_tasks_by_candidate(tasks: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for task in tasks:
        key = canary_candidate_key(task)
        if key is None:
            raise ValueError(f"canary task {task.get('task_id')!r} is missing candidate routing metadata")
        groups.setdefault(key, []).append(task)
    return groups
