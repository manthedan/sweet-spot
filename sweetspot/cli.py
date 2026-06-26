from __future__ import annotations

import argparse
import hashlib
import importlib.metadata as importlib_metadata
import json
import math
import os
import re
import shlex
import statistics
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

import boto3

from . import lane_manager, scout
from .aws_batch import ACTIVE_STATUSES, active_jobs, desired_worker_count, iso_now, queue_depth, utc_stamp
from .batch_service import (
    redact_env as _redact_env,
    safe_active_worker_count as _safe_active_worker_count,
    submit_worker_jobs as _submit_worker_jobs,
    supervisor_desired_workers as _supervisor_desired_workers,
    worker_overrides as _worker_overrides,
)
from .canary_service import collect_canary_summaries as _collect_canary_summaries
from .controller import group_canary_tasks_by_candidate
from .deployment import (
    DeploymentTarget,
    canary_deployment_target_for,
    load_deployment,
    local_manifest_identity,
    manifest_identity_from_head,
    select_canary_deployment_region,
    select_deployment_target,
    validate_local_manifest_matches_head,
    validate_target_matches_job,
)
from .enqueue_service import (
    send_tasks_to_sqs as _send_tasks_to_sqs,
    validate_tasks_for_enqueue as _validate_tasks_for_enqueue,
    wait_for_visible_backlog as _wait_for_visible_backlog,
    write_enqueue_artifacts as _write_enqueue_artifacts,
)
from .finalize_service import (
    FINALIZER_DEFAULT_MAX_INLINE_OUTPUTS,
    S3ExistenceIndex as _S3ExistenceIndex,
    check_task as _check_task,
    finalizer_existence_index as _service_finalizer_existence_index,
    iter_tasks_for_finalizer as _service_iter_tasks_for_finalizer,
    read_tasks_for_finalizer as _service_read_tasks_for_finalizer,
    run_finalizer as _run_finalizer_service,
    run_integrated_finalizer as _run_integrated_finalizer_service,
)
from .output import format_table_value as _format_table_value, print_key_values as _print_key_values, print_table as _print_table
from .planner import PlannerSpecError, initial_blocked_plan, iter_canary_tasks_from_logical_unit_count, iter_production_tasks_from_logical_unit_count, load_job_spec, plan_with_adaptive_canaries
from .reconcile_service import run_worker_reconciliation as _run_worker_reconciliation_service
from .repair_service import repair_plan_report as _repair_plan_report_service, run_repair as _run_repair_service
from .run_state import (
    load_run_state as _load_run_state,
    phase_by_name as _phase_by_name,
    phase_completed as _phase_completed,
    replace_or_append_phase as _replace_or_append_phase,
    run_state_has_apply_progress as _run_state_has_apply_progress,
    write_run_state as _write_run_state,
)
from .s3util import parse_s3_uri, s3_join, s3_upload_text
from .task_model import default_done_s3, parse_allowed_s3_prefixes
from .worker import DEFAULT_LOG_TAIL_BYTES, DEFAULT_MAX_LOG_BYTES, SAFE_TASK_TIMEOUT_SECONDS, parse_redact_patterns, run_worker, validate_worker_timing


def _iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at {path}:{line_no}") from exc
            if not isinstance(obj, dict):
                raise ValueError(f"task at {path}:{line_no} is not an object")
            yield obj


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return list(_iter_jsonl(path))


def _count_jsonl_objects(path: Path) -> int:
    return sum(1 for _ in _iter_jsonl(path))


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _env_allowed_s3_prefixes() -> list[str]:
    return list(parse_allowed_s3_prefixes(os.environ.get("SWEETSPOT_ALLOWED_S3_PREFIXES")))


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _aws_client(args: argparse.Namespace, service: str):
    profile = getattr(args, "profile", None)
    region = getattr(args, "region", None)
    if profile or region:
        return boto3.Session(profile_name=profile, region_name=region).client(service, region_name=region)
    return boto3.client(service)


def _extract_task_id_from_log_message(message: str) -> str | None:
    try:
        obj = json.loads(message)
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict):
        return None
    task_id = obj.get("task_id")
    return str(task_id) if task_id else None


def _sample_from_runtime_obj(obj: dict[str, Any]) -> tuple[float, float] | None:
    raw_telemetry = obj.get("telemetry")
    telemetry: dict[str, Any] = raw_telemetry if isinstance(raw_telemetry, dict) else {}
    candidates: list[dict[str, Any]] = [obj, telemetry]
    for source in candidates:
        units: Any = source.get("completed_units") or source.get("labels") or source.get("rows")
        seconds: Any = source.get("useful_compute_seconds") or source.get("useful_compute_sec") or source.get("elapsed_sec") or source.get("seconds")
        try:
            units_f = float(units)
            seconds_f = float(seconds)
        except (TypeError, ValueError):
            continue
        if units_f > 0 and seconds_f > 0:
            return units_f, seconds_f
    labels: Any = obj.get("labels")
    labels_per_second: Any = obj.get("labels_per_second") or obj.get("units_per_second")
    try:
        units_f = float(labels)
        rate_f = float(labels_per_second)
    except (TypeError, ValueError):
        return None
    if units_f > 0 and rate_f > 0:
        return units_f, units_f / rate_f
    return None


def _parse_index_selection(raw: str, n: int) -> list[int]:
    if raw in {"", "auto"}:
        return []
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            start = int(a)
            end = int(b)
            if start > end:
                raise SystemExit(f"invalid descending index range: {part}")
            out.extend(range(start, end + 1))
        else:
            out.append(int(part))
    if not out:
        raise SystemExit("explicit --selected-indices did not select any tasks")
    bad = [i for i in out if i < 0 or i >= n]
    if bad:
        raise SystemExit(f"selected indices out of range for {n} tasks: {bad}")
    return sorted(dict.fromkeys(out))


def _auto_canary_indices(tasks: list[dict[str, Any]], task_count: int) -> list[int]:
    if task_count <= 0:
        raise SystemExit("--task-count must be positive")
    if not tasks:
        raise SystemExit("empty tasks JSONL")
    n = len(tasks)
    selected: list[int] = []

    def add(i: int | None) -> None:
        if i is not None and 0 <= i < n and i not in selected and len(selected) < task_count:
            selected.append(i)

    add(0)
    add(n - 1)
    first_schema = tasks[0].get("schema")
    add(next((i for i, t in enumerate(tasks) if t.get("schema") != first_schema), None))
    first_run = tasks[0].get("run_id")
    add(next((i for i, t in enumerate(tasks) if t.get("run_id") != first_run), None))
    if len(selected) < task_count:
        candidates = [round(i * (n - 1) / max(1, task_count - 1)) for i in range(task_count)]
        for i in candidates:
            add(int(i))
    for i in range(n):
        add(i)
        if len(selected) >= task_count:
            break
    return selected


def _add_parser_with_examples(subparsers: Any, name: str, *, help: str | None = None, examples: str) -> argparse.ArgumentParser:
    return subparsers.add_parser(name, help=help, formatter_class=argparse.RawDescriptionHelpFormatter, epilog=f"examples:\n{examples}")


def _add_batch_worker_target_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--batch-job-queue", required=True)
    parser.add_argument("--job-definition", required=True)
    parser.add_argument("--job-name-prefix", default="sweetspot-worker")


def _add_worker_sizing_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--messages-per-worker", type=int, default=1)
    parser.add_argument("--max-workers", type=int, default=64)
    parser.add_argument("--min-workers", type=int, default=0)
    parser.add_argument("--subtract-active", action="store_true")
    parser.add_argument("--include-not-visible", action="store_true")


def _add_legacy_run_override_args(parser: argparse.ArgumentParser) -> None:
    """Accept old run sizing flags without advertising them as the normal path."""

    parser.add_argument("--messages-per-worker", type=int, default=1, help=argparse.SUPPRESS)
    parser.add_argument("--max-workers", type=int, default=64, help=argparse.SUPPRESS)
    parser.add_argument("--min-workers", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--subtract-active", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--include-not-visible", action="store_true", help=argparse.SUPPRESS)


def _add_legacy_run_runtime_args(parser: argparse.ArgumentParser, *, legacy_done_markers_help: str) -> None:
    parser.add_argument("--vcpus", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--memory", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--visibility-timeout", type=int, default=1800, help=argparse.SUPPRESS)
    parser.add_argument("--heartbeat-seconds", type=int, default=300, help=argparse.SUPPRESS)
    parser.add_argument("--task-timeout-seconds", type=float, default=SAFE_TASK_TIMEOUT_SECONDS, help=argparse.SUPPRESS)
    parser.add_argument("--retry-attempts", type=int)
    parser.add_argument("--env", action="append", type=_parse_env_pair, default=[])
    parser.add_argument("--allowed-s3-prefix", action="append", default=[], help="Pass SWEETSPOT_ALLOWED_S3_PREFIXES to workers; repeatable.")
    parser.add_argument("--log-tail-bytes", type=int, default=DEFAULT_LOG_TAIL_BYTES)
    parser.add_argument("--max-log-bytes", type=int, default=DEFAULT_MAX_LOG_BYTES)
    parser.add_argument("--redact-regex", action="append", default=[], help="Regex to redact from worker task logs; repeatable.")
    parser.add_argument("--allow-legacy-done-markers", action="store_true", help=legacy_done_markers_help)


def _add_worker_runtime_args(parser: argparse.ArgumentParser, *, legacy_done_markers_help: str) -> None:
    parser.add_argument("--vcpus", type=int)
    parser.add_argument("--memory", type=int)
    parser.add_argument("--visibility-timeout", type=int, default=1800)
    parser.add_argument("--heartbeat-seconds", type=int, default=300)
    parser.add_argument("--task-timeout-seconds", type=float, default=SAFE_TASK_TIMEOUT_SECONDS, help="Default per-task command timeout to pass to workers")
    parser.add_argument("--retry-attempts", type=int)
    parser.add_argument("--env", action="append", type=_parse_env_pair, default=[])
    parser.add_argument("--allowed-s3-prefix", action="append", default=[], help="Pass SWEETSPOT_ALLOWED_S3_PREFIXES to workers; repeatable.")
    parser.add_argument("--log-tail-bytes", type=int, default=DEFAULT_LOG_TAIL_BYTES)
    parser.add_argument("--max-log-bytes", type=int, default=DEFAULT_MAX_LOG_BYTES)
    parser.add_argument("--redact-regex", action="append", default=[], help="Regex to redact from worker task logs; repeatable.")
    parser.add_argument("--allow-legacy-done-markers", action="store_true", help=legacy_done_markers_help)


def cmd_version(args: argparse.Namespace) -> int:
    try:
        version = importlib_metadata.version("sweetspot")
    except importlib_metadata.PackageNotFoundError:
        version = "0+unknown"
    print(json.dumps({"schema": "sweetspot.version.v1", "package": "sweetspot", "version": version}, indent=2, sort_keys=True))
    return 0


def _plan_from_optional_adaptive_inputs(
    spec: dict[str, Any],
    *,
    canary_summary_jsonl: Path | None,
    input_manifest_jsonl: Path | None,
) -> tuple[dict[str, Any], int | None]:
    logical_unit_count = _count_jsonl_objects(input_manifest_jsonl) if input_manifest_jsonl else None
    if canary_summary_jsonl or input_manifest_jsonl:
        observations = _read_jsonl(canary_summary_jsonl) if canary_summary_jsonl else []
        return plan_with_adaptive_canaries(spec, observations, logical_unit_count=logical_unit_count), logical_unit_count
    return initial_blocked_plan(spec), logical_unit_count


def _plan_task_timeout_seconds(plan: dict[str, Any]) -> float | None:
    selected = plan.get("selected")
    raw: Any | None = None
    if isinstance(selected, dict):
        raw = selected.get("task_timeout_seconds")
    if raw is None:
        decision = plan.get("canaries", [{}])[0].get("decision", {}) if isinstance(plan.get("canaries"), list) else {}
        if isinstance(decision, dict) and decision.get("target_task_seconds") is not None:
            try:
                raw = min(39600.0, max(1.0, math.ceil(float(decision["target_task_seconds"]) * 2.0)))
            except (TypeError, ValueError):
                raw = None
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _task_with_plan_timeout(task: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    timeout = _plan_task_timeout_seconds(plan)
    if timeout is None or task.get("timeout_seconds") is not None:
        return task
    out = dict(task)
    out["timeout_seconds"] = timeout
    return out


def _write_production_tasks_from_plan(spec: dict[str, Any], plan: dict[str, Any], logical_unit_count: int | None, path: Path) -> int:
    if logical_unit_count is None:
        raise SystemExit("--out-production-tasks-jsonl requires --canary-summary-jsonl and --input-manifest-jsonl")
    decision = plan["canaries"][0]["decision"]
    selected_units = decision.get("selected_units_per_task")
    observations_used = decision.get("observations_used")
    resource_selection = plan.get("canaries", [{}])[0].get("resource_selection", {})
    if (
        plan.get("status") != "ready"
        or not isinstance(resource_selection, dict)
        or resource_selection.get("status") != "ready"
        or not isinstance(selected_units, int)
        or not isinstance(observations_used, int)
        or observations_used <= 0
        or decision.get("next_action") != "produce_production"
    ):
        raise SystemExit("adaptive shard and resource decisions need calibrated measured canary summaries before production tasks can be written")
    path.parent.mkdir(parents=True, exist_ok=True)
    task_count = 0
    with path.open("w", encoding="utf-8") as f:
        for task in iter_production_tasks_from_logical_unit_count(spec, logical_unit_count, selected_units):
            f.write(json.dumps(_task_with_plan_timeout(task, plan), sort_keys=True) + "\n")
            task_count += 1
    plan.setdefault("artifacts", {})["production_tasks_jsonl"] = str(path)
    plan["artifacts"]["production_task_count"] = task_count
    return task_count


def _write_canary_tasks_from_plan(spec: dict[str, Any], plan: dict[str, Any], logical_unit_count: int | None, path: Path, *, max_tasks: int = 3) -> int:
    if logical_unit_count is None:
        raise SystemExit("canary task output requires --input-manifest-jsonl")
    decision = plan.get("canaries", [{}])[0].get("decision", {})
    selected_units = decision.get("selected_units_per_task") if isinstance(decision, dict) else None
    if not isinstance(selected_units, int) or decision.get("next_action") != "run_canary":
        raise SystemExit("adaptive shard decision does not request canary tasks")
    path.parent.mkdir(parents=True, exist_ok=True)
    task_count = 0
    with path.open("w", encoding="utf-8") as f:
        for task in iter_canary_tasks_from_logical_unit_count(spec, logical_unit_count, selected_units, max_tasks=max_tasks):
            f.write(json.dumps(_task_with_plan_timeout(task, plan), sort_keys=True) + "\n")
            task_count += 1
    if task_count == 0:
        raise SystemExit("input manifest is empty; cannot write canary tasks")
    plan.setdefault("artifacts", {})["canary_tasks_jsonl"] = str(path)
    plan["artifacts"]["canary_task_count"] = task_count
    return task_count


def cmd_plan(args: argparse.Namespace) -> int:
    try:
        spec = load_job_spec(args.job_spec)
        if args.out_production_tasks_jsonl and not (args.canary_summary_jsonl and args.input_manifest_jsonl):
            raise SystemExit("--out-production-tasks-jsonl requires --canary-summary-jsonl and --input-manifest-jsonl")
        if args.out_canary_tasks_jsonl and not args.input_manifest_jsonl:
            raise SystemExit("--out-canary-tasks-jsonl requires --input-manifest-jsonl")
        plan, logical_unit_count = _plan_from_optional_adaptive_inputs(
            spec,
            canary_summary_jsonl=args.canary_summary_jsonl,
            input_manifest_jsonl=args.input_manifest_jsonl,
        )
        if args.out_production_tasks_jsonl:
            _write_production_tasks_from_plan(spec, plan, logical_unit_count, args.out_production_tasks_jsonl)
        if args.out_canary_tasks_jsonl:
            _write_canary_tasks_from_plan(spec, plan, logical_unit_count, args.out_canary_tasks_jsonl)
    except PlannerSpecError as exc:
        raise SystemExit(str(exc)) from exc
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(plan, indent=2, sort_keys=True))
    return 0


def _sha256_json_obj(obj: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _default_run_allowed_s3_prefixes(args: argparse.Namespace, spec: dict[str, Any]) -> list[str]:
    explicit = getattr(args, "allowed_s3_prefix", None) or _env_allowed_s3_prefixes()
    if explicit:
        return list(parse_allowed_s3_prefixes(explicit))
    input_bucket, input_key = parse_s3_uri(str(spec["input_manifest"]))
    input_parent = input_key.rsplit("/", 1)[0] if "/" in input_key else ""
    input_prefix = f"s3://{input_bucket}/{input_parent}" if input_parent else f"s3://{input_bucket}/"
    return list(parse_allowed_s3_prefixes([input_prefix, str(spec["output_prefix"])]))


def _build_run_report(
    *,
    spec: dict[str, Any],
    plan: dict[str, Any],
    mode: str,
    applied: bool,
    status: str,
    artifacts: dict[str, Any],
    phases: list[dict[str, Any]],
    job_spec_sha256: str,
    controller: dict[str, Any],
    next_actions: list[str],
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema": "sweetspot.run.v1",
        "run_id": spec["run_id"],
        "job_spec_sha256": job_spec_sha256,
        "mode": mode,
        "applied": applied,
        "status": status,
        "controller": controller,
        "plan": plan,
        "phases": phases,
        "next_actions": next_actions,
    }
    if artifacts:
        report["artifacts"] = artifacts
    return report


def _canary_generation_dir(artifact_dir: Path, generation: int) -> Path:
    return artifact_dir / f"canary_generation_{generation:03d}"


def _materialize_run_tasks(args: argparse.Namespace, spec: dict[str, Any], plan: dict[str, Any], logical_unit_count: int | None) -> tuple[dict[str, Any], Path | None]:
    artifacts: dict[str, Any] = {}
    tasks_path = args.out_production_tasks_jsonl
    decision = plan.get("canaries", [{}])[0].get("decision", {})
    resource_selection = plan.get("canaries", [{}])[0].get("resource_selection", {})
    if tasks_path is None and args.artifact_dir and logical_unit_count is not None and args.canary_summary_jsonl:
        if (
            plan.get("status") == "ready"
            and isinstance(resource_selection, dict)
            and resource_selection.get("status") == "ready"
            and isinstance(decision, dict)
            and isinstance(decision.get("selected_units_per_task"), int)
            and decision.get("next_action") == "produce_production"
        ):
            tasks_path = args.artifact_dir / "production_tasks.jsonl"
    if tasks_path:
        if not (args.canary_summary_jsonl and args.input_manifest_jsonl):
            raise SystemExit("--out-production-tasks-jsonl requires --canary-summary-jsonl and --input-manifest-jsonl")
        _write_production_tasks_from_plan(spec, plan, logical_unit_count, tasks_path)
        artifacts["production_tasks_jsonl"] = str(tasks_path)
        artifacts["production_task_count"] = plan.get("artifacts", {}).get("production_task_count")
        artifacts["production_tasks_sha256"] = _sha256_file(tasks_path)
    if args.artifact_dir and logical_unit_count is not None and "production_tasks_jsonl" not in artifacts:
        if isinstance(decision, dict) and decision.get("next_action") == "run_canary":
            canary_generation = 0
            canary_tasks_path = _canary_generation_dir(args.artifact_dir, canary_generation) / "canary_tasks.jsonl"
            _write_canary_tasks_from_plan(spec, plan, logical_unit_count, canary_tasks_path)
            artifacts["canary_generation"] = canary_generation
            artifacts["canary_tasks_jsonl"] = str(canary_tasks_path)
            artifacts["canary_task_count"] = plan.get("artifacts", {}).get("canary_task_count")
            artifacts["canary_tasks_sha256"] = _sha256_file(canary_tasks_path)
    return artifacts, tasks_path


def _ceil_positive_int(raw: Any, field: str) -> int:
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"ready Plan selected.{field} must be a positive number") from exc
    if value <= 0:
        raise SystemExit(f"ready Plan selected.{field} must be a positive number")
    return max(1, int(math.ceil(value)))


def _runtime_settings_from_plan(plan: dict[str, Any]) -> dict[str, int | float]:
    selected = plan.get("selected")
    if plan.get("status") != "ready" or not isinstance(selected, dict):
        raise SystemExit("sweetspot run --apply requires a ready Plan with selected execution settings")
    return {
        "vcpus": _ceil_positive_int(selected.get("vcpus"), "vcpus"),
        "memory_mib": _ceil_positive_int(selected.get("memory_mib"), "memory_mib"),
        "estimated_workers": _ceil_positive_int(selected.get("estimated_workers"), "estimated_workers"),
        "task_timeout_seconds": float(selected.get("task_timeout_seconds") or selected.get("target_task_seconds") or SAFE_TASK_TIMEOUT_SECONDS),
        "visibility_timeout": _ceil_positive_int(selected.get("visibility_timeout_seconds") or 1800, "visibility_timeout_seconds"),
        "heartbeat_seconds": _ceil_positive_int(selected.get("heartbeat_seconds") or 300, "heartbeat_seconds"),
    }


def _reject_legacy_run_sizing_overrides(args: argparse.Namespace) -> None:
    overrides: list[str] = []
    if getattr(args, "messages_per_worker", 1) != 1:
        overrides.append("--messages-per-worker")
    if getattr(args, "min_workers", 0) != 0:
        overrides.append("--min-workers")
    if getattr(args, "max_workers", 64) != 64:
        overrides.append("--max-workers")
    if getattr(args, "vcpus", None) is not None:
        overrides.append("--vcpus")
    if getattr(args, "memory", None) is not None:
        overrides.append("--memory")
    if getattr(args, "visibility_timeout", 1800) != 1800:
        overrides.append("--visibility-timeout")
    if getattr(args, "heartbeat_seconds", 300) != 300:
        overrides.append("--heartbeat-seconds")
    if getattr(args, "task_timeout_seconds", SAFE_TASK_TIMEOUT_SECONDS) != SAFE_TASK_TIMEOUT_SECONDS:
        overrides.append("--task-timeout-seconds")
    if getattr(args, "subtract_active", False):
        overrides.append("--subtract-active")
    if overrides:
        raise SystemExit("sweetspot run --apply is Plan-authoritative; legacy sizing/timing overrides are not accepted: " + ", ".join(overrides))


def _target_from_plan(args: argparse.Namespace, spec: dict[str, Any], plan: dict[str, Any]) -> tuple[DeploymentTarget, list[dict[str, Any]], str | None]:
    selected = plan.get("selected")
    if plan.get("status") != "ready" or not isinstance(selected, dict):
        raise SystemExit("sweetspot run --apply requires a ready Plan before selecting a deployment target")
    deployment_sha256: str | None = None
    if getattr(args, "deployment", None):
        deployment_sha256 = _sha256_file(args.deployment)
        registry = load_deployment(args.deployment)
        try:
            target = select_deployment_target(plan, registry)
            warnings = validate_target_matches_job(job_spec=spec, target=target)
        except PlannerSpecError as exc:
            raise SystemExit(str(exc)) from exc
        return target, warnings, deployment_sha256
    queue_url = args.queue_url
    batch_job_queue = args.batch_job_queue
    job_definition = args.job_definition
    if not queue_url:
        raise SystemExit("sweetspot run --apply requires --deployment or --queue-url/SWEETSPOT_SQS_QUEUE_URL")
    if not batch_job_queue or not job_definition:
        raise SystemExit("sweetspot run --apply requires --deployment or --batch-job-queue and --job-definition")
    return (
        DeploymentTarget(
            region=str(selected.get("region") or args.region or ""),
            architecture=str(selected.get("architecture") or ""),
            sqs_queue_url=queue_url,
            dlq_url=getattr(args, "dlq_url", None),
            batch_job_queue=batch_job_queue,
            job_definition=job_definition,
            image=None,
        ),
        [],
        deployment_sha256,
    )


def _sqs_queue_name(queue_url: str) -> str:
    return queue_url.rstrip("/").rsplit("/", 1)[-1]


def _controller_run_queue_name(run_id: str, architecture: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]+", "-", run_id).strip("-") or "run"
    suffix = f"-{architecture}" if architecture else ""
    name = f"sweetspot-{safe}{suffix}"
    return name[:80].rstrip("-")


def _queue_depth_is_zero(depth: dict[str, int]) -> bool:
    return not (int(depth.get("visible", 0) or 0) or int(depth.get("not_visible", 0) or 0) or int(depth.get("delayed", 0) or 0))


def _maybe_queue_arn(sqs: Any, queue_url: str) -> str | None:
    attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"]).get("Attributes", {})
    arn = attrs.get("QueueArn")
    return str(arn) if arn else None


def _prepare_controller_run_queue(
    args: argparse.Namespace,
    *,
    sqs: Any,
    target: DeploymentTarget,
    run_id: str,
    previous_state: dict[str, Any],
    runtime_settings: dict[str, int | float],
) -> tuple[DeploymentTarget, dict[str, Any] | None]:
    if not getattr(args, "create_run_queue", False):
        return target, None
    if not getattr(args, "deployment", None):
        raise SystemExit("sweetspot run --create-run-queue requires --deployment so the queue is bound to a reviewed deployment target")
    if not getattr(args, "dedicated_run_queue", False):
        raise SystemExit("sweetspot run --create-run-queue requires --dedicated-run-queue")
    raw_previous_controller = previous_state.get("controller")
    previous_controller: dict[str, Any] = raw_previous_controller if isinstance(raw_previous_controller, dict) else {}
    raw_previous_run_queue = previous_controller.get("run_queue")
    previous_run_queue: dict[str, Any] = raw_previous_run_queue if isinstance(raw_previous_run_queue, dict) else {}
    queue_url = str(previous_run_queue.get("queue_url") or "")
    created = False
    dlq_arn = _maybe_queue_arn(sqs, target.dlq_url) if target.dlq_url else None
    if not queue_url:
        queue_name = _controller_run_queue_name(run_id, target.architecture)
        attributes: dict[str, str] = {
            "MessageRetentionPeriod": "1209600",
            "VisibilityTimeout": str(int(runtime_settings["visibility_timeout"])),
            "SqsManagedSseEnabled": "true",
        }
        if dlq_arn:
            attributes["RedrivePolicy"] = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 5}, sort_keys=True)
        resp = sqs.create_queue(
            QueueName=queue_name,
            Attributes=attributes,
            tags={"SweetSpotOwner": "controller", "SweetSpotRunId": run_id, "SweetSpotArchitecture": target.architecture},
        )
        queue_url = str(resp.get("QueueUrl") or "")
        created = True
    if not queue_url:
        raise SystemExit("SQS create_queue did not return QueueUrl for controller-owned run queue")
    attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"]).get("Attributes", {})
    queue_arn = str(attrs.get("QueueArn") or _maybe_queue_arn(sqs, queue_url) or "")
    if dlq_arn:
        raw_redrive = attrs.get("RedrivePolicy")
        redrive = json.loads(raw_redrive) if isinstance(raw_redrive, str) and raw_redrive else {}
        if redrive.get("deadLetterTargetArn") and redrive.get("deadLetterTargetArn") != dlq_arn:
            raise SystemExit("controller-owned run queue redrive policy does not match deployment dlq_url")
    depth = queue_depth(sqs, queue_url)
    if not _enqueue_phase_started(previous_state) and not _queue_depth_is_zero(depth):
        raise SystemExit("controller-owned run queue is not empty before enqueue; review or use a fresh artifact directory/run id")
    queue_report = {
        "mode": "controller_owned_sqs",
        "created_or_existing": "created" if created else "existing",
        "queue_url": queue_url,
        "queue_arn": queue_arn,
        "queue_name": _sqs_queue_name(queue_url),
        "dlq_url": target.dlq_url,
        "dlq_arn": dlq_arn,
        "preflight_depth": depth,
    }
    return (
        DeploymentTarget(
            region=target.region,
            architecture=target.architecture,
            sqs_queue_url=queue_url,
            dlq_url=target.dlq_url,
            batch_job_queue=target.batch_job_queue,
            job_definition=target.job_definition,
            image=target.image,
        ),
        {key: value for key, value in queue_report.items() if value is not None},
    )


def _aws_client_for_target(args: argparse.Namespace, service: str, target: DeploymentTarget):
    profile = getattr(args, "profile", None)
    explicit_region = getattr(args, "region", None)
    region = target.region or explicit_region
    if profile or getattr(args, "deployment", None):
        return boto3.Session(profile_name=profile, region_name=region or None).client(service, region_name=region or None)
    if region:
        try:
            return boto3.client(service, region_name=region)
        except TypeError as exc:
            # Unit tests often monkeypatch boto3.client with a tiny one-arg
            # function; real boto3 accepts region_name and therefore honors the
            # Plan-selected fallback region.
            if "region_name" not in str(exc):
                raise
    return boto3.client(service)


def _preflight_deployment_target(batch: Any, *, target: DeploymentTarget, spec: dict[str, Any]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    if not target.image:
        return warnings
    try:
        resp = batch.describe_job_definitions(jobDefinitions=[target.job_definition])
    except Exception as exc:  # noqa: BLE001 - remote deployment image identity must fail closed.
        raise SystemExit(f"Batch job definition image preflight failed for deployment target {target.region}/{target.architecture}: {exc}") from exc
    defs = resp.get("jobDefinitions") or []
    if not defs:
        raise SystemExit(f"deployment job definition not found: {target.job_definition}")
    image = (defs[0].get("containerProperties") or {}).get("image")
    if image:
        try:
            validate_target_matches_job(job_spec={**spec, "image": image}, target=target)
        except PlannerSpecError as exc:
            raise SystemExit(f"Batch job definition image does not match deployment target: {exc}") from exc
    else:
        warnings.append({"code": "job_definition_image_unknown", "severity": "warning", "message": "Batch job definition did not expose a container image for preflight."})
    return warnings


def _manifest_identity_for_run(args: argparse.Namespace, spec: dict[str, Any], target: DeploymentTarget) -> dict[str, Any]:
    local_path = getattr(args, "input_manifest_jsonl", None)
    local_sha = _sha256_file(local_path) if local_path else None
    if getattr(args, "deployment", None):
        if local_path is None or local_sha is None:
            raise SystemExit("deployment run --apply requires --input-manifest-jsonl so the local task source can be verified against the S3 input_manifest")
        try:
            s3 = _aws_client_for_target(args, "s3", target)
            bucket, key = parse_s3_uri(str(spec["input_manifest"]))
            head = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
            validate_local_manifest_matches_head(local_path=local_path, local_sha256=local_sha, head=head)
            return manifest_identity_from_head(str(spec["input_manifest"]), head, local_path=local_path, local_sha256=local_sha).as_dict()
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(f"failed to bind remote S3 manifest identity for deployment run: {exc}") from exc
    return local_manifest_identity(str(spec["input_manifest"]), local_path=local_path, local_sha256=local_sha).as_dict()


def _enqueue_phase_started(previous_state: dict[str, Any]) -> bool:
    phase = _phase_by_name(previous_state).get("enqueue_tasks", {})
    status = phase.get("status")
    sent = int(phase.get("sent", 0) or 0)
    return bool(status in {"in_progress", "batch_in_flight", "needs_review", "completed"} or sent > 0)


def _recorded_run_tasks_artifact(previous_state: dict[str, Any], artifact_dir: Path) -> tuple[dict[str, Any], Path] | None:
    previous_artifacts = previous_state.get("artifacts") if isinstance(previous_state.get("artifacts"), dict) else {}
    raw_path = previous_artifacts.get("production_tasks_jsonl") if isinstance(previous_artifacts, dict) else None
    if not raw_path and not (_enqueue_phase_started(previous_state) or _phase_completed(previous_state, "submit_workers")):
        return None
    path = Path(str(raw_path)) if raw_path else artifact_dir / "production_tasks.jsonl"
    if not path.exists():
        raise SystemExit(f"existing run state recorded production tasks, but the task artifact is missing: {path}")
    artifacts: dict[str, Any] = {"production_tasks_jsonl": str(path), "production_task_count": _count_jsonl_objects(path), "production_tasks_sha256": _sha256_file(path)}
    previous_sha = previous_artifacts.get("production_tasks_sha256") if isinstance(previous_artifacts, dict) else None
    if previous_sha and previous_sha != artifacts["production_tasks_sha256"]:
        raise SystemExit(f"production task artifact at {path} no longer matches the SHA256 recorded in run_state.json")
    return artifacts, path


def _recorded_canary_tasks_artifact(previous_state: dict[str, Any], artifact_dir: Path) -> tuple[dict[str, Any], Path] | None:
    previous_artifacts = previous_state.get("artifacts") if isinstance(previous_state.get("artifacts"), dict) else {}
    raw_path = previous_artifacts.get("canary_tasks_jsonl") if isinstance(previous_artifacts, dict) else None
    previous_phases = _phase_by_name(previous_state)
    if not raw_path and not any(previous_phases.get(name, {}).get("status") not in {None, "not_started"} for name in ("enqueue_canary_tasks", "submit_canary_workers")):
        return None
    path = Path(str(raw_path)) if raw_path else artifact_dir / "canary_tasks.jsonl"
    if not path.exists():
        raise SystemExit(f"existing run state recorded canary tasks, but the task artifact is missing: {path}")
    artifacts: dict[str, Any] = {"canary_tasks_jsonl": str(path), "canary_task_count": _count_jsonl_objects(path), "canary_tasks_sha256": _sha256_file(path)}
    previous_sha = previous_artifacts.get("canary_tasks_sha256") if isinstance(previous_artifacts, dict) else None
    if previous_sha and previous_sha != artifacts["canary_tasks_sha256"]:
        raise SystemExit(f"canary task artifact at {path} no longer matches the SHA256 recorded in run_state.json")
    return artifacts, path


def _plan_requests_controller_canaries(plan: dict[str, Any]) -> bool:
    canaries = plan.get("canaries")
    if not isinstance(canaries, list) or not canaries:
        return False
    decision = canaries[0].get("decision") if isinstance(canaries[0], dict) else None
    return isinstance(decision, dict) and decision.get("next_action") == "run_canary"


def _canary_runtime_settings(plan: dict[str, Any], task: dict[str, Any]) -> dict[str, int | float]:
    raw_input = task.get("input")
    input_obj: dict[str, Any] = raw_input if isinstance(raw_input, dict) else {}
    try:
        vcpus = _ceil_positive_int(input_obj.get("candidate_vcpus"), "candidate_vcpus")
        memory_mib = _ceil_positive_int(input_obj.get("candidate_memory_mib"), "candidate_memory_mib")
    except SystemExit as exc:
        raise SystemExit(f"canary task {task.get('task_id')!r} is missing candidate runtime metadata: {exc}") from exc
    task_timeout_seconds = float(task.get("timeout_seconds") or _plan_task_timeout_seconds(plan) or SAFE_TASK_TIMEOUT_SECONDS)
    visibility_timeout = max(task_timeout_seconds + 60.0, min(43200.0, math.ceil(task_timeout_seconds * 1.5)))
    heartbeat_seconds = max(1.0, min(300.0, math.floor(visibility_timeout / 3.0)))
    return {
        "vcpus": vcpus,
        "memory_mib": memory_mib,
        "task_timeout_seconds": task_timeout_seconds,
        "visibility_timeout": int(math.ceil(visibility_timeout)),
        "heartbeat_seconds": int(math.ceil(heartbeat_seconds)),
    }


def _run_integrated_finalizer(args: argparse.Namespace, *, spec: dict[str, Any], tasks_path: Path, artifact_dir: Path) -> tuple[int, dict[str, Any]]:
    return _run_integrated_finalizer_service(args, spec=spec, tasks_path=tasks_path, artifact_dir=artifact_dir, finalizer_func=cmd_finalize)


def _cmd_run_canary_apply(
    args: argparse.Namespace,
    *,
    spec: dict[str, Any],
    plan: dict[str, Any],
    logical_unit_count: int | None,
    job_spec_sha256: str,
) -> dict[str, Any]:
    if getattr(args, "finalize", False):
        raise SystemExit("sweetspot run --finalize is only valid for production Plans; canary Plans must collect summaries first")
    if getattr(args, "create_run_queue", False):
        raise SystemExit("sweetspot run --create-run-queue is only valid for production Plans; canary Plans use deployment canary_routes")
    if args.artifact_dir is None:
        raise SystemExit("sweetspot run --apply for canary Plans requires --artifact-dir so run_state.json can make retries/resume safe")
    if not getattr(args, "deployment", None):
        raise SystemExit("sweetspot run --apply for canary Plans requires --deployment with isolated per-candidate canary_routes")
    if logical_unit_count is None:
        raise SystemExit("sweetspot run --apply for canary Plans requires --input-manifest-jsonl to materialize controller-owned canary tasks")
    _reject_legacy_run_sizing_overrides(args)

    deployment_sha256 = _sha256_file(args.deployment)
    try:
        registry = load_deployment(args.deployment)
        canary_region = select_canary_deployment_region(spec, registry)
    except PlannerSpecError as exc:
        raise SystemExit(str(exc)) from exc

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    state_path = args.artifact_dir / "run_state.json"
    previous_state = _load_run_state(state_path, run_id=str(spec["run_id"]), job_spec_sha256=job_spec_sha256, require_job_spec_sha256=True)
    previous_phases = _phase_by_name(previous_state)
    recorded_tasks = _recorded_canary_tasks_artifact(previous_state, args.artifact_dir)
    if recorded_tasks is not None:
        previous_plan = previous_state.get("plan")
        if not isinstance(previous_plan, dict):
            raise SystemExit("existing run_state.json records canary tasks but no reviewed plan; use a new artifact directory")
        plan = previous_plan
        artifacts, tasks_path = recorded_tasks
    else:
        canary_generation = 0
        tasks_path = _canary_generation_dir(args.artifact_dir, canary_generation) / "canary_tasks.jsonl"
        _write_canary_tasks_from_plan(spec, plan, logical_unit_count, tasks_path)
        artifacts = {
            "canary_generation": canary_generation,
            "canary_tasks_jsonl": str(tasks_path),
            "canary_task_count": plan.get("artifacts", {}).get("canary_task_count"),
            "canary_tasks_sha256": _sha256_file(tasks_path),
        }
    artifacts["run_state_json"] = str(state_path)
    tasks = _read_jsonl(tasks_path)
    if getattr(args, "collect_canary_summaries", False) and previous_phases.get("submit_canary_workers", {}).get("status") != "completed":
        raise SystemExit("--collect-canary-summaries requires an existing completed canary worker submission in run_state.json; first launch canaries without --collect-canary-summaries")
    _validate_tasks_for_enqueue(tasks, allowed_s3_prefixes=_default_run_allowed_s3_prefixes(args, spec))
    try:
        grouped = group_canary_tasks_by_candidate(tasks)
        targets = {key: canary_deployment_target_for(registry, region=canary_region, task=group[0]) for key, group in grouped.items()}
    except (PlannerSpecError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    target_warnings: list[dict[str, Any]] = []
    for key, target in sorted(targets.items()):
        try:
            target_warnings.extend({**warning, "candidate": key} for warning in validate_target_matches_job(job_spec=spec, target=target))
        except PlannerSpecError as exc:
            raise SystemExit(str(exc)) from exc
    first_target = next(iter(targets.values()))
    manifest_identity = _manifest_identity_for_run(args, spec, first_target)
    route_bindings = {key: target.as_dict() for key, target in sorted(targets.items())}
    plan_binding = {
        "deployment_sha256": deployment_sha256,
        "manifest_identity": manifest_identity,
        "canary_region": canary_region,
        "canary_routes": route_bindings,
        "canary_tasks_sha256": artifacts["canary_tasks_sha256"],
    }
    previous_controller = previous_state.get("controller", {}) if isinstance(previous_state.get("controller"), dict) else {}
    previous_binding = previous_controller.get("canary_binding") or previous_controller.get("plan_binding")
    if previous_controller.get("binding_kind") == "production":
        raise SystemExit("existing run_state.json records production apply state; use status/repair/finalize or a new artifact directory")
    if previous_binding and _sha256_json_obj(previous_binding) != _sha256_json_obj(plan_binding):
        raise SystemExit("existing run_state.json plan/deployment/manifest binding differs for canary; use the original inputs or a new artifact directory")

    allowed_s3_prefixes = _default_run_allowed_s3_prefixes(args, spec)
    enqueue_config_sha256 = _sha256_json_obj({"allowed_s3_prefixes": allowed_s3_prefixes, "profile": args.profile, "region": canary_region, "routes": route_bindings})
    worker_config_sha256 = _sha256_json_obj(
        {
            "allow_legacy_done_markers": bool(getattr(args, "allow_legacy_done_markers", False)),
            "allowed_s3_prefixes": allowed_s3_prefixes,
            "env": args.env or [],
            "log_tail_bytes": args.log_tail_bytes,
            "max_log_bytes": args.max_log_bytes,
            "profile": args.profile,
            "redact_regex": args.redact_regex or [],
            "region": canary_region,
            "retry_attempts": args.retry_attempts,
            "routes": route_bindings,
        }
    )
    controller_base = {
        "apply_supported": True,
        "mutations_allowed": True,
        "resume_state_loaded": bool(previous_state),
        "plan_authoritative": True,
        "canary_routing": "isolated_per_candidate_deployment_routes",
        "binding_kind": "canary",
        "job_binding": {
            "deployment_sha256": deployment_sha256,
            "manifest_identity": manifest_identity,
        },
        "canary_binding": plan_binding,
        "plan_binding": plan_binding,
        "deployment_warnings": target_warnings,
    }
    run_id = str(spec["run_id"])
    phases_base: list[dict[str, Any]] = [
        {"name": "plan", "status": "completed"},
        {
            "name": "materialize_canary_tasks",
            "status": "completed",
            "artifact": str(tasks_path),
            "task_count": len(tasks),
            "candidate_count": len(grouped),
            "generation": artifacts.get("canary_generation", 0),
        },
    ]
    report = _build_run_report(
        spec=spec,
        plan=plan,
        mode="apply",
        applied=True,
        status="canary_apply_started",
        artifacts=artifacts,
        phases=phases_base
        + [
            previous_phases.get("enqueue_canary_tasks", {"name": "enqueue_canary_tasks", "status": "not_started"}),
            previous_phases.get("submit_canary_workers", {"name": "submit_canary_workers", "status": "not_started"}),
            {"name": "collect_canary_summaries", "status": "not_started", "requires_worker_completion": True},
        ],
        job_spec_sha256=job_spec_sha256,
        controller=controller_base,
        next_actions=["Persisted run_state.json before canary mutation; rerun the same command to resume without re-enqueueing completed candidate routes."],
    )
    _write_run_state(state_path, report)

    sqs = _aws_client_for_target(args, "sqs", first_target)
    batch = _aws_client_for_target(args, "batch", first_target)
    for key, target in sorted(targets.items()):
        for warning in _preflight_deployment_target(batch, target=target, spec=spec):
            target_warnings.append({**warning, "candidate": key})

    previous_enqueue_phase = previous_phases.get("enqueue_canary_tasks", {})
    previous_enqueue_status = previous_enqueue_phase.get("status")
    if previous_enqueue_status in {"batch_in_flight", "needs_review"}:
        raise SystemExit("existing run_state.json has ambiguous canary enqueue progress; review SQS/task state before retrying to avoid duplicate messages")
    if previous_enqueue_phase.get("enqueue_config_sha256") and previous_enqueue_phase.get("enqueue_config_sha256") != enqueue_config_sha256:
        raise SystemExit("existing run_state.json canary enqueue progress uses different enqueue settings; use the original settings or a new artifact directory")

    def route_records_from_phase(phase: dict[str, Any]) -> dict[str, dict[str, Any]]:
        raw_routes = phase.get("routes") if isinstance(phase, dict) else None
        if isinstance(raw_routes, list):
            return {str(route.get("candidate")): dict(route) for route in raw_routes if isinstance(route, dict) and route.get("candidate")}
        return {}

    route_records = route_records_from_phase(previous_enqueue_phase)
    for key, group in grouped.items():
        route_records.setdefault(
            key,
            {
                "candidate": key,
                "status": "not_started",
                "queue_url": targets[key].sqs_queue_url,
                "task_count": len(group),
                "sent": 0,
                "next_task_index": 0,
            },
        )
    if previous_enqueue_status != "completed":
        for key in sorted(grouped):
            route = route_records[key]
            if int(route.get("sent", 0) or 0) > 0:
                continue
            try:
                preflight_depth = queue_depth(sqs, targets[key].sqs_queue_url)
            except Exception as exc:  # noqa: BLE001 - stale canary queues must fail closed before mutation.
                raise SystemExit(f"failed to verify canary route queue is empty for {key}: {exc}") from exc
            route["preflight_queue_depth"] = preflight_depth
            if int(preflight_depth.get("visible", 0)) or int(preflight_depth.get("not_visible", 0)) or int(preflight_depth.get("delayed", 0)):
                raise SystemExit(f"canary route queue for {key} is not empty; use a fresh isolated canary queue or review/drain it before apply")
    enqueue_phase: dict[str, Any] = {
        "name": "enqueue_canary_tasks",
        "status": "completed" if previous_enqueue_status == "completed" else "in_progress",
        "enqueue_config_sha256": enqueue_config_sha256,
        "routes": [route_records[key] for key in sorted(route_records)],
    }

    def persist_canary_enqueue(phase: dict[str, Any], *, report_status: str, next_actions: list[str]) -> None:
        _write_run_state(
            state_path,
            _build_run_report(
                spec=spec,
                plan=plan,
                mode="apply",
                applied=True,
                status=report_status,
                artifacts=artifacts,
                phases=phases_base
                + [
                    phase,
                    previous_phases.get("submit_canary_workers", {"name": "submit_canary_workers", "status": "not_started"}),
                    {"name": "collect_canary_summaries", "status": "not_started", "requires_worker_completion": True},
                ],
                job_spec_sha256=job_spec_sha256,
                controller=controller_base,
                next_actions=next_actions,
            ),
        )

    if previous_enqueue_status == "completed":
        enqueue_phase = dict(previous_enqueue_phase)
        enqueue_phase["resumed"] = True
    else:
        persist_canary_enqueue(enqueue_phase, report_status="canary_enqueue_in_progress", next_actions=["Canary task enqueue is in progress; rerun the same command to resume from recorded candidate route offsets."])
        for key in sorted(grouped):
            group = grouped[key]
            route = route_records[key]
            sent = int(route.get("sent", 0) or 0)
            if sent < 0 or sent > len(group):
                raise SystemExit("existing run_state.json has invalid canary enqueue sent count")
            while sent < len(group):
                task_batch = group[sent : sent + 10]
                route_in_flight = {**route, "status": "batch_in_flight", "sent": sent, "next_task_index": sent, "in_flight_count": len(task_batch)}
                route_records[key] = route_in_flight
                enqueue_phase = {**enqueue_phase, "status": "batch_in_flight", "routes": [route_records[candidate] for candidate in sorted(route_records)]}
                persist_canary_enqueue(
                    enqueue_phase, report_status="canary_enqueue_batch_in_flight", next_actions=["A canary task batch may have reached SQS; review candidate queues before retrying if this controller stops here."]
                )
                entries = [{"Id": str(sent + i), "MessageBody": json.dumps(t, sort_keys=True)} for i, t in enumerate(task_batch)]
                resp = sqs.send_message_batch(QueueUrl=targets[key].sqs_queue_url, Entries=entries)
                if resp.get("Failed"):
                    route_records[key] = {**route_in_flight, "status": "needs_review", "failed": resp.get("Failed"), "successful": resp.get("Successful", [])}
                    enqueue_phase = {**enqueue_phase, "status": "needs_review", "routes": [route_records[candidate] for candidate in sorted(route_records)]}
                    persist_canary_enqueue(enqueue_phase, report_status="canary_enqueue_needs_review", next_actions=["SQS accepted only part of a canary task batch; review candidate queues before retrying."])
                    raise SystemExit(f"send_message_batch failed: {resp['Failed']}")
                sent += len(resp.get("Successful", []))
                route = {**route, "status": "completed" if sent == len(group) else "in_progress", "sent": sent, "next_task_index": sent, "remaining": len(group) - sent}
                route_records[key] = route
                enqueue_phase = {
                    **enqueue_phase,
                    "status": "completed" if all(int(r.get("sent", 0) or 0) >= int(r.get("task_count", 0) or 0) for r in route_records.values()) else "in_progress",
                    "routes": [route_records[candidate] for candidate in sorted(route_records)],
                }
                persist_canary_enqueue(
                    enqueue_phase,
                    report_status="canary_tasks_enqueued" if enqueue_phase["status"] == "completed" else "canary_enqueue_in_progress",
                    next_actions=["Canary tasks have been enqueued; rerun the same command to resume worker submission if this controller stops."],
                )

    previous_submit_phase = previous_phases.get("submit_canary_workers", {})
    previous_submit_status = previous_submit_phase.get("status")
    if previous_submit_status in {"job_in_flight", "needs_review"}:
        raise SystemExit("existing run_state.json has ambiguous canary worker submission progress; review Batch jobs before retrying to avoid duplicate workers")
    if previous_submit_phase.get("worker_config_sha256") and previous_submit_phase.get("worker_config_sha256") != worker_config_sha256:
        raise SystemExit("existing run_state.json canary worker submission progress uses different worker settings; use the original settings or a new artifact directory")
    submitted = list(previous_submit_phase.get("submitted", [])) if isinstance(previous_submit_phase.get("submitted"), list) else []
    submitted_candidates = {str(item.get("candidate")) for item in submitted if isinstance(item, dict) and item.get("candidate")}
    submission_stamp = str(previous_submit_phase.get("submission_stamp") or utc_stamp())
    submit_phase: dict[str, Any] = {
        "name": "submit_canary_workers",
        "status": "completed" if previous_submit_status == "completed" else "in_progress",
        "worker_config_sha256": worker_config_sha256,
        "submission_stamp": submission_stamp,
        "submitted_count": len(submitted),
        "submitted": submitted,
        "worker_count": len(grouped),
    }

    def persist_canary_submit(phase: dict[str, Any], *, report_status: str, next_actions: list[str]) -> None:
        _write_run_state(
            state_path,
            _build_run_report(
                spec=spec,
                plan=plan,
                mode="apply",
                applied=True,
                status=report_status,
                artifacts=artifacts,
                phases=phases_base
                + [enqueue_phase, phase, {"name": "reconcile_canary_workers", "status": "not_started"}, {"name": "collect_canary_summaries", "status": "not_started", "requires_worker_completion": True}],
                job_spec_sha256=job_spec_sha256,
                controller=controller_base,
                next_actions=next_actions,
            ),
        )

    if previous_submit_status == "completed":
        submit_phase = dict(previous_submit_phase)
        submit_phase["resumed"] = True
    else:
        persist_canary_submit(
            submit_phase, report_status="canary_worker_submit_in_progress", next_actions=["Canary worker submission is in progress; rerun the same command to resume from recorded candidate submissions."]
        )
        for key in sorted(grouped):
            if key in submitted_candidates:
                continue
            target = targets[key]
            group = grouped[key]
            runtime = _canary_runtime_settings(plan, group[0])
            try:
                validate_worker_timing(
                    visibility_timeout=int(runtime["visibility_timeout"]),
                    heartbeat_seconds=int(runtime["heartbeat_seconds"]),
                    task_timeout_seconds=float(runtime["task_timeout_seconds"]),
                )
            except ValueError as exc:
                raise SystemExit(str(exc)) from exc
            overrides = _worker_overrides(
                sqs_queue_url=target.sqs_queue_url,
                messages_per_worker=len(group),
                visibility_timeout=int(runtime["visibility_timeout"]),
                heartbeat_seconds=int(runtime["heartbeat_seconds"]),
                task_timeout_seconds=float(runtime["task_timeout_seconds"]),
                env=args.env or [],
                allowed_s3_prefixes=allowed_s3_prefixes,
                log_tail_bytes=args.log_tail_bytes,
                max_log_bytes=args.max_log_bytes,
                redact_regexes=args.redact_regex or [],
                allow_legacy_done_markers=bool(getattr(args, "allow_legacy_done_markers", False)),
                vcpus=int(runtime["vcpus"]),
                memory=int(runtime["memory_mib"]),
            )
            job_name = f"{run_id}-canary-{key}-{submission_stamp}"
            in_flight_phase = {**submit_phase, "status": "job_in_flight", "in_flight_candidate": key, "in_flight_job_name": job_name}
            persist_canary_submit(
                in_flight_phase,
                report_status="canary_worker_submit_job_in_flight",
                next_actions=["A canary worker submit_job call may have reached Batch; review Batch jobs before retrying if this controller stops here."],
            )
            kwargs: dict[str, Any] = {"jobName": job_name, "jobQueue": target.batch_job_queue, "jobDefinition": target.job_definition, "containerOverrides": overrides}
            if args.retry_attempts is not None:
                kwargs["retryStrategy"] = {"attempts": args.retry_attempts}
            try:
                resp = batch.submit_job(**kwargs)
            except Exception as exc:
                review_phase = {**in_flight_phase, "status": "needs_review", "submit_error": str(exc)}
                persist_canary_submit(review_phase, report_status="canary_worker_submit_needs_review", next_actions=["A canary Batch submit_job call failed or is ambiguous; review Batch jobs before retrying."])
                raise
            submitted.append({"candidate": key, "jobName": job_name, "jobId": resp.get("jobId"), "jobArn": resp.get("jobArn"), "task_count": len(group), "queue_url": target.sqs_queue_url})
            submitted_candidates.add(key)
            submit_phase = {**submit_phase, "status": "completed" if len(submitted) == len(grouped) else "in_progress", "submitted_count": len(submitted), "submitted": submitted}
            persist_canary_submit(
                submit_phase,
                report_status="canary_workers_submitted" if submit_phase["status"] == "completed" else "canary_worker_submit_in_progress",
                next_actions=["Canary workers have been submitted; collect summary JSONL and rerun plan/run with --canary-summary-jsonl."],
            )

    previous_canary_reconcile_phase = previous_phases.get("reconcile_canary_workers", {})
    previous_canary_reconcile_status = previous_canary_reconcile_phase.get("status")
    if previous_canary_reconcile_status in {"job_in_flight", "needs_review"}:
        raise SystemExit("existing run_state.json has ambiguous canary reconciliation progress; review Batch jobs before retrying to avoid duplicate canary workers")
    canary_reconcile_submitted = list(previous_canary_reconcile_phase.get("submitted", [])) if isinstance(previous_canary_reconcile_phase.get("submitted"), list) else []
    canary_reconcile_decisions: list[dict[str, Any]] = []
    canary_reconcile_stamp = str(previous_canary_reconcile_phase.get("submission_stamp") or utc_stamp())
    canary_reconcile_skipped_reason = None
    if previous_submit_status != "completed":
        canary_reconcile_skipped_reason = "initial_canary_submission_not_reconciled_until_resume"
    elif getattr(args, "collect_canary_summaries", False):
        canary_reconcile_skipped_reason = "collection_reads_existing_done_or_attempt_summaries_before_relaunching_canary_work"
    canary_reconcile_phase: dict[str, Any] = {
        "name": "reconcile_canary_workers",
        "status": "skipped" if canary_reconcile_skipped_reason else "completed",
        "reason": canary_reconcile_skipped_reason,
        "submission_stamp": canary_reconcile_stamp,
        "submitted_count": len(canary_reconcile_submitted),
        "submitted": canary_reconcile_submitted,
        "decisions": canary_reconcile_decisions,
    }
    canary_reconcile_phase = {key: value for key, value in canary_reconcile_phase.items() if value is not None}

    def persist_canary_reconcile(phase: dict[str, Any], *, report_status: str, next_actions: list[str]) -> None:
        _write_run_state(
            state_path,
            _build_run_report(
                spec=spec,
                plan=plan,
                mode="apply",
                applied=True,
                status=report_status,
                artifacts=artifacts,
                phases=phases_base + [enqueue_phase, submit_phase, phase, {"name": "collect_canary_summaries", "status": "not_started", "requires_worker_completion": True}],
                job_spec_sha256=job_spec_sha256,
                controller=controller_base,
                next_actions=next_actions,
            ),
        )

    if previous_submit_status == "completed" and not getattr(args, "collect_canary_summaries", False):
        for key in sorted(grouped):
            target = targets[key]
            group = grouped[key]
            depth = queue_depth(sqs, target.sqs_queue_url)
            backlog = max(0, min(len(group), int(depth.get("visible", 0)) + int(depth.get("not_visible", 0)) + int(depth.get("delayed", 0))))
            route_record = route_records.get(key, {})
            route_preflight_depth: Any = route_record.get("preflight_queue_depth") if isinstance(route_record, dict) else None
            preflight_clean = isinstance(route_preflight_depth, dict) and not (
                int(route_preflight_depth.get("visible", 0) or 0) or int(route_preflight_depth.get("not_visible", 0) or 0) or int(route_preflight_depth.get("delayed", 0) or 0)
            )
            fallback_active = sum(1 for item in submitted + canary_reconcile_submitted if isinstance(item, dict) and item.get("candidate") == key)
            active_count, active_examples, active_warning = _safe_active_worker_count(batch, job_queue=target.batch_job_queue, job_name_prefix=f"{run_id}-canary-{key}", fallback=fallback_active)
            decision: dict[str, Any] = {
                "candidate": key,
                "queue_url": target.sqs_queue_url,
                "queue_depth": depth,
                "candidate_backlog_estimate": backlog,
                "preflight_queue_depth": route_preflight_depth,
                "preflight_queue_was_empty": preflight_clean,
                "active_matching_workers": active_count,
                "active_examples": active_examples,
                "submitted_replacement_workers": 0,
            }
            if active_warning:
                decision["warning"] = active_warning
            if backlog > 0 and not preflight_clean:
                decision["skip_reason"] = "canary_queue_backlog_is_not_run_scoped_without_empty_preflight"
                canary_reconcile_decisions.append(decision)
                continue
            if backlog > 0 and active_count <= 0:
                runtime = _canary_runtime_settings(plan, group[0])
                try:
                    validate_worker_timing(
                        visibility_timeout=int(runtime["visibility_timeout"]),
                        heartbeat_seconds=int(runtime["heartbeat_seconds"]),
                        task_timeout_seconds=float(runtime["task_timeout_seconds"]),
                    )
                except ValueError as exc:
                    raise SystemExit(str(exc)) from exc
                overrides = _worker_overrides(
                    sqs_queue_url=target.sqs_queue_url,
                    messages_per_worker=len(group),
                    visibility_timeout=int(runtime["visibility_timeout"]),
                    heartbeat_seconds=int(runtime["heartbeat_seconds"]),
                    task_timeout_seconds=float(runtime["task_timeout_seconds"]),
                    env=args.env or [],
                    allowed_s3_prefixes=allowed_s3_prefixes,
                    log_tail_bytes=args.log_tail_bytes,
                    max_log_bytes=args.max_log_bytes,
                    redact_regexes=args.redact_regex or [],
                    allow_legacy_done_markers=bool(getattr(args, "allow_legacy_done_markers", False)),
                    vcpus=int(runtime["vcpus"]),
                    memory=int(runtime["memory_mib"]),
                )
                job_name = f"{run_id}-canary-{key}-reconcile-{canary_reconcile_stamp}-{len(canary_reconcile_submitted):04d}"
                in_flight_phase = {
                    **canary_reconcile_phase,
                    "status": "job_in_flight",
                    "decisions": canary_reconcile_decisions + [decision],
                    "in_flight_candidate": key,
                    "in_flight_job_name": job_name,
                }
                persist_canary_reconcile(
                    in_flight_phase,
                    report_status="canary_reconcile_job_in_flight",
                    next_actions=["A canary reconciliation submit_job call may have reached Batch; review Batch jobs before retrying if this controller stops here."],
                )
                reconcile_kwargs: dict[str, Any] = {"jobName": job_name, "jobQueue": target.batch_job_queue, "jobDefinition": target.job_definition, "containerOverrides": overrides}
                if args.retry_attempts is not None:
                    reconcile_kwargs["retryStrategy"] = {"attempts": args.retry_attempts}
                try:
                    resp = batch.submit_job(**reconcile_kwargs)
                except Exception as exc:
                    review_phase = {**in_flight_phase, "status": "needs_review", "submit_error": str(exc)}
                    persist_canary_reconcile(
                        review_phase,
                        report_status="canary_reconcile_needs_review",
                        next_actions=["A canary reconciliation Batch submit_job call failed or is ambiguous; review Batch jobs before retrying."],
                    )
                    raise
                canary_reconcile_submitted.append({"candidate": key, "jobName": job_name, "jobId": resp.get("jobId"), "jobArn": resp.get("jobArn"), "queue_url": target.sqs_queue_url, "reason": "canary_reconcile_top_up"})
                decision["submitted_replacement_workers"] = 1
            canary_reconcile_decisions.append(decision)
        canary_reconcile_phase = {
            "name": "reconcile_canary_workers",
            "status": "completed",
            "submission_stamp": canary_reconcile_stamp,
            "submitted_count": len(canary_reconcile_submitted),
            "submitted": canary_reconcile_submitted,
            "decisions": canary_reconcile_decisions,
        }
        persist_canary_reconcile(
            canary_reconcile_phase,
            report_status="canary_reconcile_complete",
            next_actions=["Canary reconciliation observed each candidate queue and submitted one replacement worker where backlog remained and no candidate worker was active."],
        )

    collect_phase: dict[str, Any] = {"name": "collect_canary_summaries", "status": "not_started", "requires_worker_completion": True}
    if getattr(args, "collect_canary_summaries", False):
        summary_path = args.artifact_dir / "canary_summaries.jsonl"
        s3_for_summaries = _aws_client_for_target(args, "s3", first_target)
        collection = _collect_canary_summaries(s3_for_summaries, tasks=tasks, out_jsonl=summary_path)
        artifacts["canary_summary_jsonl"] = str(summary_path)
        collect_phase = {**collect_phase, **collection, "status": "completed" if collection["complete"] else "incomplete"}
        phases = phases_base + [enqueue_phase, submit_phase, canary_reconcile_phase, collect_phase]
        next_actions = ["Canary summaries are still incomplete; rerun with --collect-canary-summaries after canary workers finish."]
        report_status = "canary_summaries_incomplete"
        production_plan = plan
        if collection["complete"]:
            summaries = _read_jsonl(summary_path)
            try:
                production_plan = plan_with_adaptive_canaries(spec, summaries, logical_unit_count=logical_unit_count)
            except PlannerSpecError as exc:
                raise SystemExit(str(exc)) from exc
            production_plan_path = args.artifact_dir / "production_plan.json"
            production_plan_path.write_text(json.dumps(production_plan, indent=2, sort_keys=True) + "\n")
            artifacts["production_plan_json"] = str(production_plan_path)
            if production_plan.get("status") == "ready":
                production_tasks_path = args.artifact_dir / "production_tasks.jsonl"
                _write_production_tasks_from_plan(spec, production_plan, logical_unit_count, production_tasks_path)
                artifacts["production_tasks_jsonl"] = str(production_tasks_path)
                artifacts["production_task_count"] = production_plan.get("artifacts", {}).get("production_task_count")
                artifacts["production_tasks_sha256"] = _sha256_file(production_tasks_path)
                collect_phase = {**collect_phase, "status": "completed", "production_plan": str(production_plan_path), "production_tasks": str(production_tasks_path)}
                phases = phases_base + [enqueue_phase, submit_phase, canary_reconcile_phase, collect_phase]
                report_status = "production_plan_ready"
                next_actions = [f"Review {production_plan_path}, then run production apply with --canary-summary-jsonl {summary_path}."]
            else:
                report_status = "canary_summaries_collected"
                next_actions = [f"Review {production_plan_path}; additional canaries may be required before production apply."]
                next_decision = production_plan.get("canaries", [{}])[0].get("decision", {}) if isinstance(production_plan.get("canaries"), list) else {}
                if isinstance(next_decision, dict) and next_decision.get("next_action") == "run_canary":
                    next_generation = int(artifacts.get("canary_generation", 0) or 0) + 1
                    next_tasks_path = _canary_generation_dir(args.artifact_dir, next_generation) / "canary_tasks.jsonl"
                    _write_canary_tasks_from_plan(spec, production_plan, logical_unit_count, next_tasks_path)
                    next_artifacts = {
                        **artifacts,
                        "canary_generation": next_generation,
                        "canary_tasks_jsonl": str(next_tasks_path),
                        "canary_task_count": production_plan.get("artifacts", {}).get("canary_task_count"),
                        "canary_tasks_sha256": _sha256_file(next_tasks_path),
                    }
                    artifacts = next_artifacts
                    next_tasks = _read_jsonl(next_tasks_path)
                    next_grouped = group_canary_tasks_by_candidate(next_tasks)
                    next_targets = {key: canary_deployment_target_for(registry, region=canary_region, task=group[0]) for key, group in next_grouped.items()}
                    next_route_bindings = {key: target.as_dict() for key, target in sorted(next_targets.items())}
                    next_canary_binding = {
                        "deployment_sha256": deployment_sha256,
                        "manifest_identity": manifest_identity,
                        "canary_region": canary_region,
                        "canary_routes": next_route_bindings,
                        "canary_tasks_sha256": artifacts["canary_tasks_sha256"],
                    }
                    controller_base = {
                        **controller_base,
                        "canary_binding": next_canary_binding,
                        "plan_binding": next_canary_binding,
                        "canary_generations": [
                            {
                                "generation": int(artifacts.get("canary_generation", 0) or 0) - 1,
                                "status": "collected",
                                "canary_binding_sha256": _sha256_json_obj(plan_binding),
                                "summary_jsonl": str(summary_path),
                                "production_plan": str(production_plan_path),
                            },
                            {
                                "generation": next_generation,
                                "status": "materialized",
                                "canary_tasks_jsonl": str(next_tasks_path),
                                "canary_tasks_sha256": artifacts["canary_tasks_sha256"],
                            },
                        ],
                    }
                    phases = [
                        {"name": "plan", "status": "completed"},
                        {
                            "name": "materialize_canary_tasks",
                            "status": "completed",
                            "artifact": str(next_tasks_path),
                            "task_count": _count_jsonl_objects(next_tasks_path),
                            "candidate_count": len(next_grouped),
                            "generation": next_generation,
                            "previous_generation_collected": True,
                        },
                        {"name": "enqueue_canary_tasks", "status": "not_started"},
                        {"name": "submit_canary_workers", "status": "not_started"},
                        {"name": "reconcile_canary_workers", "status": "not_started"},
                        {"name": "collect_canary_summaries", "status": "not_started", "requires_worker_completion": True},
                    ]
                    next_actions = [f"Review {production_plan_path}; then rerun the same canary apply command to launch canary generation {next_generation}."]
        report = _build_run_report(
            spec=spec,
            plan=production_plan,
            mode="apply",
            applied=True,
            status=report_status,
            artifacts=artifacts,
            phases=phases,
            job_spec_sha256=job_spec_sha256,
            controller=controller_base,
            next_actions=next_actions,
        )
        _write_run_state(state_path, report)
        return report

    phases = phases_base + [enqueue_phase, submit_phase, canary_reconcile_phase, collect_phase]
    report = _build_run_report(
        spec=spec,
        plan=plan,
        mode="apply",
        applied=True,
        status="canary_workers_submitted",
        artifacts=artifacts,
        phases=phases,
        job_spec_sha256=job_spec_sha256,
        controller=controller_base,
        next_actions=["Collect canary summary JSONL from each candidate output prefix, then rerun `sweetspot plan`/`sweetspot run` with --canary-summary-jsonl to select production settings."],
    )
    _write_run_state(state_path, report)
    return report


def _cmd_run_apply(
    args: argparse.Namespace,
    *,
    spec: dict[str, Any],
    plan: dict[str, Any],
    logical_unit_count: int | None,
    job_spec_sha256: str,
) -> dict[str, Any]:
    if _plan_requests_controller_canaries(plan):
        return _cmd_run_canary_apply(args, spec=spec, plan=plan, logical_unit_count=logical_unit_count, job_spec_sha256=job_spec_sha256)
    if args.artifact_dir is None:
        raise SystemExit("sweetspot run --apply requires --artifact-dir so run_state.json can make retries/resume safe")
    _reject_legacy_run_sizing_overrides(args)
    runtime_settings = _runtime_settings_from_plan(plan)
    target, target_warnings, deployment_sha256 = _target_from_plan(args, spec, plan)
    try:
        validate_worker_timing(
            visibility_timeout=int(runtime_settings["visibility_timeout"]),
            heartbeat_seconds=int(runtime_settings["heartbeat_seconds"]),
            task_timeout_seconds=float(runtime_settings["task_timeout_seconds"]),
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    state_path = args.artifact_dir / "run_state.json"
    previous_state = _load_run_state(state_path, run_id=str(spec["run_id"]), job_spec_sha256=job_spec_sha256, require_job_spec_sha256=True)
    recorded_tasks = _recorded_run_tasks_artifact(previous_state, args.artifact_dir)
    if recorded_tasks is not None:
        previous_plan = previous_state.get("plan")
        if not isinstance(previous_plan, dict):
            raise SystemExit("existing run_state.json records production tasks but no reviewed plan; use a new artifact directory")
        plan = previous_plan
        artifacts, tasks_path = recorded_tasks
    else:
        if not args.canary_summary_jsonl:
            raise SystemExit("sweetspot run --apply requires measured canary summaries before production kickoff; run without --apply and --input-manifest-jsonl to materialize canary_tasks.jsonl")
        artifacts, materialized_tasks_path = _materialize_run_tasks(args, spec, plan, logical_unit_count)
        if materialized_tasks_path is None:
            raise SystemExit("sweetspot run --apply requires calibrated production tasks; pass --canary-summary-jsonl and --input-manifest-jsonl")
        tasks_path = materialized_tasks_path
    artifacts["run_state_json"] = str(state_path)
    tasks = _read_jsonl(tasks_path)
    run_id = str(spec["run_id"])
    job_name_prefix = args.job_name_prefix or f"{run_id}-worker"
    if not job_name_prefix.startswith(f"{run_id}-"):
        raise SystemExit("sweetspot run --job-name-prefix must start with RUN_ID- for run-scoped worker management")
    allowed_s3_prefixes = _default_run_allowed_s3_prefixes(args, spec)
    _validate_tasks_for_enqueue(tasks, allowed_s3_prefixes=allowed_s3_prefixes)
    plan_worker_count = max(1, min(int(runtime_settings["estimated_workers"]), len(tasks) or 1))
    plan_messages_per_worker = max(1, math.ceil((len(tasks) or 1) / plan_worker_count))
    if getattr(args, "reconcile_until_drained", False) and not getattr(args, "dedicated_run_queue", False):
        raise SystemExit("sweetspot run --reconcile-until-drained requires --dedicated-run-queue so queue depth is run-scoped")
    raw_previous_controller = previous_state.get("controller")
    previous_controller: dict[str, Any] = raw_previous_controller if isinstance(raw_previous_controller, dict) else {}
    raw_previous_run_queue = previous_controller.get("run_queue")
    previous_run_queue: dict[str, Any] = raw_previous_run_queue if isinstance(raw_previous_run_queue, dict) else {}
    if getattr(args, "create_run_queue", False) and previous_run_queue.get("queue_url"):
        target = DeploymentTarget(
            region=target.region,
            architecture=target.architecture,
            sqs_queue_url=str(previous_run_queue["queue_url"]),
            dlq_url=target.dlq_url,
            batch_job_queue=target.batch_job_queue,
            job_definition=target.job_definition,
            image=target.image,
        )
        artifacts["run_queue"] = dict(previous_run_queue)
    preliminary_manifest_identity = _manifest_identity_for_run(args, spec, target)
    preliminary_plan_binding = {
        "deployment_sha256": deployment_sha256,
        "manifest_identity": preliminary_manifest_identity,
        "selected": plan.get("selected"),
        "target": target.as_dict(),
    }
    preliminary_previous_binding = previous_controller.get("production_binding")
    if preliminary_previous_binding is None and previous_controller.get("binding_kind") != "canary":
        preliminary_previous_binding = previous_controller.get("plan_binding")
    if preliminary_previous_binding and _sha256_json_obj(preliminary_previous_binding) != _sha256_json_obj(preliminary_plan_binding):
        raise SystemExit("existing run_state.json plan/deployment/manifest binding differs for production; use the original inputs or a new artifact directory")
    preliminary_canary_binding = previous_controller.get("canary_binding")
    if preliminary_canary_binding is None and previous_controller.get("binding_kind") == "canary":
        preliminary_canary_binding = previous_controller.get("plan_binding")
    if preliminary_canary_binding:
        if previous_state.get("job_spec_sha256") and previous_state.get("job_spec_sha256") != job_spec_sha256:
            raise SystemExit("existing run_state.json canary binding does not match production JobSpec; use the original inputs or a new artifact directory")
        for common_key in ("deployment_sha256", "manifest_identity"):
            if preliminary_canary_binding.get(common_key) != preliminary_plan_binding.get(common_key):
                raise SystemExit("existing run_state.json canary binding does not match production deployment/manifest inputs; use the original inputs or a new artifact directory")
    sqs = _aws_client_for_target(args, "sqs", target)
    target, run_queue_report = _prepare_controller_run_queue(
        args,
        sqs=sqs,
        target=target,
        run_id=run_id,
        previous_state=previous_state,
        runtime_settings=runtime_settings,
    )
    if run_queue_report:
        artifacts["run_queue"] = run_queue_report
    manifest_identity = _manifest_identity_for_run(args, spec, target)
    plan_binding = {
        "deployment_sha256": deployment_sha256,
        "manifest_identity": manifest_identity,
        "selected": plan.get("selected"),
        "target": target.as_dict(),
    }
    enqueue_config_sha256 = _sha256_json_obj(
        {
            "allowed_s3_prefixes": allowed_s3_prefixes,
            "profile": args.profile,
            "queue_url": target.sqs_queue_url,
            "region": target.region,
        }
    )
    worker_config_payload = {
        "allow_legacy_done_markers": bool(getattr(args, "allow_legacy_done_markers", False)),
        "allowed_s3_prefixes": allowed_s3_prefixes,
        "batch_job_queue": target.batch_job_queue,
        "dedicated_run_queue": bool(getattr(args, "dedicated_run_queue", False)),
        "env": args.env or [],
        "heartbeat_seconds": runtime_settings["heartbeat_seconds"],
        "include_not_visible": args.include_not_visible,
        "job_definition": target.job_definition,
        "job_name_prefix": job_name_prefix,
        "log_tail_bytes": args.log_tail_bytes,
        "max_log_bytes": args.max_log_bytes,
        "memory": runtime_settings["memory_mib"],
        "messages_per_worker": plan_messages_per_worker,
        "plan_estimated_workers": runtime_settings["estimated_workers"],
        "profile": args.profile,
        "queue_url": target.sqs_queue_url,
        "redact_regex": args.redact_regex or [],
        "region": target.region,
        "retry_attempts": args.retry_attempts,
        "task_timeout_seconds": runtime_settings["task_timeout_seconds"],
        "vcpus": runtime_settings["vcpus"],
        "visibility_timeout": runtime_settings["visibility_timeout"],
        "wait_for_visible_min": args.wait_for_visible_min,
        "wait_for_visible_seconds": args.wait_for_visible_seconds,
        "wait_interval_seconds": args.wait_interval_seconds,
    }
    worker_config_sha256 = _sha256_json_obj(worker_config_payload)
    legacy_worker_config_sha256 = _sha256_json_obj(
        {
            **worker_config_payload,
            "kickoff_only": bool(getattr(args, "kickoff_only", False)),
            "reconcile_interval_seconds": float(getattr(args, "reconcile_interval_seconds", 0.0)),
            "reconcile_rounds": int(getattr(args, "reconcile_rounds", 1)),
        }
    )

    promoted_from_canary = None
    previous_canary_binding = previous_controller.get("canary_binding")
    if previous_canary_binding is None and previous_controller.get("binding_kind") == "canary":
        previous_canary_binding = previous_controller.get("plan_binding")
    if previous_canary_binding:
        if previous_state.get("job_spec_sha256") and previous_state.get("job_spec_sha256") != job_spec_sha256:
            raise SystemExit("existing run_state.json canary binding does not match production JobSpec; use the original inputs or a new artifact directory")
        for common_key in ("deployment_sha256", "manifest_identity"):
            if previous_canary_binding.get(common_key) != plan_binding.get(common_key):
                raise SystemExit("existing run_state.json canary binding does not match production deployment/manifest inputs; use the original inputs or a new artifact directory")
        promoted_from_canary = {
            "canary_binding_sha256": _sha256_json_obj(previous_canary_binding),
            "canary_binding": previous_canary_binding,
        }
    controller_base = {
        "apply_supported": True,
        "mutations_allowed": True,
        "resume_state_loaded": bool(previous_state),
        "plan_authoritative": True,
        "binding_kind": "production",
        "job_binding": {
            "deployment_sha256": deployment_sha256,
            "manifest_identity": manifest_identity,
        },
        "production_binding": plan_binding,
        "plan_binding": plan_binding,
        "deployment_warnings": target_warnings,
    }
    if promoted_from_canary:
        controller_base["promoted_from_canary"] = promoted_from_canary
    if run_queue_report:
        controller_base["run_queue"] = run_queue_report
    previous_binding = previous_controller.get("production_binding")
    if previous_binding is None and previous_controller.get("binding_kind") != "canary":
        previous_binding = previous_controller.get("plan_binding")
    if previous_binding and _sha256_json_obj(previous_binding) != _sha256_json_obj(plan_binding):
        raise SystemExit("existing run_state.json plan/deployment/manifest binding differs for production; use the original inputs or a new artifact directory")
    previous_phases = _phase_by_name(previous_state)
    phases: list[dict[str, Any]] = [
        {"name": "plan", "status": "completed"},
        {
            "name": "materialize_production_tasks",
            "status": "completed",
            "artifact": str(tasks_path),
            "task_count": len(tasks),
        },
        previous_phases.get("enqueue_tasks", {"name": "enqueue_tasks", "status": "not_started"}),
        previous_phases.get("submit_workers", {"name": "submit_workers", "status": "not_started"}),
        previous_phases.get("finalize", {"name": "finalize", "status": "not_started", "requires_resume_after_workers": True}),
    ]
    report = _build_run_report(
        spec=spec,
        plan=plan,
        mode="apply",
        applied=True,
        status="apply_started",
        artifacts=artifacts,
        phases=phases,
        job_spec_sha256=job_spec_sha256,
        controller=controller_base,
        next_actions=["Persisted run_state.json before mutation; rerun the same command to resume without re-enqueueing completed phases."],
    )
    _write_run_state(state_path, report)

    batch = _aws_client_for_target(args, "batch", target)
    target_warnings.extend(_preflight_deployment_target(batch, target=target, spec=spec))
    previous_enqueue_phase = previous_phases.get("enqueue_tasks", {})
    previous_enqueue_status = previous_enqueue_phase.get("status")
    if _enqueue_phase_started(previous_state):
        if previous_enqueue_phase.get("queue_url") and previous_enqueue_phase.get("queue_url") != target.sqs_queue_url:
            raise SystemExit("existing run_state.json enqueue progress uses a different queue_url; use the original queue or a new artifact directory")
        if previous_enqueue_phase.get("enqueue_config_sha256") and previous_enqueue_phase.get("enqueue_config_sha256") != enqueue_config_sha256:
            raise SystemExit("existing run_state.json enqueue progress uses different enqueue settings; use the original settings or a new artifact directory")
    if previous_enqueue_status in {"batch_in_flight", "needs_review"}:
        raise SystemExit("existing run_state.json has ambiguous enqueue progress; review SQS/task state before retrying to avoid duplicate messages")
    sent = int(previous_enqueue_phase.get("sent", 0) or 0)
    if sent < 0 or sent > len(tasks):
        raise SystemExit("existing run_state.json has invalid enqueue sent count")

    def persist_enqueue_phase(phase: dict[str, Any], *, report_status: str, next_actions: list[str]) -> None:
        progress_report = _build_run_report(
            spec=spec,
            plan=plan,
            mode="apply",
            applied=True,
            status=report_status,
            artifacts=artifacts,
            phases=[
                {"name": "plan", "status": "completed"},
                {"name": "materialize_production_tasks", "status": "completed", "artifact": str(tasks_path), "task_count": len(tasks)},
                phase,
                previous_phases.get("submit_workers", {"name": "submit_workers", "status": "not_started"}),
                previous_phases.get("finalize", {"name": "finalize", "status": "not_started", "requires_resume_after_workers": True}),
            ],
            job_spec_sha256=job_spec_sha256,
            controller=controller_base,
            next_actions=next_actions,
        )
        _write_run_state(state_path, progress_report)

    enqueue_phase: dict[str, Any]
    if _phase_completed(previous_state, "enqueue_tasks"):
        enqueue_phase = dict(previous_phases["enqueue_tasks"])
        enqueue_phase["resumed"] = True
    else:
        enqueue_phase = {
            "name": "enqueue_tasks",
            "status": "in_progress" if sent < len(tasks) else "completed",
            "queue_url": target.sqs_queue_url,
            "task_count": len(tasks),
            "sent": sent,
            "next_task_index": sent,
            "remaining": len(tasks) - sent,
            "allowed_s3_prefixes": allowed_s3_prefixes,
            "enqueue_config_sha256": enqueue_config_sha256,
        }
        persist_enqueue_phase(enqueue_phase, report_status="enqueue_in_progress", next_actions=["Task enqueue is in progress; rerun the same command to resume from the recorded sent index if this controller stops."])
        while sent < len(tasks):
            batch_start = sent
            task_batch = tasks[batch_start : batch_start + 10]
            in_flight_phase = {
                **enqueue_phase,
                "status": "batch_in_flight",
                "sent": sent,
                "next_task_index": sent,
                "remaining": len(tasks) - sent,
                "in_flight_start_index": batch_start,
                "in_flight_count": len(task_batch),
            }
            persist_enqueue_phase(
                in_flight_phase,
                report_status="enqueue_batch_in_flight",
                next_actions=["A task-message batch may have reached SQS; if this controller stops here, review SQS/task state before retrying."],
            )
            entries = [{"Id": str(batch_start + i), "MessageBody": json.dumps(t, sort_keys=True)} for i, t in enumerate(task_batch)]
            resp = sqs.send_message_batch(QueueUrl=target.sqs_queue_url, Entries=entries)
            if resp.get("Failed"):
                review_phase = {
                    **in_flight_phase,
                    "status": "needs_review",
                    "failed": resp.get("Failed"),
                    "successful": resp.get("Successful", []),
                }
                persist_enqueue_phase(review_phase, report_status="enqueue_needs_review", next_actions=["SQS accepted only part of a task-message batch; review queue/task state before retrying."])
                raise SystemExit(f"send_message_batch failed: {resp['Failed']}")
            sent += len(resp.get("Successful", []))
            enqueue_phase = {
                "name": "enqueue_tasks",
                "status": "in_progress" if sent < len(tasks) else "completed",
                "queue_url": target.sqs_queue_url,
                "task_count": len(tasks),
                "sent": sent,
                "next_task_index": sent,
                "remaining": len(tasks) - sent,
                "allowed_s3_prefixes": allowed_s3_prefixes,
                "enqueue_config_sha256": enqueue_config_sha256,
            }
            persist_enqueue_phase(
                enqueue_phase,
                report_status="tasks_enqueued" if sent == len(tasks) else "enqueue_in_progress",
                next_actions=["Tasks have been enqueued; rerun the same command to resume worker submission if this controller stops."]
                if sent == len(tasks)
                else ["Task enqueue is in progress; rerun the same command to resume from the recorded sent index if this controller stops."],
            )

    depth = queue_depth(sqs, target.sqs_queue_url)
    wait_history: list[dict[str, Any]] = []
    if sent and not _phase_completed(previous_state, "enqueue_tasks"):
        min_visible = args.wait_for_visible_min if args.wait_for_visible_min is not None else depth["visible"]
        depth, wait_history = _wait_for_visible_backlog(
            sqs,
            queue_url=target.sqs_queue_url,
            min_visible=min_visible,
            max_seconds=args.wait_for_visible_seconds,
            interval_seconds=args.wait_interval_seconds,
        )

    def persist_submit_phase(phase: dict[str, Any], *, report_status: str, next_actions: list[str]) -> None:
        progress_report = _build_run_report(
            spec=spec,
            plan=plan,
            mode="apply",
            applied=True,
            status=report_status,
            artifacts=artifacts,
            phases=[
                {"name": "plan", "status": "completed"},
                {"name": "materialize_production_tasks", "status": "completed", "artifact": str(tasks_path), "task_count": len(tasks)},
                enqueue_phase,
                phase,
                previous_phases.get("finalize", {"name": "finalize", "status": "not_started", "requires_resume_after_workers": True}),
            ],
            job_spec_sha256=job_spec_sha256,
            controller=controller_base,
            next_actions=next_actions,
        )
        _write_run_state(state_path, progress_report)

    overrides = _worker_overrides(
        sqs_queue_url=target.sqs_queue_url,
        messages_per_worker=plan_messages_per_worker,
        visibility_timeout=int(runtime_settings["visibility_timeout"]),
        heartbeat_seconds=int(runtime_settings["heartbeat_seconds"]),
        task_timeout_seconds=float(runtime_settings["task_timeout_seconds"]),
        env=args.env or [],
        allowed_s3_prefixes=allowed_s3_prefixes,
        log_tail_bytes=args.log_tail_bytes,
        max_log_bytes=args.max_log_bytes,
        redact_regexes=args.redact_regex or [],
        allow_legacy_done_markers=bool(getattr(args, "allow_legacy_done_markers", False)),
        vcpus=int(runtime_settings["vcpus"]),
        memory=int(runtime_settings["memory_mib"]),
    )

    submit_phase: dict[str, Any]
    previous_submit_phase = previous_phases.get("submit_workers", {})
    previous_submit_status = previous_submit_phase.get("status")
    if previous_submit_status in {"in_progress", "job_in_flight", "needs_review", "completed"}:
        previous_reconcile_for_hash = previous_phases.get("reconcile_workers", {})
        legacy_previous_reconcile_rounds = int(previous_reconcile_for_hash.get("requested_rounds", previous_reconcile_for_hash.get("rounds_completed", int(getattr(args, "reconcile_rounds", 1)))) or 1)
        legacy_previous_worker_config_sha256 = _sha256_json_obj(
            {
                **worker_config_payload,
                "kickoff_only": bool(getattr(args, "kickoff_only", False)),
                "reconcile_interval_seconds": float(previous_reconcile_for_hash.get("reconcile_interval_seconds", 0.0) or 0.0),
                "reconcile_rounds": legacy_previous_reconcile_rounds,
            }
        )
        accepted_worker_config_sha256es = {worker_config_sha256, legacy_worker_config_sha256, legacy_previous_worker_config_sha256}
        if previous_submit_phase.get("worker_config_sha256") and previous_submit_phase.get("worker_config_sha256") not in accepted_worker_config_sha256es:
            raise SystemExit("existing run_state.json worker submission progress uses different worker settings; use the original settings or a new artifact directory")
        if previous_submit_phase.get("queue_url") and previous_submit_phase.get("queue_url") != target.sqs_queue_url:
            raise SystemExit("existing run_state.json worker submission progress uses a different queue_url; use the original queue or a new artifact directory")
        if previous_submit_phase.get("batch_job_queue") and previous_submit_phase.get("batch_job_queue") != target.batch_job_queue:
            raise SystemExit("existing run_state.json worker submission progress uses a different Batch job queue; use the original queue or a new artifact directory")
        if previous_submit_phase.get("job_definition") and previous_submit_phase.get("job_definition") != target.job_definition:
            raise SystemExit("existing run_state.json worker submission progress uses a different job definition; use the original job definition or a new artifact directory")
    if previous_submit_status in {"job_in_flight", "needs_review"}:
        raise SystemExit("existing run_state.json has ambiguous worker submission progress; review Batch jobs before retrying to avoid duplicate workers")
    if _phase_completed(previous_state, "submit_workers"):
        submit_phase = dict(previous_phases["submit_workers"])
        submit_phase["resumed"] = True
    else:
        if previous_submit_status == "in_progress":
            submitted = list(previous_submit_phase.get("submitted", []))
            to_submit = int(previous_submit_phase.get("to_submit", len(submitted)) or 0)
            raw_desired = int(previous_submit_phase.get("raw_desired_workers", to_submit) or 0)
            active_count = int(previous_submit_phase.get("active_matching_workers", 0) or 0)
            active_examples = previous_submit_phase.get("active_examples", []) if isinstance(previous_submit_phase.get("active_examples"), list) else []
            backlog = int(previous_submit_phase.get("backlog_used_for_sizing", 0) or 0)
            submission_stamp = str(previous_submit_phase.get("submission_stamp") or utc_stamp())
        else:
            submitted = []
            # SQS depth is queue-global and may include other runs. The high-level
            # run controller only owns messages it just enqueued (or recorded as
            # enqueued), so size this initial wave from the run-scoped sent count.
            backlog = sent
            raw_desired = plan_worker_count if backlog else 0
            active = active_jobs(batch, target.batch_job_queue, job_name_prefix) if args.subtract_active else []
            active_count = len(active)
            active_examples = active[:20]
            to_submit = max(0, raw_desired - active_count) if args.subtract_active else raw_desired
            submission_stamp = utc_stamp()
        if len(submitted) > to_submit:
            raise SystemExit("existing run_state.json has invalid worker submission progress")
        submit_phase = {
            "name": "submit_workers",
            "status": "in_progress" if len(submitted) < to_submit else "completed",
            "batch_job_queue": target.batch_job_queue,
            "job_definition": target.job_definition,
            "job_name_prefix": job_name_prefix,
            "queue_url": target.sqs_queue_url,
            "worker_config_sha256": worker_config_sha256,
            "submission_stamp": submission_stamp,
            "queue_depth": depth,
            "wait_history": wait_history,
            "backlog_used_for_sizing": backlog,
            "messages_per_worker": plan_messages_per_worker,
            "raw_desired_workers": raw_desired,
            "active_matching_workers": active_count,
            "to_submit": to_submit,
            "submitted_count": len(submitted),
            "submitted": submitted,
            "active_examples": active_examples,
        }
        persist_submit_phase(
            submit_phase, report_status="worker_submit_in_progress", next_actions=["Worker submission is in progress; rerun the same command to resume from the recorded submitted count if this controller stops."]
        )
        while len(submitted) < to_submit:
            worker_index = len(submitted)
            job_name = f"{job_name_prefix}-{submission_stamp}-{worker_index:04d}"
            in_flight_phase = {
                **submit_phase,
                "status": "job_in_flight",
                "submitted_count": len(submitted),
                "submitted": submitted,
                "in_flight_worker_index": worker_index,
                "in_flight_job_name": job_name,
            }
            persist_submit_phase(
                in_flight_phase,
                report_status="worker_submit_job_in_flight",
                next_actions=["A worker submit_job call may have reached Batch; if this controller stops here, review Batch jobs before retrying."],
            )
            kwargs: dict[str, Any] = {
                "jobName": job_name,
                "jobQueue": target.batch_job_queue,
                "jobDefinition": target.job_definition,
                "containerOverrides": overrides,
            }
            if args.retry_attempts is not None:
                kwargs["retryStrategy"] = {"attempts": args.retry_attempts}
            try:
                resp = batch.submit_job(**kwargs)
            except Exception as exc:
                review_phase = {**in_flight_phase, "status": "needs_review", "submit_error": str(exc)}
                persist_submit_phase(review_phase, report_status="worker_submit_needs_review", next_actions=["A Batch submit_job call failed or is ambiguous; review Batch jobs before retrying."])
                raise
            submitted.append({"jobName": job_name, "jobId": resp.get("jobId"), "jobArn": resp.get("jobArn")})
            submit_phase = {
                **submit_phase,
                "status": "in_progress" if len(submitted) < to_submit else "completed",
                "submitted_count": len(submitted),
                "submitted": submitted,
            }
            persist_submit_phase(
                submit_phase,
                report_status="workers_submitted" if len(submitted) == to_submit else "worker_submit_in_progress",
                next_actions=["Workers have been submitted; use status/finalize/repair commands to continue the run lifecycle."]
                if len(submitted) == to_submit
                else ["Worker submission is in progress; rerun the same command to resume from the recorded submitted count if this controller stops."],
            )

    submitted_for_reconcile = list(submit_phase.get("submitted", [])) if isinstance(submit_phase.get("submitted"), list) else []
    previous_reconcile_phase = previous_phases.get("reconcile_workers", {})

    def persist_reconcile_phase(phase: dict[str, Any], *, report_status: str, next_actions: list[str]) -> None:
        progress_report = _build_run_report(
            spec=spec,
            plan=plan,
            mode="apply",
            applied=True,
            status=report_status,
            artifacts=artifacts,
            phases=[
                {"name": "plan", "status": "completed"},
                {"name": "materialize_production_tasks", "status": "completed", "artifact": str(tasks_path), "task_count": len(tasks)},
                enqueue_phase,
                submit_phase,
                phase,
                previous_phases.get("finalize", {"name": "finalize", "status": "not_started", "requires_resume_after_workers": True}),
            ],
            job_spec_sha256=job_spec_sha256,
            controller=controller_base,
            next_actions=next_actions,
        )
        _write_run_state(state_path, progress_report)

    reconcile_phase = _run_worker_reconciliation_service(
        args,
        sqs=sqs,
        batch=batch,
        target=target,
        sent=sent,
        submitted_for_reconcile=submitted_for_reconcile,
        previous_reconcile_phase=previous_reconcile_phase,
        plan_worker_count=plan_worker_count,
        plan_messages_per_worker=plan_messages_per_worker,
        job_name_prefix=job_name_prefix,
        overrides=overrides,
        persist_phase=persist_reconcile_phase,
    )

    phases = [
        {"name": "plan", "status": "completed"},
        {"name": "materialize_production_tasks", "status": "completed", "artifact": str(tasks_path), "task_count": len(tasks)},
        enqueue_phase,
        submit_phase,
        reconcile_phase,
        previous_phases.get("finalize", {"name": "finalize", "status": "not_started", "requires_resume_after_workers": True}),
    ]
    report = _build_run_report(
        spec=spec,
        plan=plan,
        mode="apply",
        applied=True,
        status="workers_submitted",
        artifacts=artifacts,
        phases=phases,
        job_spec_sha256=job_spec_sha256,
        controller=controller_base,
        next_actions=[
            f"Use `sweetspot status {run_id} --artifact-dir {args.artifact_dir}` to watch local/run-scoped state.",
            "For interactive agent sessions, prefer `--kickoff-only` plus an external/scheduled status checkpoint instead of keeping the foreground command in a long drain-watch loop.",
            "After workers finish, rerun this command with --finalize or run `sweetspot repair` with the persisted production_tasks.jsonl if repair is needed.",
        ],
    )
    _write_run_state(state_path, report)

    if getattr(args, "finalize", False):
        finalizer_dir = args.artifact_dir / "finalizer"
        finalize_phase: dict[str, Any] = {
            "name": "finalize",
            "status": "in_progress",
            "artifact_dir": str(finalizer_dir),
            "tasks_jsonl": str(tasks_path),
            "upload": bool(getattr(args, "finalize_upload", False)),
            "publish_ready": bool(getattr(args, "finalize_publish_ready", False)),
            "dry_run": bool(getattr(args, "finalize_dry_run", False)),
        }
        phases = _replace_or_append_phase(phases, finalize_phase)
        _write_run_state(
            state_path,
            _build_run_report(
                spec=spec,
                plan=plan,
                mode="apply",
                applied=True,
                status="finalize_in_progress",
                artifacts=artifacts,
                phases=phases,
                job_spec_sha256=job_spec_sha256,
                controller=controller_base,
                next_actions=["Integrated finalization is scanning done markers and writing finalizer artifacts."],
            ),
        )
        try:
            finalize_rc, finalize_report = _run_integrated_finalizer(args, spec=spec, tasks_path=tasks_path, artifact_dir=finalizer_dir)
        except Exception as exc:  # noqa: BLE001 - persist finalizer failure before surfacing it.
            failed_phase = {**finalize_phase, "status": "failed", "error": str(exc)}
            _write_run_state(
                state_path,
                _build_run_report(
                    spec=spec,
                    plan=plan,
                    mode="apply",
                    applied=True,
                    status="finalize_failed",
                    artifacts=artifacts,
                    phases=_replace_or_append_phase(phases, failed_phase),
                    job_spec_sha256=job_spec_sha256,
                    controller=controller_base,
                    next_actions=["Finalization failed; inspect the persisted finalizer artifacts/logs before retrying."],
                ),
            )
            raise
        complete = bool(finalize_report.get("complete"))
        finalize_phase = {
            **finalize_phase,
            "status": "completed" if complete else "incomplete",
            "return_code": finalize_rc,
            "complete": complete,
            "task_count": finalize_report.get("task_count"),
            "done_count": finalize_report.get("done_count"),
            "missing_count": finalize_report.get("missing_count"),
            "repair_task_count": finalize_report.get("missing_count"),
            "final_manifest": finalize_report.get("final_manifest"),
            "task_status": finalize_report.get("task_status"),
            "repair_tasks": finalize_report.get("repair_tasks"),
            "outputs_manifest": finalize_report.get("outputs_manifest"),
            "final_manifest_s3": finalize_report.get("final_manifest_s3"),
            "ready_s3": finalize_report.get("ready_s3"),
        }
        artifacts.update(
            {
                "final_manifest": finalize_report.get("final_manifest"),
                "task_status_jsonl": finalize_report.get("task_status"),
                "repair_tasks_jsonl": finalize_report.get("repair_tasks"),
                "outputs_manifest": finalize_report.get("outputs_manifest"),
            }
        )
        phases = _replace_or_append_phase(phases, finalize_phase)
        report = _build_run_report(
            spec=spec,
            plan=plan,
            mode="apply",
            applied=True,
            status="finalized_complete" if complete else "finalized_incomplete",
            artifacts=artifacts,
            phases=phases,
            job_spec_sha256=job_spec_sha256,
            controller=controller_base,
            next_actions=[
                f"Run is complete; READY is {finalize_phase.get('ready_s3') or 'not published by integrated finalization'}."
                if complete
                else f"Run is incomplete; review {finalize_phase.get('repair_tasks')} and run `sweetspot repair {run_id}` when ready."
            ],
        )
        _write_run_state(state_path, report)
    return report


def cmd_run(args: argparse.Namespace) -> int:
    try:
        spec = load_job_spec(args.job_spec)
        job_spec_sha256 = _sha256_file(args.job_spec)
        plan, logical_unit_count = _plan_from_optional_adaptive_inputs(
            spec,
            canary_summary_jsonl=args.canary_summary_jsonl,
            input_manifest_jsonl=args.input_manifest_jsonl,
        )
        if args.apply:
            report = _cmd_run_apply(args, spec=spec, plan=plan, logical_unit_count=logical_unit_count, job_spec_sha256=job_spec_sha256)
        else:
            if getattr(args, "finalize", False):
                raise SystemExit("sweetspot run --finalize requires --apply and a production Plan")
            if args.artifact_dir:
                state_path = args.artifact_dir / "run_state.json"
                previous_state = _load_run_state(state_path, run_id=str(spec["run_id"]), job_spec_sha256=job_spec_sha256)
                if _run_state_has_apply_progress(previous_state):
                    raise SystemExit("sweetspot run dry-run refuses to overwrite existing apply/resume state; use a new --artifact-dir")
            artifacts, _tasks_path = _materialize_run_tasks(args, spec, plan, logical_unit_count)
            report = _build_run_report(
                spec=spec,
                plan=plan,
                mode="dry_run",
                applied=False,
                status="dry_run_complete",
                artifacts=artifacts,
                phases=[
                    {"name": "plan", "status": "completed"},
                    {
                        "name": "materialize_production_tasks",
                        "status": "completed" if "production_tasks_jsonl" in artifacts else "skipped",
                        "artifact": artifacts.get("production_tasks_jsonl"),
                    },
                    {
                        "name": "materialize_canary_tasks",
                        "status": "completed" if "canary_tasks_jsonl" in artifacts else "skipped",
                        "artifact": artifacts.get("canary_tasks_jsonl"),
                    },
                    {"name": "enqueue_tasks", "status": "not_started", "requires_apply": True},
                    {"name": "submit_workers", "status": "not_started", "requires_apply": True},
                    {"name": "finalize", "status": "not_started", "requires_apply": True},
                ],
                job_spec_sha256=job_spec_sha256,
                controller={"apply_supported": False, "mutations_allowed": False},
                next_actions=[
                    "Review the JSON plan and any local production task artifact before mutation.",
                    "Rerun with --apply plus queue and Batch worker settings to enqueue tasks and submit workers.",
                ],
            )
            if args.artifact_dir:
                args.artifact_dir.mkdir(parents=True, exist_ok=True)
                state_path = args.artifact_dir / "run_state.json"
                report.setdefault("artifacts", {})["run_state_json"] = str(state_path)
                _write_run_state(state_path, report)
    except PlannerSpecError as exc:
        raise SystemExit(str(exc)) from exc
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def cmd_scout(args: argparse.Namespace) -> int:
    return int(scout.main(args.scout_args, prog="sweetspot scout"))


def cmd_lane_manager(args: argparse.Namespace) -> int:
    return int(lane_manager.main(args.lane_manager_args, prog="sweetspot lane-manager"))


def _print_status_table(report: dict[str, Any]) -> None:
    identity = report.get("identity") or {}
    run = report.get("run") or {}
    _print_key_values(
        "SweetSpot status",
        {
            "checked_at": report.get("checked_at"),
            "run_id": run.get("run_id"),
            "run_status": run.get("status"),
            "region": report.get("region"),
            "account": identity.get("account"),
            "arn": identity.get("arn"),
        },
    )
    if run:
        print()
        artifacts = run.get("artifacts") or {}
        task_status = run.get("task_status") or {}
        _print_key_values(
            "run",
            {
                "artifact_dir": run.get("artifact_dir"),
                "run_state_json": artifacts.get("run_state_json"),
                "production_tasks": artifacts.get("production_tasks_jsonl"),
                "production_task_count": run.get("production_task_count"),
                "task_status_jsonl": artifacts.get("task_status_jsonl"),
                "task_status_count": task_status.get("total"),
                "missing_task_status_count": run.get("missing_task_status_count"),
                "repair_tasks_jsonl": artifacts.get("repair_tasks_jsonl"),
                "repair_task_count": run.get("repair_task_count"),
            },
        )
        by_status = task_status.get("by_status") or {}
        if by_status:
            print("status\tcount")
            for status, count in by_status.items():
                print(f"{_format_table_value(status)}\t{_format_table_value(count)}")
    output_s3 = report.get("output_s3") or {}
    if output_s3:
        print()
        progress = output_s3.get("done_markers") or {}
        _print_key_values(
            "output_s3",
            {
                "output_prefix": output_s3.get("output_prefix"),
                "done_marker_count": progress.get("count"),
                "expected_count": progress.get("expected_count"),
                "completion_fraction": progress.get("completion_fraction"),
                "estimated_remaining_seconds": progress.get("estimated_remaining_seconds"),
            },
        )
    queues = report.get("queues") or {}
    if queues:
        print()
        rows = []
        for name, queue in queues.items():
            depth = queue.get("depth") or {}
            rows.append(
                {
                    "name": name,
                    "visible": depth.get("visible", 0),
                    "not_visible": depth.get("not_visible", 0),
                    "delayed": depth.get("delayed", 0),
                    "queue_url": queue.get("queue_url"),
                }
            )
        _print_table("queues", ["name", "visible", "not_visible", "delayed", "queue_url"], rows)
    batch = report.get("batch")
    if batch:
        print()
        _print_key_values(
            "batch",
            {
                "job_queue": batch.get("job_queue"),
                "job_name_prefix": batch.get("job_name_prefix"),
                "active_count": batch.get("active_count"),
            },
        )
        by_status = batch.get("active_by_status") or {}
        if by_status:
            print("status\tcount")
            for status, count in by_status.items():
                print(f"{_format_table_value(status)}\t{_format_table_value(count)}")


def _first_existing_path(paths: Iterable[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def _jsonl_task_id_set(path: Path) -> tuple[int, set[str], int]:
    row_count = 0
    task_ids: set[str] = set()
    duplicate_count = 0
    for obj in _iter_jsonl(path):
        row_count += 1
        task_id = obj.get("task_id")
        if task_id is None:
            continue
        task_id_s = str(task_id)
        if task_id_s in task_ids:
            duplicate_count += 1
        task_ids.add(task_id_s)
    return row_count, task_ids, duplicate_count


def _jsonl_status_counts(path: Path, *, expected_run_id: str | None = None, expected_task_ids: set[str] | None = None) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    total = 0
    wrong_run_examples: list[str] = []
    status_task_ids: set[str] = set()
    duplicate_task_status_count = 0
    unknown_task_status_count = 0
    for obj in _iter_jsonl(path):
        total += 1
        if expected_run_id and obj.get("run_id") is not None and obj.get("run_id") != expected_run_id and len(wrong_run_examples) < 10:
            wrong_run_examples.append(str(obj.get("task_id") or f"line-{total}"))
        task_id = obj.get("task_id")
        if task_id is not None:
            task_id_s = str(task_id)
            if task_id_s in status_task_ids:
                duplicate_task_status_count += 1
            status_task_ids.add(task_id_s)
            if expected_task_ids is not None and task_id_s not in expected_task_ids:
                unknown_task_status_count += 1
        status = str(obj.get("status") or obj.get("state") or "<missing>")
        counts[status] += 1
    if wrong_run_examples:
        raise SystemExit(f"status RUN_ID found task_status records for another run; mismatched task_ids: {wrong_run_examples}")
    missing_task_status_count = None if expected_task_ids is None else len(expected_task_ids - status_task_ids)
    return {
        "total": total,
        "unique_task_count": len(status_task_ids),
        "duplicate_task_status_count": duplicate_task_status_count,
        "unknown_task_status_count": unknown_task_status_count,
        "missing_task_status_count": missing_task_status_count,
        "by_status": dict(counts.most_common()),
    }


def _is_run_scoped_job_prefix(run_id: str, job_name_prefix: str) -> bool:
    return job_name_prefix.startswith(f"{run_id}-")


def _extract_state_output_prefix(state: dict[str, Any]) -> str | None:
    plan = state.get("plan")
    if isinstance(plan, dict):
        job = plan.get("job")
        if isinstance(job, dict) and job.get("output_prefix"):
            return str(job["output_prefix"])
    return None


def _s3_done_marker_progress(s3: Any, output_prefix: str, expected_count: int | None = None) -> dict[str, Any]:
    bucket, key_prefix = parse_s3_uri(output_prefix)
    prefix = key_prefix.rstrip("/")
    token = None
    count = 0
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    sample_keys: list[str] = []
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix + "/" if prefix else ""}
        if token:
            kwargs["ContinuationToken"] = token
        page = s3.list_objects_v2(**kwargs)
        for obj in page.get("Contents", []) or []:
            key = str(obj.get("Key") or "")
            rel = key[len(prefix) :].lstrip("/") if prefix and key.startswith(prefix) else key
            if not (key.endswith(".done.json") and (rel.startswith("done/") or "/done/" in rel)):
                continue
            count += 1
            if len(sample_keys) < 5:
                sample_keys.append(key)
            last_modified = obj.get("LastModified")
            if isinstance(last_modified, datetime):
                if last_modified.tzinfo is None:
                    last_modified = last_modified.replace(tzinfo=timezone.utc)
                first_seen = last_modified if first_seen is None or last_modified < first_seen else first_seen
                last_seen = last_modified if last_seen is None or last_modified > last_seen else last_seen
        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")
        if not token:
            break
    completion_fraction = None
    remaining = None
    eta = None
    if expected_count is not None and expected_count > 0:
        completion_fraction = min(1.0, count / expected_count)
        remaining = max(0, expected_count - count)
        if remaining == 0:
            eta = 0.0
        elif count > 0 and first_seen is not None:
            elapsed = max(1.0, (datetime.now(timezone.utc) - first_seen).total_seconds())
            rate = count / elapsed
            if rate > 0:
                eta = remaining / rate
    return {
        "count": count,
        "expected_count": expected_count,
        "remaining_count": remaining,
        "completion_fraction": completion_fraction,
        "estimated_remaining_seconds": eta,
        "first_done_at": first_seen.isoformat() if first_seen else None,
        "last_done_at": last_seen.isoformat() if last_seen else None,
        "sample_keys": sample_keys,
    }


def _run_status_report(run_id: str | None, artifact_dir: Path | None) -> dict[str, Any] | None:
    if not run_id and artifact_dir is None:
        return None
    effective_artifact_dir = artifact_dir or Path("artifacts") / str(run_id)
    run_state_path = effective_artifact_dir / "run_state.json"
    state: dict[str, Any] = {}
    if run_state_path.exists():
        loaded = json.loads(run_state_path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError(f"run state at {run_state_path} is not a JSON object")
        state = loaded
    effective_run_id = run_id or str(state.get("run_id") or "")
    if run_id and state.get("run_id") not in (None, run_id):
        raise SystemExit(f"status RUN_ID expected run_state run_id={run_id!r}, found {state.get('run_id')!r}")
    raw_state_artifacts = state.get("artifacts")
    state_artifacts: dict[str, Any] = raw_state_artifacts if isinstance(raw_state_artifacts, dict) else {}

    def artifact_path(key: str, candidates: list[Path]) -> Path | None:
        raw = state_artifacts.get(key)
        if isinstance(raw, str):
            path = Path(raw)
            if path.exists():
                return path
        return _first_existing_path(candidates)

    production_tasks_path = artifact_path("production_tasks_jsonl", [effective_artifact_dir / "production_tasks.jsonl"])
    task_status_path = artifact_path(
        "task_status_jsonl",
        [
            effective_artifact_dir / "task_status.jsonl",
            effective_artifact_dir / "finalizer" / "task_status.jsonl",
        ],
    )
    repair_tasks_path = artifact_path(
        "repair_tasks_jsonl",
        [
            effective_artifact_dir / "repair_tasks.jsonl",
            effective_artifact_dir / "finalizer" / "repair_tasks.jsonl",
            effective_artifact_dir / "repair" / "repair_tasks.jsonl",
        ],
    )
    artifacts: dict[str, str] = {}
    if run_state_path.exists():
        artifacts["run_state_json"] = str(run_state_path)
    if production_tasks_path is not None:
        artifacts["production_tasks_jsonl"] = str(production_tasks_path)
    if task_status_path is not None:
        artifacts["task_status_jsonl"] = str(task_status_path)
    if repair_tasks_path is not None:
        artifacts["repair_tasks_jsonl"] = str(repair_tasks_path)

    production_task_count = None
    production_task_ids: set[str] | None = None
    duplicate_production_task_count = None
    if production_tasks_path is not None:
        production_task_count, production_task_ids, duplicate_production_task_count = _jsonl_task_id_set(production_tasks_path)
    task_status = _jsonl_status_counts(task_status_path, expected_run_id=effective_run_id or None, expected_task_ids=production_task_ids) if task_status_path is not None else None
    repair_task_count = _count_jsonl_objects(repair_tasks_path) if repair_tasks_path is not None else None
    status = "unknown"
    missing_task_status_count = None
    if task_status is not None:
        by_status = task_status["by_status"]
        total = task_status["total"]
        if production_task_count is not None:
            missing_task_status_count = task_status["missing_task_status_count"]
        if duplicate_production_task_count or task_status["duplicate_task_status_count"] or task_status["unknown_task_status_count"]:
            status = "invalid_artifacts"
        elif missing_task_status_count:
            status = "incomplete"
        elif total == 0:
            status = "empty"
        elif any(by_status.get(state, 0) for state in ("failed", "incomplete", "invalid_done_marker", "missing", "missing_output", "output_without_done")):
            status = "repair_needed"
        elif by_status.get("done", 0) + by_status.get("completed", 0) == total:
            status = "complete"
        else:
            status = "incomplete"
    elif state.get("status"):
        status = str(state["status"])

    return {
        "run_id": effective_run_id or None,
        "artifact_dir": str(effective_artifact_dir),
        "status": status,
        "artifacts": artifacts,
        "run_state_status": state.get("status"),
        "output_prefix": _extract_state_output_prefix(state),
        "production_task_count": production_task_count,
        "duplicate_production_task_count": duplicate_production_task_count,
        "task_status": task_status,
        "missing_task_status_count": missing_task_status_count,
        "repair_task_count": repair_task_count,
    }


def cmd_status(args: argparse.Namespace) -> int:
    run_id = getattr(args, "run_id", None)
    artifact_dir = getattr(args, "artifact_dir", None)
    run_report = _run_status_report(run_id, artifact_dir)
    effective_run_id = str(run_report.get("run_id")) if run_report and run_report.get("run_id") else run_id
    job_name_prefix = getattr(args, "job_name_prefix", None)
    if effective_run_id and job_name_prefix and not _is_run_scoped_job_prefix(effective_run_id, job_name_prefix):
        raise SystemExit("status --job-name-prefix must start with RUN_ID-; omit it to use the safe default")
    effective_job_name_prefix = job_name_prefix or (f"{effective_run_id}-" if effective_run_id else "sweetspot-worker")
    queue_url = args.queue_url
    if queue_url is None and run_id is None and artifact_dir is None:
        queue_url = os.environ.get("SWEETSPOT_SQS_QUEUE_URL", "")
    output_prefix = getattr(args, "output_prefix", None)
    needs_aws = bool(queue_url or args.dlq_url or args.job_queue or output_prefix or (run_id is None and artifact_dir is None))
    session = boto3.Session(profile_name=args.profile, region_name=args.region) if needs_aws else None
    identity: dict[str, Any] | None = None
    if session is not None:
        sts = session.client("sts", region_name=args.region)
        raw_identity = sts.get_caller_identity()
        identity = {"account": raw_identity.get("Account"), "arn": raw_identity.get("Arn"), "user_id": raw_identity.get("UserId")}
    queues: dict[str, Any] = {}
    if queue_url:
        assert session is not None
        sqs = session.client("sqs", region_name=args.region)
        queues["source"] = {"queue_url": queue_url, "depth": queue_depth(sqs, queue_url)}
    if args.dlq_url:
        assert session is not None
        sqs = session.client("sqs", region_name=args.region)
        queues["dlq"] = {"queue_url": args.dlq_url, "depth": queue_depth(sqs, args.dlq_url)}
    output_s3: dict[str, Any] | None = None
    if output_prefix:
        assert session is not None
        s3 = session.client("s3", region_name=args.region)
        expected_count = run_report.get("production_task_count") if run_report else None
        output_s3 = {"output_prefix": output_prefix, "done_markers": _s3_done_marker_progress(s3, output_prefix, expected_count)}
    batch_status: dict[str, Any] | None = None
    if args.job_queue:
        assert session is not None
        batch = session.client("batch", region_name=args.region)
        active = active_jobs(batch, args.job_queue, effective_job_name_prefix)
        by_status = dict(Counter(str(job.get("status")) for job in active).most_common())
        batch_status = {
            "job_queue": args.job_queue,
            "job_name_prefix": effective_job_name_prefix,
            "active_count": len(active),
            "active_by_status": by_status,
            "active_examples": active[:20],
        }
    report = {
        "schema": "sweetspot.status.v1",
        "checked_at": iso_now(),
        "run": run_report,
        "region": args.region or (session.region_name if session is not None else None),
        "identity": identity,
        "queues": queues,
        "batch": batch_status,
        "output_s3": output_s3,
    }
    if args.format == "table":
        _print_status_table(report)
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def _shell_command(parts: Iterable[str | Path | None]) -> str:
    return " ".join(shlex.quote(str(p)) for p in parts if p not in (None, ""))


def cmd_monitor(args: argparse.Namespace) -> int:
    status_parts: list[str | Path | None] = ["sweetspot", "status", args.run_id]
    if args.profile:
        status_parts.extend(["--profile", args.profile])
    if args.region:
        status_parts.extend(["--region", args.region])
    if args.artifact_dir:
        status_parts.extend(["--artifact-dir", args.artifact_dir])
    if args.queue_url:
        status_parts.extend(["--queue-url", args.queue_url])
    if args.dlq_url:
        status_parts.extend(["--dlq-url", args.dlq_url])
    if args.job_queue:
        status_parts.extend(["--job-queue", args.job_queue])
    if args.job_name_prefix:
        status_parts.extend(["--job-name-prefix", args.job_name_prefix])
    if args.output_prefix:
        status_parts.extend(["--output-prefix", args.output_prefix])
    status_parts.extend(["--format", args.format])
    status_command = _shell_command(status_parts)

    finalize_command = None
    if args.job_spec:
        finalize_parts: list[str | Path | None] = ["sweetspot", "run", args.job_spec]
        if args.profile:
            finalize_parts.extend(["--profile", args.profile])
        if args.region:
            finalize_parts.extend(["--region", args.region])
        if args.artifact_dir:
            finalize_parts.extend(["--artifact-dir", args.artifact_dir])
        if args.deployment:
            finalize_parts.extend(["--deployment", args.deployment])
        if args.input_manifest_jsonl:
            finalize_parts.extend(["--input-manifest-jsonl", args.input_manifest_jsonl])
        if args.canary_summary_jsonl:
            finalize_parts.extend(["--canary-summary-jsonl", args.canary_summary_jsonl])
        finalize_parts.extend(["--apply", "--finalize"])
        finalize_command = _shell_command(finalize_parts)

    report = {
        "schema": "sweetspot.monitor.v1",
        "run_id": args.run_id,
        "interval": args.interval,
        "until_complete": bool(args.until_complete),
        "commands": {
            "status": status_command,
            "finalize": finalize_command or "rerun the reviewed production `sweetspot run ... --apply` command with `--finalize` after status shows drained workers/queues",
        },
        "completion_condition": "source queue visible=0 and not_visible=0, DLQ=0, and Batch active_count=0; then run finalization/validation",
        "failure_condition": "DLQ depth > 0 or run-scoped Batch FAILED jobs; stop and diagnose before repair",
        "notes": [
            "This command is a non-blocking monitor scaffold; run the status command from cron/CI/Pi scheduled jobs instead of blocking an interactive agent.",
            "Use `sweetspot status --output-prefix ...` to include S3 done-marker progress when the output prefix is known.",
        ],
    }
    if args.emit_command:
        print("# SweetSpot monitor checkpoint")
        print(status_command)
        if finalize_command:
            print("# Finalize after completion condition is true")
            print(finalize_command)
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def cmd_enqueue_jsonl(args: argparse.Namespace) -> int:
    tasks = _read_jsonl(args.tasks_jsonl)
    if args.run_id:
        for t in tasks:
            t.setdefault("run_id", args.run_id)
    allowed_s3_prefixes = parse_allowed_s3_prefixes(getattr(args, "allowed_s3_prefix", None) or _env_allowed_s3_prefixes())
    _validate_tasks_for_enqueue(tasks, allowed_s3_prefixes=allowed_s3_prefixes)
    artifact_dir = args.artifact_dir or Path("artifacts") / (args.run_id or f"run-{utc_stamp()}")
    tasks_out = _write_enqueue_artifacts(tasks, artifact_dir)

    sent = 0
    if args.submit:
        if not args.queue_url:
            raise SystemExit("--submit requires --queue-url")
        sqs = _aws_client(args, "sqs")
        sent = _send_tasks_to_sqs(sqs, queue_url=args.queue_url, tasks=tasks)
    print(
        json.dumps(
            {
                "schema": "sweetspot.enqueue_summary.v1",
                "checked_at": iso_now(),
                "queue_url": args.queue_url,
                "task_count": len(tasks),
                "sent": sent,
                "submitted": bool(args.submit),
                "allowed_s3_prefixes": list(allowed_s3_prefixes),
                "tasks_jsonl": str(tasks_out),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _marker_or_none(task: dict[str, Any], key: str) -> str | None:
    val = task.get(key)
    return str(val) if val else None


def _done_marker_or_none(task: dict[str, Any]) -> str | None:
    try:
        return default_done_s3(task)
    except ValueError:
        return None


def cmd_derive_canary(args: argparse.Namespace) -> int:
    source_hash = _sha256_file(args.tasks_jsonl)
    tasks = _read_jsonl(args.tasks_jsonl)
    selected = _parse_index_selection(args.selected_indices, len(tasks)) or _auto_canary_indices(tasks, args.task_count)
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    canary_tasks = [dict(tasks[i]) for i in selected]
    if args.rewrite_run_id:
        marker_fields = ("output_s3", "summary_s3", "done_s3")
        if any(any(task.get(k) for k in marker_fields) for task in canary_tasks):
            raise SystemExit("--rewrite-run-id requires tasks without explicit output_s3/summary_s3/done_s3 markers; derive new tasks with canary S3 paths first")
        for task in canary_tasks:
            task["run_id"] = args.run_id
    selected_run_ids = sorted({str(t.get("run_id")) for t in canary_tasks if t.get("run_id") is not None})
    effective_run_id = args.run_id if args.rewrite_run_id or len(selected_run_ids) != 1 else selected_run_ids[0]
    canary_tasks_path = out_dir / "canary_tasks.jsonl"
    manifest_path = out_dir / "canary_manifest.json"
    bad_task_path = out_dir / "dlq_probe_task.jsonl" if args.include_dlq_probe else None
    dlq_probe_done_s3 = None
    generated_paths = [canary_tasks_path, manifest_path] + ([bad_task_path] if bad_task_path else [])
    if args.tasks_jsonl.resolve() in {p.resolve() for p in generated_paths}:
        raise SystemExit("--out-dir would overwrite --tasks-jsonl; choose a different output directory")
    canary_tasks_path.write_text("".join(json.dumps(t, sort_keys=True) + "\n" for t in canary_tasks))
    if args.include_dlq_probe:
        probe_task_id = f"{effective_run_id}-intentional-dlq-probe"
        probe_prefix = getattr(args, "dlq_probe_prefix", None)
        if not probe_prefix:
            for task in canary_tasks:
                done_s3 = _done_marker_or_none(task)
                if done_s3:
                    bucket, key = parse_s3_uri(done_s3)
                    parent = key.rsplit("/", 1)[0] if "/" in key else ""
                    probe_prefix = f"s3://{bucket}/{parent}" if parent else f"s3://{bucket}/"
                    break
        if not probe_prefix:
            raise SystemExit("--include-dlq-probe requires --dlq-probe-prefix when selected tasks do not expose an S3 done-marker prefix")
        dlq_probe_done_s3 = s3_join(probe_prefix, f"{probe_task_id}.done.json")
        bad_task = {
            "schema": "sweetspot.task.v1",
            "run_id": effective_run_id,
            "task_id": probe_task_id,
            "command": ["bash", "-lc", "echo intentional SweetSpot DLQ probe >&2; exit 42"],
            "timeout_seconds": 120,
            "done_s3": dlq_probe_done_s3,
            "purpose": "intentional_dlq_probe_not_part_of_valid_canary",
        }
        assert bad_task_path is not None
        bad_task_path.write_text(json.dumps(bad_task, sort_keys=True) + "\n")
    manifest = {
        "schema": "sweetspot.canary_manifest.v1",
        "created_at": iso_now(),
        "run_id": effective_run_id,
        "requested_run_id": args.run_id,
        "selected_source_run_ids": selected_run_ids,
        "source_tasks_jsonl": str(args.tasks_jsonl),
        "source_tasks_sha256": source_hash,
        "selected_indices": selected,
        "task_count": len(canary_tasks),
        "canary_tasks_jsonl": str(canary_tasks_path),
        "canary_tasks_sha256": _sha256_file(canary_tasks_path),
        "dlq_probe_task_jsonl": str(bad_task_path) if bad_task_path else None,
        "dlq_probe_done_s3": dlq_probe_done_s3,
        "rewrite_run_id": bool(args.rewrite_run_id),
        "expected_task_ids": [t.get("task_id") for t in canary_tasks],
        "expected_output_s3": [_marker_or_none(t, "output_s3") for t in canary_tasks],
        "expected_summary_s3": [_marker_or_none(t, "summary_s3") for t in canary_tasks],
        "expected_done_s3": [_done_marker_or_none(t) for t in canary_tasks],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "schema": "sweetspot.derive_canary_summary.v1",
                "run_id": effective_run_id,
                "requested_run_id": args.run_id,
                "task_count": len(canary_tasks),
                "selected_indices": selected,
                "canary_tasks_jsonl": str(canary_tasks_path),
                "canary_manifest": str(manifest_path),
                "dlq_probe_task_jsonl": str(bad_task_path) if bad_task_path else None,
                "dlq_probe_done_s3": dlq_probe_done_s3,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _parse_env_pair(s: str) -> dict[str, str]:
    if "=" not in s:
        raise argparse.ArgumentTypeError(f"expected KEY=VALUE, got {s!r}")
    k, v = s.split("=", 1)
    if not k:
        raise argparse.ArgumentTypeError(f"empty env key in {s!r}")
    return {"name": k, "value": v}


def _status_counts(jobs: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for job in jobs:
        status = str(job.get("status", "UNKNOWN"))
        counts[status] = counts.get(status, 0) + 1
    return counts


def cmd_submit_workers(args: argparse.Namespace) -> int:
    if not args.sqs_queue_url:
        raise SystemExit("missing --sqs-queue-url or SWEETSPOT_SQS_QUEUE_URL")
    try:
        validate_worker_timing(visibility_timeout=args.visibility_timeout, heartbeat_seconds=args.heartbeat_seconds, task_timeout_seconds=args.task_timeout_seconds)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    sqs = _aws_client(args, "sqs")
    batch = _aws_client(args, "batch")
    depth = queue_depth(sqs, args.sqs_queue_url)
    backlog = depth["visible"] + (depth["not_visible"] if args.include_not_visible else 0)
    raw_desired = desired_worker_count(backlog, args.messages_per_worker, args.min_workers, args.max_workers)
    active = active_jobs(batch, args.batch_job_queue, args.job_name_prefix) if args.subtract_active else []
    to_submit = max(0, raw_desired - len(active)) if args.subtract_active else raw_desired
    to_submit = min(to_submit, args.max_workers)

    overrides = _worker_overrides(
        sqs_queue_url=args.sqs_queue_url,
        messages_per_worker=args.messages_per_worker,
        visibility_timeout=args.visibility_timeout,
        heartbeat_seconds=args.heartbeat_seconds,
        task_timeout_seconds=args.task_timeout_seconds,
        env=args.env or [],
        allowed_s3_prefixes=getattr(args, "allowed_s3_prefix", []) or [],
        log_tail_bytes=args.log_tail_bytes,
        max_log_bytes=args.max_log_bytes,
        redact_regexes=args.redact_regex or [],
        allow_legacy_done_markers=bool(getattr(args, "allow_legacy_done_markers", False)),
        vcpus=args.vcpus,
        memory=args.memory,
    )

    submitted = []
    if args.submit and to_submit > 0:
        submitted = _submit_worker_jobs(
            batch,
            count=to_submit,
            job_name_prefix=args.job_name_prefix,
            batch_job_queue=args.batch_job_queue,
            job_definition=args.job_definition,
            overrides=overrides,
            retry_attempts=args.retry_attempts,
        )

    print(
        json.dumps(
            {
                "schema": "sweetspot.worker_submitter_summary.v1",
                "checked_at": iso_now(),
                "submit": bool(args.submit),
                "queue_depth": depth,
                "backlog_used_for_sizing": backlog,
                "messages_per_worker": args.messages_per_worker,
                "raw_desired_workers": raw_desired,
                "active_matching_workers": len(active),
                "to_submit": to_submit,
                "submitted_count": len(submitted),
                "submitted": submitted,
                "active_examples": active[:20],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def cmd_enqueue_and_submit(args: argparse.Namespace) -> int:
    if not args.queue_url:
        raise SystemExit("missing --queue-url or SWEETSPOT_SQS_QUEUE_URL")
    try:
        validate_worker_timing(visibility_timeout=args.visibility_timeout, heartbeat_seconds=args.heartbeat_seconds, task_timeout_seconds=args.task_timeout_seconds)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    tasks = _read_jsonl(args.tasks_jsonl)
    if args.run_id:
        for task in tasks:
            task.setdefault("run_id", args.run_id)
    allowed_s3_prefixes = parse_allowed_s3_prefixes(getattr(args, "allowed_s3_prefix", None) or _env_allowed_s3_prefixes())
    _validate_tasks_for_enqueue(tasks, allowed_s3_prefixes=allowed_s3_prefixes)
    artifact_dir = args.artifact_dir or Path("artifacts") / (args.run_id or f"run-{utc_stamp()}")
    tasks_out = _write_enqueue_artifacts(tasks, artifact_dir)

    sqs = _aws_client(args, "sqs")
    batch = _aws_client(args, "batch")
    sent = 0
    wait_history: list[dict[str, Any]] = []
    depth = queue_depth(sqs, args.queue_url)
    submitted: list[dict[str, Any]] = []
    active: list[dict[str, Any]] = []
    initial_visible = depth["visible"]
    initial_backlog = depth["visible"] + (depth["not_visible"] if args.include_not_visible else 0)
    sizing_sent = 0 if args.submit else len(tasks)
    backlog_floor = initial_backlog + sizing_sent
    backlog = max(initial_backlog, backlog_floor)
    raw_desired = desired_worker_count(backlog, args.messages_per_worker, args.min_workers, args.max_workers)
    active = active_jobs(batch, args.batch_job_queue, args.job_name_prefix) if args.subtract_active else []
    to_submit = max(0, raw_desired - len(active)) if args.subtract_active else raw_desired
    to_submit = min(to_submit, args.max_workers)

    if args.submit:
        sent = _send_tasks_to_sqs(sqs, queue_url=args.queue_url, tasks=tasks)
        expected_visible = initial_visible + sent
        min_visible = args.wait_for_visible_min if args.wait_for_visible_min is not None else expected_visible
        depth, wait_history = _wait_for_visible_backlog(
            sqs,
            queue_url=args.queue_url,
            min_visible=min_visible,
            max_seconds=args.wait_for_visible_seconds,
            interval_seconds=args.wait_interval_seconds,
        )
        observed_backlog = depth["visible"] + (depth["not_visible"] if args.include_not_visible else 0)
        backlog_floor = initial_backlog + sent
        backlog = max(observed_backlog, backlog_floor)
        raw_desired = desired_worker_count(backlog, args.messages_per_worker, args.min_workers, args.max_workers)
        active = active_jobs(batch, args.batch_job_queue, args.job_name_prefix) if args.subtract_active else []
        to_submit = max(0, raw_desired - len(active)) if args.subtract_active else raw_desired
        to_submit = min(to_submit, args.max_workers)
        overrides = _worker_overrides(
            sqs_queue_url=args.queue_url,
            messages_per_worker=args.messages_per_worker,
            visibility_timeout=args.visibility_timeout,
            heartbeat_seconds=args.heartbeat_seconds,
            task_timeout_seconds=args.task_timeout_seconds,
            env=args.env or [],
            allowed_s3_prefixes=allowed_s3_prefixes,
            log_tail_bytes=args.log_tail_bytes,
            max_log_bytes=args.max_log_bytes,
            redact_regexes=args.redact_regex or [],
            allow_legacy_done_markers=bool(getattr(args, "allow_legacy_done_markers", False)),
            vcpus=args.vcpus,
            memory=args.memory,
        )
        if to_submit > 0:
            submitted = _submit_worker_jobs(
                batch,
                count=to_submit,
                job_name_prefix=args.job_name_prefix,
                batch_job_queue=args.batch_job_queue,
                job_definition=args.job_definition,
                overrides=overrides,
                retry_attempts=args.retry_attempts,
            )

    print(
        json.dumps(
            {
                "schema": "sweetspot.enqueue_and_submit_summary.v1",
                "checked_at": iso_now(),
                "submit": bool(args.submit),
                "queue_url": args.queue_url,
                "task_count": len(tasks),
                "sent": sent,
                "simulated_sent_for_sizing": sizing_sent,
                "tasks_jsonl": str(tasks_out),
                "allowed_s3_prefixes": list(allowed_s3_prefixes),
                "wait_for_visible_seconds": args.wait_for_visible_seconds,
                "wait_history": wait_history,
                "queue_depth": depth,
                "backlog_floor_used_for_sizing": backlog_floor,
                "backlog_used_for_sizing": backlog,
                "messages_per_worker": args.messages_per_worker,
                "raw_desired_workers": raw_desired,
                "active_matching_workers": len(active),
                "to_submit": to_submit,
                "submitted_count": len(submitted),
                "submitted": submitted,
                "active_examples": active[:20],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _terminal_job_counts(batch, job_queue: str, job_name_prefix: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for status in ("SUCCEEDED", "FAILED"):
        total = 0
        paginator = batch.get_paginator("list_jobs")
        for page in paginator.paginate(jobQueue=job_queue, jobStatus=status):
            total += sum(1 for j in page.get("jobSummaryList", []) if not job_name_prefix or str(j.get("jobName", "")).startswith(job_name_prefix))
        counts[status] = total
    return counts


def cmd_supervise_workers(args: argparse.Namespace) -> int:
    if not args.sqs_queue_url:
        raise SystemExit("missing --sqs-queue-url or SWEETSPOT_SQS_QUEUE_URL")
    if args.stop_on_dlq and not args.dlq_url:
        raise SystemExit("--stop-on-dlq requires --dlq-url")
    try:
        validate_worker_timing(visibility_timeout=args.visibility_timeout, heartbeat_seconds=args.heartbeat_seconds, task_timeout_seconds=args.task_timeout_seconds)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    sqs = session.client("sqs", region_name=args.region)
    batch = session.client("batch", region_name=args.region)
    artifact_dir = args.artifact_dir or Path("artifacts") / (args.run_id or f"supervise-{utc_stamp()}")
    artifact_dir.mkdir(parents=True, exist_ok=True)
    status_path = artifact_dir / "supervisor_status.jsonl"
    summary_path = artifact_dir / "supervisor_summary.json"
    config_path = artifact_dir / "supervisor_config.json"
    config = {
        "schema": "sweetspot.supervisor_config.v1",
        "created_at": iso_now(),
        "run_id": args.run_id,
        "sqs_queue_url": args.sqs_queue_url,
        "dlq_url": args.dlq_url,
        "batch_job_queue": args.batch_job_queue,
        "job_definition": args.job_definition,
        "job_name_prefix": args.job_name_prefix,
        "target_active_workers": args.target_active_workers,
        "max_active_workers": args.max_active_workers,
        "max_submit_per_loop": args.max_submit_per_loop,
        "messages_per_worker": args.messages_per_worker,
        "keep_full_pool": bool(args.keep_full_pool),
        "submit": bool(args.submit),
        "env": _redact_env(args.env or []),
        "allowed_s3_prefixes": list(parse_allowed_s3_prefixes(getattr(args, "allowed_s3_prefix", []) or [])),
        "log_tail_bytes": args.log_tail_bytes,
        "max_log_bytes": args.max_log_bytes,
        "redact_regex_count": len(args.redact_regex or []),
    }
    config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n")

    loops: list[dict[str, Any]] = []
    stop_reason = None
    for loop_index in range(args.loops):
        depth = queue_depth(sqs, args.sqs_queue_url)
        backlog = depth["visible"] + (depth["not_visible"] if args.include_not_visible else 0)
        dlq_depth = queue_depth(sqs, args.dlq_url) if args.dlq_url else None
        dlq_total = sum(dlq_depth.values()) if dlq_depth else 0
        active = active_jobs(batch, args.batch_job_queue, args.job_name_prefix)
        active_count = len(active)
        desired = _supervisor_desired_workers(
            backlog=backlog,
            messages_per_worker=args.messages_per_worker,
            target_active_workers=args.target_active_workers,
            max_active_workers=args.max_active_workers,
            keep_full_pool=bool(args.keep_full_pool),
        )
        capacity_left = max(0, args.max_active_workers - active_count)
        to_submit = min(args.max_submit_per_loop, capacity_left, max(0, desired - active_count))
        loop_stop_reason = None
        if args.stop_on_dlq and dlq_total > 0:
            to_submit = 0
            loop_stop_reason = "dlq_not_empty"
            stop_reason = loop_stop_reason
        overrides = _worker_overrides(
            sqs_queue_url=args.sqs_queue_url,
            messages_per_worker=args.messages_per_worker,
            visibility_timeout=args.visibility_timeout,
            heartbeat_seconds=args.heartbeat_seconds,
            task_timeout_seconds=args.task_timeout_seconds,
            env=args.env or [],
            allowed_s3_prefixes=getattr(args, "allowed_s3_prefix", []) or [],
            log_tail_bytes=args.log_tail_bytes,
            max_log_bytes=args.max_log_bytes,
            redact_regexes=args.redact_regex or [],
            allow_legacy_done_markers=bool(getattr(args, "allow_legacy_done_markers", False)),
            vcpus=args.vcpus,
            memory=args.memory,
        )
        submitted = []
        if args.submit and to_submit > 0:
            submitted = _submit_worker_jobs(
                batch,
                count=to_submit,
                job_name_prefix=args.job_name_prefix,
                batch_job_queue=args.batch_job_queue,
                job_definition=args.job_definition,
                overrides=overrides,
                retry_attempts=args.retry_attempts,
            )
        record = {
            "schema": "sweetspot.supervisor_loop.v1",
            "checked_at": iso_now(),
            "loop_index": loop_index,
            "submit": bool(args.submit),
            "queue_depth": depth,
            "dlq_depth": dlq_depth,
            "backlog_used_for_sizing": backlog,
            "target_active_workers": args.target_active_workers,
            "max_active_workers": args.max_active_workers,
            "desired_active_workers": desired,
            "active_count": active_count,
            "active_status_counts": _status_counts(active),
            "terminal_status_counts": _terminal_job_counts(batch, args.batch_job_queue, args.job_name_prefix) if args.include_terminal_counts else None,
            "to_submit": to_submit,
            "submitted_count": len(submitted),
            "submitted": submitted[:50],
            "stop_reason": loop_stop_reason,
        }
        with status_path.open("a") as f:
            f.write(json.dumps(record, sort_keys=True) + "\n")
        loops.append(record)
        if loop_stop_reason:
            break
        if loop_index + 1 < args.loops:
            time.sleep(args.interval_seconds)

    summary = {
        "schema": "sweetspot.supervisor_summary.v1",
        "finished_at": iso_now(),
        "run_id": args.run_id,
        "submit": bool(args.submit),
        "loops": len(loops),
        "submitted_count": sum(int(r["submitted_count"]) for r in loops),
        "last_loop": loops[-1] if loops else None,
        "stop_reason": stop_reason,
        "status_jsonl": str(status_path),
        "config_json": str(config_path),
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps({**summary, "summary_json": str(summary_path)}, indent=2, sort_keys=True))
    return 2 if stop_reason and args.fail_on_stop else 0


def _iter_tasks_for_finalizer(args: argparse.Namespace, s3) -> Iterator[dict[str, Any]]:
    yield from _service_iter_tasks_for_finalizer(args, s3, iter_jsonl=_iter_jsonl)


def _finalizer_existence_index(args: argparse.Namespace, s3) -> _S3ExistenceIndex | None:
    return _service_finalizer_existence_index(args, s3)


def _read_tasks_for_finalizer(args: argparse.Namespace, s3) -> list[dict[str, Any]]:
    return _service_read_tasks_for_finalizer(args, s3, iter_jsonl=_iter_jsonl)


def cmd_finalize(args: argparse.Namespace) -> int:
    return _run_finalizer_service(args, aws_client=_aws_client, iter_jsonl=_iter_jsonl, check_task_fn=_check_task)


def _containers_log_stream(task_properties: Any) -> str | None:
    for task_prop in reversed(task_properties or []):
        for container in reversed(task_prop.get("containers") or []):
            stream = container.get("logStreamName")
            if stream:
                return str(stream)
    return None


def _job_log_stream(job: dict[str, Any]) -> str | None:
    attempts = job.get("attempts") or []
    for attempt in reversed(attempts):
        stream = ((attempt.get("container") or {}).get("logStreamName")) or _containers_log_stream(attempt.get("taskProperties"))
        if stream:
            return str(stream)
    stream = ((job.get("container") or {}).get("logStreamName")) or _containers_log_stream(((job.get("ecsProperties") or {}).get("taskProperties")))
    return str(stream) if stream else None


def _container_log_group(container: dict[str, Any] | None) -> str | None:
    options = ((container or {}).get("logConfiguration") or {}).get("options") or {}
    group = options.get("awslogs-group")
    return str(group) if group else None


def _job_log_group(job: dict[str, Any]) -> str | None:
    attempts = job.get("attempts") or []
    for attempt in reversed(attempts):
        group = _container_log_group(attempt.get("container"))
        if group:
            return group
        for task_prop in reversed(attempt.get("taskProperties") or []):
            for container in reversed(task_prop.get("containers") or []):
                group = _container_log_group(container)
                if group:
                    return group
    group = _container_log_group(job.get("container"))
    if group:
        return group
    for task_prop in ((job.get("ecsProperties") or {}).get("taskProperties")) or []:
        for container in task_prop.get("containers") or []:
            group = _container_log_group(container)
            if group:
                return group
    return None


def _describe_one_job(batch, job_id: str) -> dict[str, Any]:
    jobs = batch.describe_jobs(jobs=[job_id]).get("jobs", [])
    if not jobs:
        raise SystemExit(f"job not found: {job_id}")
    return jobs[0]


def cmd_jobs(args: argparse.Namespace) -> int:
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    batch = session.client("batch", region_name=args.region)
    statuses = args.status or ACTIVE_STATUSES
    rows: list[dict[str, Any]] = []
    for status in statuses:
        paginator = batch.get_paginator("list_jobs")
        for page in paginator.paginate(jobQueue=args.job_queue, jobStatus=status):
            for job in page.get("jobSummaryList", []):
                if args.name_regex and not re.search(args.name_regex, str(job.get("jobName", ""))):
                    continue
                rows.append({"jobId": job.get("jobId"), "jobName": job.get("jobName"), "status": status, "createdAt": job.get("createdAt"), "startedAt": job.get("startedAt"), "stoppedAt": job.get("stoppedAt")})
                if len(rows) >= args.max_jobs:
                    break
            if len(rows) >= args.max_jobs:
                break
        if len(rows) >= args.max_jobs:
            break
    report = {"schema": "sweetspot.jobs.v1", "checked_at": iso_now(), "job_queue": args.job_queue, "statuses": statuses, "count": len(rows), "jobs": rows}
    if getattr(args, "format", "json") == "table":
        _print_key_values("SweetSpot jobs", {"checked_at": report["checked_at"], "job_queue": args.job_queue, "statuses": ",".join(statuses), "count": len(rows)})
        if rows:
            print()
            _print_table("jobs", ["jobId", "jobName", "status", "createdAt", "startedAt", "stoppedAt"], rows)
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def _list_matching_jobs(batch, *, job_queues: list[str], statuses: list[str], name_regex: str | None, max_jobs: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pattern = re.compile(name_regex) if name_regex else None
    for job_queue in job_queues:
        for status in statuses:
            paginator = batch.get_paginator("list_jobs")
            for page in paginator.paginate(jobQueue=job_queue, jobStatus=status):
                ids: list[str] = []
                for job in page.get("jobSummaryList", []):
                    if pattern and not pattern.search(str(job.get("jobName", ""))):
                        continue
                    ids.append(str(job.get("jobId")))
                    if len(rows) + len(ids) >= max_jobs:
                        break
                for i in range(0, len(ids), 100):
                    rows.extend(batch.describe_jobs(jobs=ids[i : i + 100]).get("jobs", []))
                if len(rows) >= max_jobs:
                    return rows[:max_jobs]
    return rows


_CANCEL_JOB_STATUSES = {"SUBMITTED", "PENDING", "RUNNABLE"}
_TERMINATE_JOB_STATUSES = {"STARTING", "RUNNING"}


def _cancel_jobs_report(args: argparse.Namespace) -> dict[str, Any]:
    if not args.job_name_regex:
        raise SystemExit("cancel-jobs requires --job-name-regex to avoid broad cancellation")
    if args.max_jobs <= 0:
        raise SystemExit("--max-jobs must be positive")
    statuses = args.status or (["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"] if args.terminate_running else ["SUBMITTED", "PENDING", "RUNNABLE"])
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    batch = session.client("batch", region_name=args.region)
    jobs = _list_matching_jobs(batch, job_queues=args.job_queue, statuses=statuses, name_regex=args.job_name_regex, max_jobs=args.max_jobs)
    rows: list[dict[str, Any]] = []
    cancelled = terminated = skipped = 0
    for job in jobs:
        status = str(job.get("status") or "UNKNOWN")
        action = "skip"
        reason = None
        if status in _CANCEL_JOB_STATUSES:
            action = "cancel"
            if args.apply:
                batch.cancel_job(jobId=str(job.get("jobId")), reason=args.reason)
                cancelled += 1
        elif status in _TERMINATE_JOB_STATUSES:
            if args.terminate_running:
                action = "terminate"
                if args.apply:
                    batch.terminate_job(jobId=str(job.get("jobId")), reason=args.reason)
                    terminated += 1
            else:
                reason = "requires --terminate-running"
                skipped += 1
        else:
            reason = "status is not cancellable"
            skipped += 1
        rows.append(
            {
                "jobId": job.get("jobId"),
                "jobName": job.get("jobName"),
                "jobQueue": job.get("jobQueue"),
                "status": status,
                "action": action,
                "skip_reason": reason,
            }
        )
    if not args.apply:
        skipped = sum(1 for row in rows if row["action"] == "skip")
    return {
        "schema": "sweetspot.cancel_jobs.v1",
        "checked_at": iso_now(),
        "apply": bool(args.apply),
        "job_queues": args.job_queue,
        "statuses": statuses,
        "job_name_regex": args.job_name_regex,
        "max_jobs": args.max_jobs,
        "matched_count": len(rows),
        "actionable_count": sum(1 for row in rows if row["action"] in {"cancel", "terminate"}),
        "cancelled_count": cancelled,
        "terminated_count": terminated,
        "skipped_count": skipped,
        "terminate_running": bool(args.terminate_running),
        "reason": args.reason,
        "jobs": rows,
    }


def _print_cancel_jobs_report(report: dict[str, Any], *, output_format: str) -> None:
    if output_format == "table":
        _print_key_values(
            "SweetSpot cancel-jobs",
            {
                "checked_at": report["checked_at"],
                "apply": report["apply"],
                "matched_count": report["matched_count"],
                "actionable_count": report["actionable_count"],
                "cancelled_count": report["cancelled_count"],
                "terminated_count": report["terminated_count"],
                "skipped_count": report["skipped_count"],
                "terminate_running": report["terminate_running"],
                "job_name_regex": report["job_name_regex"],
            },
        )
        rows = report.get("jobs") or []
        if rows:
            print()
            _print_table("jobs", ["jobId", "jobName", "jobQueue", "status", "action", "skip_reason"], rows)
    else:
        print(json.dumps(report, indent=2, sort_keys=True))


def cmd_cancel_jobs(args: argparse.Namespace) -> int:
    _print_cancel_jobs_report(_cancel_jobs_report(args), output_format=getattr(args, "format", "json"))
    return 0


def _run_scoped_job_name_regex(run_id: str, job_name_prefix: str | None) -> str:
    prefix = job_name_prefix or run_id
    if run_id not in prefix:
        raise SystemExit("cancel --job-name-prefix must include RUN_ID; use cancel-jobs for advanced broad matching")
    return rf"^{re.escape(prefix)}(?:-|$)"


def cmd_cancel(args: argparse.Namespace) -> int:
    cancel_args = argparse.Namespace(
        apply=args.apply,
        format="json",
        job_name_regex=_run_scoped_job_name_regex(args.run_id, args.job_name_prefix),
        job_queue=args.job_queue,
        max_jobs=args.max_jobs,
        profile=args.profile,
        reason=args.reason or f"SweetSpot cancel requested for run {args.run_id}",
        region=args.region,
        status=args.status,
        terminate_running=args.terminate_running,
    )
    inner = _cancel_jobs_report(cancel_args)
    report = {
        "schema": "sweetspot.cancel.v1",
        "checked_at": inner["checked_at"],
        "run_id": args.run_id,
        "apply": bool(args.apply),
        "job_name_prefix": args.job_name_prefix or args.run_id,
        "job_name_regex": inner["job_name_regex"],
        "matched_count": inner["matched_count"],
        "actionable_count": inner["actionable_count"],
        "cancelled_count": inner["cancelled_count"],
        "terminated_count": inner["terminated_count"],
        "terminate_running": inner["terminate_running"],
        "cancel_jobs": inner,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def _record_task_ids_from_events(events: list[dict[str, Any]], task_ids: list[str]) -> None:
    for ev in events:
        task_id = _extract_task_id_from_log_message(str(ev.get("message", "")))
        if task_id and task_id not in task_ids:
            task_ids.append(task_id)


def _job_task_ids_from_logs(session, *, jobs: list[dict[str, Any]], region: str | None, log_group: str | None, max_events: int) -> dict[str, list[str]]:
    logs = session.client("logs", region_name=region)
    out: dict[str, list[str]] = {}
    scan_limit = max(1, max_events)
    for job in jobs:
        stream = _job_log_stream(job)
        group = log_group or _job_log_group(job) or "/aws/batch/job"
        task_ids: list[str] = []
        events_seen = 0
        next_token: str | None = None
        if stream:
            try:
                while events_seen < scan_limit:
                    remaining = scan_limit - events_seen
                    kwargs: dict[str, Any] = {"logGroupName": group, "logStreamNames": [stream], "filterPattern": '"task_id"', "limit": min(10000, remaining)}
                    if next_token:
                        kwargs["nextToken"] = next_token
                    resp = logs.filter_log_events(**kwargs)
                    events = resp.get("events", [])
                    events_seen += len(events)
                    _record_task_ids_from_events(events, task_ids)
                    new_token = resp.get("nextToken")
                    if not new_token or new_token == next_token:
                        break
                    next_token = str(new_token)
            except Exception:  # noqa: BLE001 - fall back when FilterLogEvents is unavailable or denied
                try:
                    resp = logs.get_log_events(logGroupName=group, logStreamName=stream, limit=min(10000, scan_limit), startFromHead=False)
                except Exception:  # noqa: BLE001 - repair planning should degrade when a job has no readable logs yet
                    resp = {"events": []}
                _record_task_ids_from_events(resp.get("events", []), task_ids)
        out[str(job.get("jobId"))] = task_ids
    return out


def _repair_plan_report(args: argparse.Namespace) -> dict[str, Any]:
    return _repair_plan_report_service(
        args,
        read_jsonl=_read_jsonl,
        iter_jsonl=_iter_jsonl,
        session_factory=boto3.Session,
        list_matching_jobs=_list_matching_jobs,
        job_task_ids_from_logs=_job_task_ids_from_logs,
    )


def cmd_repair_plan(args: argparse.Namespace) -> int:
    print(json.dumps(_repair_plan_report(args), indent=2, sort_keys=True))
    return 0


def cmd_repair(args: argparse.Namespace) -> int:
    report = _run_repair_service(
        args,
        read_jsonl=_read_jsonl,
        iter_jsonl=_iter_jsonl,
        session_factory=boto3.Session,
        list_matching_jobs=_list_matching_jobs,
        job_task_ids_from_logs=_job_task_ids_from_logs,
        run_scoped_job_name_regex=_run_scoped_job_name_regex,
        env_allowed_s3_prefixes=_env_allowed_s3_prefixes,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def cmd_cleanup_stale_messages(args: argparse.Namespace) -> int:
    if not args.queue_url:
        raise SystemExit("missing --queue-url or SWEETSPOT_SQS_QUEUE_URL")
    sqs = _aws_client(args, "sqs")
    s3 = _aws_client(args, "s3")
    scanned = deleted = done = invalid = kept = 0
    examples: list[dict[str, Any]] = []
    while scanned < args.max_messages:
        resp = sqs.receive_message(
            QueueUrl=args.queue_url,
            MaxNumberOfMessages=min(10, args.max_messages - scanned),
            WaitTimeSeconds=args.wait_time,
            VisibilityTimeout=args.visibility_timeout,
            AttributeNames=["ApproximateReceiveCount", "SentTimestamp"],
        )
        messages = resp.get("Messages", [])
        if not messages:
            break
        for msg in messages:
            scanned += 1
            receipt = msg.get("ReceiptHandle")
            task: dict[str, Any] | None = None
            try:
                parsed = json.loads(msg.get("Body", "{}"))
                if not isinstance(parsed, dict):
                    raise ValueError("message body is not a JSON object")
                task = parsed
                if args.run_id and task.get("run_id") != args.run_id:
                    kept += 1
                    if len(examples) < 20:
                        examples.append({"task_id": task.get("task_id"), "state": "skipped_run_id", "deleted": False})
                    continue
                record = _check_task(s3, task, None, allow_legacy_done_markers=bool(args.allow_legacy_done_markers))
                is_done = record.get("state") == "done"
            except Exception as exc:  # noqa: BLE001 - report malformed messages; do not delete by default
                invalid += 1
                is_done = False
                record = {"state": "invalid_message", "error": str(exc)}
            if is_done:
                done += 1
                if args.apply and receipt:
                    sqs.delete_message(QueueUrl=args.queue_url, ReceiptHandle=receipt)
                    deleted += 1
            else:
                kept += 1
            if len(examples) < 20:
                examples.append({"task_id": task.get("task_id") if task else None, "state": record.get("state"), "deleted": bool(args.apply and is_done)})
    print(
        json.dumps(
            {
                "schema": "sweetspot.stale_message_cleanup.v1",
                "checked_at": iso_now(),
                "queue_url": args.queue_url,
                "apply": bool(args.apply),
                "scanned": scanned,
                "done_messages": done,
                "deleted": deleted,
                "kept": kept,
                "invalid": invalid,
                "examples": examples,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def cmd_estimate_runtime(args: argparse.Namespace) -> int:
    samples: list[tuple[float, float]] = []
    if args.sample_jsonl:
        for path in args.sample_jsonl:
            for obj in _iter_jsonl(path):
                sample = _sample_from_runtime_obj(obj)
                if sample:
                    samples.append(sample)
    if args.completed_units is not None or args.elapsed_seconds is not None:
        if args.completed_units is None or args.elapsed_seconds is None:
            raise SystemExit("--completed-units and --elapsed-seconds must be provided together")
        samples.append((float(args.completed_units), float(args.elapsed_seconds)))
    if not samples:
        raise SystemExit("provide --sample-jsonl or --completed-units/--elapsed-seconds")
    rates = [units / seconds for units, seconds in samples if units > 0 and seconds > 0]
    if not rates:
        raise SystemExit("no positive throughput samples")
    target_units = args.target_units
    if target_units is None:
        if args.task_count is None or args.units_per_task is None:
            raise SystemExit("provide --target-units or both --task-count and --units-per-task")
        target_units = args.task_count * args.units_per_task
    median_rate = statistics.median(rates)
    p10_rate = sorted(rates)[max(0, int(0.1 * (len(rates) - 1)))]
    total_worker_seconds = target_units / median_rate
    conservative_worker_seconds = target_units / max(p10_rate, 1e-9)
    parallelism = max(1, args.active_workers)
    predicted_wall_seconds = total_worker_seconds / parallelism
    conservative_wall_seconds = conservative_worker_seconds / parallelism
    vcpu_hours = total_worker_seconds * args.vcpus_per_worker / 3600.0
    estimated_cost = vcpu_hours * args.price_per_vcpu_hour if args.price_per_vcpu_hour is not None else None
    per_task_seconds = (args.units_per_task / median_rate) if args.units_per_task else None
    warnings: list[str] = []
    if per_task_seconds and args.task_timeout_seconds and per_task_seconds > args.task_timeout_seconds * args.timeout_safety_fraction:
        warnings.append("predicted per-task runtime is too close to or above timeout; split tasks smaller or raise timeout/visibility")
    if args.spot and per_task_seconds and per_task_seconds > args.max_spot_task_seconds:
        warnings.append("long uncheckpointed Spot tasks waste too much work on interruption; use smaller chunks")
    print(
        json.dumps(
            {
                "schema": "sweetspot.runtime_estimate.v1",
                "checked_at": iso_now(),
                "sample_count": len(samples),
                "median_units_per_second_per_worker": median_rate,
                "p10_units_per_second_per_worker": p10_rate,
                "target_units": target_units,
                "active_workers": parallelism,
                "predicted_wall_seconds": predicted_wall_seconds,
                "conservative_wall_seconds": conservative_wall_seconds,
                "worker_vcpu_hours": vcpu_hours,
                "estimated_compute_cost": estimated_cost,
                "predicted_seconds_per_task": per_task_seconds,
                "warnings": warnings,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def cmd_describe_job(args: argparse.Namespace) -> int:
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    batch = session.client("batch", region_name=args.region)
    job = _describe_one_job(batch, args.job_id)
    container = job.get("container") or {}
    report = {
        "schema": "sweetspot.job_description.v1",
        "checked_at": iso_now(),
        "jobId": job.get("jobId"),
        "jobName": job.get("jobName"),
        "jobQueue": job.get("jobQueue"),
        "status": job.get("status"),
        "statusReason": job.get("statusReason"),
        "createdAt": job.get("createdAt"),
        "startedAt": job.get("startedAt"),
        "stoppedAt": job.get("stoppedAt"),
        "containerReason": container.get("reason"),
        "exitCode": container.get("exitCode"),
        "logStreamName": _job_log_stream(job),
        "attempts": job.get("attempts", []),
    }
    if getattr(args, "format", "json") == "table":
        _print_key_values(
            "SweetSpot job",
            {
                "checked_at": report["checked_at"],
                "jobId": report["jobId"],
                "jobName": report["jobName"],
                "jobQueue": report["jobQueue"],
                "status": report["status"],
                "statusReason": report["statusReason"],
                "containerReason": report["containerReason"],
                "exitCode": report["exitCode"],
                "logStreamName": report["logStreamName"],
                "attempts": len(report["attempts"]),
            },
        )
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def _log_target_from_args(session, args: argparse.Namespace) -> tuple[str, str]:
    if args.log_stream:
        log_group = args.log_group or "/aws/batch/job"
        return args.log_stream, log_group
    if not args.job_id:
        raise SystemExit("logs requires --log-stream or --job-id")
    batch = session.client("batch", region_name=args.region)
    job = _describe_one_job(batch, args.job_id)
    stream = _job_log_stream(job)
    if not stream:
        raise SystemExit(f"job has no log stream yet: {args.job_id}")
    log_group = args.log_group or _job_log_group(job) or "/aws/batch/job"
    return stream, log_group


def cmd_logs(args: argparse.Namespace) -> int:
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    logs = session.client("logs", region_name=args.region)
    stream, log_group = _log_target_from_args(session, args)
    kwargs: dict[str, Any] = {"logGroupName": log_group, "logStreamName": stream, "limit": args.limit, "startFromHead": args.start_from_head or bool(args.next_token)}
    if args.next_token:
        kwargs["nextToken"] = args.next_token
    resp = logs.get_log_events(**kwargs)
    events = []
    for ev in resp.get("events", []):
        msg = str(ev.get("message", ""))
        if args.filter_regex and not re.search(args.filter_regex, msg):
            continue
        events.append({"timestamp": ev.get("timestamp"), "message": msg})
    visible_events = events[-args.tail :] if args.tail else events
    report = {
        "schema": "sweetspot.logs.v1",
        "checked_at": iso_now(),
        "log_group": log_group,
        "log_stream": stream,
        "count": len(events),
        "nextForwardToken": resp.get("nextForwardToken"),
        "events": visible_events,
    }
    if getattr(args, "format", "json") == "table":
        _print_key_values(
            "SweetSpot logs", {"checked_at": report["checked_at"], "log_group": log_group, "log_stream": stream, "count": report["count"], "returned": len(visible_events), "nextForwardToken": report["nextForwardToken"]}
        )
        if visible_events:
            print()
            _print_table("events", ["timestamp", "message"], visible_events)
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def cmd_watch_job(args: argparse.Namespace) -> int:
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    batch = session.client("batch", region_name=args.region)
    deadline = time.time() + args.max_seconds if args.max_seconds else None
    last_report = None
    printed_table_header = False
    while True:
        job = _describe_one_job(batch, args.job_id)
        status = str(job.get("status"))
        last_report = {
            "schema": "sweetspot.watch_job.v1",
            "checked_at": iso_now(),
            "jobId": job.get("jobId"),
            "jobName": job.get("jobName"),
            "status": status,
            "statusReason": job.get("statusReason"),
            "logStreamName": _job_log_stream(job),
        }
        if getattr(args, "format", "json") == "table":
            if not printed_table_header:
                print("SweetSpot watch-job")
                print("checked_at\tjobId\tjobName\tstatus\tstatusReason\tlogStreamName")
                printed_table_header = True
            print("\t".join(_format_table_value(last_report.get(key)) for key in ["checked_at", "jobId", "jobName", "status", "statusReason", "logStreamName"]))
        else:
            print(json.dumps(last_report, indent=2, sort_keys=True))
        if status in {"SUCCEEDED", "FAILED"}:
            return 0 if status == "SUCCEEDED" else 2
        if deadline and time.time() >= deadline:
            return 3
        time.sleep(args.interval_seconds)


def _validate_s3_delete_prefix(prefix: str, *, min_prefix_chars: int) -> tuple[str, str]:
    bucket, key = parse_s3_uri(prefix)
    key = key.strip("/")
    if not key or len(key) < min_prefix_chars:
        raise SystemExit(f"refusing dangerous S3 prefix {prefix!r}; require at least {min_prefix_chars} key characters")
    if "*" in key or ".." in key.split("/"):
        raise SystemExit(f"refusing suspicious S3 prefix {prefix!r}")
    return bucket, key.rstrip("/") + "/"


def cmd_s3_delete_prefix(args: argparse.Namespace) -> int:
    bucket, prefix_key = _validate_s3_delete_prefix(args.prefix, min_prefix_chars=args.min_prefix_chars)
    if args.delete and args.confirm_prefix != args.prefix:
        raise SystemExit("--delete requires --confirm-prefix exactly matching --prefix")
    if args.batch_size <= 0 or args.batch_size > 1000:
        raise SystemExit("--batch-size must be in 1..1000")
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3", region_name=args.region)
    artifact_dir = args.artifact_dir or Path("artifacts") / "s3-delete-prefix" / utc_stamp()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    status_path = artifact_dir / "s3_delete_prefix_status.json"
    listed = deleted = delete_markers = 0
    batches = 0
    examples: list[dict[str, str]] = []
    paginator_name = "list_object_versions" if getattr(args, "include_versions", False) else "list_objects_v2"
    paginator = s3.get_paginator(paginator_name)
    batch: list[dict[str, str]] = []

    def flush() -> None:
        nonlocal batch, deleted, batches
        if not batch:
            return
        batches += 1
        if args.delete:
            resp = s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
            errors = resp.get("Errors") or []
            if errors:
                raise SystemExit(f"S3 DeleteObjects reported {len(errors)} errors; first={errors[0]!r}")
            deleted += len(batch)
        batch = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix_key):
        if getattr(args, "include_versions", False):
            page_objects = list(page.get("Versions", [])) + list(page.get("DeleteMarkers", []))
        else:
            page_objects = list(page.get("Contents", []))
        for obj in page_objects:
            key = obj["Key"]
            listed += 1
            entry = {"Key": key}
            if getattr(args, "include_versions", False):
                version_id = str(obj.get("VersionId") or "")
                if version_id:
                    entry["VersionId"] = version_id
                if obj in page.get("DeleteMarkers", []):
                    delete_markers += 1
            if len(examples) < 20:
                examples.append(dict(entry))
            batch.append(entry)
            if len(batch) >= args.batch_size:
                flush()
        status_path.write_text(
            json.dumps(
                {
                    "schema": "sweetspot.s3_delete_prefix_status.v1",
                    "updated_at": iso_now(),
                    "prefix": args.prefix,
                    "delete": bool(args.delete),
                    "include_versions": bool(getattr(args, "include_versions", False)),
                    "listed": listed,
                    "deleted": deleted,
                    "delete_markers": delete_markers,
                    "batches": batches,
                    "examples": examples,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
    flush()
    marker_s3 = None
    if args.delete and args.completion_marker_s3:
        marker_s3 = args.completion_marker_s3
        s3_upload_text(
            s3,
            json.dumps(
                {"schema": "sweetspot.s3_delete_prefix_marker.v1", "completed_at": iso_now(), "prefix": args.prefix, "include_versions": bool(getattr(args, "include_versions", False)), "deleted": deleted},
                indent=2,
                sort_keys=True,
            )
            + "\n",
            marker_s3,
        )
    summary = {
        "schema": "sweetspot.s3_delete_prefix_summary.v1",
        "finished_at": iso_now(),
        "prefix": args.prefix,
        "bucket": bucket,
        "key_prefix": prefix_key,
        "delete": bool(args.delete),
        "include_versions": bool(getattr(args, "include_versions", False)),
        "listed": listed,
        "deleted": deleted,
        "delete_markers": delete_markers,
        "batches": batches,
        "completion_marker_s3": marker_s3,
        "status_json": str(status_path),
        "examples": examples,
    }
    status_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def _parse_body(msg: dict[str, Any]) -> dict[str, Any]:
    try:
        body = json.loads(msg.get("Body", "{}"))
        return body if isinstance(body, dict) else {"_raw_body_type": type(body).__name__}
    except json.JSONDecodeError as exc:
        return {"_json_error": str(exc), "_raw_body": msg.get("Body", "")[:500]}


def _queue_arn(sqs, queue_url: str) -> str:
    attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"]).get("Attributes", {})
    arn = attrs.get("QueueArn")
    if not arn:
        raise SystemExit(f"queue has no QueueArn attribute: {queue_url}")
    return str(arn)


def cmd_dlq(args: argparse.Namespace) -> int:
    if args.apply and not args.queue_url and not getattr(args, "native_redrive", False):
        raise SystemExit("--apply requires --queue-url")
    sqs = _aws_client(args, "sqs")
    if getattr(args, "native_redrive", False):
        if not args.apply:
            raise SystemExit("--native-redrive requires --apply")
        if args.run_id or args.task_id_regex:
            raise SystemExit("--native-redrive moves the whole DLQ; use manual redrive for --run-id/--task-id-regex filters")
        kwargs: dict[str, Any] = {"SourceArn": _queue_arn(sqs, args.dlq_url)}
        if args.queue_url:
            kwargs["DestinationArn"] = _queue_arn(sqs, args.queue_url)
        if getattr(args, "max_messages_per_second", None):
            kwargs["MaxNumberOfMessagesPerSecond"] = args.max_messages_per_second
        resp = sqs.start_message_move_task(**kwargs)
        report = {
            "schema": "sweetspot.dlq_redrive_summary.v1",
            "checked_at": iso_now(),
            "native_redrive": True,
            "source_arn": kwargs["SourceArn"],
            "destination_arn": kwargs.get("DestinationArn"),
            "task_handle": resp.get("TaskHandle"),
            "max_messages_per_second": kwargs.get("MaxNumberOfMessagesPerSecond"),
        }
        if getattr(args, "format", "json") == "table":
            _print_key_values("SweetSpot DLQ redrive", report)
        else:
            print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    scanned = matched = moved = 0
    by_run: Counter[str] = Counter()
    by_schema: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    while scanned < args.max_messages:
        resp = sqs.receive_message(
            QueueUrl=args.dlq_url,
            MaxNumberOfMessages=min(10, args.max_messages - scanned),
            WaitTimeSeconds=args.wait_time,
            VisibilityTimeout=args.visibility_timeout,
            AttributeNames=["ApproximateReceiveCount", "SentTimestamp"],
        )
        messages = resp.get("Messages", [])
        if not messages:
            break
        for msg in messages:
            scanned += 1
            task = _parse_body(msg)
            by_run[str(task.get("run_id", "<missing>"))] += 1
            by_schema[str(task.get("schema", "<missing>"))] += 1
            ok = True
            if args.run_id and task.get("run_id") != args.run_id:
                ok = False
            if args.task_id_regex and not re.search(args.task_id_regex, str(task.get("task_id", ""))):
                ok = False
            if ok:
                matched += 1
                if len(examples) < 10:
                    examples.append({"task_id": task.get("task_id"), "run_id": task.get("run_id"), "receive_count": msg.get("Attributes", {}).get("ApproximateReceiveCount")})
                if args.apply:
                    sqs.send_message(QueueUrl=args.queue_url, MessageBody=msg.get("Body", ""))
                    sqs.delete_message(QueueUrl=args.dlq_url, ReceiptHandle=msg["ReceiptHandle"])
                    moved += 1
    report = {
        "schema": "sweetspot.dlq_summary.v1",
        "checked_at": iso_now(),
        "apply": bool(args.apply),
        "scanned": scanned,
        "matched": matched,
        "moved": moved,
        "by_run": dict(by_run.most_common()),
        "by_schema": dict(by_schema.most_common()),
        "examples": examples,
    }
    if getattr(args, "format", "json") == "table":
        _print_key_values("SweetSpot DLQ", {key: report[key] for key in ["checked_at", "apply", "scanned", "matched", "moved", "by_run", "by_schema"]})
        if examples:
            print()
            _print_table("examples", ["task_id", "run_id", "receive_count"], examples)
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def _doctor_check(name: str, fn) -> dict[str, Any]:
    started = time.time()
    try:
        details = fn()
        return {"name": name, "ok": True, "elapsed_sec": time.time() - started, "details": details or {}}
    except Exception as exc:
        return {"name": name, "ok": False, "elapsed_sec": time.time() - started, "error_type": type(exc).__name__, "error": str(exc)}


def _job_definition_log_group(job_def: dict[str, Any]) -> str | None:
    container = job_def.get("containerProperties") or {}
    return _container_log_group(container)


def _sqs_queue_name_from_url(queue_url: str) -> str:
    return queue_url.rstrip("/").rsplit("/", 1)[-1]


def _sqs_queue_arn(*, region: str, account_id: str, queue_name: str) -> str:
    return f"arn:aws:sqs:{region}:{account_id}:{queue_name}"


def cmd_doctor(args: argparse.Namespace) -> int:
    try:
        validate_worker_timing(visibility_timeout=args.visibility_timeout, heartbeat_seconds=args.heartbeat_seconds, task_timeout_seconds=args.task_timeout_seconds)
        parse_redact_patterns(args.redact_regex or [])
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    checks: list[dict[str, Any]] = []
    discovered_log_group = args.log_group

    if args.queue_url:

        def check_queue() -> dict[str, Any]:
            sqs = session.client("sqs", region_name=args.region)
            attrs = sqs.get_queue_attributes(QueueUrl=args.queue_url, AttributeNames=["All"]).get("Attributes", {})
            return {
                "queue_url": args.queue_url,
                "attributes": {k: attrs.get(k) for k in ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateAgeOfOldestMessage", "VisibilityTimeout", "RedrivePolicy"] if k in attrs},
            }

        checks.append(_doctor_check("sqs_work_queue", check_queue))

    if args.dlq_url:

        def check_dlq() -> dict[str, Any]:
            sqs = session.client("sqs", region_name=args.region)
            attrs = sqs.get_queue_attributes(QueueUrl=args.dlq_url, AttributeNames=["All"]).get("Attributes", {})
            return {"dlq_url": args.dlq_url, "attributes": {k: attrs.get(k) for k in ["ApproximateNumberOfMessages", "MessageRetentionPeriod"] if k in attrs}}

        checks.append(_doctor_check("sqs_dlq", check_dlq))

    if args.job_queue:

        def check_job_queue() -> dict[str, Any]:
            batch = session.client("batch", region_name=args.region)
            queues = batch.describe_job_queues(jobQueues=[args.job_queue]).get("jobQueues", [])
            if not queues:
                raise ValueError(f"job queue not found: {args.job_queue}")
            queue = queues[0]
            if queue.get("state") != "ENABLED" or queue.get("status") not in {"VALID", None}:
                raise ValueError(f"job queue not ready: state={queue.get('state')} status={queue.get('status')}")
            return {"jobQueueName": queue.get("jobQueueName"), "state": queue.get("state"), "status": queue.get("status"), "computeEnvironmentOrder": queue.get("computeEnvironmentOrder")}

        checks.append(_doctor_check("batch_job_queue", check_job_queue))

    if args.job_definition:

        def check_job_definition() -> dict[str, Any]:
            nonlocal discovered_log_group
            batch = session.client("batch", region_name=args.region)
            defs = batch.describe_job_definitions(jobDefinitions=[args.job_definition], status="ACTIVE").get("jobDefinitions", [])
            if not defs:
                raise ValueError(f"active job definition not found: {args.job_definition}")
            job_def = defs[0]
            container = job_def.get("containerProperties") or {}
            log_group = _job_definition_log_group(job_def)
            discovered_log_group = discovered_log_group or log_group
            return {
                "jobDefinitionName": job_def.get("jobDefinitionName"),
                "revision": job_def.get("revision"),
                "image": container.get("image"),
                "jobRoleArn": container.get("jobRoleArn"),
                "log_group": log_group,
                "command": container.get("command"),
            }

        checks.append(_doctor_check("batch_job_definition", check_job_definition))

    if args.s3_prefix:
        for prefix in args.s3_prefix:

            def check_s3(prefix=prefix) -> dict[str, Any]:
                s3 = session.client("s3", region_name=args.region)
                bucket, key = parse_s3_uri(prefix)
                list_prefix = key.rstrip("/")
                s3.list_objects_v2(Bucket=bucket, Prefix=list_prefix, MaxKeys=1)
                probe_uri = None
                if args.write_probe:
                    probe_key = f"{list_prefix.rstrip('/')}/.sweetspot-doctor-{utc_stamp()}.json" if list_prefix else f".sweetspot-doctor-{utc_stamp()}.json"
                    body = json.dumps({"schema": "sweetspot.doctor_probe.v1", "created_at": iso_now()}) + "\n"
                    s3.put_object(Bucket=bucket, Key=probe_key, Body=body.encode("utf-8"), ContentType="application/json")
                    s3.delete_object(Bucket=bucket, Key=probe_key)
                    probe_uri = f"s3://{bucket}/{probe_key}"
                return {"prefix": prefix, "bucket": bucket, "key_prefix": list_prefix, "write_probe": probe_uri}

            checks.append(_doctor_check(f"s3_prefix:{prefix}", check_s3))

    if getattr(args, "validate_batch_metrics", False) and args.job_queue:

        def check_batch_metrics() -> dict[str, Any]:
            cloudwatch = session.client("cloudwatch", region_name=args.region)
            job_queue_name = str(args.job_queue).split("/")[-1]
            metric_names = ["RunnableJobs", "FailedJobs"]
            found: dict[str, int] = {}
            for metric_name in metric_names:
                metrics = cloudwatch.list_metrics(
                    Namespace="AWS/Batch",
                    MetricName=metric_name,
                    Dimensions=[{"Name": "JobQueue", "Value": job_queue_name}],
                ).get("Metrics", [])
                found[metric_name] = len(metrics)
            if not any(found.values()):
                raise ValueError(f"no AWS/Batch metrics found for JobQueue={job_queue_name}; validate dimensions after jobs have emitted data or use worker logs/EventBridge alarms")
            return {"job_queue": job_queue_name, "metrics_found": found}

        checks.append(_doctor_check("batch_metrics", check_batch_metrics))

    if getattr(args, "check_run_queue_create", False):

        def check_run_queue_create() -> dict[str, Any]:
            if not args.run_queue_name:
                raise ValueError("--check-run-queue-create requires --run-queue-name")
            sts = session.client("sts", region_name=args.region)
            identity = sts.get_caller_identity()
            account_id = str(identity.get("Account") or "")
            principal_arn = str(identity.get("Arn") or "")
            region = args.region or session.region_name
            if not account_id or not principal_arn or not region:
                raise ValueError("could not resolve account, principal ARN, or region for IAM simulation")
            run_queue_arn = _sqs_queue_arn(region=region, account_id=account_id, queue_name=args.run_queue_name)
            resource_arns = [run_queue_arn]
            if args.run_queue_dlq_url:
                resource_arns.append(_sqs_queue_arn(region=region, account_id=account_id, queue_name=_sqs_queue_name_from_url(args.run_queue_dlq_url)))
            actions = ["sqs:CreateQueue", "sqs:TagQueue", "sqs:SetQueueAttributes", "sqs:GetQueueAttributes"]
            iam = session.client("iam", region_name=args.region)
            response = iam.simulate_principal_policy(PolicySourceArn=principal_arn, ActionNames=actions, ResourceArns=resource_arns)
            evaluations = response.get("EvaluationResults", [])
            denied = [
                {"action": ev.get("EvalActionName"), "resource": ev.get("EvalResourceName"), "decision": ev.get("EvalDecision")}
                for ev in evaluations
                if ev.get("EvalDecision") != "allowed"
            ]
            if denied:
                raise PermissionError(f"run-queue creation/update permissions are not all allowed: {denied}")
            return {"principal_arn": principal_arn, "run_queue_arn": run_queue_arn, "resource_arns": resource_arns, "actions": actions}

        checks.append(_doctor_check("run_queue_create_permissions", check_run_queue_create))

    if discovered_log_group:

        def check_logs() -> dict[str, Any]:
            logs = session.client("logs", region_name=args.region)
            groups = logs.describe_log_groups(logGroupNamePrefix=discovered_log_group).get("logGroups", [])
            match = next((g for g in groups if g.get("logGroupName") == discovered_log_group), None)
            if not match:
                raise ValueError(f"log group not found: {discovered_log_group}")
            return {"log_group": discovered_log_group, "retentionInDays": match.get("retentionInDays"), "storedBytes": match.get("storedBytes")}

        checks.append(_doctor_check("cloudwatch_log_group", check_logs))

    checks.append(
        {"name": "service_quotas", "ok": None, "details": {"status": "not_checked", "reason": "AWS Batch quota codes vary by account/Region; verify max vCPUs and queue limits in Service Quotas for production runs."}}
    )
    ok = all(c.get("ok") is not False for c in checks)
    report = {"schema": "sweetspot.doctor.v1", "checked_at": iso_now(), "ok": ok, "region": args.region, "checks": checks}
    if getattr(args, "format", "json") == "table":
        _print_key_values("SweetSpot doctor", {"checked_at": report["checked_at"], "ok": ok, "region": args.region})
        print()
        _print_table("checks", ["name", "ok", "elapsed_sec", "error_type", "error", "details"], checks)
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if ok else 2


PRIMARY_COMMANDS = frozenset({"admin", "cancel", "monitor", "plan", "repair", "run", "status", "version"})


def _print_primary_help() -> None:
    print("usage: sweetspot [--config CONFIG] {version,plan,run,monitor,status,repair,cancel,admin} ...")
    print()
    print("Primary controller workflow:")
    print("  version   Print the installed SweetSpot package version")
    print("  plan      Validate a JobSpec and emit a machine-readable Plan JSON envelope")
    print("  run       Dry-run or apply the Plan-authoritative run controller")
    print("  monitor   Emit non-blocking status/finalize checkpoint commands")
    print("  status    Show run artifacts, queue depth, DLQ depth, active workers, and S3 progress")
    print("  repair    Dry-run/apply run-scoped repair planning")
    print("  cancel    Dry-run/apply run-scoped Batch job cancellation")
    print("  admin     List and run advanced/operator commands")
    print()
    print("Use `sweetspot admin --help` for lower-level enqueue, worker, finalize, scout, diagnostics, and cleanup commands.")


ADMIN_COMMANDS = frozenset(
    {
        "cancel-jobs",
        "cleanup-stale-messages",
        "derive-canary",
        "describe-job",
        "dlq",
        "doctor",
        "enqueue-and-submit",
        "enqueue-jsonl",
        "estimate-runtime",
        "finalize",
        "jobs",
        "lane-manager",
        "logs",
        "repair-plan",
        "s3-delete-prefix",
        "scout",
        "submit-workers",
        "supervise-workers",
        "watch-job",
        "worker",
    }
)


CONFIG_COMMAND_KEYS: dict[str, set[str]] = {
    "cancel": {"apply", "job_name_prefix", "job_queue", "max_jobs", "profile", "reason", "region", "status", "terminate_running"},
    "cancel-jobs": {"apply", "format", "job_name_regex", "job_queue", "max_jobs", "profile", "reason", "region", "status", "terminate_running"},
    "cleanup-stale-messages": {"allow_legacy_done_markers", "apply", "max_messages", "profile", "queue_url", "region", "run_id", "visibility_timeout"},
    "derive-canary": {"out_dir", "run_id", "task_count", "tasks_jsonl"},
    "dlq": {"apply", "dlq_url", "format", "profile", "queue_url", "region", "run_id", "visibility_timeout"},
    "doctor": {
        "check_run_queue_create",
        "dlq_url",
        "format",
        "heartbeat_seconds",
        "job_definition",
        "job_queue",
        "profile",
        "queue_url",
        "region",
        "run_queue_dlq_url",
        "run_queue_name",
        "s3_prefix",
        "task_timeout_seconds",
        "visibility_timeout",
    },
    "enqueue-and-submit": {
        "allow_legacy_done_markers",
        "allowed_s3_prefix",
        "artifact_dir",
        "batch_job_queue",
        "heartbeat_seconds",
        "include_not_visible",
        "job_definition",
        "job_name_prefix",
        "max_workers",
        "memory",
        "messages_per_worker",
        "min_workers",
        "profile",
        "queue_url",
        "region",
        "run_id",
        "sqs_queue_url",
        "submit",
        "subtract_active",
        "task_timeout_seconds",
        "tasks_jsonl",
        "vcpus",
        "visibility_timeout",
    },
    "enqueue-jsonl": {"allowed_s3_prefix", "artifact_dir", "profile", "queue_url", "region", "run_id", "sqs_queue_url", "submit", "tasks_jsonl"},
    "finalize": {"allow_legacy_done_markers", "artifact_dir", "dry_run", "output_prefix", "profile", "publish_ready", "region", "run_id", "tasks_jsonl", "upload"},
    "describe-job": {"format", "job_id", "profile", "region"},
    "jobs": {"format", "job_name_regex", "job_queue", "max_jobs", "profile", "region"},
    "logs": {"format", "job_id", "log_group", "log_stream", "profile", "region"},
    "repair": {
        "active_status",
        "allow_legacy_done_markers",
        "allowed_s3_prefix",
        "apply",
        "artifact_dir",
        "batch_job_queue",
        "failed_status",
        "heartbeat_seconds",
        "include_active",
        "job_definition",
        "job_name_prefix",
        "job_queue",
        "log_group",
        "max_jobs",
        "max_workers",
        "memory",
        "messages_per_worker",
        "min_workers",
        "only_known_failed",
        "out_jsonl",
        "profile",
        "region",
        "sqs_queue_url",
        "submit_workers",
        "subtract_active",
        "task_status_jsonl",
        "tasks_jsonl",
        "vcpus",
        "visibility_timeout",
        "worker_job_name_prefix",
    },
    "monitor": {
        "artifact_dir",
        "canary_summary_jsonl",
        "deployment",
        "dlq_url",
        "emit_command",
        "format",
        "input_manifest_jsonl",
        "interval",
        "job_name_prefix",
        "job_queue",
        "job_spec",
        "output_prefix",
        "profile",
        "queue_url",
        "region",
        "until_complete",
    },
    "repair-plan": {"job_name_regex", "job_queue", "log_group", "max_jobs", "out_jsonl", "profile", "region", "task_status_jsonl", "tasks_jsonl"},
    "run": {
        "allowed_s3_prefix",
        "apply",
        "artifact_dir",
        "batch_job_queue",
        "canary_summary_jsonl",
        "collect_canary_summaries",
        "deployment",
        "dedicated_run_queue",
        "create_run_queue",
        "env",
        "heartbeat_seconds",
        "include_not_visible",
        "input_manifest_jsonl",
        "job_definition",
        "job_name_prefix",
        "kickoff_only",
        "log_tail_bytes",
        "max_log_bytes",
        "max_workers",
        "memory",
        "messages_per_worker",
        "min_workers",
        "out_production_tasks_jsonl",
        "profile",
        "queue_url",
        "redact_regex",
        "reconcile_interval_seconds",
        "reconcile_rounds",
        "reconcile_until_drained",
        "region",
        "finalize",
        "finalize_dry_run",
        "finalize_publish_ready",
        "finalize_upload",
        "retry_attempts",
        "sqs_queue_url",
        "subtract_active",
        "task_timeout_seconds",
        "vcpus",
        "visibility_timeout",
        "wait_for_visible_min",
        "wait_for_visible_seconds",
        "wait_interval_seconds",
    },
    "s3-delete-prefix": {"artifact_dir", "completion_marker_s3", "delete", "min_prefix_chars", "prefix", "profile", "region"},
    "status": {"artifact_dir", "dlq_url", "format", "job_name_prefix", "job_queue", "output_prefix", "profile", "queue_url", "region"},
    "submit-workers": {
        "allow_legacy_done_markers",
        "allowed_s3_prefix",
        "batch_job_queue",
        "heartbeat_seconds",
        "include_not_visible",
        "job_definition",
        "job_name_prefix",
        "max_workers",
        "memory",
        "messages_per_worker",
        "min_workers",
        "profile",
        "queue_url",
        "region",
        "sqs_queue_url",
        "submit",
        "subtract_active",
        "task_timeout_seconds",
        "vcpus",
        "visibility_timeout",
    },
    "supervise-workers": {
        "allow_legacy_done_markers",
        "allowed_s3_prefix",
        "artifact_dir",
        "batch_job_queue",
        "dlq_url",
        "heartbeat_seconds",
        "include_not_visible",
        "job_definition",
        "job_name_prefix",
        "max_active_workers",
        "memory",
        "messages_per_worker",
        "profile",
        "queue_url",
        "region",
        "run_id",
        "sqs_queue_url",
        "submit",
        "target_active_workers",
        "task_timeout_seconds",
        "vcpus",
        "visibility_timeout",
    },
    "watch-job": {"format", "job_id", "profile", "region"},
    "worker": {"allow_legacy_done_markers", "allowed_s3_prefix", "heartbeat_seconds", "profile", "queue_url", "region", "task_timeout_seconds", "visibility_timeout"},
}

CONFIG_FLAG_MAP: dict[str, tuple[str, bool]] = {
    "active_status": ("--active-status", True),
    "active_workers": ("--active-workers", False),
    "allowed_s3_prefix": ("--allowed-s3-prefix", True),
    "apply": ("--apply", False),
    "allow_legacy_done_markers": ("--allow-legacy-done-markers", False),
    "artifact_dir": ("--artifact-dir", False),
    "batch_job_queue": ("--batch-job-queue", False),
    "collect_canary_summaries": ("--collect-canary-summaries", False),
    "deployment": ("--deployment", False),
    "dedicated_run_queue": ("--dedicated-run-queue", False),
    "dlq_url": ("--dlq-url", False),
    "dry_run": ("--dry-run", False),
    "failed_status": ("--failed-status", True),
    "format": ("--format", False),
    "finalize": ("--finalize", False),
    "finalize_dry_run": ("--finalize-dry-run", False),
    "finalize_publish_ready": ("--finalize-publish-ready", False),
    "finalize_upload": ("--finalize-upload", False),
    "heartbeat_seconds": ("--heartbeat-seconds", False),
    "include_active": ("--include-active", False),
    "include_not_visible": ("--include-not-visible", False),
    "job_definition": ("--job-definition", False),
    "job_id": ("--job-id", False),
    "job_name_prefix": ("--job-name-prefix", False),
    "job_name_regex": ("--job-name-regex", False),
    "kickoff_only": ("--kickoff-only", False),
    "job_queue": ("--job-queue", False),
    "canary_summary_jsonl": ("--canary-summary-jsonl", False),
    "input_manifest_jsonl": ("--input-manifest-jsonl", False),
    "log_group": ("--log-group", False),
    "log_stream": ("--log-stream", False),
    "max_jobs": ("--max-jobs", False),
    "max_messages": ("--max-messages", False),
    "max_workers": ("--max-workers", False),
    "memory": ("--memory", False),
    "messages_per_worker": ("--messages-per-worker", False),
    "min_workers": ("--min-workers", False),
    "only_known_failed": ("--only-known-failed", False),
    "out_dir": ("--out-dir", False),
    "out_jsonl": ("--out-jsonl", False),
    "out_production_tasks_jsonl": ("--out-production-tasks-jsonl", False),
    "output_prefix": ("--output-prefix", False),
    "prefix": ("--prefix", False),
    "profile": ("--profile", False),
    "publish_ready": ("--publish-ready", False),
    "queue_url": ("--queue-url", False),
    "reconcile_interval_seconds": ("--reconcile-interval-seconds", False),
    "reconcile_rounds": ("--reconcile-rounds", False),
    "reconcile_until_drained": ("--reconcile-until-drained", False),
    "region": ("--region", False),
    "reason": ("--reason", False),
    "run_id": ("--run-id", False),
    "sqs_queue_url": ("--queue-url", False),
    "s3_prefix": ("--s3-prefix", True),
    "submit": ("--submit", False),
    "subtract_active": ("--subtract-active", False),
    "submit_workers": ("--submit-workers", False),
    "terminate_running": ("--terminate-running", False),
    "target_active_workers": ("--target-active-workers", False),
    "task_count": ("--task-count", False),
    "task_status_jsonl": ("--task-status-jsonl", False),
    "task_timeout_seconds": ("--task-timeout-seconds", False),
    "tasks_jsonl": ("--tasks-jsonl", False),
    "upload": ("--upload", False),
    "delete": ("--delete", False),
    "completion_marker_s3": ("--completion-marker-s3", False),
    "min_prefix_chars": ("--min-prefix-chars", False),
    "vcpus": ("--vcpus", False),
    "visibility_timeout": ("--visibility-timeout", False),
    "worker_job_name_prefix": ("--worker-job-name-prefix", False),
}


def _extract_config_arg(argv: list[str]) -> tuple[Path | None, list[str]]:
    config_path: Path | None = Path(os.environ["SWEETSPOT_CONFIG"]) if os.environ.get("SWEETSPOT_CONFIG") else None
    stripped: list[str] = []
    command: str | None = None
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--":
            stripped.extend(argv[i:])
            break
        if command is None and not arg.startswith("-"):
            command = arg
        if command == "lane-manager" or (command == "admin" and arg == "lane-manager"):
            stripped.extend(argv[i:])
            break
        if arg == "--config":
            if i + 1 >= len(argv):
                raise SystemExit("--config requires a path")
            config_path = Path(argv[i + 1])
            i += 2
            continue
        if arg.startswith("--config="):
            config_path = Path(arg.split("=", 1)[1])
            i += 1
            continue
        stripped.append(arg)
        i += 1
    return config_path, stripped


def _load_config(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise SystemExit(f"failed to read --config {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid JSON in --config {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise SystemExit("--config must contain a JSON object")
    return data


def _command_name(argv: list[str]) -> str | None:
    saw_admin = False
    for arg in argv:
        if arg == "--":
            return None
        if arg.startswith("-"):
            continue
        if arg == "admin" and not saw_admin:
            saw_admin = True
            continue
        return arg
    return "admin" if saw_admin else None


def _config_values(config: dict[str, Any], command: str | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for section in ("defaults", "default"):
        raw = config.get(section)
        if isinstance(raw, dict):
            merged.update(raw)
    if command:
        for section in (command, command.replace("-", "_")):
            raw = config.get(section)
            if isinstance(raw, dict):
                merged.update(raw)
    return {str(k).replace("-", "_"): v for k, v in merged.items()}


def _argv_has_flag(argv: list[str], flag: str) -> bool:
    return any(arg == flag or arg.startswith(f"{flag}=") for arg in argv)


def _apply_config_defaults(argv: list[str], config: dict[str, Any], command: str | None) -> list[str]:
    if command is None or command not in CONFIG_COMMAND_KEYS:
        return argv
    allowed_keys = CONFIG_COMMAND_KEYS[command]
    values = _config_values(config, command)
    injected: list[str] = []
    for key, value in values.items():
        if key not in allowed_keys:
            continue
        mapped = CONFIG_FLAG_MAP.get(key)
        if mapped is None or value is None:
            continue
        flag, repeatable = mapped
        if _argv_has_flag(argv, flag):
            continue
        if isinstance(value, bool):
            if value:
                injected.append(flag)
            continue
        if repeatable and isinstance(value, list):
            for item in value:
                injected.extend([flag, str(item)])
            continue
        injected.extend([flag, str(value)])
    if command and command in argv:
        idx = argv.index(command)
        return [*argv[: idx + 1], *injected, *argv[idx + 1 :]]
    return [*injected, *argv]


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    config_path, raw_argv = _extract_config_arg(raw_argv)
    config = _load_config(config_path)
    raw_argv = _apply_config_defaults(raw_argv, config, _command_name(raw_argv))
    if raw_argv and raw_argv[0] in {"-h", "--help"}:
        _print_primary_help()
        return 0
    prog = "sweetspot"
    if raw_argv and raw_argv[0] == "admin":
        if len(raw_argv) == 1 or raw_argv[1] in {"-h", "--help"}:
            print("primary workflow commands: " + ", ".join(sorted(PRIMARY_COMMANDS - {"admin"})))
            print("advanced/admin commands: " + ", ".join(sorted(ADMIN_COMMANDS)))
            print("use: sweetspot admin <command> [args]")
            return 0
        admin_command = raw_argv[1]
        if admin_command not in ADMIN_COMMANDS:
            raise SystemExit(f"sweetspot admin supports advanced commands only; use top-level `{admin_command}` if it is part of the primary controller workflow")
        raw_argv = raw_argv[1:]
        prog = "sweetspot admin"
    if raw_argv and raw_argv[0] == "scout":
        return int(scout.main(raw_argv[1:], prog=f"{prog} scout"))
    if raw_argv and raw_argv[0] == "lane-manager":
        return int(lane_manager.main(raw_argv[1:], prog=f"{prog} lane-manager"))

    ap = argparse.ArgumentParser(prog=prog)
    ap.add_argument("--config", type=Path, help="JSON config file with 'defaults' and per-command sections")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("version", help="Print the installed SweetSpot package version")
    p.set_defaults(func=cmd_version)

    p = _add_parser_with_examples(
        sub,
        "plan",
        help="Validate a SweetSpot JobSpec and emit a machine-readable Plan JSON envelope",
        examples="  sweetspot plan examples/job.x86.example.json\n  sweetspot plan examples/job.arm-eligible.example.json",
    )
    p.add_argument("job_spec", type=Path, help="Path to a sweetspot.job.v1 JSON JobSpec")
    p.add_argument("--canary-summary-jsonl", type=Path, help="Optional local JSONL of canary worker summaries or normalized observations for adaptive shard sizing")
    p.add_argument(
        "--input-manifest-jsonl", type=Path, help="Optional local JSONL copy of logical work units; with --canary-summary-jsonl, emits adaptive production shard counts; without summaries, plans initial canaries"
    )
    p.add_argument("--out-production-tasks-jsonl", type=Path, help="Optional local output path for calibrated production sweetspot.task.v1 JSONL; requires --canary-summary-jsonl and --input-manifest-jsonl")
    p.add_argument("--out-canary-tasks-jsonl", type=Path, help="Optional local output path for controller-owned adaptive canary sweetspot.task.v1 JSONL; requires --input-manifest-jsonl")
    p.set_defaults(func=cmd_plan)

    p = sub.add_parser("admin", help="List and run advanced/admin commands")
    p.add_argument("admin_args", nargs=argparse.REMAINDER)
    p.set_defaults(func=lambda _args: 0)

    p = _add_parser_with_examples(
        sub,
        "run",
        help="Dry-run or apply a run controller for a SweetSpot JobSpec",
        examples="  sweetspot run examples/job.x86.example.json\n  sweetspot run examples/job.x86.example.json --canary-summary-jsonl summaries.jsonl --input-manifest-jsonl manifest.jsonl --artifact-dir artifacts/run-1\n  sweetspot run examples/job.x86.example.json --canary-summary-jsonl summaries.jsonl --input-manifest-jsonl manifest.jsonl --artifact-dir artifacts/run-1 --deployment deployment.json --apply",
    )
    p.add_argument("job_spec", type=Path, help="Path to a sweetspot.job.v1 JSON JobSpec")
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--canary-summary-jsonl", type=Path, help="Optional local JSONL of canary worker summaries or normalized observations for adaptive shard sizing")
    p.add_argument("--collect-canary-summaries", action="store_true", help="After controller canary workers are submitted, collect canary task summaries from S3 and write production plan/task artifacts when ready")
    p.add_argument("--input-manifest-jsonl", type=Path, help="Optional local JSONL copy of logical work units for calibrated task materialization")
    p.add_argument("--out-production-tasks-jsonl", type=Path, help="Optional local output path for calibrated production sweetspot.task.v1 JSONL")
    p.add_argument("--artifact-dir", type=Path, help="Optional local directory for run_state.json and default production task artifacts")
    p.add_argument("--deployment", type=Path, help="sweetspot.deployment.v1 registry; normal --apply selects queue/job definition from the ready Plan, and canary apply requires isolated canary_routes")
    p.add_argument(
        "--queue-url",
        "--sqs-queue-url",
        dest="queue_url",
        default=os.environ.get("SWEETSPOT_SQS_QUEUE_URL"),
        help="Compatibility fallback SQS queue URL when --deployment is omitted; bypasses deployment-registry binding",
    )
    p.add_argument("--batch-job-queue", help="Compatibility fallback AWS Batch job queue when --deployment is omitted; prefer --deployment for agent workflows")
    p.add_argument("--job-definition", help="Compatibility fallback AWS Batch job definition when --deployment is omitted; prefer --deployment for digest/revision binding")
    p.add_argument("--job-name-prefix", help="Run-scoped Batch worker name prefix; defaults to RUN_ID-worker and must start with RUN_ID-")
    _add_legacy_run_override_args(p)
    _add_legacy_run_runtime_args(p, legacy_done_markers_help="Migration mode: pass SWEETSPOT_ALLOW_LEGACY_DONE_MARKERS=1 to submitted workers")
    p.add_argument("--wait-for-visible-min", type=int, help="minimum visible messages before submitting workers; default uses observed queue depth")
    p.add_argument("--wait-for-visible-seconds", type=float, default=0.0)
    p.add_argument("--wait-interval-seconds", type=float, default=1.0)
    p.add_argument("--dedicated-run-queue", action="store_true", help="Allow reconciliation to treat SQS depth as run-specific backlog; use only with a fresh/dedicated queue")
    p.add_argument("--create-run-queue", action="store_true", help="With --deployment and --dedicated-run-queue, create or verify a controller-owned per-run SQS queue and bind production workers to it")
    p.add_argument("--kickoff-only", action="store_true", help="Submit the initial Plan-sized worker wave and return immediately; useful for interactive agents/CI when external or scheduled status checks handle monitoring")
    p.add_argument("--reconcile-rounds", type=int, default=1, help="Bounded controller observation rounds after initial submission (default: 1)")
    p.add_argument("--reconcile-interval-seconds", type=float, default=0.0, help="Sleep between reconciliation rounds")
    p.add_argument("--reconcile-until-drained", action="store_true", help="With --dedicated-run-queue, continue reconciliation rounds until queue backlog and active workers drain or --reconcile-rounds is reached")
    p.add_argument("--finalize", action="store_true", help="After production worker reconciliation, run the finalizer with persisted production_tasks.jsonl and record finalizer artifacts in run_state.json")
    p.add_argument("--finalize-upload", action="store_true", help="With --finalize, upload finalizer manifests to output_prefix/manifests")
    p.add_argument("--finalize-dry-run", action="store_true", help="With --finalize, scan/write local finalizer artifacts but skip S3 uploads and READY mutations")
    p.add_argument("--finalize-publish-ready", action="store_true", help="With --finalize-upload, publish READY if the finalizer is complete")
    p.add_argument("--apply", action="store_true", help="Materialize tasks, enqueue once, submit Plan-sized workers, reconcile bounded dedicated-queue top-ups, optionally finalize, and persist run_state.json")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("scout", help=argparse.SUPPRESS, add_help=False)
    p.add_argument("scout_args", nargs=argparse.REMAINDER)
    p.set_defaults(func=cmd_scout)

    p = sub.add_parser("lane-manager", help=argparse.SUPPRESS, add_help=False)
    p.add_argument("lane_manager_args", nargs=argparse.REMAINDER)
    p.set_defaults(func=cmd_lane_manager)

    p = _add_parser_with_examples(
        sub,
        "monitor",
        help="Emit non-blocking status/finalize checkpoint commands for schedulers or CI",
        examples="  sweetspot monitor example-run --artifact-dir artifacts/example-run --queue-url https://sqs... --job-queue jq --output-prefix s3://bucket/run --emit-command",
    )
    p.add_argument("run_id")
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--artifact-dir", type=Path)
    p.add_argument("--queue-url")
    p.add_argument("--dlq-url")
    p.add_argument("--job-queue")
    p.add_argument("--job-name-prefix")
    p.add_argument("--output-prefix", help="Optional S3 output prefix to include in the generated status command")
    p.add_argument("--interval", default="10m", help="Suggested scheduler interval; informational only")
    p.add_argument("--until-complete", action="store_true", help="Annotate the scaffold as an until-complete checkpoint loop for external schedulers")
    p.add_argument("--emit-command", action="store_true", help="Print shell commands instead of JSON scaffold")
    p.add_argument("--format", choices=["json", "table"], default="json", help="Format for generated status command")
    p.add_argument("--job-spec", type=Path, help="Optional JobSpec path; when provided, emit an exact finalize command")
    p.add_argument("--deployment", type=Path, help="Optional deployment path for the generated finalize command")
    p.add_argument("--input-manifest-jsonl", type=Path, help="Optional manifest path for the generated finalize command")
    p.add_argument("--canary-summary-jsonl", type=Path, help="Optional canary summary path for the generated finalize command")
    p.set_defaults(func=cmd_monitor)

    p = _add_parser_with_examples(
        sub,
        "cancel",
        help="Dry-run/apply cancellation for Batch jobs whose names are scoped to a SweetSpot run ID",
        examples="  sweetspot cancel example-run --job-queue jq\n  sweetspot cancel example-run --job-queue jq --terminate-running --apply",
    )
    p.add_argument("run_id")
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--job-queue", action="append", required=True)
    p.add_argument("--job-name-prefix", help="Run-scoped Batch job-name prefix; must include RUN_ID. Defaults to RUN_ID.")
    p.add_argument("--status", action="append")
    p.add_argument("--max-jobs", type=int, default=100)
    p.add_argument("--terminate-running", action="store_true")
    p.add_argument("--reason")
    p.add_argument("--apply", action="store_true")
    p.set_defaults(func=cmd_cancel)

    p = _add_parser_with_examples(
        sub,
        "repair",
        help="Dry-run/apply run-scoped repair planning and optional repair-task enqueueing",
        examples="  sweetspot repair example-run --tasks-jsonl tasks.jsonl --task-status-jsonl artifacts/finalizer/task_status.jsonl --job-queue jq\n  sweetspot repair example-run --tasks-jsonl tasks.jsonl --task-status-jsonl artifacts/finalizer/task_status.jsonl --job-queue jq --sqs-queue-url https://sqs... --apply",
    )
    p.add_argument("run_id")
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--tasks-jsonl", type=Path, required=True)
    p.add_argument("--task-status-jsonl", type=Path, required=True)
    p.add_argument("--out-jsonl", type=Path)
    p.add_argument("--artifact-dir", type=Path)
    p.add_argument("--job-queue", action="append", required=True, help="Batch queue to inspect for active/failed jobs; repeatable")
    p.add_argument("--job-name-prefix", help="Run-scoped Batch job-name prefix to inspect; must include RUN_ID. Defaults to RUN_ID.")
    p.add_argument("--active-status", action="append", choices=list(ACTIVE_STATUSES), help="active statuses to exclude; default all active statuses")
    p.add_argument("--failed-status", action="append", choices=["FAILED"], help="failed statuses to classify; default FAILED")
    p.add_argument("--include-active", action="store_true", help="unsafe: include missing tasks even if logs show an active job owns them")
    p.add_argument("--only-known-failed", action="store_true", help="repair only missing tasks observed in failed job logs")
    p.add_argument("--log-group")
    p.add_argument("--log-tail", type=int, default=50000, help="maximum task-id CloudWatch log events to collect from each matching job stream")
    p.add_argument("--max-jobs", type=int, default=1000)
    p.add_argument("--sqs-queue-url", "--queue-url", dest="sqs_queue_url", default=os.environ.get("SWEETSPOT_SQS_QUEUE_URL", ""), help="SQS queue for repair tasks when --apply is set")
    p.add_argument("--apply", action="store_true", help="enqueue repair tasks; default is dry-run")
    p.add_argument("--submit-workers", action="store_true", help="after enqueueing, submit Batch workers sized to repair-task count")
    p.add_argument("--batch-job-queue")
    p.add_argument("--job-definition")
    p.add_argument("--worker-job-name-prefix", help="Batch job-name prefix for submitted repair workers; defaults to RUN_ID-repair-worker")
    p.add_argument("--messages-per-worker", type=int, default=1)
    p.add_argument("--max-workers", type=int, default=64)
    p.add_argument("--min-workers", type=int, default=0)
    p.add_argument("--subtract-active", action="store_true")
    _add_worker_runtime_args(p, legacy_done_markers_help="Migration mode: pass SWEETSPOT_ALLOW_LEGACY_DONE_MARKERS=1 to repair workers")
    p.set_defaults(func=cmd_repair)

    p = _add_parser_with_examples(
        sub,
        "status",
        help="Show run artifacts, queue depth, DLQ depth, and active Batch worker summary",
        examples="  sweetspot status example-run --artifact-dir artifacts/example-run\n  sweetspot status example-run --profile prod --region us-west-2 --queue-url https://sqs... --dlq-url https://sqs... --job-queue jq\n  sweetspot status --queue-url https://sqs... --format table",
    )
    p.add_argument("run_id", nargs="?", help="Optional SweetSpot run id; when set, local artifacts are summarized and Batch job-name prefix defaults to RUN_ID-")
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--queue-url", default=None, help="Source SQS queue URL; for legacy non-run status only, SWEETSPOT_SQS_QUEUE_URL is used when omitted")
    p.add_argument("--dlq-url")
    p.add_argument("--job-queue")
    p.add_argument("--job-name-prefix", help="Batch job-name prefix to inspect; with RUN_ID it must start with RUN_ID- and defaults to RUN_ID-")
    p.add_argument("--artifact-dir", type=Path, help="Local run artifact directory; defaults to artifacts/RUN_ID when RUN_ID is provided")
    p.add_argument("--output-prefix", help="Optional S3 output prefix; when set, status counts done markers and estimates remaining work")
    p.add_argument("--format", choices=["json", "table"], default="json")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("worker", help=argparse.SUPPRESS)
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--queue-url", default=os.environ.get("SWEETSPOT_SQS_QUEUE_URL", ""))
    p.add_argument("--max-messages", type=int, default=int(os.environ.get("SWEETSPOT_MAX_MESSAGES", "1")))
    p.add_argument("--visibility-timeout", type=int, default=int(os.environ.get("SWEETSPOT_VISIBILITY_TIMEOUT", "1800")))
    p.add_argument("--heartbeat-seconds", type=int, default=int(os.environ.get("SWEETSPOT_HEARTBEAT_SECONDS", "300")))
    p.add_argument(
        "--task-timeout-seconds", type=float, default=float(os.environ.get("SWEETSPOT_TASK_TIMEOUT_SECONDS", str(SAFE_TASK_TIMEOUT_SECONDS))), help="Default per-task command timeout when a task omits timeout_seconds"
    )
    p.add_argument("--wait-time", type=int, default=10)
    p.add_argument("--work-dir", type=Path, default=Path(os.environ.get("SWEETSPOT_WORK_DIR", "/tmp/sweetspot-work")))
    p.add_argument("--allowed-s3-prefix", action="append", default=_env_allowed_s3_prefixes(), help="S3 prefix allowed in task payloads; repeatable. Also read from SWEETSPOT_ALLOWED_S3_PREFIXES.")
    p.add_argument("--log-tail-bytes", type=int, default=int(os.environ.get("SWEETSPOT_LOG_TAIL_BYTES", str(DEFAULT_LOG_TAIL_BYTES))), help="Bytes of redacted stdout/stderr tail to keep in task summaries")
    p.add_argument("--max-log-bytes", type=int, default=int(os.environ.get("SWEETSPOT_MAX_LOG_BYTES", str(DEFAULT_MAX_LOG_BYTES))), help="Maximum redacted bytes per stdout/stderr stream to upload to S3")
    p.add_argument("--redact-regex", action="append", default=[], help="Regex to redact from streamed/uploaded task logs; repeatable. SWEETSPOT_REDACT_REGEXES may provide newline-separated defaults.")
    p.add_argument("--allow-legacy-done-markers", action="store_true", default=_env_bool("SWEETSPOT_ALLOW_LEGACY_DONE_MARKERS"), help="Migration mode: accept v1 done markers without task hashes/attempt checks")
    p.set_defaults(
        func=lambda a: run_worker(
            queue_url=a.queue_url,
            max_messages=a.max_messages,
            visibility_timeout=a.visibility_timeout,
            heartbeat_seconds=a.heartbeat_seconds,
            wait_time=a.wait_time,
            work_dir=a.work_dir,
            task_timeout_seconds=a.task_timeout_seconds,
            allowed_s3_prefixes=a.allowed_s3_prefix,
            log_tail_bytes=a.log_tail_bytes,
            max_log_bytes=a.max_log_bytes,
            redact_regexes=a.redact_regex,
            allow_legacy_done_markers=a.allow_legacy_done_markers,
            profile=a.profile,
            region=a.region,
        )
    )

    p = _add_parser_with_examples(
        sub,
        "enqueue-jsonl",
        help=argparse.SUPPRESS,
        examples="  sweetspot admin enqueue-jsonl --tasks-jsonl tasks.jsonl --allowed-s3-prefix s3://bucket/run --submit --queue-url https://sqs...",
    )
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--queue-url", "--sqs-queue-url", dest="queue_url", default=os.environ.get("SWEETSPOT_SQS_QUEUE_URL", ""))
    p.add_argument("--tasks-jsonl", type=Path, required=True)
    p.add_argument("--run-id")
    p.add_argument("--artifact-dir", type=Path)
    p.add_argument("--allowed-s3-prefix", action="append", default=[], help="Reject tasks containing S3 URIs outside this prefix; repeatable. Defaults to SWEETSPOT_ALLOWED_S3_PREFIXES when unset.")
    p.add_argument("--submit", action="store_true")
    p.set_defaults(func=cmd_enqueue_jsonl)

    p = _add_parser_with_examples(
        sub,
        "enqueue-and-submit",
        help=argparse.SUPPRESS,
        examples="  sweetspot admin enqueue-and-submit --tasks-jsonl tasks.jsonl --queue-url https://sqs... --batch-job-queue jq --job-definition jd --allowed-s3-prefix s3://bucket/run --submit",
    )
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--queue-url", "--sqs-queue-url", dest="queue_url", default=os.environ.get("SWEETSPOT_SQS_QUEUE_URL", ""))
    p.add_argument("--tasks-jsonl", type=Path, required=True)
    p.add_argument("--run-id")
    p.add_argument("--artifact-dir", type=Path)
    _add_batch_worker_target_args(p)
    _add_worker_sizing_args(p)
    _add_worker_runtime_args(p, legacy_done_markers_help="Migration mode: pass SWEETSPOT_ALLOW_LEGACY_DONE_MARKERS=1 to submitted workers")
    p.add_argument("--wait-for-visible-seconds", type=float, default=30.0)
    p.add_argument("--wait-for-visible-min", type=int, help="minimum visible messages before submitting workers; default initial visible backlog plus sent messages")
    p.add_argument("--wait-interval-seconds", type=float, default=2.0)
    p.add_argument("--submit", action="store_true")
    p.set_defaults(func=cmd_enqueue_and_submit)

    p = _add_parser_with_examples(
        sub,
        "derive-canary",
        help=argparse.SUPPRESS,
        examples="  sweetspot admin derive-canary --tasks-jsonl tasks.jsonl --out-dir artifacts/canary --task-count 4 --rewrite-run-id",
    )
    p.add_argument("--tasks-jsonl", type=Path, required=True)
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--run-id", default=f"canary-{utc_stamp()}")
    p.add_argument("--selected-indices", default="auto", help="auto or comma/range list, e.g. 0,5,10-12")
    p.add_argument("--task-count", type=int, default=4)
    p.add_argument("--rewrite-run-id", action="store_true", help="rewrite selected tasks to use --run-id")
    p.add_argument("--include-dlq-probe", action="store_true")
    p.add_argument("--dlq-probe-prefix", help="S3 prefix for the intentional DLQ probe done marker; required when selected tasks do not already have a done-marker prefix")
    p.set_defaults(func=cmd_derive_canary)

    p = _add_parser_with_examples(
        sub,
        "submit-workers",
        help=argparse.SUPPRESS,
        examples="  sweetspot admin submit-workers --queue-url https://sqs... --batch-job-queue jq --job-definition jd --messages-per-worker 1 --submit",
    )
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--sqs-queue-url", "--queue-url", dest="sqs_queue_url", default=os.environ.get("SWEETSPOT_SQS_QUEUE_URL", ""))
    _add_batch_worker_target_args(p)
    _add_worker_sizing_args(p)
    _add_worker_runtime_args(p, legacy_done_markers_help="Migration mode: pass SWEETSPOT_ALLOW_LEGACY_DONE_MARKERS=1 to submitted workers")
    p.add_argument("--submit", action="store_true")
    p.set_defaults(func=cmd_submit_workers)

    p = _add_parser_with_examples(
        sub,
        "supervise-workers",
        help=argparse.SUPPRESS,
        examples="  sweetspot admin supervise-workers --queue-url https://sqs... --batch-job-queue jq --job-definition jd --target-active-workers 16 --loops 10 --submit",
    )
    p.add_argument("--run-id")
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--sqs-queue-url", "--queue-url", dest="sqs_queue_url", default=os.environ.get("SWEETSPOT_SQS_QUEUE_URL", ""))
    p.add_argument("--dlq-url")
    p.add_argument("--stop-on-dlq", action="store_true")
    p.add_argument("--fail-on-stop", action="store_true")
    _add_batch_worker_target_args(p)
    p.add_argument("--target-active-workers", type=int, default=64)
    p.add_argument("--max-active-workers", type=int, default=64)
    p.add_argument("--max-submit-per-loop", type=int, default=64)
    p.add_argument("--messages-per-worker", type=int, default=1)
    p.add_argument("--include-not-visible", action="store_true")
    p.add_argument("--include-terminal-counts", action="store_true")
    p.add_argument("--keep-full-pool", action="store_true")
    p.add_argument("--loops", type=int, default=1)
    p.add_argument("--interval-seconds", type=float, default=60.0)
    p.add_argument("--artifact-dir", type=Path)
    _add_worker_runtime_args(p, legacy_done_markers_help="Migration mode: pass SWEETSPOT_ALLOW_LEGACY_DONE_MARKERS=1 to supervised workers")
    p.add_argument("--submit", action="store_true")
    p.set_defaults(func=cmd_supervise_workers)

    p = _add_parser_with_examples(
        sub,
        "finalize",
        help=argparse.SUPPRESS,
        examples="  sweetspot admin finalize --run-id run-1 --output-prefix s3://bucket/run-1 --tasks-jsonl tasks.jsonl --upload --publish-ready",
    )
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--run-id", required=True)
    p.add_argument("--output-prefix", required=True)
    p.add_argument("--tasks-jsonl", type=Path)
    p.add_argument("--tasks-s3")
    p.add_argument("--artifact-dir", type=Path)
    p.add_argument("--workers", type=int, default=32)
    p.add_argument("--progress-interval", type=int, default=1000)
    p.add_argument("--max-inline-outputs", type=int, default=FINALIZER_DEFAULT_MAX_INLINE_OUTPUTS, help="Max output URIs to inline in final_manifest.json; all outputs are written to outputs.jsonl")
    p.add_argument("--use-listing-index", action="store_true", help="Preload default run S3 prefixes with ListObjectsV2 to reduce per-task HeadObject calls")
    p.add_argument("--preload-s3-prefix", action="append", default=[], help="Additional s3://bucket/prefix to preload into the finalizer existence index; repeatable")
    p.add_argument("--write-repair-jsonl", type=Path)
    p.add_argument("--upload", action="store_true")
    p.add_argument("--dry-run", action="store_true", help="Scan and write local artifacts, but skip S3 manifest uploads, READY deletion, and READY publishing")
    p.add_argument("--publish-ready", action="store_true")
    p.add_argument("--ready-key", default="READY")
    p.add_argument("--allow-incomplete-ready", action="store_true", help="unsafe: publish READY even when tasks are incomplete")
    p.add_argument("--allow-legacy-done-markers", action="store_true", help="Migration mode: accept legacy v1 done markers without task hashes/attempt checks")
    p.add_argument("--require-complete", action="store_true")
    p.set_defaults(func=cmd_finalize)

    p = sub.add_parser("jobs", help=argparse.SUPPRESS)
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--job-queue", required=True)
    p.add_argument("--status", action="append", choices=list(ACTIVE_STATUSES) + ["SUCCEEDED", "FAILED"], help="repeatable; default active statuses")
    p.add_argument("--name-regex")
    p.add_argument("--max-jobs", type=int, default=1000)
    p.add_argument("--format", choices=["json", "table"], default="json")
    p.set_defaults(func=cmd_jobs)

    p = _add_parser_with_examples(
        sub,
        "cancel-jobs",
        help=argparse.SUPPRESS,
        examples="  sweetspot admin cancel-jobs --job-queue jq --job-name-regex '^sweetspot-worker-.*'\n  sweetspot admin cancel-jobs --job-queue jq --job-name-regex '^sweetspot-worker-.*' --apply",
    )
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--job-queue", action="append", required=True, help="Batch queue to inspect; repeatable")
    p.add_argument("--status", action="append", choices=list(ACTIVE_STATUSES), help="repeatable; default SUBMITTED/PENDING/RUNNABLE, plus STARTING/RUNNING when --terminate-running is set")
    p.add_argument("--job-name-regex", required=True, help="required guardrail: only jobs whose names match are considered")
    p.add_argument("--max-jobs", type=int, default=100)
    p.add_argument("--reason", default="Cancelled by sweetspot cancel-jobs")
    p.add_argument("--terminate-running", action="store_true", help="also terminate STARTING/RUNNING jobs instead of skipping them")
    p.add_argument("--apply", action="store_true", help="perform cancellation/termination; default is dry-run")
    p.add_argument("--format", choices=["json", "table"], default="json")
    p.set_defaults(func=cmd_cancel_jobs)

    p = _add_parser_with_examples(
        sub,
        "repair-plan",
        help=argparse.SUPPRESS,
        examples="  sweetspot admin repair-plan --tasks-jsonl tasks.jsonl --task-status-jsonl artifacts/finalizer/task_status.jsonl --out-jsonl repair.jsonl --job-queue jq --job-name-regex run-1",
    )
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--tasks-jsonl", type=Path, required=True)
    p.add_argument("--task-status-jsonl", type=Path, required=True)
    p.add_argument("--out-jsonl", type=Path, required=True)
    p.add_argument("--job-queue", action="append", default=[], help="Batch queue to inspect for active/failed jobs; repeatable")
    p.add_argument("--job-name-regex")
    p.add_argument("--active-status", action="append", choices=list(ACTIVE_STATUSES), help="active statuses to exclude; default all active statuses")
    p.add_argument("--failed-status", action="append", choices=["FAILED"], help="failed statuses to classify; default FAILED")
    p.add_argument("--include-active", action="store_true", help="unsafe: include missing tasks even if logs show an active job owns them")
    p.add_argument("--only-known-failed", action="store_true", help="repair only missing tasks observed in failed job logs")
    p.add_argument("--log-group")
    p.add_argument("--log-tail", type=int, default=50000, help="maximum task-id CloudWatch log events to collect from each matching job stream")
    p.add_argument("--max-jobs", type=int, default=1000)
    p.set_defaults(func=cmd_repair_plan)

    p = _add_parser_with_examples(
        sub,
        "cleanup-stale-messages",
        help=argparse.SUPPRESS,
        examples="  sweetspot admin cleanup-stale-messages --queue-url https://sqs... --run-id run-1 --max-messages 100\n  sweetspot admin cleanup-stale-messages --queue-url https://sqs... --run-id run-1 --apply",
    )
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--queue-url", default=os.environ.get("SWEETSPOT_SQS_QUEUE_URL", ""))
    p.add_argument("--run-id", help="Only consider messages for this run_id")
    p.add_argument("--max-messages", type=int, default=100)
    p.add_argument("--wait-time", type=int, default=1)
    p.add_argument("--visibility-timeout", type=int, default=5, help="Short dry-run lease so skipped messages quickly reappear")
    p.add_argument("--allow-legacy-done-markers", action="store_true")
    p.add_argument("--apply", action="store_true", help="Delete stale done messages; default is dry-run")
    p.set_defaults(func=cmd_cleanup_stale_messages)

    p = _add_parser_with_examples(
        sub,
        "estimate-runtime",
        help=argparse.SUPPRESS,
        examples="  sweetspot admin estimate-runtime --sample-jsonl canary_summaries.jsonl --target-units 10000000 --active-workers 32 --price-per-vcpu-hour 0.02",
    )
    p.add_argument("--sample-jsonl", action="append", type=Path, default=[], help="JSONL with task summaries/metrics containing completed_units+seconds; repeatable")
    p.add_argument("--completed-units", type=float)
    p.add_argument("--elapsed-seconds", type=float)
    p.add_argument("--target-units", type=float)
    p.add_argument("--task-count", type=float)
    p.add_argument("--units-per-task", type=float)
    p.add_argument("--active-workers", type=int, default=1)
    p.add_argument("--vcpus-per-worker", type=float, default=1.0)
    p.add_argument("--price-per-vcpu-hour", type=float)
    p.add_argument("--task-timeout-seconds", type=float)
    p.add_argument("--timeout-safety-fraction", type=float, default=0.8)
    p.add_argument("--spot", action="store_true")
    p.add_argument("--max-spot-task-seconds", type=float, default=1800.0)
    p.set_defaults(func=cmd_estimate_runtime)

    p = sub.add_parser("describe-job", help=argparse.SUPPRESS)
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--job-id", required=True)
    p.add_argument("--format", choices=["json", "table"], default="json")
    p.set_defaults(func=cmd_describe_job)

    p = sub.add_parser("logs", help=argparse.SUPPRESS)
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--job-id")
    p.add_argument("--log-group", help="CloudWatch log group; with --job-id, defaults to the job definition log group when discoverable")
    p.add_argument("--log-stream")
    p.add_argument("--limit", "--max-events", dest="limit", type=int, default=100, help="Maximum CloudWatch events to request; --max-events is the clearer alias")
    p.add_argument("--tail", "--last", dest="tail", type=int, default=0, help="Return only the last N events after filtering; --last is the clearer alias")
    p.add_argument("--filter-regex")
    p.add_argument("--next-token")
    p.add_argument("--start-from-head", action="store_true")
    p.add_argument("--format", choices=["json", "table"], default="json")
    p.set_defaults(func=cmd_logs)

    p = sub.add_parser("watch-job", help=argparse.SUPPRESS)
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--job-id", required=True)
    p.add_argument("--interval-seconds", type=float, default=30.0)
    p.add_argument("--max-seconds", type=float, default=0.0)
    p.add_argument("--format", choices=["json", "table"], default="json")
    p.set_defaults(func=cmd_watch_job)

    p = sub.add_parser("s3-delete-prefix", help=argparse.SUPPRESS)
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--prefix", required=True, help="s3://bucket/prefix/ to inspect or delete")
    p.add_argument("--delete", action="store_true", help="actually delete objects; default is dry-run")
    p.add_argument("--confirm-prefix", default="", help="must exactly match --prefix when --delete is set")
    p.add_argument("--min-prefix-chars", type=int, default=8)
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--include-versions", action="store_true", help="Delete/list object versions and delete markers, not just current objects")
    p.add_argument("--artifact-dir", type=Path)
    p.add_argument("--completion-marker-s3")
    p.set_defaults(func=cmd_s3_delete_prefix)

    p = _add_parser_with_examples(
        sub,
        "doctor",
        help=argparse.SUPPRESS,
        examples="  sweetspot admin doctor --profile prod --region us-west-2 --queue-url https://sqs... --job-queue jq --job-definition jd --s3-prefix s3://bucket/run",
    )
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--queue-url", default=os.environ.get("SWEETSPOT_SQS_QUEUE_URL", ""))
    p.add_argument("--dlq-url")
    p.add_argument("--job-queue")
    p.add_argument("--job-definition")
    p.add_argument("--log-group")
    p.add_argument("--validate-batch-metrics", action="store_true", help="Check CloudWatch AWS/Batch JobQueue metric dimensions for this account/Region")
    p.add_argument("--check-run-queue-create", action="store_true", help="Use IAM simulation to preflight permissions needed for controller-created per-run SQS queues")
    p.add_argument("--run-queue-name", help="Queue name to use with --check-run-queue-create")
    p.add_argument("--run-queue-dlq-url", help="DLQ URL whose redrive allow policy may need updates for a controller-created run queue")
    p.add_argument("--s3-prefix", action="append", default=[], help="S3 prefix to validate with ListBucket; repeatable")
    p.add_argument("--write-probe", action="store_true", help="Also write/delete a tiny object under each --s3-prefix")
    p.add_argument("--visibility-timeout", type=int, default=1800)
    p.add_argument("--heartbeat-seconds", type=int, default=300)
    p.add_argument("--task-timeout-seconds", type=float, default=SAFE_TASK_TIMEOUT_SECONDS)
    p.add_argument("--redact-regex", action="append", default=[], help="Validate worker log redaction regexes")
    p.add_argument("--format", choices=["json", "table"], default="json")
    p.set_defaults(func=cmd_doctor)

    p = sub.add_parser("dlq", help=argparse.SUPPRESS)
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--dlq-url", required=True)
    p.add_argument("--queue-url")
    p.add_argument("--run-id")
    p.add_argument("--task-id-regex")
    p.add_argument("--max-messages", type=int, default=100)
    p.add_argument("--native-redrive", action="store_true", help="Use SQS StartMessageMoveTask to move the whole DLQ instead of manual receive/send/delete")
    p.add_argument("--max-messages-per-second", type=int, help="Rate limit for --native-redrive")
    p.add_argument("--visibility-timeout", type=int, default=10)
    p.add_argument("--wait-time", type=int, default=1)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--format", choices=["json", "table"], default="json")
    p.set_defaults(func=cmd_dlq)

    args = ap.parse_args(raw_argv)
    args.config = config_path
    if getattr(args, "cmd", None) == "worker" and not args.queue_url:
        raise SystemExit("worker requires --queue-url or SWEETSPOT_SQS_QUEUE_URL")
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
