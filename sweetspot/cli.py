from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Iterator

import boto3

from .aws_batch import ACTIVE_STATUSES, active_jobs, desired_worker_count, iso_now, queue_depth, utc_stamp
from .s3util import parse_s3_uri, s3_delete, s3_download_text, s3_exists, s3_join, s3_upload_file, s3_upload_text
from .task_model import default_done_s3, parse_allowed_s3_prefixes, task_hash, validate_task_model
from .worker import DEFAULT_LOG_TAIL_BYTES, DEFAULT_MAX_LOG_BYTES, SAFE_TASK_TIMEOUT_SECONDS, parse_redact_patterns, run_worker, validate_done_marker, validate_worker_timing


FINALIZER_DEFAULT_MAX_INLINE_OUTPUTS = 1000
FINALIZER_FUTURE_BUFFER_MULTIPLIER = 4


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


def _chunks(xs: list[dict[str, Any]], n: int) -> Iterable[list[dict[str, Any]]]:
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


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


def _validate_unique_task_ids(tasks: list[dict[str, Any]], *, context: str) -> None:
    seen: dict[str, int] = {}
    duplicates: list[str] = []
    for i, task in enumerate(tasks, start=1):
        task_id = str(task.get("task_id") or "")
        if task_id in seen:
            duplicates.append(f"{task_id!r} at lines {seen[task_id]} and {i}")
        elif task_id:
            seen[task_id] = i
    if duplicates:
        raise SystemExit(f"duplicate task_id values in {context}: {', '.join(duplicates[:10])}")


def _validate_tasks_for_enqueue(tasks: list[dict[str, Any]], *, allowed_s3_prefixes: list[str] | tuple[str, ...] | None) -> None:
    _validate_unique_task_ids(tasks, context="enqueue JSONL")
    for i, task in enumerate(tasks, start=1):
        try:
            validate_task_model(task, default_timeout_seconds=SAFE_TASK_TIMEOUT_SECONDS, max_timeout_seconds=SAFE_TASK_TIMEOUT_SECONDS, allowed_s3_prefixes=allowed_s3_prefixes)
        except ValueError as exc:
            raise SystemExit(f"invalid task at line {i}: {exc}") from exc


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


def cmd_enqueue_jsonl(args: argparse.Namespace) -> int:
    tasks = _read_jsonl(args.tasks_jsonl)
    if args.run_id:
        for t in tasks:
            t.setdefault("run_id", args.run_id)
    allowed_s3_prefixes = parse_allowed_s3_prefixes(getattr(args, "allowed_s3_prefix", None) or _env_allowed_s3_prefixes())
    _validate_tasks_for_enqueue(tasks, allowed_s3_prefixes=allowed_s3_prefixes)
    artifact_dir = args.artifact_dir or Path("artifacts") / (args.run_id or f"run-{utc_stamp()}")
    artifact_dir.mkdir(parents=True, exist_ok=True)
    tasks_out = artifact_dir / "tasks.jsonl"
    tasks_out.write_text("".join(json.dumps(t, sort_keys=True) + "\n" for t in tasks))

    sent = 0
    if args.submit:
        if not args.queue_url:
            raise SystemExit("--submit requires --queue-url")
        sqs = boto3.client("sqs")
        for batch in _chunks(tasks, 10):
            entries = [{"Id": str(i), "MessageBody": json.dumps(t, sort_keys=True)} for i, t in enumerate(batch)]
            resp = sqs.send_message_batch(QueueUrl=args.queue_url, Entries=entries)
            if resp.get("Failed"):
                raise RuntimeError(f"send_message_batch failed: {resp['Failed']}")
            sent += len(resp.get("Successful", []))
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


def _redact_env(env: list[dict[str, str]]) -> list[dict[str, str]]:
    return [{"name": str(x.get("name", "")), "value": "<redacted>"} for x in env]


def _status_counts(jobs: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for job in jobs:
        status = str(job.get("status", "UNKNOWN"))
        counts[status] = counts.get(status, 0) + 1
    return counts


def _worker_overrides(
    *,
    sqs_queue_url: str,
    messages_per_worker: int,
    visibility_timeout: int,
    heartbeat_seconds: int,
    task_timeout_seconds: float,
    env: list[dict[str, str]],
    allowed_s3_prefixes: list[str] | tuple[str, ...] | None,
    log_tail_bytes: int | None = None,
    max_log_bytes: int | None = None,
    redact_regexes: list[str] | tuple[str, ...] | None = None,
    allow_legacy_done_markers: bool = False,
    vcpus: int | None = None,
    memory: int | None = None,
) -> dict[str, Any]:
    base_env = [
        {"name": "SWEETSPOT_SQS_QUEUE_URL", "value": sqs_queue_url},
        {"name": "SWEETSPOT_MAX_MESSAGES", "value": str(messages_per_worker)},
        {"name": "SWEETSPOT_VISIBILITY_TIMEOUT", "value": str(visibility_timeout)},
        {"name": "SWEETSPOT_HEARTBEAT_SECONDS", "value": str(heartbeat_seconds)},
        {"name": "SWEETSPOT_TASK_TIMEOUT_SECONDS", "value": str(task_timeout_seconds)},
    ]
    normalized_prefixes = parse_allowed_s3_prefixes(allowed_s3_prefixes)
    if normalized_prefixes:
        base_env.append({"name": "SWEETSPOT_ALLOWED_S3_PREFIXES", "value": ",".join(normalized_prefixes)})
    if log_tail_bytes is not None:
        base_env.append({"name": "SWEETSPOT_LOG_TAIL_BYTES", "value": str(log_tail_bytes)})
    if max_log_bytes is not None:
        base_env.append({"name": "SWEETSPOT_MAX_LOG_BYTES", "value": str(max_log_bytes)})
    if redact_regexes:
        parse_redact_patterns(redact_regexes)
        base_env.append({"name": "SWEETSPOT_REDACT_REGEXES", "value": "\n".join(redact_regexes)})
    if allow_legacy_done_markers:
        base_env.append({"name": "SWEETSPOT_ALLOW_LEGACY_DONE_MARKERS", "value": "1"})
    base_env.extend(env or [])
    overrides: dict[str, Any] = {"environment": base_env}
    if vcpus is not None:
        overrides["vcpus"] = vcpus
    if memory is not None:
        overrides["memory"] = memory
    return overrides


def _submit_worker_jobs(
    batch,
    *,
    count: int,
    job_name_prefix: str,
    batch_job_queue: str,
    job_definition: str,
    overrides: dict[str, Any],
    retry_attempts: int | None,
) -> list[dict[str, Any]]:
    submitted = []
    stamp = utc_stamp()
    for i in range(count):
        job_name = f"{job_name_prefix}-{stamp}-{i:04d}"
        kwargs: dict[str, Any] = {
            "jobName": job_name,
            "jobQueue": batch_job_queue,
            "jobDefinition": job_definition,
            "containerOverrides": overrides,
        }
        if retry_attempts is not None:
            kwargs["retryStrategy"] = {"attempts": retry_attempts}
        resp = batch.submit_job(**kwargs)
        submitted.append({"jobName": job_name, "jobId": resp.get("jobId"), "jobArn": resp.get("jobArn")})
    return submitted


def cmd_submit_workers(args: argparse.Namespace) -> int:
    if not args.sqs_queue_url:
        raise SystemExit("missing --sqs-queue-url or SWEETSPOT_SQS_QUEUE_URL")
    try:
        validate_worker_timing(visibility_timeout=args.visibility_timeout, heartbeat_seconds=args.heartbeat_seconds, task_timeout_seconds=args.task_timeout_seconds)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    sqs = boto3.client("sqs")
    batch = boto3.client("batch")
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


def _terminal_job_counts(batch, job_queue: str, job_name_prefix: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for status in ("SUCCEEDED", "FAILED"):
        total = 0
        paginator = batch.get_paginator("list_jobs")
        for page in paginator.paginate(jobQueue=job_queue, jobStatus=status):
            total += sum(1 for j in page.get("jobSummaryList", []) if not job_name_prefix or str(j.get("jobName", "")).startswith(job_name_prefix))
        counts[status] = total
    return counts


def _supervisor_desired_workers(*, backlog: int, messages_per_worker: int, target_active_workers: int, max_active_workers: int, keep_full_pool: bool) -> int:
    if backlog <= 0 and not keep_full_pool:
        return 0
    desired = target_active_workers if keep_full_pool else min(target_active_workers, desired_worker_count(backlog, messages_per_worker, 0, target_active_workers))
    return min(desired, max_active_workers)


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


def _iter_s3_jsonl(s3, uri: str) -> Iterator[dict[str, Any]]:
    bucket, key = parse_s3_uri(uri)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    if hasattr(body, "iter_lines"):
        lines = body.iter_lines()
    else:
        lines = body.read().splitlines()
    for line_no, line in enumerate(lines, start=1):
        if isinstance(line, bytes):
            line = line.decode("utf-8")
        line = str(line).strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSONL at {uri}:{line_no}") from exc
        if not isinstance(obj, dict):
            raise ValueError(f"task at {uri}:{line_no} is not an object")
        yield obj


def _iter_tasks_for_finalizer(args: argparse.Namespace, s3) -> Iterator[dict[str, Any]]:
    if args.tasks_jsonl:
        yield from _iter_jsonl(args.tasks_jsonl)
        return
    tasks_s3 = args.tasks_s3 or s3_join(args.output_prefix, "manifests", "tasks.jsonl")
    yield from _iter_s3_jsonl(s3, tasks_s3)


class _S3ExistenceIndex:
    def __init__(self, s3, prefixes: Iterable[str]) -> None:
        self.s3 = s3
        self.prefixes: list[tuple[str, str]] = []
        self.keys_by_bucket: dict[str, set[str]] = {}
        for uri in prefixes:
            if not uri:
                continue
            bucket, key = parse_s3_uri(uri)
            key = key.rstrip("/") + "/" if key else ""
            self.prefixes.append((bucket, key))

    def load(self) -> None:
        for bucket, prefix in self.prefixes:
            keys = self.keys_by_bucket.setdefault(bucket, set())
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj.get("Key")
                    if key is not None:
                        keys.add(str(key))

    def indexed_prefixes(self) -> list[str]:
        return [f"s3://{bucket}/{prefix}" for bucket, prefix in self.prefixes]

    def exists(self, uri: str) -> bool:
        bucket, key = parse_s3_uri(uri)
        for indexed_bucket, indexed_prefix in self.prefixes:
            if bucket == indexed_bucket and key.startswith(indexed_prefix):
                return key in self.keys_by_bucket.get(bucket, set())
        return s3_exists(self.s3, uri)


def _finalizer_existence_index(args: argparse.Namespace, s3) -> _S3ExistenceIndex | None:
    prefixes = list(getattr(args, "preload_s3_prefix", None) or [])
    if getattr(args, "use_listing_index", False):
        prefixes.extend(
            [
                s3_join(args.output_prefix, "done"),
                s3_join(args.output_prefix, "shards"),
                s3_join(args.output_prefix, "summaries"),
            ]
        )
    if not prefixes:
        return None
    index = _S3ExistenceIndex(s3, prefixes)
    index.load()
    return index


def _s3_exists_indexed(s3, uri: str, existence_index: _S3ExistenceIndex | None) -> bool:
    return existence_index.exists(uri) if existence_index else s3_exists(s3, uri)


def _repair_done_marker_candidates(s3, canonical_done_s3: str) -> Iterator[str]:
    bucket, key = parse_s3_uri(canonical_done_s3)
    prefix = key + ".repair-"
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            candidate_key = obj.get("Key")
            if candidate_key:
                yield f"s3://{bucket}/{candidate_key}"


def _repair_task_candidates_for_marker_validation(task: dict[str, Any], repair_done_s3: str) -> Iterator[dict[str, Any]]:
    """Yield task payload variants that could have produced a repair marker.

    Current repair tasks include sweetspot_repair_reason, which participates in
    the full task hash. Older repair tasks did not, so validate both forms.
    """
    base = dict(task)
    base["done_s3"] = repair_done_s3
    yield base
    for reason in ("invalid_done_marker", "missing_output", "output_without_done", "incomplete"):
        with_reason = dict(base)
        with_reason["sweetspot_repair_reason"] = reason
        yield with_reason


def _valid_repair_done_marker(s3, task: dict[str, Any], canonical_done_s3: str, *, allow_legacy_done_markers: bool = False) -> tuple[str, dict[str, Any]] | None:
    for repair_done_s3 in _repair_done_marker_candidates(s3, canonical_done_s3):
        repair_marker = _done_marker_for_task(s3, task, repair_done_s3, None)
        if repair_marker is None or repair_marker.get("_sweetspot_marker_parse_error"):
            continue
        for repair_task in _repair_task_candidates_for_marker_validation(task, repair_done_s3):
            try:
                validate_done_marker(s3, repair_task, repair_marker, task_hash(repair_task), allow_legacy_done_markers=allow_legacy_done_markers)
            except ValueError:
                continue
            return repair_done_s3, repair_marker
    return None


def _read_tasks_for_finalizer(args: argparse.Namespace, s3) -> list[dict[str, Any]]:
    return list(_iter_tasks_for_finalizer(args, s3))


def _done_marker_for_task(s3, task: dict[str, Any], done_s3: str, existence_index: _S3ExistenceIndex | None = None) -> dict[str, Any] | None:
    if not _s3_exists_indexed(s3, done_s3, existence_index):
        return None
    try:
        marker = json.loads(s3_download_text(s3, done_s3))
    except json.JSONDecodeError as exc:
        return {"_sweetspot_marker_parse_error": f"done marker is not valid JSON: {exc}"}
    if not isinstance(marker, dict):
        return {"_sweetspot_marker_parse_error": f"done marker is not an object: {done_s3}"}
    return marker


def _check_task(s3, task: dict[str, Any], existence_index: _S3ExistenceIndex | None = None, *, allow_legacy_done_markers: bool = False) -> dict[str, Any]:
    logical_output_s3 = str(task.get("output_s3") or "")
    summary_s3 = str(task.get("summary_s3") or "")
    done_s3 = default_done_s3(task)
    marker = _done_marker_for_task(s3, task, done_s3, existence_index)
    marker_validation_error = None
    if marker is not None:
        marker_validation_error = marker.get("_sweetspot_marker_parse_error")
        if not marker_validation_error:
            try:
                # validate_done_marker verifies schema/run/task/hash and, for
                # v2, HEADs the immutable attempt output to check size/SHA
                # metadata. Validation failures are status/repair inputs, not
                # finalizer crashes.
                validate_done_marker(s3, task, marker, task_hash(task), allow_legacy_done_markers=allow_legacy_done_markers)
            except ValueError as exc:
                marker_validation_error = str(exc)
    if marker is not None and marker_validation_error:
        repair_candidate = _valid_repair_done_marker(s3, task, done_s3, allow_legacy_done_markers=allow_legacy_done_markers)
        if repair_candidate is not None:
            done_s3, marker = repair_candidate
            marker_validation_error = None
    done_exists = marker is not None
    marker_valid = marker is not None and marker_validation_error is None
    output_s3 = logical_output_s3
    output_exists = False if marker_validation_error else (_s3_exists_indexed(s3, logical_output_s3, existence_index) if logical_output_s3 else False)
    summary_exists = _s3_exists_indexed(s3, summary_s3, existence_index) if summary_s3 else False
    if marker and isinstance(marker.get("output"), dict):
        output_s3 = str(marker["output"].get("uri") or logical_output_s3)
        # Keep an explicit existence check here even though v2 validation
        # already verified metadata, so the status record reflects current S3
        # availability and listing-index decisions.
        output_exists = False if marker_validation_error else _s3_exists_indexed(s3, output_s3, existence_index)
    if marker and marker.get("attempt_summary_s3"):
        summary_s3 = str(marker.get("attempt_summary_s3"))
        summary_exists = _s3_exists_indexed(s3, summary_s3, existence_index)
    state = "done" if marker_valid else ("invalid_done_marker" if done_exists else "incomplete")
    if done_exists and logical_output_s3 and not output_exists:
        state = "missing_output"
    elif output_exists and not marker_valid:
        state = "output_without_done"
    return {
        "task_id": task.get("task_id"),
        "output_s3": output_s3,
        "logical_output_s3": logical_output_s3,
        "summary_s3": summary_s3,
        "done_s3": done_s3,
        "done_exists": done_exists,
        "marker_valid": marker_valid,
        "output_exists": output_exists,
        "summary_exists": summary_exists,
        "state": state,
        "marker_validation_error": marker_validation_error,
    }


def _repair_task_for_record(task: dict[str, Any], record: dict[str, Any], repair_suffix: str) -> dict[str, Any]:
    repair = dict(task)
    if record["done_exists"] and (record["state"] == "missing_output" or not record.get("marker_valid", False)):
        # Existing invalid/incomplete done markers make normal workers collide
        # on the canonical marker. Keep the original output_s3 so missing
        # objects are regenerated, but write the repair completion marker
        # elsewhere; the next finalize validates the original output location.
        repair["done_s3"] = str(record["done_s3"]) + f".repair-{repair_suffix}"
    return repair


def cmd_finalize(args: argparse.Namespace) -> int:
    import concurrent.futures as cf
    import sys

    if args.publish_ready and not args.upload:
        raise SystemExit("--publish-ready requires --upload")
    args.ready_key = str(args.ready_key).strip("/")
    reserved_ready_keys = {"manifests/final_manifest.json", "manifests/repair_tasks.jsonl", "manifests/task_status.jsonl", "manifests/outputs.jsonl"}
    if args.publish_ready and (not args.ready_key or args.ready_key in reserved_ready_keys):
        raise SystemExit("--ready-key must not be empty or collide with SweetSpot manifest paths")
    if args.workers <= 0:
        raise SystemExit("--workers must be positive")
    s3 = boto3.client("s3")
    existence_index = _finalizer_existence_index(args, s3)
    artifact_dir = args.artifact_dir or Path("artifacts") / args.run_id / "finalizer"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    final_path = artifact_dir / "final_manifest.json"
    repair_path = args.write_repair_jsonl or artifact_dir / "repair_tasks.jsonl"
    status_path = artifact_dir / "task_status.jsonl"
    outputs_path = artifact_dir / "outputs.jsonl"
    final_s3 = s3_join(args.output_prefix, "manifests", "final_manifest.json")
    repair_s3 = s3_join(args.output_prefix, "manifests", "repair_tasks.jsonl")
    status_s3 = s3_join(args.output_prefix, "manifests", "task_status.jsonl")
    outputs_s3 = s3_join(args.output_prefix, "manifests", "outputs.jsonl")
    ready_s3 = s3_join(args.output_prefix, args.ready_key)

    counts: Counter[str] = Counter()
    seen_task_ids: dict[str, int] = {}
    checked = submitted = 0
    max_inline_outputs = max(0, int(getattr(args, "max_inline_outputs", FINALIZER_DEFAULT_MAX_INLINE_OUTPUTS)))
    inline_outputs: list[str] = []
    missing_task_ids: list[Any] = []
    output_without_done_task_ids: list[Any] = []
    missing_output_task_ids: list[Any] = []
    repair_suffix = str(time.time_ns())
    pending: dict[cf.Future, tuple[int, dict[str, Any]]] = {}
    ready_records: dict[int, tuple[dict[str, Any], dict[str, Any]]] = {}
    next_to_emit = 0
    buffer_limit = max(1, args.workers * FINALIZER_FUTURE_BUFFER_MULTIPLIER)

    def remember(xs: list[Any], value: Any) -> None:
        if len(xs) < 1000:
            xs.append(value)

    def process_record(task: dict[str, Any], record: dict[str, Any], status_f, repair_f, outputs_f) -> None:
        nonlocal checked
        checked += 1
        counts["task"] += 1
        if record["done_exists"]:
            counts["done_marker"] += 1
        if record.get("marker_validation_error"):
            counts["invalid_marker"] += 1
        if record["marker_valid"]:
            counts["done"] += 1
        if record["output_exists"]:
            counts["output"] += 1
        if record["summary_exists"]:
            counts["summary"] += 1
        is_missing_done = not record["marker_valid"]
        is_missing_output = bool(record["output_s3"] and not record["output_exists"])
        is_missing = is_missing_done or is_missing_output
        if record["state"] == "output_without_done":
            counts["output_without_done"] += 1
            remember(output_without_done_task_ids, record["task_id"])
        if is_missing_done:
            counts["missing_done"] += 1
        if is_missing_output:
            counts["missing_output"] += 1
            remember(missing_output_task_ids, record["task_id"])
        if is_missing:
            counts["missing"] += 1
            remember(missing_task_ids, record["task_id"])
            repair = _repair_task_for_record(task, record, repair_suffix)
            repair_f.write(json.dumps(repair, sort_keys=True) + "\n")
        if record["marker_valid"] and (not record["output_s3"] or record["output_exists"]):
            output_uri = record["output_s3"]
            counts["output_manifest"] += 1
            if len(inline_outputs) < max_inline_outputs:
                inline_outputs.append(output_uri)
            outputs_f.write(json.dumps({"task_id": record["task_id"], "output_s3": output_uri}, sort_keys=True) + "\n")
        status_f.write(json.dumps(record, sort_keys=True) + "\n")
        if args.progress_interval and checked % args.progress_interval == 0:
            print(f"sweetspot finalize progress: checked={checked}", file=sys.stderr)

    def drain(done_futures: set[cf.Future], status_f, repair_f, outputs_f) -> None:
        nonlocal next_to_emit
        for fut in done_futures:
            index, task = pending.pop(fut)
            ready_records[index] = (task, fut.result())
        while next_to_emit in ready_records:
            task, record = ready_records.pop(next_to_emit)
            process_record(task, record, status_f, repair_f, outputs_f)
            next_to_emit += 1

    with (
        status_path.open("w", encoding="utf-8") as status_f,
        repair_path.open("w", encoding="utf-8") as repair_f,
        outputs_path.open("w", encoding="utf-8") as outputs_f,
        cf.ThreadPoolExecutor(max_workers=args.workers) as ex,
    ):
        for line_no, task in enumerate(_iter_tasks_for_finalizer(args, s3), start=1):
            task_id = str(task.get("task_id") or "")
            if task_id in seen_task_ids:
                raise SystemExit(f"duplicate task_id values in finalizer tasks: {task_id!r} at lines {seen_task_ids[task_id]} and {line_no}")
            if task_id:
                seen_task_ids[task_id] = line_no
            pending[ex.submit(_check_task, s3, task, existence_index, allow_legacy_done_markers=bool(getattr(args, "allow_legacy_done_markers", False)))] = (submitted, task)
            submitted += 1
            while len(pending) + len(ready_records) >= buffer_limit and pending:
                done_futures, _ = cf.wait(pending.keys(), return_when=cf.FIRST_COMPLETED)
                drain(done_futures, status_f, repair_f, outputs_f)
        while pending:
            done_futures, _ = cf.wait(pending.keys(), return_when=cf.FIRST_COMPLETED)
            drain(done_futures, status_f, repair_f, outputs_f)
    if args.progress_interval and checked and checked % args.progress_interval != 0:
        print(f"sweetspot finalize progress: checked={checked}", file=sys.stderr)

    final_manifest = {
        "schema": "sweetspot.final_manifest.v1",
        "run_id": args.run_id,
        "finalized_at": iso_now(),
        "output_prefix": args.output_prefix.rstrip("/"),
        "task_count": counts["task"],
        "done_count": counts["done"],
        "done_marker_count": counts["done_marker"],
        "invalid_marker_count": counts["invalid_marker"],
        "output_count": counts["output"],
        "summary_count": counts["summary"],
        "missing_count": counts["missing"],
        "missing_done_count": counts["missing_done"],
        "output_without_done_count": counts["output_without_done"],
        "missing_output_count": counts["missing_output"],
        "complete": counts["missing"] == 0,
        "missing_task_ids": missing_task_ids,
        "output_without_done_task_ids": output_without_done_task_ids,
        "missing_output_task_ids": missing_output_task_ids,
        "outputs": inline_outputs,
        "outputs_truncated": counts["output_manifest"] > len(inline_outputs),
        "outputs_manifest": str(outputs_path),
        "outputs_manifest_s3": outputs_s3 if args.upload else None,
        "task_status": str(status_path),
        "task_status_s3": status_s3 if args.upload else None,
        "repair_task_count": counts["missing"],
        "final_manifest_s3": final_s3 if args.upload else None,
        "repair_tasks_s3": repair_s3 if args.upload and counts["missing"] else None,
        "ready_s3": ready_s3 if args.publish_ready else None,
        "existence_index_prefixes": existence_index.indexed_prefixes() if existence_index else [],
    }
    if submitted != checked:
        raise RuntimeError(f"finalizer internal error: submitted {submitted}, checked {checked}")
    if args.publish_ready and not final_manifest["complete"] and not args.allow_incomplete_ready:
        final_manifest["ready_s3"] = None

    final_path.write_text(json.dumps(final_manifest, indent=2, sort_keys=True) + "\n")

    if args.upload:
        if args.publish_ready:
            s3_delete(s3, ready_s3)
        s3_upload_text(s3, json.dumps(final_manifest, indent=2, sort_keys=True) + "\n", final_s3)
        s3_upload_file(s3, status_path, status_s3, "application/jsonl")
        s3_upload_file(s3, outputs_path, outputs_s3, "application/jsonl")
        if counts["missing"]:
            s3_upload_file(s3, repair_path, repair_s3, "application/jsonl")

    if args.publish_ready and not final_manifest["complete"] and not args.allow_incomplete_ready:
        print(
            json.dumps(
                {
                    **{k: final_manifest[k] for k in ["schema", "run_id", "task_count", "done_count", "output_count", "summary_count", "missing_count", "missing_output_count", "output_without_done_count", "complete"]},
                    "final_manifest": str(final_path),
                    "repair_tasks": str(repair_path),
                    "task_status": str(status_path),
                    "outputs_manifest": str(outputs_path),
                    "final_manifest_s3": final_s3 if args.upload else None,
                    "ready_s3": None,
                    "refused_ready": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2

    if args.upload and args.publish_ready:
        ready = {"schema": "sweetspot.ready_marker.v1", "run_id": args.run_id, "ready_at": iso_now(), "final_manifest_s3": final_s3, "complete": final_manifest["complete"]}
        s3_upload_text(s3, json.dumps(ready, indent=2, sort_keys=True) + "\n", ready_s3)
    print(
        json.dumps(
            {
                **{k: final_manifest[k] for k in ["schema", "run_id", "task_count", "done_count", "output_count", "summary_count", "missing_count", "missing_output_count", "output_without_done_count", "complete"]},
                "final_manifest": str(final_path),
                "repair_tasks": str(repair_path),
                "task_status": str(status_path),
                "outputs_manifest": str(outputs_path),
                "final_manifest_s3": final_s3 if args.upload else None,
                "ready_s3": ready_s3 if args.publish_ready and args.upload else None,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 2 if args.require_complete and not final_manifest["complete"] else 0


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
    print(json.dumps({"schema": "sweetspot.jobs.v1", "checked_at": iso_now(), "job_queue": args.job_queue, "statuses": statuses, "count": len(rows), "jobs": rows}, indent=2, sort_keys=True))
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
    print(
        json.dumps(
            {
                "schema": "sweetspot.logs.v1",
                "checked_at": iso_now(),
                "log_group": log_group,
                "log_stream": stream,
                "count": len(events),
                "nextForwardToken": resp.get("nextForwardToken"),
                "events": events[-args.tail :] if args.tail else events,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def cmd_watch_job(args: argparse.Namespace) -> int:
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    batch = session.client("batch", region_name=args.region)
    deadline = time.time() + args.max_seconds if args.max_seconds else None
    last_report = None
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
                raise RuntimeError(f"S3 DeleteObjects reported {len(errors)} errors; first={errors[0]!r}")
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
        raise RuntimeError(f"queue has no QueueArn attribute: {queue_url}")
    return str(arn)


def cmd_dlq(args: argparse.Namespace) -> int:
    if args.apply and not args.queue_url and not getattr(args, "native_redrive", False):
        raise SystemExit("--apply requires --queue-url")
    sqs = boto3.client("sqs")
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
        print(
            json.dumps(
                {
                    "schema": "sweetspot.dlq_redrive_summary.v1",
                    "checked_at": iso_now(),
                    "native_redrive": True,
                    "source_arn": kwargs["SourceArn"],
                    "destination_arn": kwargs.get("DestinationArn"),
                    "task_handle": resp.get("TaskHandle"),
                    "max_messages_per_second": kwargs.get("MaxNumberOfMessagesPerSecond"),
                },
                indent=2,
                sort_keys=True,
            )
        )
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
    print(
        json.dumps(
            {
                "schema": "sweetspot.dlq_summary.v1",
                "checked_at": iso_now(),
                "apply": bool(args.apply),
                "scanned": scanned,
                "matched": matched,
                "moved": moved,
                "by_run": dict(by_run.most_common()),
                "by_schema": dict(by_schema.most_common()),
                "examples": examples,
            },
            indent=2,
            sort_keys=True,
        )
    )
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
                raise RuntimeError(f"job queue not found: {args.job_queue}")
            queue = queues[0]
            if queue.get("state") != "ENABLED" or queue.get("status") not in {"VALID", None}:
                raise RuntimeError(f"job queue not ready: state={queue.get('state')} status={queue.get('status')}")
            return {"jobQueueName": queue.get("jobQueueName"), "state": queue.get("state"), "status": queue.get("status"), "computeEnvironmentOrder": queue.get("computeEnvironmentOrder")}

        checks.append(_doctor_check("batch_job_queue", check_job_queue))

    if args.job_definition:

        def check_job_definition() -> dict[str, Any]:
            nonlocal discovered_log_group
            batch = session.client("batch", region_name=args.region)
            defs = batch.describe_job_definitions(jobDefinitions=[args.job_definition], status="ACTIVE").get("jobDefinitions", [])
            if not defs:
                raise RuntimeError(f"active job definition not found: {args.job_definition}")
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
                raise RuntimeError(f"no AWS/Batch metrics found for JobQueue={job_queue_name}; validate dimensions after jobs have emitted data or use worker logs/EventBridge alarms")
            return {"job_queue": job_queue_name, "metrics_found": found}

        checks.append(_doctor_check("batch_metrics", check_batch_metrics))

    if discovered_log_group:

        def check_logs() -> dict[str, Any]:
            logs = session.client("logs", region_name=args.region)
            groups = logs.describe_log_groups(logGroupNamePrefix=discovered_log_group).get("logGroups", [])
            match = next((g for g in groups if g.get("logGroupName") == discovered_log_group), None)
            if not match:
                raise RuntimeError(f"log group not found: {discovered_log_group}")
            return {"log_group": discovered_log_group, "retentionInDays": match.get("retentionInDays"), "storedBytes": match.get("storedBytes")}

        checks.append(_doctor_check("cloudwatch_log_group", check_logs))

    checks.append(
        {"name": "service_quotas", "ok": None, "details": {"status": "not_checked", "reason": "AWS Batch quota codes vary by account/Region; verify max vCPUs and queue limits in Service Quotas for production runs."}}
    )
    ok = all(c.get("ok") is not False for c in checks)
    print(json.dumps({"schema": "sweetspot.doctor.v1", "checked_at": iso_now(), "ok": ok, "region": args.region, "checks": checks}, indent=2, sort_keys=True))
    return 0 if ok else 2


def main() -> int:
    ap = argparse.ArgumentParser(prog="sweetspot")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("worker", help="Run an SQS worker inside AWS Batch")
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
        )
    )

    p = sub.add_parser("enqueue-jsonl")
    p.add_argument("--queue-url", default=os.environ.get("SWEETSPOT_SQS_QUEUE_URL", ""))
    p.add_argument("--tasks-jsonl", type=Path, required=True)
    p.add_argument("--run-id")
    p.add_argument("--artifact-dir", type=Path)
    p.add_argument("--allowed-s3-prefix", action="append", default=[], help="Reject tasks containing S3 URIs outside this prefix; repeatable. Defaults to SWEETSPOT_ALLOWED_S3_PREFIXES when unset.")
    p.add_argument("--submit", action="store_true")
    p.set_defaults(func=cmd_enqueue_jsonl)

    p = sub.add_parser("derive-canary")
    p.add_argument("--tasks-jsonl", type=Path, required=True)
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--run-id", default=f"canary-{utc_stamp()}")
    p.add_argument("--selected-indices", default="auto", help="auto or comma/range list, e.g. 0,5,10-12")
    p.add_argument("--task-count", type=int, default=4)
    p.add_argument("--rewrite-run-id", action="store_true", help="rewrite selected tasks to use --run-id")
    p.add_argument("--include-dlq-probe", action="store_true")
    p.add_argument("--dlq-probe-prefix", help="S3 prefix for the intentional DLQ probe done marker; required when selected tasks do not already have a done-marker prefix")
    p.set_defaults(func=cmd_derive_canary)

    p = sub.add_parser("submit-workers")
    p.add_argument("--sqs-queue-url", default=os.environ.get("SWEETSPOT_SQS_QUEUE_URL", ""))
    p.add_argument("--batch-job-queue", required=True)
    p.add_argument("--job-definition", required=True)
    p.add_argument("--job-name-prefix", default="sweetspot-worker")
    p.add_argument("--messages-per-worker", type=int, default=1)
    p.add_argument("--max-workers", type=int, default=64)
    p.add_argument("--min-workers", type=int, default=0)
    p.add_argument("--subtract-active", action="store_true")
    p.add_argument("--include-not-visible", action="store_true")
    p.add_argument("--vcpus", type=int)
    p.add_argument("--memory", type=int)
    p.add_argument("--visibility-timeout", type=int, default=1800)
    p.add_argument("--heartbeat-seconds", type=int, default=300)
    p.add_argument("--task-timeout-seconds", type=float, default=SAFE_TASK_TIMEOUT_SECONDS, help="Default per-task command timeout to pass to workers")
    p.add_argument("--retry-attempts", type=int)
    p.add_argument("--env", action="append", type=_parse_env_pair, default=[])
    p.add_argument("--allowed-s3-prefix", action="append", default=[], help="Pass SWEETSPOT_ALLOWED_S3_PREFIXES to workers; repeatable.")
    p.add_argument("--log-tail-bytes", type=int, default=DEFAULT_LOG_TAIL_BYTES)
    p.add_argument("--max-log-bytes", type=int, default=DEFAULT_MAX_LOG_BYTES)
    p.add_argument("--redact-regex", action="append", default=[], help="Regex to redact from worker task logs; repeatable.")
    p.add_argument("--allow-legacy-done-markers", action="store_true", help="Migration mode: pass SWEETSPOT_ALLOW_LEGACY_DONE_MARKERS=1 to submitted workers")
    p.add_argument("--submit", action="store_true")
    p.set_defaults(func=cmd_submit_workers)

    p = sub.add_parser("supervise-workers")
    p.add_argument("--run-id")
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--sqs-queue-url", default=os.environ.get("SWEETSPOT_SQS_QUEUE_URL", ""))
    p.add_argument("--dlq-url")
    p.add_argument("--stop-on-dlq", action="store_true")
    p.add_argument("--fail-on-stop", action="store_true")
    p.add_argument("--batch-job-queue", required=True)
    p.add_argument("--job-definition", required=True)
    p.add_argument("--job-name-prefix", default="sweetspot-worker")
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
    p.add_argument("--vcpus", type=int)
    p.add_argument("--memory", type=int)
    p.add_argument("--visibility-timeout", type=int, default=1800)
    p.add_argument("--heartbeat-seconds", type=int, default=300)
    p.add_argument("--task-timeout-seconds", type=float, default=SAFE_TASK_TIMEOUT_SECONDS, help="Default per-task command timeout to pass to workers")
    p.add_argument("--retry-attempts", type=int)
    p.add_argument("--env", action="append", type=_parse_env_pair, default=[])
    p.add_argument("--allowed-s3-prefix", action="append", default=[], help="Pass SWEETSPOT_ALLOWED_S3_PREFIXES to workers; repeatable.")
    p.add_argument("--log-tail-bytes", type=int, default=DEFAULT_LOG_TAIL_BYTES)
    p.add_argument("--max-log-bytes", type=int, default=DEFAULT_MAX_LOG_BYTES)
    p.add_argument("--redact-regex", action="append", default=[], help="Regex to redact from worker task logs; repeatable.")
    p.add_argument("--allow-legacy-done-markers", action="store_true", help="Migration mode: pass SWEETSPOT_ALLOW_LEGACY_DONE_MARKERS=1 to supervised workers")
    p.add_argument("--submit", action="store_true")
    p.set_defaults(func=cmd_supervise_workers)

    p = sub.add_parser("finalize")
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
    p.add_argument("--publish-ready", action="store_true")
    p.add_argument("--ready-key", default="READY")
    p.add_argument("--allow-incomplete-ready", action="store_true", help="unsafe: publish READY even when tasks are incomplete")
    p.add_argument("--allow-legacy-done-markers", action="store_true", help="Migration mode: accept legacy v1 done markers without task hashes/attempt checks")
    p.add_argument("--require-complete", action="store_true")
    p.set_defaults(func=cmd_finalize)

    p = sub.add_parser("jobs")
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--job-queue", required=True)
    p.add_argument("--status", action="append", choices=list(ACTIVE_STATUSES) + ["SUCCEEDED", "FAILED"], help="repeatable; default active statuses")
    p.add_argument("--name-regex")
    p.add_argument("--max-jobs", type=int, default=1000)
    p.set_defaults(func=cmd_jobs)

    p = sub.add_parser("describe-job")
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--job-id", required=True)
    p.set_defaults(func=cmd_describe_job)

    p = sub.add_parser("logs")
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--job-id")
    p.add_argument("--log-group", help="CloudWatch log group; with --job-id, defaults to the job definition log group when discoverable")
    p.add_argument("--log-stream")
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--tail", type=int, default=0)
    p.add_argument("--filter-regex")
    p.add_argument("--next-token")
    p.add_argument("--start-from-head", action="store_true")
    p.set_defaults(func=cmd_logs)

    p = sub.add_parser("watch-job")
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--job-id", required=True)
    p.add_argument("--interval-seconds", type=float, default=30.0)
    p.add_argument("--max-seconds", type=float, default=0.0)
    p.set_defaults(func=cmd_watch_job)

    p = sub.add_parser("s3-delete-prefix")
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

    p = sub.add_parser("doctor", help="Validate common AWS/SQS/S3/Batch/CloudWatch operator prerequisites")
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--queue-url", default=os.environ.get("SWEETSPOT_SQS_QUEUE_URL", ""))
    p.add_argument("--dlq-url")
    p.add_argument("--job-queue")
    p.add_argument("--job-definition")
    p.add_argument("--log-group")
    p.add_argument("--validate-batch-metrics", action="store_true", help="Check CloudWatch AWS/Batch JobQueue metric dimensions for this account/Region")
    p.add_argument("--s3-prefix", action="append", default=[], help="S3 prefix to validate with ListBucket; repeatable")
    p.add_argument("--write-probe", action="store_true", help="Also write/delete a tiny object under each --s3-prefix")
    p.add_argument("--visibility-timeout", type=int, default=1800)
    p.add_argument("--heartbeat-seconds", type=int, default=300)
    p.add_argument("--task-timeout-seconds", type=float, default=SAFE_TASK_TIMEOUT_SECONDS)
    p.add_argument("--redact-regex", action="append", default=[], help="Validate worker log redaction regexes")
    p.set_defaults(func=cmd_doctor)

    p = sub.add_parser("dlq")
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
    p.set_defaults(func=cmd_dlq)

    args = ap.parse_args()
    if getattr(args, "cmd", None) == "worker" and not args.queue_url:
        raise SystemExit("worker requires --queue-url or SWEETSPOT_SQS_QUEUE_URL")
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
