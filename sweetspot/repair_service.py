from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Iterator

from .aws_batch import ACTIVE_STATUSES, active_jobs, desired_worker_count, iso_now, queue_depth
from .batch_service import submit_worker_jobs, worker_overrides
from .enqueue_service import send_tasks_to_sqs, validate_tasks_for_enqueue
from .task_model import parse_allowed_s3_prefixes
from .worker import validate_worker_timing


JsonlReader = Callable[[Path], list[dict[str, Any]]]
JsonlIterator = Callable[[Path], Iterator[dict[str, Any]]]
SessionFactory = Callable[..., Any]
ListMatchingJobs = Callable[..., list[dict[str, Any]]]
JobTaskIdsFromLogs = Callable[..., dict[str, list[str]]]
RunScopedRegex = Callable[[str, str | None], str]
EnvAllowedPrefixes = Callable[[], list[str]]


def repair_plan_report(
    args: argparse.Namespace,
    *,
    read_jsonl: JsonlReader,
    iter_jsonl: JsonlIterator,
    session_factory: SessionFactory,
    list_matching_jobs: ListMatchingJobs,
    job_task_ids_from_logs: JobTaskIdsFromLogs,
) -> dict[str, Any]:
    tasks = read_jsonl(args.tasks_jsonl)
    expected_run_id = getattr(args, "run_id", None)
    if expected_run_id:
        wrong_run_tasks = [str(task.get("task_id") or "<missing-task-id>") for task in tasks if task.get("run_id") != expected_run_id]
        if wrong_run_tasks:
            raise SystemExit(f"repair RUN_ID requires every task to have run_id={expected_run_id!r}; mismatched task_ids: {wrong_run_tasks[:10]}")
    task_by_id = {str(task.get("task_id")): task for task in tasks if task.get("task_id")}
    if len(task_by_id) != len(tasks):
        raise SystemExit("repair-plan requires every task to have a unique non-empty task_id")
    missing_ids: set[str] = set()
    state_counts: Counter[str] = Counter()
    for rec in iter_jsonl(args.task_status_jsonl):
        task_id = str(rec.get("task_id") or "")
        if expected_run_id and rec.get("run_id") is not None and rec.get("run_id") != expected_run_id:
            raise SystemExit(f"repair RUN_ID requires task_status records to match run_id={expected_run_id!r}; mismatched task_id: {task_id or '<missing-task-id>'}")
        state = str(rec.get("state") or "unknown")
        state_counts[state] += 1
        if task_id and state != "done":
            missing_ids.add(task_id)
    session = session_factory(profile_name=args.profile, region_name=args.region)
    batch = session.client("batch", region_name=args.region)
    active_statuses = args.active_status or ACTIVE_STATUSES
    failed_statuses = args.failed_status or ["FAILED"]
    job_queues = args.job_queue or []
    active_jobs_found = list_matching_jobs(batch, job_queues=job_queues, statuses=active_statuses, name_regex=args.job_name_regex, max_jobs=args.max_jobs) if job_queues else []
    failed_jobs_found = list_matching_jobs(batch, job_queues=job_queues, statuses=failed_statuses, name_regex=args.job_name_regex, max_jobs=args.max_jobs) if job_queues else []
    active_task_ids_by_job = job_task_ids_from_logs(session, jobs=active_jobs_found, region=args.region, log_group=args.log_group, max_events=args.log_tail) if active_jobs_found else {}
    failed_task_ids_by_job = job_task_ids_from_logs(session, jobs=failed_jobs_found, region=args.region, log_group=args.log_group, max_events=args.log_tail) if failed_jobs_found else {}
    active_task_ids = {task_id for ids in active_task_ids_by_job.values() for task_id in ids}
    failed_task_ids = {task_id for ids in failed_task_ids_by_job.values() for task_id in ids}
    blocked_active = missing_ids & active_task_ids
    repair_ids = set(missing_ids)
    if not args.include_active:
        repair_ids -= blocked_active
    if args.only_known_failed:
        repair_ids &= failed_task_ids
    unknown_ids = sorted(repair_ids - set(task_by_id))
    if unknown_ids:
        raise SystemExit(f"task_status contains task_id values absent from tasks JSONL: {unknown_ids[:10]}")
    ordered_repair_ids = [str(task.get("task_id")) for task in tasks if str(task.get("task_id")) in repair_ids]
    args.out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    args.out_jsonl.write_text("".join(json.dumps(task_by_id[task_id], sort_keys=True) + "\n" for task_id in ordered_repair_ids))
    return {
        "schema": "sweetspot.repair_plan.v1",
        "checked_at": iso_now(),
        "tasks_jsonl": str(args.tasks_jsonl),
        "task_status_jsonl": str(args.task_status_jsonl),
        "out_jsonl": str(args.out_jsonl),
        "task_count": len(tasks),
        "state_counts": dict(state_counts),
        "missing_count": len(missing_ids),
        "active_job_count": len(active_jobs_found),
        "failed_job_count": len(failed_jobs_found),
        "active_task_count": len(active_task_ids),
        "failed_task_count": len(failed_task_ids),
        "blocked_active_count": len(blocked_active),
        "repair_task_count": len(ordered_repair_ids),
        "repair_task_ids": ordered_repair_ids[:1000],
        "repair_task_ids_truncated": len(ordered_repair_ids) > 1000,
        "blocked_active_task_ids": sorted(blocked_active)[:1000],
        "only_known_failed": bool(args.only_known_failed),
        "include_active": bool(args.include_active),
    }


def run_repair(
    args: argparse.Namespace,
    *,
    read_jsonl: JsonlReader,
    iter_jsonl: JsonlIterator,
    session_factory: SessionFactory,
    list_matching_jobs: ListMatchingJobs,
    job_task_ids_from_logs: JobTaskIdsFromLogs,
    run_scoped_job_name_regex: RunScopedRegex,
    env_allowed_s3_prefixes: EnvAllowedPrefixes,
) -> dict[str, Any]:
    artifact_dir = args.artifact_dir or Path("artifacts") / args.run_id / "repair"
    repair_jsonl = args.out_jsonl or artifact_dir / "repair_tasks.jsonl"
    if args.job_name_prefix and args.run_id not in args.job_name_prefix:
        raise SystemExit("repair --job-name-prefix must include RUN_ID; use repair-plan for advanced broad matching")
    repair_args = argparse.Namespace(
        active_status=args.active_status,
        failed_status=args.failed_status,
        include_active=args.include_active,
        job_name_regex=run_scoped_job_name_regex(args.run_id, args.job_name_prefix),
        job_queue=args.job_queue,
        log_group=args.log_group,
        log_tail=args.log_tail,
        max_jobs=args.max_jobs,
        only_known_failed=args.only_known_failed,
        out_jsonl=repair_jsonl,
        profile=args.profile,
        region=args.region,
        run_id=args.run_id,
        task_status_jsonl=args.task_status_jsonl,
        tasks_jsonl=args.tasks_jsonl,
    )
    repair_plan = repair_plan_report(
        repair_args,
        read_jsonl=read_jsonl,
        iter_jsonl=iter_jsonl,
        session_factory=session_factory,
        list_matching_jobs=list_matching_jobs,
        job_task_ids_from_logs=job_task_ids_from_logs,
    )
    repair_tasks = read_jsonl(repair_jsonl)
    allowed_s3_prefixes = parse_allowed_s3_prefixes(getattr(args, "allowed_s3_prefix", None) or env_allowed_s3_prefixes())
    validate_tasks_for_enqueue(repair_tasks, allowed_s3_prefixes=allowed_s3_prefixes)
    sent = 0
    submitted: list[dict[str, Any]] = []
    queue_depth_after: dict[str, int] | None = None
    active_matching_workers: list[dict[str, Any]] = []
    to_submit = 0
    raw_desired_workers = 0
    if args.apply:
        if not args.sqs_queue_url:
            raise SystemExit("repair --apply requires --sqs-queue-url")
        if args.submit_workers:
            if not args.batch_job_queue or not args.job_definition:
                raise SystemExit("repair --submit-workers requires --batch-job-queue and --job-definition")
            try:
                validate_worker_timing(visibility_timeout=args.visibility_timeout, heartbeat_seconds=args.heartbeat_seconds, task_timeout_seconds=args.task_timeout_seconds)
            except ValueError as exc:
                raise SystemExit(str(exc)) from exc
        session = session_factory(profile_name=args.profile, region_name=args.region)
        sqs = session.client("sqs", region_name=args.region)
        sent = send_tasks_to_sqs(sqs, queue_url=args.sqs_queue_url, tasks=repair_tasks)
        queue_depth_after = queue_depth(sqs, args.sqs_queue_url)
        raw_desired_workers = desired_worker_count(sent, args.messages_per_worker, args.min_workers, args.max_workers)
        if args.submit_workers:
            batch = session.client("batch", region_name=args.region)
            worker_prefix = args.worker_job_name_prefix or f"{args.run_id}-repair-worker"
            active_matching_workers = active_jobs(batch, args.batch_job_queue, worker_prefix) if args.subtract_active else []
            to_submit = max(0, raw_desired_workers - len(active_matching_workers)) if args.subtract_active else raw_desired_workers
            to_submit = min(to_submit, args.max_workers)
            overrides = worker_overrides(
                sqs_queue_url=args.sqs_queue_url,
                messages_per_worker=args.messages_per_worker,
                visibility_timeout=args.visibility_timeout,
                heartbeat_seconds=args.heartbeat_seconds,
                task_timeout_seconds=args.task_timeout_seconds,
                env=args.env or [],
                allowed_s3_prefixes=allowed_s3_prefixes,
                log_tail_bytes=args.log_tail_bytes,
                max_log_bytes=args.max_log_bytes,
                redact_regexes=args.redact_regex or [],
                allow_legacy_done_markers=bool(args.allow_legacy_done_markers),
                vcpus=args.vcpus,
                memory=args.memory,
            )
            if to_submit > 0:
                submitted = submit_worker_jobs(
                    batch,
                    count=to_submit,
                    job_name_prefix=worker_prefix,
                    batch_job_queue=args.batch_job_queue,
                    job_definition=args.job_definition,
                    overrides=overrides,
                    retry_attempts=args.retry_attempts,
                )
    return {
        "schema": "sweetspot.repair.v1",
        "checked_at": iso_now(),
        "run_id": args.run_id,
        "apply": bool(args.apply),
        "sqs_queue_url": args.sqs_queue_url,
        "repair_plan": repair_plan,
        "repair_task_count": repair_plan["repair_task_count"],
        "sent": sent,
        "submit_workers": bool(args.submit_workers),
        "raw_desired_workers": raw_desired_workers,
        "to_submit": to_submit,
        "submitted_count": len(submitted),
        "submitted": submitted,
        "active_matching_workers": len(active_matching_workers),
        "queue_depth_after": queue_depth_after,
    }
