from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

import boto3

from .aws_batch import active_jobs, desired_worker_count, iso_now, queue_depth, utc_stamp
from .s3util import s3_download_text, s3_exists, s3_join, s3_upload_text
from .worker import default_done_s3, run_worker


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    out = []
    for line_no, line in enumerate(path.read_text().splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSONL at {path}:{line_no}") from exc
        if not isinstance(obj, dict):
            raise ValueError(f"task at {path}:{line_no} is not an object")
        out.append(obj)
    return out


def _chunks(xs: list[dict[str, Any]], n: int) -> Iterable[list[dict[str, Any]]]:
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


def cmd_enqueue_jsonl(args: argparse.Namespace) -> int:
    tasks = _read_jsonl(args.tasks_jsonl)
    if args.run_id:
        for t in tasks:
            t.setdefault("run_id", args.run_id)
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
    print(json.dumps({
        "schema": "spotbatch.enqueue_summary.v1",
        "checked_at": iso_now(),
        "queue_url": args.queue_url,
        "task_count": len(tasks),
        "sent": sent,
        "submitted": bool(args.submit),
        "tasks_jsonl": str(tasks_out),
    }, indent=2, sort_keys=True))
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
    return dict(Counter(str(j.get("status", "UNKNOWN")) for j in jobs))


def _worker_overrides(
    *,
    sqs_queue_url: str,
    messages_per_worker: int,
    visibility_timeout: int,
    heartbeat_seconds: int,
    task_timeout_seconds: float,
    env: list[dict[str, str]],
    vcpus: int | None,
    memory: int | None,
) -> dict[str, Any]:
    base_env = [
        {"name": "SPOTBATCH_SQS_QUEUE_URL", "value": sqs_queue_url},
        {"name": "SPOTBATCH_MAX_MESSAGES", "value": str(messages_per_worker)},
        {"name": "SPOTBATCH_VISIBILITY_TIMEOUT", "value": str(visibility_timeout)},
        {"name": "SPOTBATCH_HEARTBEAT_SECONDS", "value": str(heartbeat_seconds)},
        {"name": "SPOTBATCH_TASK_TIMEOUT_SECONDS", "value": str(task_timeout_seconds)},
    ]
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
        raise SystemExit("missing --sqs-queue-url or SPOTBATCH_SQS_QUEUE_URL")
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

    print(json.dumps({
        "schema": "spotbatch.worker_submitter_summary.v1",
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
    }, indent=2, sort_keys=True))
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
        raise SystemExit("missing --sqs-queue-url or SPOTBATCH_SQS_QUEUE_URL")
    if args.stop_on_dlq and not args.dlq_url:
        raise SystemExit("--stop-on-dlq requires --dlq-url")
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    sqs = session.client("sqs", region_name=args.region)
    batch = session.client("batch", region_name=args.region)
    artifact_dir = args.artifact_dir or Path("artifacts") / (args.run_id or f"supervise-{utc_stamp()}")
    artifact_dir.mkdir(parents=True, exist_ok=True)
    status_path = artifact_dir / "supervisor_status.jsonl"
    summary_path = artifact_dir / "supervisor_summary.json"
    config_path = artifact_dir / "supervisor_config.json"
    config = {
        "schema": "spotbatch.supervisor_config.v1",
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
            "schema": "spotbatch.supervisor_loop.v1",
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
        "schema": "spotbatch.supervisor_summary.v1",
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


def _read_tasks_for_finalizer(args: argparse.Namespace, s3) -> list[dict[str, Any]]:
    if args.tasks_jsonl:
        return _read_jsonl(args.tasks_jsonl)
    tasks_s3 = args.tasks_s3 or s3_join(args.output_prefix, "manifests", "tasks.jsonl")
    tmp = []
    for line in s3_download_text(s3, tasks_s3).splitlines():
        if line.strip():
            tmp.append(json.loads(line))
    return tmp


def _check_task(s3, task: dict[str, Any]) -> dict[str, Any]:
    output_s3 = str(task.get("output_s3") or "")
    summary_s3 = str(task.get("summary_s3") or "")
    done_s3 = default_done_s3(task)
    done_exists = s3_exists(s3, done_s3)
    output_exists = s3_exists(s3, output_s3) if output_s3 else False
    summary_exists = s3_exists(s3, summary_s3) if summary_s3 else False
    state = "done" if done_exists else "incomplete"
    if output_exists and not done_exists:
        state = "output_without_done"
    return {"task_id": task.get("task_id"), "output_s3": output_s3, "summary_s3": summary_s3, "done_s3": done_s3, "done_exists": done_exists, "output_exists": output_exists, "summary_exists": summary_exists, "state": state}


def cmd_finalize(args: argparse.Namespace) -> int:
    import concurrent.futures as cf
    s3 = boto3.client("s3")
    tasks = _read_tasks_for_finalizer(args, s3)
    artifact_dir = args.artifact_dir or Path("artifacts") / args.run_id / "finalizer"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    with cf.ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        records = list(ex.map(lambda t: _check_task(s3, t), tasks))
    done = sum(r["done_exists"] for r in records)
    output = sum(r["output_exists"] for r in records)
    summary = sum(r["summary_exists"] for r in records)
    output_without_done = [r for r in records if r["state"] == "output_without_done"]
    missing = [r for r in records if not r["done_exists"]]
    final_manifest = {
        "schema": "spotbatch.final_manifest.v1",
        "run_id": args.run_id,
        "finalized_at": iso_now(),
        "output_prefix": args.output_prefix.rstrip("/"),
        "task_count": len(records),
        "done_count": done,
        "output_count": output,
        "summary_count": summary,
        "missing_done_count": len(missing),
        "output_without_done_count": len(output_without_done),
        "complete": done == len(records),
        "missing_task_ids": [r["task_id"] for r in missing[:1000]],
        "output_without_done_task_ids": [r["task_id"] for r in output_without_done[:1000]],
        "outputs": [r["output_s3"] for r in records if r["done_exists"]],
    }
    final_path = artifact_dir / "final_manifest.json"
    missing_path = artifact_dir / "missing_or_incomplete_tasks.jsonl"
    final_path.write_text(json.dumps(final_manifest, indent=2, sort_keys=True) + "\n")
    missing_path.write_text("".join(json.dumps(r, sort_keys=True) + "\n" for r in missing))
    final_s3 = s3_join(args.output_prefix, "manifests", "final_manifest.json")
    if args.upload:
        s3_upload_text(s3, json.dumps(final_manifest, indent=2, sort_keys=True) + "\n", final_s3)
    print(json.dumps({**{k: final_manifest[k] for k in ["schema", "run_id", "task_count", "done_count", "output_count", "summary_count", "missing_done_count", "output_without_done_count", "complete"]}, "final_manifest": str(final_path), "missing_or_incomplete": str(missing_path), "final_manifest_s3": final_s3 if args.upload else None}, indent=2, sort_keys=True))
    return 2 if args.require_complete and not final_manifest["complete"] else 0


def _parse_body(msg: dict[str, Any]) -> dict[str, Any]:
    try:
        body = json.loads(msg.get("Body", "{}"))
        return body if isinstance(body, dict) else {"_raw_body_type": type(body).__name__}
    except json.JSONDecodeError as exc:
        return {"_json_error": str(exc), "_raw_body": msg.get("Body", "")[:500]}


def cmd_dlq(args: argparse.Namespace) -> int:
    if args.apply and not args.queue_url:
        raise SystemExit("--apply requires --queue-url")
    sqs = boto3.client("sqs")
    scanned = matched = moved = 0
    by_run: Counter[str] = Counter(); by_schema: Counter[str] = Counter(); examples = []
    while scanned < args.max_messages:
        resp = sqs.receive_message(QueueUrl=args.dlq_url, MaxNumberOfMessages=min(10, args.max_messages - scanned), WaitTimeSeconds=args.wait_time, VisibilityTimeout=args.visibility_timeout, AttributeNames=["ApproximateReceiveCount", "SentTimestamp"])
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
    print(json.dumps({"schema": "spotbatch.dlq_summary.v1", "checked_at": iso_now(), "apply": bool(args.apply), "scanned": scanned, "matched": matched, "moved": moved, "by_run": dict(by_run.most_common()), "by_schema": dict(by_schema.most_common()), "examples": examples}, indent=2, sort_keys=True))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(prog="spotbatch")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("worker", help="Run an SQS worker inside AWS Batch")
    p.add_argument("--queue-url", default=os.environ.get("SPOTBATCH_SQS_QUEUE_URL", ""))
    p.add_argument("--max-messages", type=int, default=int(os.environ.get("SPOTBATCH_MAX_MESSAGES", "1")))
    p.add_argument("--visibility-timeout", type=int, default=int(os.environ.get("SPOTBATCH_VISIBILITY_TIMEOUT", "1800")))
    p.add_argument("--heartbeat-seconds", type=int, default=int(os.environ.get("SPOTBATCH_HEARTBEAT_SECONDS", "300")))
    p.add_argument("--task-timeout-seconds", type=float, default=float(os.environ.get("SPOTBATCH_TASK_TIMEOUT_SECONDS", "86400")), help="Default per-task command timeout when a task omits timeout_seconds")
    p.add_argument("--wait-time", type=int, default=10)
    p.add_argument("--work-dir", type=Path, default=Path(os.environ.get("SPOTBATCH_WORK_DIR", "/tmp/spotbatch-work")))
    p.set_defaults(func=lambda a: run_worker(queue_url=a.queue_url, max_messages=a.max_messages, visibility_timeout=a.visibility_timeout, heartbeat_seconds=a.heartbeat_seconds, wait_time=a.wait_time, work_dir=a.work_dir, task_timeout_seconds=a.task_timeout_seconds))

    p = sub.add_parser("enqueue-jsonl")
    p.add_argument("--queue-url", default=os.environ.get("SPOTBATCH_SQS_QUEUE_URL", ""))
    p.add_argument("--tasks-jsonl", type=Path, required=True)
    p.add_argument("--run-id")
    p.add_argument("--artifact-dir", type=Path)
    p.add_argument("--submit", action="store_true")
    p.set_defaults(func=cmd_enqueue_jsonl)

    p = sub.add_parser("submit-workers")
    p.add_argument("--sqs-queue-url", default=os.environ.get("SPOTBATCH_SQS_QUEUE_URL", ""))
    p.add_argument("--batch-job-queue", required=True)
    p.add_argument("--job-definition", required=True)
    p.add_argument("--job-name-prefix", default="spotbatch-worker")
    p.add_argument("--messages-per-worker", type=int, default=1)
    p.add_argument("--max-workers", type=int, default=64)
    p.add_argument("--min-workers", type=int, default=0)
    p.add_argument("--subtract-active", action="store_true")
    p.add_argument("--include-not-visible", action="store_true")
    p.add_argument("--vcpus", type=int)
    p.add_argument("--memory", type=int)
    p.add_argument("--visibility-timeout", type=int, default=1800)
    p.add_argument("--heartbeat-seconds", type=int, default=300)
    p.add_argument("--task-timeout-seconds", type=float, default=86400, help="Default per-task command timeout to pass to workers")
    p.add_argument("--retry-attempts", type=int)
    p.add_argument("--env", action="append", type=_parse_env_pair, default=[])
    p.add_argument("--submit", action="store_true")
    p.set_defaults(func=cmd_submit_workers)

    p = sub.add_parser("supervise-workers")
    p.add_argument("--run-id")
    p.add_argument("--profile")
    p.add_argument("--region")
    p.add_argument("--sqs-queue-url", default=os.environ.get("SPOTBATCH_SQS_QUEUE_URL", ""))
    p.add_argument("--dlq-url")
    p.add_argument("--stop-on-dlq", action="store_true")
    p.add_argument("--fail-on-stop", action="store_true")
    p.add_argument("--batch-job-queue", required=True)
    p.add_argument("--job-definition", required=True)
    p.add_argument("--job-name-prefix", default="spotbatch-worker")
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
    p.add_argument("--task-timeout-seconds", type=float, default=86400, help="Default per-task command timeout to pass to workers")
    p.add_argument("--retry-attempts", type=int)
    p.add_argument("--env", action="append", type=_parse_env_pair, default=[])
    p.add_argument("--submit", action="store_true")
    p.set_defaults(func=cmd_supervise_workers)

    p = sub.add_parser("finalize")
    p.add_argument("--run-id", required=True)
    p.add_argument("--output-prefix", required=True)
    p.add_argument("--tasks-jsonl", type=Path)
    p.add_argument("--tasks-s3")
    p.add_argument("--artifact-dir", type=Path)
    p.add_argument("--workers", type=int, default=32)
    p.add_argument("--upload", action="store_true")
    p.add_argument("--require-complete", action="store_true")
    p.set_defaults(func=cmd_finalize)

    p = sub.add_parser("dlq")
    p.add_argument("--dlq-url", required=True)
    p.add_argument("--queue-url")
    p.add_argument("--run-id")
    p.add_argument("--task-id-regex")
    p.add_argument("--max-messages", type=int, default=100)
    p.add_argument("--visibility-timeout", type=int, default=10)
    p.add_argument("--wait-time", type=int, default=1)
    p.add_argument("--apply", action="store_true")
    p.set_defaults(func=cmd_dlq)

    args = ap.parse_args()
    if getattr(args, "cmd", None) == "worker" and not args.queue_url:
        raise SystemExit("worker requires --queue-url or SPOTBATCH_SQS_QUEUE_URL")
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
