from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable

from .aws_batch import iso_now, queue_depth
from .task_model import validate_task_model
from .worker import SAFE_TASK_TIMEOUT_SECONDS


def chunks(xs: list[dict[str, Any]], n: int) -> Iterable[list[dict[str, Any]]]:
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


def validate_unique_task_ids(tasks: list[dict[str, Any]], *, context: str) -> None:
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


def validate_tasks_for_enqueue(tasks: list[dict[str, Any]], *, allowed_s3_prefixes: list[str] | tuple[str, ...] | None) -> None:
    validate_unique_task_ids(tasks, context="enqueue JSONL")
    for i, task in enumerate(tasks, start=1):
        try:
            validate_task_model(task, default_timeout_seconds=SAFE_TASK_TIMEOUT_SECONDS, max_timeout_seconds=SAFE_TASK_TIMEOUT_SECONDS, allowed_s3_prefixes=allowed_s3_prefixes)
        except ValueError as exc:
            raise SystemExit(f"invalid task at line {i}: {exc}") from exc


def write_enqueue_artifacts(tasks: list[dict[str, Any]], artifact_dir: Path) -> Path:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    tasks_out = artifact_dir / "tasks.jsonl"
    tasks_out.write_text("".join(json.dumps(t, sort_keys=True) + "\n" for t in tasks))
    return tasks_out


def send_tasks_to_sqs(sqs, *, queue_url: str, tasks: list[dict[str, Any]]) -> int:
    sent = 0
    for batch in chunks(tasks, 10):
        entries = [{"Id": str(i), "MessageBody": json.dumps(t, sort_keys=True)} for i, t in enumerate(batch)]
        resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
        if resp.get("Failed"):
            raise SystemExit(f"send_message_batch failed: {resp['Failed']}")
        sent += len(resp.get("Successful", []))
    return sent


def wait_for_visible_backlog(sqs, *, queue_url: str, min_visible: int, max_seconds: float, interval_seconds: float) -> tuple[dict[str, int], list[dict[str, Any]]]:
    deadline = time.time() + max(0.0, max_seconds)
    history: list[dict[str, Any]] = []
    while True:
        depth = queue_depth(sqs, queue_url)
        history.append({"checked_at": iso_now(), **depth})
        if depth["visible"] >= min_visible or max_seconds <= 0 or time.time() >= deadline:
            return depth, history
        time.sleep(max(0.1, interval_seconds))
