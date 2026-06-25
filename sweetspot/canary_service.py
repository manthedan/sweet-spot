from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .s3util import s3_download_text


def s3_missing_exception(exc: Exception) -> bool:
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", "")) if isinstance(response, dict) else ""
    return code in {"404", "NoSuchKey", "NotFound"}


def collect_canary_summaries(s3: Any, *, tasks: list[dict[str, Any]], out_jsonl: Path) -> dict[str, Any]:
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    collected = 0
    missing_task_ids: list[str] = []
    missing_summary_s3: list[str] = []
    with out_jsonl.open("w", encoding="utf-8") as f:
        for task in tasks:
            task_id = str(task.get("task_id") or "")
            summary_s3 = str(task.get("summary_s3") or "")
            if not summary_s3:
                missing_task_ids.append(task_id or "<missing-task-id>")
                continue
            try:
                raw = s3_download_text(s3, summary_s3)
            except Exception as exc:  # noqa: BLE001 - missing summaries are expected while canaries are still running
                if s3_missing_exception(exc):
                    missing_task_ids.append(task_id or "<missing-task-id>")
                    missing_summary_s3.append(summary_s3)
                    continue
                raise SystemExit(f"failed to download canary summary for task {task_id or '<missing-task-id>'}: {exc}") from exc
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"canary summary for task {task_id or '<missing-task-id>'} is not valid JSON: {summary_s3}") from exc
            if not isinstance(obj, dict):
                raise SystemExit(f"canary summary for task {task_id or '<missing-task-id>'} is not a JSON object: {summary_s3}")
            f.write(json.dumps(obj, sort_keys=True) + "\n")
            collected += 1
    return {
        "summary_jsonl": str(out_jsonl),
        "task_count": len(tasks),
        "collected_count": collected,
        "missing_count": len(tasks) - collected,
        "missing_task_ids": missing_task_ids[:1000],
        "missing_task_ids_truncated": len(missing_task_ids) > 1000,
        "missing_summary_s3": missing_summary_s3[:1000],
        "complete": collected == len(tasks),
    }
