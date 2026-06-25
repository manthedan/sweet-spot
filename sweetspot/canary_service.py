from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .s3util import parse_s3_uri, s3_download_text


def s3_missing_exception(exc: Exception) -> bool:
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", "")) if isinstance(response, dict) else ""
    return code in {"404", "NoSuchKey", "NotFound"}


def _attempt_summary_prefix(summary_s3: str) -> str:
    return f"{summary_s3.rstrip('/')}.attempts/"


def _list_attempt_summary_s3(s3: Any, summary_s3: str) -> list[str]:
    if not summary_s3:
        return []
    bucket, prefix = parse_s3_uri(_attempt_summary_prefix(summary_s3))
    keys: list[tuple[str, str]] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for item in resp.get("Contents", []) or []:
            key = str(item.get("Key") or "")
            if key.startswith(prefix) and key.endswith("/summary.json"):
                keys.append((str(item.get("LastModified") or ""), key))
        if not resp.get("IsTruncated"):
            break
        token = str(resp.get("NextContinuationToken") or "")
        if not token:
            break
    return [f"s3://{bucket}/{key}" for _last_modified, key in sorted(set(keys))]


def _load_json_object(s3: Any, uri: str, *, description: str, task_id: str) -> dict[str, Any]:
    raw = s3_download_text(s3, uri)
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{description} for task {task_id or '<missing-task-id>'} is not valid JSON: {uri}") from exc
    if not isinstance(obj, dict):
        raise SystemExit(f"{description} for task {task_id or '<missing-task-id>'} is not a JSON object: {uri}")
    return obj


def _done_marker_attempt_summary_s3(s3: Any, *, done_s3: str, summary_s3: str, task_id: str) -> str | None:
    if not done_s3:
        return None
    try:
        marker = _load_json_object(s3, done_s3, description="canary done marker", task_id=task_id)
    except Exception as exc:  # noqa: BLE001 - a missing done marker means the task may still be running or failed before commit.
        if s3_missing_exception(exc):
            return None
        raise
    marker_task_id = marker.get("task_id")
    if task_id and marker_task_id and str(marker_task_id) != task_id:
        raise SystemExit(f"canary done marker task_id mismatch for task {task_id}: {done_s3}")
    marker_done_s3 = marker.get("done_s3")
    if marker_done_s3 and str(marker_done_s3) != done_s3:
        raise SystemExit(f"canary done marker done_s3 mismatch for task {task_id or '<missing-task-id>'}: {done_s3}")
    attempt_summary_s3 = marker.get("attempt_summary_s3")
    if not attempt_summary_s3:
        return None
    attempt_summary_uri = str(attempt_summary_s3)
    expected_prefix = _attempt_summary_prefix(summary_s3)
    if summary_s3 and (not attempt_summary_uri.startswith(expected_prefix) or not attempt_summary_uri.endswith("/summary.json")):
        raise SystemExit(f"canary done marker attempt_summary_s3 is outside the expected attempt summary prefix for task {task_id or '<missing-task-id>'}: {done_s3}")
    return attempt_summary_uri


def _summary_uris_for_task(s3: Any, task: dict[str, Any]) -> tuple[list[str], list[str]]:
    """Return candidate summary URIs plus source labels for one canary task.

    Successful workers commit an immutable done marker at ``done_s3`` and write
    the actual worker summary to ``attempt_summary_s3``.  Failed/OOM canaries do
    not write a done marker, but they still upload best-effort attempt summaries
    under ``<summary_s3>.attempts/<attempt_id>/summary.json``.  Legacy logical
    ``summary_s3`` is used only as a compatibility fallback.
    """

    task_id = str(task.get("task_id") or "")
    summary_s3 = str(task.get("summary_s3") or "")
    done_s3 = str(task.get("done_s3") or "")
    uris: list[str] = []
    sources: list[str] = []
    done_attempt_summary = _done_marker_attempt_summary_s3(s3, done_s3=done_s3, summary_s3=summary_s3, task_id=task_id)
    if done_attempt_summary:
        uris.append(done_attempt_summary)
        sources.append("done_marker_attempt_summary_s3")
    if summary_s3 and not done_attempt_summary:
        try:
            listed_attempt_summaries = _list_attempt_summary_s3(s3, summary_s3)
            if listed_attempt_summaries:
                uri = listed_attempt_summaries[-1]
                if uri not in uris:
                    uris.append(uri)
                    sources.append("attempt_summary_listing_latest")
        except Exception:
            # Preserve compatibility with older canaries and least-privilege
            # readers that can GetObject for known summary keys but cannot list
            # the bucket.  The later download step will either collect the
            # legacy object or mark it missing without relaunching work.
            uris.append(summary_s3)
            sources.append("legacy_summary_s3")
    if not uris and summary_s3:
        # Backward compatibility for summaries produced before attempt-scoped
        # uploads existed and for older tests/fixtures.  Normal workers do not
        # upload this canonical object anymore.
        uris.append(summary_s3)
        sources.append("legacy_summary_s3")
    return uris, sources


def _summary_is_terminal_failure(obj: dict[str, Any]) -> bool:
    raw_telemetry = obj.get("telemetry")
    telemetry: dict[str, Any] = raw_telemetry if isinstance(raw_telemetry, dict) else {}
    terminal = bool(obj.get("canary_terminal_failure") or obj.get("retry_exhausted") or telemetry.get("canary_terminal_failure") or telemetry.get("retry_exhausted"))
    failed = bool(obj.get("timed_out") or obj.get("framework_error") or (obj.get("returncode") is not None and obj.get("returncode") != 0))
    return terminal and failed


def collect_canary_summaries(s3: Any, *, tasks: list[dict[str, Any]], out_jsonl: Path) -> dict[str, Any]:
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)
    collected_tasks = 0
    collected_summaries = 0
    missing_task_ids: list[str] = []
    missing_summary_s3: list[str] = []
    summary_sources: dict[str, int] = {}
    with out_jsonl.open("w", encoding="utf-8") as f:
        for task in tasks:
            task_id = str(task.get("task_id") or "")
            summary_s3 = str(task.get("summary_s3") or "")
            if not summary_s3:
                missing_task_ids.append(task_id or "<missing-task-id>")
                continue
            summary_uris, sources = _summary_uris_for_task(s3, task)
            task_collected = False
            missing_for_task: list[str] = []
            for uri, source in zip(summary_uris, sources):
                try:
                    obj = _load_json_object(s3, uri, description="canary summary", task_id=task_id)
                except Exception as exc:  # noqa: BLE001 - missing summaries are expected while canaries are still running
                    if s3_missing_exception(exc):
                        missing_for_task.append(uri)
                        continue
                    raise
                if source == "attempt_summary_listing_latest" and not _summary_is_terminal_failure(obj):
                    missing_for_task.append(uri)
                    continue
                f.write(json.dumps(obj, sort_keys=True) + "\n")
                task_collected = True
                collected_summaries += 1
                summary_sources[source] = summary_sources.get(source, 0) + 1
            if task_collected:
                collected_tasks += 1
            else:
                missing_task_ids.append(task_id or "<missing-task-id>")
                missing_summary_s3.extend(missing_for_task or [summary_s3])
    return {
        "summary_jsonl": str(out_jsonl),
        "task_count": len(tasks),
        "collected_count": collected_tasks,
        "collected_summary_count": collected_summaries,
        "missing_count": len(tasks) - collected_tasks,
        "missing_task_ids": missing_task_ids[:1000],
        "missing_task_ids_truncated": len(missing_task_ids) > 1000,
        "missing_summary_s3": missing_summary_s3[:1000],
        "summary_sources": summary_sources,
        "complete": collected_tasks == len(tasks),
    }
