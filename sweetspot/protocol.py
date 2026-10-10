"""The completion protocol, shared by the SQS worker and `sweetspot local`.

Layout under a run prefix:

    tasks.jsonl                          the ledger
    done/<id>.json                       done marker, created with a conditional write: first finisher wins
    out/<id>/<files>                     canonical outputs, copied from the winning attempt
    attempts/<id>/<attempt>/             summary.json, stdout.txt, stderr.txt, out/<files> (staged)
    failures/<id>/<attempt>.json         one small record per failed attempt (what `status` reads)

An attempt stages its outputs under its own prefix, then tries to create the done marker. Only the winner's
files are copied to out/<id>/, so a duplicate delivery can never mix files from two attempts. If the winner dies
between the marker and the copy, `finish` repeats the copy from the marker's attempt.
"""

from __future__ import annotations

import json
import os
import platform
import socket
import time
import uuid
from typing import Any
from urllib.request import Request, urlopen

from .runner import Result
from .store import Store, join


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def new_attempt_id() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:8]


def done_uri(prefix: str, task_id: str) -> str:
    return join(prefix, "done", f"{task_id}.json")


def out_uri(prefix: str, task_id: str, rel: str = "") -> str:
    return join(prefix, "out", task_id, rel)


def attempt_uri(prefix: str, task_id: str, attempt: str, leaf: str = "") -> str:
    return join(prefix, "attempts", task_id, attempt, leaf)


def read_json(store: Store, uri: str) -> Any:
    data = store.get(uri)
    return None if data is None else json.loads(data)


def put_json(store: Store, uri: str, obj: Any) -> None:
    store.put(uri, (json.dumps(obj, indent=1, sort_keys=True) + "\n").encode())


def check_marker(marker: Any, task_id: str, task_hash: str) -> None:
    if not isinstance(marker, dict) or marker.get("task_id") != task_id:
        raise ValueError(f"done marker for {task_id} is malformed")
    if marker.get("task_hash") != task_hash:
        raise ValueError(f"task {task_id} was already completed with different contents (task_hash {marker.get('task_hash', '?')[:12]} != {task_hash[:12]})")


def _imds(path: str) -> str | None:
    try:
        token_req = Request("http://169.254.169.254/latest/api/token", method="PUT", headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"})
        with urlopen(token_req, timeout=0.5) as r:  # noqa: S310 - fixed link-local metadata endpoint
            token = r.read().decode()
        with urlopen(Request(f"http://169.254.169.254/latest/meta-data/{path}", headers={"X-aws-ec2-metadata-token": token}), timeout=0.5) as r:  # noqa: S310
            return r.read().decode().strip()
    except Exception:  # noqa: BLE001 - metadata is optional telemetry
        return None


def worker_info() -> dict[str, Any]:
    """Where this worker runs; collected once per process and copied into every summary and marker."""
    env = os.environ
    info: dict[str, Any] = {"host": socket.gethostname(), "arch": platform.machine(), "cpus": os.cpu_count()}
    # Set by the job definition `sweetspot submit` registers.
    for key, var in (("image", "SWEETSPOT_IMAGE"), ("lane", "SWEETSPOT_LANE"), ("vcpus", "SWEETSPOT_VCPUS"), ("memory_mib", "SWEETSPOT_MEMORY_MIB"), ("gpus", "SWEETSPOT_GPUS")):
        if env.get(var):
            info[key] = env[var] if key in ("image", "lane") else (int(env[var]) if key == "gpus" else float(env[var]))
    if env.get("AWS_BATCH_JOB_ID"):
        info["batch_job_id"] = env["AWS_BATCH_JOB_ID"]
        info["instance_type"] = _imds("instance-type")
        info["az"] = _imds("placement/availability-zone")
        info["lifecycle"] = _imds("instance-life-cycle")  # "spot" or "on-demand"
    return {k: v for k, v in info.items() if v is not None}


def _best_effort(what: str, fn: Any, *args: Any) -> None:
    """Diagnostics (logs, summaries, failure records) must never cost a successful attempt."""
    try:
        fn(*args)
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"event": "diagnostics_write_failed", "what": what, "error": f"{type(exc).__name__}: {exc}"}), flush=True)


def record_failure(store: Store, prefix: str, task_id: str, attempt: str, status: str, error: str, worker: dict[str, Any], **extra: Any) -> None:
    rec = {"task_id": task_id, "attempt": attempt, "finished_at": now_iso(), "status": status, "error": error, "worker": worker, **extra}
    _best_effort("failure record", put_json, store, join(prefix, "failures", task_id, f"{attempt}.json"), rec)


def record_attempt(
    store: Store,
    prefix: str,
    task: dict[str, Any],
    task_hash: str,
    attempt: str,
    result: Result,
    worker: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> str:
    """Persist one finished attempt. Returns "committed", "duplicate" (another attempt won) or "failed".

    Raises only if a successful attempt could not be committed (output upload or marker write failed).
    """
    task_id = task["id"]
    for path in (result.stdout_path, result.stderr_path):
        if path is not None and path.exists() and path.stat().st_size:
            _best_effort(path.name, store.upload, path, attempt_uri(prefix, task_id, attempt, path.name))
    summary = {"task_id": task_id, "task_hash": task_hash, "attempt": attempt, "finished_at": now_iso(), "worker": worker, **(extra or {}), **result.summary()}
    summary_uri = attempt_uri(prefix, task_id, attempt, "summary.json")

    if not result.ok:
        summary["outcome"] = "interrupted" if result.status == "interrupted" else "failed"
        _best_effort("summary", put_json, store, summary_uri, summary)
        record_failure(store, prefix, task_id, attempt, result.status, result.error or "", worker, exit_code=result.exit_code, elapsed=round(result.elapsed, 3), stderr_tail=result.stderr_tail[-2000:])
        return "failed"

    for f in result.outputs:
        store.upload(f.path, attempt_uri(prefix, task_id, attempt, f"out/{f.rel}"))
    marker = {
        "task_id": task_id,
        "task_hash": task_hash,
        "attempt": attempt,
        "finished_at": summary["finished_at"],
        "files": [{"path": f.rel, "size": f.size, "sha256": f.sha256} for f in result.outputs],
        "inputs": result.inputs,
        "elapsed": round(result.elapsed, 3),
        "units": result.metrics.get("units"),
        "peak_rss_mib": result.peak_rss_mib,
        "cpu_seconds": result.cpu_seconds,
        "worker": worker,
    }
    won = store.put_if_absent(done_uri(prefix, task_id), (json.dumps(marker, indent=1, sort_keys=True) + "\n").encode())
    if not won:
        existing = read_json(store, done_uri(prefix, task_id))
        check_marker(existing, task_id, task_hash)
        won = existing.get("attempt") == attempt  # our earlier PUT succeeded but its response was lost
    summary["outcome"] = "committed" if won else "duplicate"
    _best_effort("summary", put_json, store, summary_uri, summary)
    if won:
        promote(store, prefix, marker)
        return "committed"
    # Another attempt committed first. Our staged files will never be referenced; drop them.
    _best_effort("staged cleanup", store.delete_many, [attempt_uri(prefix, task_id, attempt, f"out/{f.rel}") for f in result.outputs])
    return "duplicate"


def promote(store: Store, prefix: str, marker: dict[str, Any]) -> None:
    task_id, attempt = marker["task_id"], marker["attempt"]
    for f in marker["files"]:
        store.copy(attempt_uri(prefix, task_id, attempt, f"out/{f['path']}"), out_uri(prefix, task_id, f["path"]))
