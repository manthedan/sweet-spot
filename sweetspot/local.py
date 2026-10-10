"""`sweetspot local`: run tasks on this machine with the same runner and commit protocol as the cloud worker."""

from __future__ import annotations

import concurrent.futures as cf
import json
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any

from . import protocol
from .runner import run_task
from .store import Store, is_local, join, local_path, sha256_file
from .tasks import task_hash, validate


def run_local(
    tasks: list[dict[str, Any]],
    *,
    prefix: str,
    ledger: list[dict[str, Any]] | None = None,
    store: Store,
    jobs: int = 1,
    work_dir: Path | None = None,
    require_output: bool = True,
    echo: bool = True,
    stop: threading.Event | None = None,
) -> dict[str, int]:
    """Run every task that has no done marker under prefix. Returns outcome counts.

    `ledger` is the full task list `tasks` was selected from (default: tasks). If prefix belongs to a cloud run
    (it has run.json), the tasks must be in that run's ledger. Local mode never writes a ledger. Setting `stop`
    interrupts running tasks (their process groups are killed) and skips the rest.
    """
    _check_ledger(store, prefix, tasks, ledger or tasks)
    worker = protocol.worker_info()
    counts = {"committed": 0, "duplicate": 0, "failed": 0, "skipped": 0, "interrupted": 0}
    lock = threading.Lock()
    stop = stop or threading.Event()

    def one(task: dict[str, Any]) -> str:
        thash = task_hash(task)
        marker = protocol.read_json(store, protocol.done_uri(prefix, task["id"]))
        if marker is not None:
            protocol.check_marker(marker, task["id"], thash)
            _repair_promotion(store, prefix, marker)  # a previous run may have died between marker and copy
            return "skipped"
        if stop.is_set():
            return "interrupted"
        attempt = protocol.new_attempt_id()
        with tempfile.TemporaryDirectory(prefix=f"sweetspot-{task['id'][:40]}-", dir=work_dir) as tmp:
            result = run_task(task, store=store, task_dir=Path(tmp) / "task", stop=stop, require_output=require_output, echo=echo and jobs == 1)
            outcome = protocol.record_attempt(store, prefix, task, thash, attempt, result, worker)
            if result.status == "interrupted":
                outcome = "interrupted"
        line = f"[{outcome}] {task['id']} {result.status} {result.elapsed:.1f}s"
        if result.error:
            line += f": {result.error}"
        print(line, file=sys.stderr, flush=True)
        return outcome

    with cf.ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
        futures = [pool.submit(one, t) for t in tasks]
        try:
            for fut in cf.as_completed(futures):
                outcome = fut.result()
                with lock:
                    counts[outcome] += 1
        except KeyboardInterrupt:
            stop.set()  # running tasks are killed; the pool waits for them before re-raising
            raise
    return counts


def _check_ledger(store: Store, prefix: str, tasks: list[dict[str, Any]], ledger: list[dict[str, Any]]) -> None:
    """Refuse tasks that conflict with what prefix already holds, before writing anything."""
    if store.get(join(prefix, "run.json")) is not None:  # a cloud run: its ledger is fixed
        data = store.get(join(prefix, "tasks.jsonl")) or b""
        known = {t["id"]: task_hash(t) for t in (validate(json.loads(line)) for line in data.decode().splitlines() if line.strip())}
        for t in tasks:
            if known.get(t["id"]) != task_hash(t):
                raise ValueError(f"task {t['id']!r} is not part of the run at {prefix} (or differs from it)")
        return
    for t in ledger:
        marker = protocol.read_json(store, protocol.done_uri(prefix, t["id"]))
        if marker is not None:
            protocol.check_marker(marker, t["id"], task_hash(t))


def _repair_promotion(store: Store, prefix: str, marker: dict[str, Any]) -> None:
    """Copy the winner's staged files to out/ again if out/ does not hold exactly them."""
    for f in marker["files"]:
        uri = protocol.out_uri(prefix, marker["task_id"], f["path"])
        if not store.exists(uri) or (is_local(uri) and sha256_file(local_path(uri)) != f["sha256"]) or (not is_local(uri) and _size(store, uri) != f["size"]):
            protocol.promote(store, prefix, marker)
            return


def _size(store: Store, uri: str) -> int | None:
    return next((size for u, size, _ in store.list(uri) if u == uri), None)
