"""Run one task in a scratch directory: stage inputs, execute the command, capture logs, collect outputs.

Shared by `sweetspot local` and the SQS worker; it knows nothing about queues or commit markers.
"""

from __future__ import annotations

import fnmatch
import json
import math
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

from .store import Store, sha256_file
from .tasks import expand

METRICS_FILE = "_metrics.json"
DEFAULT_TIMEOUT = 3600.0
TAIL_BYTES = 8192


@dataclass
class OutputFile:
    rel: str
    path: Path
    size: int
    sha256: str


@dataclass
class Result:
    # ok | failed (the task's fault) | timeout | interrupted | error (possibly this host's fault: network, disk)
    status: str
    exit_code: int | None = None
    error: str | None = None
    elapsed: float = 0.0
    outputs: list[OutputFile] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    inputs: dict[str, Any] = field(default_factory=dict)
    peak_rss_mib: float | None = None
    cpu_seconds: float | None = None
    stdout_path: Path | None = None
    stderr_path: Path | None = None
    stdout_tail: str = ""
    stderr_tail: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "ok"

    def summary(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "exit_code": self.exit_code,
            "error": self.error,
            "elapsed": round(self.elapsed, 3),
            "outputs": [{"path": o.rel, "size": o.size, "sha256": o.sha256} for o in self.outputs],
            "metrics": self.metrics,
            "inputs": self.inputs,
            "peak_rss_mib": self.peak_rss_mib,
            "cpu_seconds": self.cpu_seconds,
            "stdout_tail": self.stdout_tail,
            "stderr_tail": self.stderr_tail,
        }


class _LogCapture:
    """Tee a pipe to a capped file and to the worker's own stream, keeping the last TAIL_BYTES."""

    def __init__(self, pipe: IO[bytes], path: Path, echo: IO[str] | None, max_bytes: int) -> None:
        self.pipe, self.path, self.echo, self.max_bytes = pipe, path, echo, max_bytes
        self.tail = bytearray()
        self.thread = threading.Thread(target=self._pump, daemon=True)
        self.thread.start()

    def _pump(self) -> None:
        # Keep draining the pipe whatever happens to the sinks, or a chatty command would block on a full pipe.
        written = 0
        try:
            f: IO[bytes] | None = self.path.open("wb")
        except OSError:
            f = None
        echo = self.echo
        for chunk in iter(lambda: self.pipe.read1(65536), b""):  # type: ignore[attr-defined]
            self.tail.extend(chunk)
            del self.tail[:-TAIL_BYTES]
            if f is not None and written < self.max_bytes:
                try:
                    f.write(chunk[: self.max_bytes - written])
                    written += min(len(chunk), self.max_bytes - written)
                except OSError:
                    f = None
            if echo is not None:
                try:
                    echo.write(chunk.decode("utf-8", "replace"))
                    echo.flush()
                except (OSError, ValueError):
                    echo = None
        if f is not None:
            try:
                f.close()
            except OSError:
                pass
        self.pipe.close()

    def text(self) -> str:
        return bytes(self.tail).decode("utf-8", "replace")


def _input_missing(exc: BaseException) -> bool:
    if isinstance(exc, (FileNotFoundError, IsADirectoryError, PermissionError)):
        return True
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = str(response.get("Error", {}).get("Code", ""))
        return code in {"404", "403", "NoSuchKey", "NoSuchBucket", "NotFound", "AccessDenied", "Forbidden"}
    return getattr(exc, "code", None) in (403, 404)  # urllib HTTPError


def _kill_group(pid: int, sig: int) -> None:
    try:
        os.killpg(pid, sig)
    except (ProcessLookupError, PermissionError):
        pass


def _collect_outputs(out_dir: Path, allow: list[str] | None) -> tuple[list[OutputFile], dict[str, Any], str | None]:
    """Hash the files under out_dir. With an allowlist, only files whose relative path matches a glob are kept."""
    outputs: list[OutputFile] = []
    metrics: dict[str, Any] = {}
    for root, dirs, files in os.walk(out_dir):
        dirs.sort()
        for name in sorted(files):
            path = Path(root) / name
            rel = path.relative_to(out_dir).as_posix()
            if rel != METRICS_FILE and allow is not None and not any(fnmatch.fnmatchcase(rel, g) for g in allow):
                continue
            if path.is_symlink() or not path.is_file():
                return [], {}, f"output {rel!r} is not a regular file"
            if rel == METRICS_FILE:
                try:
                    loaded = json.loads(path.read_text())
                    metrics = loaded if isinstance(loaded, dict) else {"value": loaded}
                except (OSError, ValueError) as exc:
                    metrics = {"error": f"unreadable {METRICS_FILE}: {exc}"}
                continue
            outputs.append(OutputFile(rel, path, path.stat().st_size, sha256_file(path)))
    return outputs, metrics, None


def run_task(
    task: dict[str, Any],
    *,
    store: Store,
    task_dir: Path,
    stop: threading.Event | None = None,
    default_timeout: float = DEFAULT_TIMEOUT,
    require_output: bool = True,
    echo: bool = True,
    max_log_bytes: int = 5 << 20,
) -> Result:
    """Run `task` inside the (empty, caller-owned) task_dir. Never raises for task-level failures."""
    task_dir = task_dir.resolve()  # the command runs elsewhere (cwd), so {in}/{out} must be absolute
    in_dir, out_dir = task_dir / "in", task_dir / "out"
    in_dir.mkdir(parents=True)
    out_dir.mkdir()
    started = time.monotonic()
    staged: dict[str, Any] = {}
    try:
        for name, uri in (task.get("inputs") or {}).items():
            staged[name] = store.download(uri, in_dir / name)
    except Exception as exc:  # noqa: BLE001 - any staging failure fails the attempt, not the worker
        # A missing or forbidden input is the task's problem; anything else (network, disk) may be this host's.
        status = "failed" if _input_missing(exc) else "error"
        return Result(status, error=f"staging input {name!r} failed: {type(exc).__name__}: {exc}", elapsed=time.monotonic() - started, inputs=staged)

    values = {"in_dir": str(in_dir), "out_dir": str(out_dir), "task_id": task["id"]}
    command = [expand(arg, **values) for arg in task["command"]]
    cwd = expand(task["cwd"], **values) if task.get("cwd") else str(task_dir)
    env = {**os.environ, **(task.get("env") or {}), "SWEETSPOT_TASK_ID": task["id"], "SWEETSPOT_IN": str(in_dir), "SWEETSPOT_OUT": str(out_dir)}
    timeout = float(task.get("timeout") or default_timeout)
    stdout_path, stderr_path = task_dir / "stdout.txt", task_dir / "stderr.txt"
    try:
        proc = subprocess.Popen(command, cwd=cwd, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    except (OSError, ValueError) as exc:  # missing binary or cwd, bad argument: the task's problem
        return Result("failed", error=f"could not start {command[0]!r} in {cwd}: {exc}", elapsed=time.monotonic() - started, inputs=staged)
    assert proc.stdout is not None and proc.stderr is not None
    out_cap = _LogCapture(proc.stdout, stdout_path, sys.stdout if echo else None, max_log_bytes)
    err_cap = _LogCapture(proc.stderr, stderr_path, sys.stderr if echo else None, max_log_bytes)

    deadline = started + timeout
    status, wait_status, rusage = "", 0, None
    while True:
        # wait4 instead of Popen.wait: it returns this child's own rusage (peak RSS, CPU time).
        pid, wait_status, rusage = os.wait4(proc.pid, os.WNOHANG)
        if pid:
            break
        if stop is not None and stop.is_set():
            status = "interrupted"
        elif time.monotonic() > deadline:
            status = "timeout"
        if status:
            _kill_group(proc.pid, signal.SIGTERM)
            # Interrupted: the host is going away and the message still has to be released, so be brief.
            grace = time.monotonic() + (5 if status == "interrupted" else 10)
            while time.monotonic() < grace:
                pid, wait_status, rusage = os.wait4(proc.pid, os.WNOHANG)
                if pid:
                    break
                time.sleep(0.1)
            else:
                _kill_group(proc.pid, signal.SIGKILL)
                _, wait_status, rusage = os.wait4(proc.pid, 0)
            break
        time.sleep(0.2)
    proc.returncode = exit_code = os.waitstatus_to_exitcode(wait_status)
    _kill_group(proc.pid, signal.SIGKILL)  # background descendants left in the task's process group
    out_cap.thread.join(timeout=10)
    err_cap.thread.join(timeout=10)

    result = Result(
        status or ("ok" if exit_code == 0 else "failed"),
        exit_code=exit_code,
        elapsed=time.monotonic() - started,
        peak_rss_mib=round(rusage.ru_maxrss / 1024, 1) if rusage else None,  # Linux reports KiB
        cpu_seconds=round(rusage.ru_utime + rusage.ru_stime, 3) if rusage else None,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        stdout_tail=out_cap.text(),
        stderr_tail=err_cap.text(),
        inputs=staged,
    )
    if result.status == "timeout":
        result.error = f"timed out after {timeout:g}s"
    elif result.status == "failed":
        result.error = f"command exited with {exit_code}"
    if result.status != "ok":
        return result
    outputs, metrics, problem = _collect_outputs(out_dir, task.get("outputs"))
    units = metrics.get("units")
    if units is not None and (isinstance(units, bool) or not isinstance(units, (int, float)) or not math.isfinite(units) or units < 0):
        metrics["units_error"] = f"ignored non-numeric units {units!r} from {METRICS_FILE}"
        del metrics["units"]
    if "units" not in metrics and task.get("units") is not None:
        metrics["units"] = task["units"]
    result.outputs, result.metrics = outputs, metrics
    if problem:
        result.status, result.error = "failed", problem
    elif require_output and not outputs:
        result.status, result.error = "failed", "command succeeded but wrote no output files to {out}" + (f" matching {task['outputs']}" if task.get("outputs") else "")
    return result
