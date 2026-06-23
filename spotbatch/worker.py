from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import tempfile as _tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Pattern

import boto3
from botocore.exceptions import ClientError

from .aws_batch import iso_now
from .s3util import s3_download_text, s3_exists, s3_head_object, s3_upload_file, s3_upload_text, s3_upload_text_if_absent
from .task_model import default_done_s3, parse_allowed_s3_prefixes, task_env_overrides, task_hash, validate_task_model, validate_task_s3_prefixes, validate_timeout_seconds


SQS_MAX_VISIBILITY_SECONDS = 12 * 60 * 60
SAFE_TASK_TIMEOUT_SECONDS = 11 * 60 * 60
DONE_MARKER_SCHEMA_V1 = "spotbatch.done_marker.v1"
DONE_MARKER_SCHEMA_V2 = "spotbatch.done_marker.v2"
DEFAULT_LOG_TAIL_BYTES = 12_000
DEFAULT_MAX_LOG_BYTES = 5 * 1024 * 1024
REDACTION_CARRY_CHARS = 65_536


def _emit_event(event: str, **fields: Any) -> None:
    payload = {"schema": "spotbatch.worker_event.v1", "event": event, "emitted_at": iso_now(), **fields}
    print(json.dumps(payload, sort_keys=True), flush=True)


def parse_redact_patterns(patterns: Iterable[str] | str | None) -> list[Pattern[str]]:
    if patterns is None:
        return []
    if isinstance(patterns, str):
        raw_patterns = [p for p in patterns.splitlines() if p.strip()]
    else:
        raw_patterns = [str(p) for p in patterns if str(p).strip()]
    compiled: list[Pattern[str]] = []
    for pattern in raw_patterns:
        try:
            compiled.append(re.compile(pattern))
        except re.error as exc:
            raise ValueError(f"invalid redact regex {pattern!r}: {exc}") from exc
    return compiled


def _redact_text(text: str, patterns: list[Pattern[str]]) -> str:
    for pattern in patterns:
        text = pattern.sub("<redacted>", text)
    return text


class _BoundedLogCapture:
    def __init__(self, path: Path, *, stream, max_bytes: int, tail_bytes: int, redact_patterns: list[Pattern[str]]) -> None:
        self.path = path
        self.stream = stream
        self.max_bytes = max(0, int(max_bytes))
        self.tail_bytes = max(0, int(tail_bytes))
        self.redact_patterns = redact_patterns
        self.total_bytes = 0
        self.stored_bytes = 0
        self.truncated_bytes = 0
        self._tail = bytearray()
        self._fh = path.open("wb")
        self._lock = threading.Lock()

    def write_text(self, text: str) -> None:
        text = _redact_text(text, self.redact_patterns)
        data = text.encode("utf-8", errors="replace")
        with self._lock:
            self.total_bytes += len(data)
            if self.tail_bytes:
                self._tail.extend(data)
                if len(self._tail) > self.tail_bytes:
                    del self._tail[: len(self._tail) - self.tail_bytes]
            if self.max_bytes == 0:
                self.truncated_bytes += len(data)
            else:
                remaining = self.max_bytes - self.stored_bytes
                if remaining > 0:
                    to_store = data[:remaining]
                    self._fh.write(to_store)
                    self.stored_bytes += len(to_store)
                    self.truncated_bytes += max(0, len(data) - len(to_store))
                else:
                    self.truncated_bytes += len(data)
        self.stream.write(text)
        self.stream.flush()

    def write(self, chunk: bytes) -> None:
        self.write_text(chunk.decode("utf-8", errors="replace"))

    def close(self) -> None:
        self._fh.flush()
        self._fh.close()

    def tail_text(self) -> str:
        return bytes(self._tail).decode("utf-8", errors="replace")

    def upload_text(self) -> str:
        return self.path.read_bytes().decode("utf-8", errors="replace")

    def summary(self) -> dict[str, Any]:
        return {
            "stored_bytes": self.stored_bytes,
            "total_bytes": self.total_bytes,
            "truncated_bytes": self.truncated_bytes,
            "truncated": self.truncated_bytes > 0,
            "max_bytes": self.max_bytes,
        }


def _pump_pipe(pipe: BinaryIO, capture: _BoundedLogCapture) -> None:
    try:
        if not capture.redact_patterns:
            for chunk in iter(lambda: pipe.read(8192), b""):
                if chunk:
                    capture.write(chunk)
        else:
            pending = ""
            suppress_until_newline = False
            for chunk in iter(lambda: pipe.read(8192), b""):
                if not chunk:
                    continue
                text = chunk.decode("utf-8", errors="replace")
                if suppress_until_newline:
                    newline_index = text.find("\n")
                    if newline_index < 0:
                        continue
                    text = text[newline_index + 1 :]
                    suppress_until_newline = False
                pending += text
                while "\n" in pending:
                    line, pending = pending.split("\n", 1)
                    capture.write_text(line + "\n")
                if len(pending) > REDACTION_CARRY_CHARS:
                    # Regex redaction cannot be proven safe for an unbounded
                    # unterminated record. Fail closed: emit a placeholder and
                    # suppress the rest of the overlong line/record rather than
                    # leaking the suffix of a long secret whose prefix matched.
                    capture.write_text("<redacted:stream-redaction-window-exceeded>\n")
                    pending = ""
                    suppress_until_newline = True
            if pending and not suppress_until_newline:
                capture.write_text(pending)
    finally:
        try:
            pipe.close()
        finally:
            capture.close()


def _task_dir_prefix(task_id: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", task_id).strip("._-") or "task"
    digest = hashlib.sha256(task_id.encode("utf-8")).hexdigest()[:16]
    return f"{slug[:48]}-{digest}-"


def _timeout_seconds(task: dict[str, Any], default_timeout_seconds: float, *, max_timeout_seconds: float = SAFE_TASK_TIMEOUT_SECONDS) -> float:
    return validate_timeout_seconds(task.get("timeout_seconds"), default_timeout_seconds, max_timeout_seconds=max_timeout_seconds)


def validate_worker_timing(*, visibility_timeout: int, heartbeat_seconds: int, task_timeout_seconds: float) -> None:
    if visibility_timeout <= 0 or visibility_timeout > SQS_MAX_VISIBILITY_SECONDS:
        raise ValueError(f"visibility_timeout must be in 1..{SQS_MAX_VISIBILITY_SECONDS} seconds")
    if heartbeat_seconds <= 0 or heartbeat_seconds >= visibility_timeout:
        raise ValueError("heartbeat_seconds must be positive and less than visibility_timeout")
    _timeout_seconds({}, task_timeout_seconds)


def _safe_attempt_component(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", s).strip(".-_")[:160] or "attempt"


def _new_attempt_id(task_id: str) -> str:
    parts = [os.environ.get("AWS_BATCH_JOB_ID", ""), os.environ.get("AWS_BATCH_JOB_ATTEMPT", ""), uuid.uuid4().hex]
    return _safe_attempt_component("-".join(p for p in parts if p) or f"local-{task_id}-{uuid.uuid4().hex}")


def _attempt_uri(logical_uri: str, attempt_id: str, leaf: str) -> str:
    if not logical_uri:
        return ""
    return f"{logical_uri.rstrip('/')}.attempts/{attempt_id}/{leaf}"


def _file_sha256_and_size(path: Path) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            size += len(block)
            h.update(block)
    return h.hexdigest(), size


def _signal_process_group(pid: int, sig: int) -> None:
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def _heartbeat(sqs, queue_url: str, receipt_handle: str, timeout: int, every: int, stop: threading.Event) -> None:
    while not stop.wait(max(1, every)):
        try:
            sqs.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=timeout,
            )
            _emit_event("lease_renewed", queue_url=queue_url, visibility_timeout=timeout, heartbeat_seconds=every)
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "schema": "spotbatch.heartbeat_error.v1",
                        "checked_at": iso_now(),
                        "queue_url": queue_url,
                        "visibility_timeout": timeout,
                        "heartbeat_seconds": every,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )


def _load_done_marker(s3, done_s3: str) -> dict[str, Any] | None:
    if not s3_exists(s3, done_s3):
        return None
    text = s3_download_text(s3, done_s3)
    try:
        marker = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"existing done marker is not valid JSON: {done_s3}") from exc
    if not isinstance(marker, dict):
        raise ValueError(f"existing done marker is not a JSON object: {done_s3}")
    return marker


def _head_matches_output_marker(s3, output: dict[str, Any], *, expected_uri: str, expected_task_hash: str, expected_attempt_id: str) -> None:
    uri = str(output.get("uri") or "")
    size_raw = output.get("size_bytes")
    if not isinstance(size_raw, int | str):
        raise ValueError("done marker output must include size_bytes")
    expected_size = int(size_raw)
    expected_sha = str(output.get("sha256") or "")
    if not uri or not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
        raise ValueError("done marker output must include uri and sha256")
    if uri != expected_uri:
        raise ValueError("done marker output uri does not match attempt id")
    try:
        head = s3_head_object(s3, uri)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
            raise ValueError("done marker output is missing") from exc
        raise
    content_length = head.get("ContentLength", expected_size)
    actual_size = int(content_length) if isinstance(content_length, int | str) else expected_size
    metadata_raw = head.get("Metadata") or {}
    metadata = {str(k).lower(): str(v) for k, v in metadata_raw.items()} if isinstance(metadata_raw, dict) else {}
    actual_sha = metadata.get("sha256")
    if actual_size != expected_size:
        raise ValueError(f"done marker output size mismatch for {uri}: marker={expected_size} s3={actual_size}")
    if actual_sha != expected_sha:
        raise ValueError(f"done marker output sha256 metadata mismatch for {uri}")
    if metadata.get("spotbatch-task-hash") != expected_task_hash:
        raise ValueError(f"done marker output task hash metadata mismatch for {uri}")
    if metadata.get("spotbatch-attempt-id") != expected_attempt_id:
        raise ValueError(f"done marker output attempt metadata mismatch for {uri}")


def validate_done_marker(s3, task: dict[str, Any], marker: dict[str, Any], expected_task_hash: str) -> None:
    run_id = str(task.get("run_id", ""))
    task_id = str(task.get("task_id", ""))
    output_s3 = str(task.get("output_s3") or "")
    schema = marker.get("schema")
    if schema == DONE_MARKER_SCHEMA_V1:
        # Backward-compatible legacy markers are accepted only after checking the
        # original identifiers and the canonical output object, if any. They do
        # not provide the v2 checksum/attempt guarantees.
        if marker.get("run_id") != run_id or marker.get("task_id") != task_id:
            raise ValueError("legacy done marker run_id/task_id mismatch")
        marker_output = str(marker.get("output_s3") or "")
        if output_s3 and marker_output != output_s3:
            raise ValueError("legacy done marker output_s3 mismatch")
        if output_s3 and not s3_exists(s3, output_s3):
            raise ValueError("legacy done marker output is missing")
        return
    if schema != DONE_MARKER_SCHEMA_V2:
        raise ValueError(f"unsupported done marker schema: {schema!r}")
    checks = {
        "run_id": run_id,
        "task_id": task_id,
        "task_hash": expected_task_hash,
        "output_s3": output_s3,
        "done_s3": default_done_s3(task),
    }
    for key, expected in checks.items():
        if str(marker.get(key) or "") != expected:
            raise ValueError(f"done marker {key} mismatch")
    attempt_id = str(marker.get("attempt_id") or "")
    if not attempt_id:
        raise ValueError("done marker missing attempt_id")
    output = marker.get("output")
    if output_s3:
        if not isinstance(output, dict):
            raise ValueError("done marker missing output record")
        if str(output.get("logical_uri") or "") != output_s3:
            raise ValueError("done marker output logical_uri mismatch")
        _head_matches_output_marker(s3, output, expected_uri=_attempt_uri(output_s3, attempt_id, "output"), expected_task_hash=expected_task_hash, expected_attempt_id=attempt_id)
    elif output is not None:
        raise ValueError("done marker has output record for task without output_s3")


def run_task(
    task: dict[str, Any],
    *,
    s3,
    work_root: Path,
    default_timeout_seconds: float = SAFE_TASK_TIMEOUT_SECONDS,
    allowed_s3_prefixes: list[str] | tuple[str, ...] | None = None,
    log_tail_bytes: int = DEFAULT_LOG_TAIL_BYTES,
    max_log_bytes: int = DEFAULT_MAX_LOG_BYTES,
    redact_regexes: Iterable[str] | str | None = None,
) -> dict[str, Any]:
    run_id = str(task.get("run_id", ""))
    task_id = str(task.get("task_id", ""))
    if not run_id or not task_id:
        raise ValueError("task requires run_id and task_id")
    validate_task_s3_prefixes(task, allowed_s3_prefixes)
    if log_tail_bytes < 0 or max_log_bytes < 0:
        raise ValueError("log_tail_bytes and max_log_bytes must be non-negative")
    redact_patterns = parse_redact_patterns(redact_regexes)
    done_s3 = default_done_s3(task)
    output_s3 = str(task.get("output_s3") or "")
    summary_s3 = str(task.get("summary_s3") or "")
    this_task_hash = task_hash(task)

    existing_marker = _load_done_marker(s3, done_s3)
    if existing_marker is not None:
        validate_done_marker(s3, task, existing_marker, this_task_hash)
        _emit_event("skip_existing_done", run_id=run_id, task_id=task_id, task_hash=this_task_hash, done_s3=done_s3)
        return {
            "event": "skip_existing_done",
            "run_id": run_id,
            "task_id": task_id,
            "task_hash": this_task_hash,
            "done_s3": done_s3,
            "checked_at": iso_now(),
        }

    validate_task_model(task, default_timeout_seconds=default_timeout_seconds, max_timeout_seconds=SAFE_TASK_TIMEOUT_SECONDS, allowed_s3_prefixes=allowed_s3_prefixes)
    command = task.get("command")
    assert isinstance(command, list)
    timeout = _timeout_seconds(task, default_timeout_seconds)
    attempt_id = _new_attempt_id(task_id)
    attempt_output_s3 = _attempt_uri(output_s3, attempt_id, "output") if output_s3 else ""
    attempt_summary_s3 = _attempt_uri(summary_s3, attempt_id, "summary.json") if summary_s3 else ""
    attempt_stdout_s3 = _attempt_uri(done_s3, attempt_id, "stdout.txt")
    attempt_stderr_s3 = _attempt_uri(done_s3, attempt_id, "stderr.txt")
    _emit_event(
        "task_started",
        run_id=run_id,
        task_id=task_id,
        task_hash=this_task_hash,
        attempt_id=attempt_id,
        timeout_seconds=timeout,
        output_s3=output_s3 or None,
        done_s3=done_s3,
    )

    work_root.mkdir(parents=True, exist_ok=True)
    with _tempfile.TemporaryDirectory(prefix=_task_dir_prefix(task_id), dir=work_root) as tmp_dir:
        task_dir = Path(tmp_dir)
        task_json = task_dir / "task.json"
        output_path = task_dir / "output"
        task_json.write_text(json.dumps(task, indent=2, sort_keys=True) + "\n")

        env = os.environ.copy()
        env.update(
            {
                "SPOTBATCH_TASK_JSON": str(task_json),
                "SPOTBATCH_TASK_ID": task_id,
                "SPOTBATCH_RUN_ID": run_id,
                "SPOTBATCH_TASK_HASH": this_task_hash,
                "SPOTBATCH_ATTEMPT_ID": attempt_id,
                "SPOTBATCH_OUTPUT_PATH": str(output_path),
                "SPOTBATCH_OUTPUT_S3": attempt_output_s3 or output_s3,
                "SPOTBATCH_SUMMARY_S3": attempt_summary_s3 or summary_s3,
                "SPOTBATCH_DONE_S3": done_s3,
            }
        )
        env.update(task_env_overrides(task))

        started = time.time()
        stdout_path = task_dir / "stdout.txt"
        stderr_path = task_dir / "stderr.txt"
        timed_out = False
        stdout_capture = _BoundedLogCapture(stdout_path, stream=sys.stdout, max_bytes=max_log_bytes, tail_bytes=log_tail_bytes, redact_patterns=redact_patterns)
        stderr_capture = _BoundedLogCapture(stderr_path, stream=sys.stderr, max_bytes=max_log_bytes, tail_bytes=log_tail_bytes, redact_patterns=redact_patterns)
        proc = subprocess.Popen(
            command,
            cwd=str(task_dir),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        assert proc.stdout is not None and proc.stderr is not None
        stdout_thread = threading.Thread(target=_pump_pipe, args=(proc.stdout, stdout_capture), daemon=True)
        stderr_thread = threading.Thread(target=_pump_pipe, args=(proc.stderr, stderr_capture), daemon=True)
        stdout_thread.start()
        stderr_thread.start()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            _emit_event("task_timeout", run_id=run_id, task_id=task_id, task_hash=this_task_hash, attempt_id=attempt_id, timeout_seconds=timeout)
            _signal_process_group(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
            _signal_process_group(proc.pid, signal.SIGKILL)
            proc.wait(timeout=10)
        if not timed_out:
            # Clean up background descendants that stayed in the task process group
            # after the top-level command exited.
            _signal_process_group(proc.pid, signal.SIGTERM)
            time.sleep(0.1)
            _signal_process_group(proc.pid, signal.SIGKILL)
        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)
        elapsed = time.time() - started
        stdout = stdout_capture.tail_text()
        stderr = stderr_capture.tail_text()
        _emit_event(
            "command_finished",
            run_id=run_id,
            task_id=task_id,
            task_hash=this_task_hash,
            attempt_id=attempt_id,
            returncode=proc.returncode,
            timed_out=timed_out,
            elapsed_sec=elapsed,
            stdout_bytes=stdout_capture.total_bytes,
            stderr_bytes=stderr_capture.total_bytes,
            stdout_truncated=stdout_capture.truncated_bytes > 0,
            stderr_truncated=stderr_capture.truncated_bytes > 0,
        )

        uploaded_output = False
        output_record: dict[str, Any] | None = None
        framework_error = None
        if timed_out:
            framework_error = f"task command timed out after {timeout:g}s"
        elif proc.returncode == 0 and output_s3:
            if output_path.is_file():
                sha256, size = _file_sha256_and_size(output_path)
                s3_upload_file(
                    s3,
                    output_path,
                    attempt_output_s3,
                    task.get("output_content_type"),
                    metadata={"sha256": sha256, "spotbatch-task-hash": this_task_hash, "spotbatch-attempt-id": attempt_id},
                )
                uploaded_output = True
                output_record = {"logical_uri": output_s3, "uri": attempt_output_s3, "size_bytes": size, "sha256": sha256}
                _emit_event("output_uploaded", run_id=run_id, task_id=task_id, task_hash=this_task_hash, attempt_id=attempt_id, uri=attempt_output_s3, size_bytes=size, sha256=sha256)
            else:
                framework_error = f"expected output file was not produced: {output_path}"

        summary = {
            "schema": "spotbatch.task_summary.v2",
            "run_id": run_id,
            "task_id": task_id,
            "task_hash": this_task_hash,
            "attempt_id": attempt_id,
            "finished_at": iso_now(),
            "elapsed_sec": elapsed,
            "returncode": proc.returncode,
            "timed_out": timed_out,
            "timeout_seconds": timeout,
            "command": command,
            "output_s3": output_s3,
            "attempt_output_s3": attempt_output_s3 or None,
            "summary_s3": summary_s3,
            "attempt_summary_s3": attempt_summary_s3 or None,
            "done_s3": done_s3,
            "attempt_stdout_s3": attempt_stdout_s3,
            "attempt_stderr_s3": attempt_stderr_s3,
            "uploaded_output": uploaded_output,
            "output": output_record,
            "framework_error": framework_error,
            "stdout_tail": stdout,
            "stderr_tail": stderr,
            "stdout_log": stdout_capture.summary(),
            "stderr_log": stderr_capture.summary(),
            "worker": {
                "hostname": os.environ.get("HOSTNAME"),
                "aws_batch_job_id": os.environ.get("AWS_BATCH_JOB_ID"),
                "aws_batch_job_attempt": os.environ.get("AWS_BATCH_JOB_ATTEMPT"),
                "ecs_container_metadata_uri_v4": os.environ.get("ECS_CONTAINER_METADATA_URI_V4"),
            },
        }
        if attempt_summary_s3:
            s3_upload_text(s3, json.dumps(summary, indent=2, sort_keys=True) + "\n", attempt_summary_s3)
            _emit_event("summary_uploaded", run_id=run_id, task_id=task_id, task_hash=this_task_hash, attempt_id=attempt_id, uri=attempt_summary_s3)
        # Attempt-scoped logs make postmortems possible even when a duplicate
        # attempt loses the conditional done-marker race. Logs are redacted and
        # capped before upload and while streaming to CloudWatch.
        if stdout_capture.stored_bytes:
            s3_upload_text(s3, stdout_capture.upload_text(), attempt_stdout_s3, "text/plain")
            _emit_event("log_uploaded", run_id=run_id, task_id=task_id, task_hash=this_task_hash, attempt_id=attempt_id, stream="stdout", uri=attempt_stdout_s3, **stdout_capture.summary())
        if stderr_capture.stored_bytes:
            s3_upload_text(s3, stderr_capture.upload_text(), attempt_stderr_s3, "text/plain")
            _emit_event("log_uploaded", run_id=run_id, task_id=task_id, task_hash=this_task_hash, attempt_id=attempt_id, stream="stderr", uri=attempt_stderr_s3, **stderr_capture.summary())

        if timed_out:
            raise subprocess.TimeoutExpired(command, timeout, output=stdout, stderr=stderr)
        if proc.returncode != 0:
            raise RuntimeError(f"task {task_id} failed rc={proc.returncode}")
        if framework_error:
            raise RuntimeError(f"task {task_id} failed framework validation: {framework_error}")

        done = {
            "schema": DONE_MARKER_SCHEMA_V2,
            "run_id": run_id,
            "task_id": task_id,
            "task_hash": this_task_hash,
            "attempt_id": attempt_id,
            "done_at": iso_now(),
            "done_s3": done_s3,
            "output_s3": output_s3,
            "summary_s3": summary_s3,
            "attempt_summary_s3": attempt_summary_s3 or None,
            "attempt_stdout_s3": attempt_stdout_s3,
            "attempt_stderr_s3": attempt_stderr_s3,
            "output": output_record,
            "returncode": proc.returncode,
            "elapsed_sec": elapsed,
        }
        committed = s3_upload_text_if_absent(s3, json.dumps(done, indent=2, sort_keys=True) + "\n", done_s3)
        if committed:
            _emit_event("commit_succeeded", run_id=run_id, task_id=task_id, task_hash=this_task_hash, attempt_id=attempt_id, done_s3=done_s3)
            return {"event": "processed", **done}

        _emit_event("commit_lost", run_id=run_id, task_id=task_id, task_hash=this_task_hash, attempt_id=attempt_id, done_s3=done_s3)
        winning_marker = _load_done_marker(s3, done_s3)
        if winning_marker is None:
            raise RuntimeError(f"conditional done marker write lost but no marker is readable: {done_s3}")
        validate_done_marker(s3, task, winning_marker, this_task_hash)
        return {
            "event": "commit_lost_existing_done",
            "run_id": run_id,
            "task_id": task_id,
            "task_hash": this_task_hash,
            "attempt_id": attempt_id,
            "winning_attempt_id": winning_marker.get("attempt_id"),
            "done_s3": done_s3,
            "checked_at": iso_now(),
        }


def run_worker(
    *,
    queue_url: str,
    max_messages: int,
    visibility_timeout: int,
    heartbeat_seconds: int,
    wait_time: int,
    work_dir: Path,
    task_timeout_seconds: float,
    allowed_s3_prefixes: list[str] | tuple[str, ...] | None = None,
    log_tail_bytes: int = DEFAULT_LOG_TAIL_BYTES,
    max_log_bytes: int = DEFAULT_MAX_LOG_BYTES,
    redact_regexes: Iterable[str] | str | None = None,
) -> int:
    validate_worker_timing(visibility_timeout=visibility_timeout, heartbeat_seconds=heartbeat_seconds, task_timeout_seconds=task_timeout_seconds)
    allowed_s3_prefixes = parse_allowed_s3_prefixes(allowed_s3_prefixes)
    redact_regexes = list(redact_regexes or []) if not isinstance(redact_regexes, str) else [redact_regexes]
    env_redact_regexes = os.environ.get("SPOTBATCH_REDACT_REGEXES", "")
    if env_redact_regexes:
        redact_regexes.extend([p for p in env_redact_regexes.splitlines() if p.strip()])
    parse_redact_patterns(redact_regexes)
    sqs = boto3.client("sqs")
    s3 = boto3.client("s3")
    work_dir.mkdir(parents=True, exist_ok=True)
    processed = 0
    while processed < max_messages:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=wait_time,
            VisibilityTimeout=visibility_timeout,
            AttributeNames=["ApproximateReceiveCount", "SentTimestamp"],
        )
        messages = resp.get("Messages", [])
        if not messages:
            break
        msg = messages[0]
        receipt = msg["ReceiptHandle"]
        stop = threading.Event()
        hb = threading.Thread(
            target=_heartbeat,
            args=(sqs, queue_url, receipt, visibility_timeout, heartbeat_seconds, stop),
            daemon=True,
        )
        hb.start()
        try:
            task = json.loads(msg.get("Body", "{}"))
            attrs = msg.get("Attributes", {})
            _emit_event(
                "message_received",
                queue_url=queue_url,
                run_id=task.get("run_id") if isinstance(task, dict) else None,
                task_id=task.get("task_id") if isinstance(task, dict) else None,
                receive_count=attrs.get("ApproximateReceiveCount"),
                sent_timestamp=attrs.get("SentTimestamp"),
                retry=str(attrs.get("ApproximateReceiveCount") or "1") != "1",
            )
            result = run_task(
                task,
                s3=s3,
                work_root=work_dir,
                default_timeout_seconds=task_timeout_seconds,
                allowed_s3_prefixes=allowed_s3_prefixes,
                log_tail_bytes=log_tail_bytes,
                max_log_bytes=max_log_bytes,
                redact_regexes=redact_regexes,
            )
            print(json.dumps(result, sort_keys=True), flush=True)
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            _emit_event("message_deleted", queue_url=queue_url, run_id=task.get("run_id"), task_id=task.get("task_id"), receipt_deleted=True)
            processed += 1
        except Exception as exc:
            _emit_event("task_failed", queue_url=queue_url, error_type=type(exc).__name__, error=str(exc))
            raise
        finally:
            stop.set()
    print(json.dumps({"schema": "spotbatch.worker_summary.v1", "processed": processed, "finished_at": iso_now()}), flush=True)
    return 0
