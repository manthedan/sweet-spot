from __future__ import annotations

import hashlib
import json
import math
import os
import re
import signal
import subprocess
import threading
from tempfile import TemporaryDirectory
import time
from pathlib import Path
from typing import Any

import boto3

from .aws_batch import iso_now
from .s3util import s3_exists, s3_upload_file, s3_upload_text


def _tail(s: str, n: int = 12000) -> str:
    return s[-n:]


def _task_dir_prefix(task_id: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", task_id).strip("._-") or "task"
    digest = hashlib.sha256(task_id.encode("utf-8")).hexdigest()[:16]
    return f"{slug[:48]}-{digest}-"


def _timeout_seconds(task: dict[str, Any], default_timeout_seconds: float) -> float:
    raw = task.get("timeout_seconds", default_timeout_seconds)
    try:
        timeout = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("timeout_seconds must be a positive number") from exc
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout_seconds must be a positive finite number")
    return timeout


def default_done_s3(task: dict[str, Any]) -> str:
    if task.get("done_s3"):
        return str(task["done_s3"])
    output = str(task.get("output_s3") or "")
    if not output:
        raise ValueError("task needs done_s3 or output_s3")
    return output.replace("/shards/", "/done/") + ".done.json"


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
        except Exception:
            # Best effort; if this fails, SQS will make the task visible again.
            pass


def run_task(
    task: dict[str, Any],
    *,
    s3,
    work_root: Path,
    default_timeout_seconds: float = 24 * 60 * 60,
) -> dict[str, Any]:
    run_id = str(task.get("run_id", ""))
    task_id = str(task.get("task_id", ""))
    if not run_id or not task_id:
        raise ValueError("task requires run_id and task_id")
    done_s3 = default_done_s3(task)
    output_s3 = str(task.get("output_s3") or "")
    summary_s3 = str(task.get("summary_s3") or "")

    if s3_exists(s3, done_s3):
        return {
            "event": "skip_existing_done",
            "run_id": run_id,
            "task_id": task_id,
            "done_s3": done_s3,
            "checked_at": iso_now(),
        }

    command = task.get("command")
    if not isinstance(command, list) or not all(isinstance(x, str) for x in command) or not command:
        raise ValueError("task requires command: list[str]")
    timeout = _timeout_seconds(task, default_timeout_seconds)

    work_root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=_task_dir_prefix(task_id), dir=work_root) as tmp_dir:
        task_dir = Path(tmp_dir)
        task_json = task_dir / "task.json"
        output_path = task_dir / "output"
        task_json.write_text(json.dumps(task, indent=2, sort_keys=True) + "\n")

        env = os.environ.copy()
        env.update({
            "SPOTBATCH_TASK_JSON": str(task_json),
            "SPOTBATCH_TASK_ID": task_id,
            "SPOTBATCH_RUN_ID": run_id,
            "SPOTBATCH_OUTPUT_PATH": str(output_path),
            "SPOTBATCH_OUTPUT_S3": output_s3,
            "SPOTBATCH_SUMMARY_S3": summary_s3,
            "SPOTBATCH_DONE_S3": done_s3,
        })
        for k, v in dict(task.get("env") or {}).items():
            env[str(k)] = str(v)

        started = time.time()
        stdout_path = task_dir / "stdout.txt"
        stderr_path = task_dir / "stderr.txt"
        timed_out = False
        with stdout_path.open("w+", encoding="utf-8") as stdout_fh, stderr_path.open("w+", encoding="utf-8") as stderr_fh:
            proc = subprocess.Popen(
                command,
                cwd=str(task_dir),
                env=env,
                text=True,
                stdout=stdout_fh,
                stderr=stderr_fh,
                start_new_session=True,
            )
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
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
            stdout_fh.flush()
            stderr_fh.flush()
            stdout_fh.seek(0)
            stderr_fh.seek(0)
            stdout = stdout_fh.read()
            stderr = stderr_fh.read()
        elapsed = time.time() - started

        uploaded_output = False
        framework_error = None
        if timed_out:
            framework_error = f"task command timed out after {timeout:g}s"
        elif proc.returncode == 0 and output_s3:
            if output_path.is_file():
                s3_upload_file(s3, output_path, output_s3, task.get("output_content_type"))
                uploaded_output = True
            else:
                framework_error = f"expected output file was not produced: {output_path}"

        summary = {
            "schema": "spotbatch.task_summary.v1",
            "run_id": run_id,
            "task_id": task_id,
            "finished_at": iso_now(),
            "elapsed_sec": elapsed,
            "returncode": proc.returncode,
            "timed_out": timed_out,
            "timeout_seconds": timeout,
            "command": command,
            "output_s3": output_s3,
            "summary_s3": summary_s3,
            "done_s3": done_s3,
            "uploaded_output": uploaded_output,
            "framework_error": framework_error,
            "stdout_tail": _tail(stdout),
            "stderr_tail": _tail(stderr),
            "worker": {
                "hostname": os.environ.get("HOSTNAME"),
                "aws_batch_job_id": os.environ.get("AWS_BATCH_JOB_ID"),
                "aws_batch_job_attempt": os.environ.get("AWS_BATCH_JOB_ATTEMPT"),
                "ecs_container_metadata_uri_v4": os.environ.get("ECS_CONTAINER_METADATA_URI_V4"),
            },
        }
        if summary_s3:
            s3_upload_text(s3, json.dumps(summary, indent=2, sort_keys=True) + "\n", summary_s3)

        if timed_out:
            raise subprocess.TimeoutExpired(command, timeout, output=stdout, stderr=stderr)
        if proc.returncode != 0:
            raise RuntimeError(f"task {task_id} failed rc={proc.returncode}")
        if framework_error:
            raise RuntimeError(f"task {task_id} failed framework validation: {framework_error}")

        done = {
            "schema": "spotbatch.done_marker.v1",
            "run_id": run_id,
            "task_id": task_id,
            "done_at": iso_now(),
            "output_s3": output_s3,
            "summary_s3": summary_s3,
            "returncode": proc.returncode,
            "elapsed_sec": elapsed,
        }
        s3_upload_text(s3, json.dumps(done, indent=2, sort_keys=True) + "\n", done_s3)
        return {"event": "processed", **done}


def run_worker(
    *,
    queue_url: str,
    max_messages: int,
    visibility_timeout: int,
    heartbeat_seconds: int,
    wait_time: int,
    work_dir: Path,
    task_timeout_seconds: float,
) -> int:
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
            result = run_task(task, s3=s3, work_root=work_dir, default_timeout_seconds=task_timeout_seconds)
            print(json.dumps(result, sort_keys=True), flush=True)
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            processed += 1
        finally:
            stop.set()
    print(json.dumps({"schema": "spotbatch.worker_summary.v1", "processed": processed, "finished_at": iso_now()}), flush=True)
    return 0
