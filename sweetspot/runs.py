"""Cloud runs: submit, status, finish, cancel, attempts.

Everything about a run lives under its S3 prefix, so any machine with credentials and the stack config can
inspect, resume, or finish it:

    run.json              immutable identity: name, ledger sha256, task count (conditional create)
    tasks.jsonl           the ledger (conditional create)
    launches/<id>.json    one record per worker launch (image, shape, lane, Batch job id)
    enqueues/<id>.json    one record per enqueue wave, marked complete when every message was accepted
    manifest.jsonl, READY published by `finish` (both conditional creates)

AWS resources are named from the project and run (queue "<name>-<run>", DLQ "<name>-<run>_dlq", job definition
"<name>-<run>", jobs "<name>-<run>--<launch>") and tagged sweetspot-project/sweetspot-run; nothing is adopted,
counted or deleted unless its tags say it belongs to this run.
"""

from __future__ import annotations

import concurrent.futures as cf
import hashlib
import json
import math
import re
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from botocore.exceptions import ClientError

from . import protocol
from .config import Config
from .store import Store, join
from .tasks import dumps_tasks, read_tasks, task_hash, validate

# Lowercase (Batch job-name filters are case-insensitive), no "_" (reserved for the DLQ suffix), no "--" and no
# trailing "-" (reserved for the job-name separator).
RUN_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9]|-(?!-)){0,46}[a-z0-9]$|^[a-z0-9]$")
MAX_BATCH_BYTES = 256 * 1024  # SQS limit for one message and for a whole SendMessageBatch (the older, lower limit)
ACTIVE_JOB_STATES = {"SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"}


class RunError(RuntimeError):
    pass


@dataclass
class Clients:
    store: Store
    sqs: Any
    batch: Any

    @classmethod
    def from_config(cls, cfg: Config, profile: str | None = None) -> "Clients":
        import boto3

        session = boto3.Session(profile_name=profile, region_name=cfg.region)
        return cls(Store(session=session), session.client("sqs"), session.client("batch"))


@dataclass
class Run:
    cfg: Config
    name: str

    @property
    def prefix(self) -> str:
        return self.cfg.run_prefix(self.name)

    @property
    def base(self) -> str:
        return f"{self.cfg.name}-{self.name}"

    @property
    def queue_name(self) -> str:
        return self.base

    @property
    def dlq_name(self) -> str:
        return f"{self.base}_dlq"

    @property
    def tags(self) -> dict[str, str]:
        return {"sweetspot-project": self.cfg.name, "sweetspot-run": self.name}

    def uri(self, *parts: str) -> str:
        return join(self.prefix, *parts)

    def owns(self, tags: dict[str, Any] | None) -> bool:
        return all((tags or {}).get(k) == v for k, v in self.tags.items())


def _code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def check_run_name(run: str) -> None:
    if not RUN_NAME_RE.fullmatch(run):
        raise RunError(f"run names are 1-48 lowercase letters, digits and single hyphens, e.g. 'labels-v2' (got {run!r})")


def _json_bytes(obj: Any) -> bytes:
    return (json.dumps(obj, indent=1, sort_keys=True) + "\n").encode()


def load_ledger(c: Clients, r: Run) -> list[dict[str, Any]]:
    data = c.store.get(r.uri("tasks.jsonl"))
    if data is None:
        raise RunError(f"no task ledger at {r.uri('tasks.jsonl')}")
    return [validate(json.loads(line)) for line in data.decode().splitlines() if line.strip()]


def load_identity(c: Clients, r: Run) -> dict[str, Any]:
    ident = protocol.read_json(c.store, r.uri("run.json"))
    if ident is None:
        raise RunError(f"no run {r.name!r} under {r.cfg.prefix}")
    return ident


def records(c: Clients, r: Run, kind: str) -> list[dict[str, Any]]:
    """launches/ or enqueues/ records, oldest first (their keys start with a nanosecond timestamp)."""
    out = []
    for uri, _size, _mtime in sorted(c.store.list(r.uri(kind) + "/")):
        rec = protocol.read_json(c.store, uri)
        if isinstance(rec, dict):
            out.append(rec)
    return out


def _record_key(kind: str) -> str:
    return f"{kind}/{time.time_ns():020d}-{uuid.uuid4().hex[:8]}.json"


def done_ids(c: Clients, r: Run) -> dict[str, float]:
    """task id -> marker mtime, from one listing of done/."""
    out: dict[str, float] = {}
    for uri, _size, mtime in c.store.list(r.uri("done") + "/"):
        name = uri.rsplit("/", 1)[-1]
        if name.endswith(".json"):
            out[name[: -len(".json")]] = mtime
    return out


def failure_records(c: Clients, r: Run) -> dict[str, list[tuple[float, str]]]:
    """task id -> [(mtime, failure record uri)], for every recorded failed or interrupted attempt."""
    out: dict[str, list[tuple[float, str]]] = {}
    root = r.uri("failures") + "/"
    for uri, _size, mtime in c.store.list(root):
        out.setdefault(uri[len(root) :].split("/", 1)[0], []).append((mtime, uri))
    return out


# --- queues ------------------------------------------------------------------------------------------------


def queue_url(c: Clients, r: Run, name: str) -> str | None:
    """URL of one of this run's queues; refuses a same-named queue that belongs to something else."""
    try:
        url = c.sqs.get_queue_url(QueueName=name)["QueueUrl"]
    except ClientError as exc:
        if "NonExistentQueue" in _code(exc):
            return None
        raise
    if not r.owns(c.sqs.list_queue_tags(QueueUrl=url).get("Tags")):
        raise RunError(f"SQS queue {name} exists but is not tagged as run {r.name!r} of project {r.cfg.name!r}; refusing to use it")
    return url


def _create_queue(c: Clients, r: Run, name: str, attrs: dict[str, str]) -> str:
    try:
        url = c.sqs.create_queue(QueueName=name, Attributes=attrs, tags=r.tags)["QueueUrl"]
    except ClientError as exc:
        if "QueueDeletedRecently" in _code(exc):
            raise RunError(f"queue {name} was deleted less than 60 s ago; SQS needs a minute before it can be recreated") from exc
        raise
    time.sleep(1)  # SQS asks for a short pause before using a new queue
    return url


def ensure_queues(c: Clients, r: Run, max_attempts: int) -> tuple[str, str]:
    dlq = queue_url(c, r, r.dlq_name) or _create_queue(c, r, r.dlq_name, {"MessageRetentionPeriod": "1209600", "SqsManagedSseEnabled": "true"})
    dlq_arn = c.sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    redrive_policy = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": str(max_attempts)})
    url = queue_url(c, r, r.queue_name)
    if url is None:
        attrs = {
            "VisibilityTimeout": "300",
            "MessageRetentionPeriod": "1209600",  # same as the DLQ's: moved messages keep their original enqueue time
            "ReceiveMessageWaitTimeSeconds": "20",
            "SqsManagedSseEnabled": "true",
            "RedrivePolicy": redrive_policy,
        }
        url = _create_queue(c, r, r.queue_name, attrs)
    else:
        c.sqs.set_queue_attributes(QueueUrl=url, Attributes={"RedrivePolicy": redrive_policy})
    return url, dlq


def queue_counts(c: Clients, url: str | None) -> dict[str, int]:
    if not url:
        return {}
    try:
        attrs = c.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"])["Attributes"]
    except ClientError as exc:
        if "NonExistentQueue" in _code(exc):
            return {}
        raise
    return {
        "visible": int(attrs.get("ApproximateNumberOfMessages", 0)),
        "in_flight": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        "delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0)),
    }


def _batches(bodies: list[str]) -> Iterable[list[str]]:
    """Up to 10 messages per batch, and under the batch byte limit."""
    batch: list[str] = []
    size = 0
    for body in bodies:
        n = len(body.encode())
        if n > MAX_BATCH_BYTES:
            raise RunError(f"task message is {n} bytes; SQS allows {MAX_BATCH_BYTES}: {body[:120]}...")
        if batch and (len(batch) == 10 or size + n > MAX_BATCH_BYTES):
            yield batch
            batch, size = [], 0
        batch.append(body)
        size += n
    if batch:
        yield batch


def send(c: Clients, url: str, bodies: list[str], threads: int = 16) -> int:
    def send_batch(batch: list[str]) -> None:
        entries = [{"Id": str(i), "MessageBody": b} for i, b in enumerate(batch)]
        for attempt in range(6):
            resp = c.sqs.send_message_batch(QueueUrl=url, Entries=entries)
            failed = {f["Id"] for f in resp.get("Failed", [])}
            if not failed:
                return
            entries = [e for e in entries if e["Id"] in failed]
            time.sleep(0.5 * 2**attempt)
        raise RunError(f"SQS rejected {len(entries)} messages after retries")

    with cf.ThreadPoolExecutor(max_workers=threads) as pool:
        list(pool.map(send_batch, list(_batches(bodies))))
    return len(bodies)


def enqueue(c: Clients, url: str, tasks: Iterable[dict[str, Any]]) -> int:
    return send(c, url, [json.dumps(t, sort_keys=True, separators=(",", ":")) for t in tasks])


def redrive(c: Clients, dlq: str, queue: str) -> int:
    """Move every message from the DLQ back to the work queue (each starts a fresh retry budget)."""
    moved = 0
    while True:
        msgs = c.sqs.receive_message(QueueUrl=dlq, MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=60).get("Messages", [])
        if not msgs:
            return moved
        send(c, queue, [m["Body"] for m in msgs])
        entries = [{"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)]
        for attempt in range(4):
            failed = {f["Id"] for f in c.sqs.delete_message_batch(QueueUrl=dlq, Entries=entries).get("Failed", [])}
            entries = [e for e in entries if e["Id"] in failed]
            if not entries:
                break
            time.sleep(0.5 * 2**attempt)
        if entries:
            # Already re-sent; a leftover DLQ copy would be retried again later. Say so rather than hide it.
            raise RunError(f"moved {moved + len(msgs)} DLQ message(s) but could not delete {len(entries)} of them from the DLQ; rerun `submit --retry` later")
        moved += len(msgs)


# --- control fence -----------------------------------------------------------------------------------------
# `cancel` must win over a `submit` that was already running. Each cancel leaves a uniquely named record; submit
# notes which ones exist when it starts and stops before creating queues or launching workers if a new one appears.
# No clocks involved.


def _cancels(c: Clients, r: Run) -> set[str]:
    return {uri for uri, _s, _m in c.store.list(r.uri("control") + "/") if uri.rsplit("/", 1)[-1].startswith("cancel-")}


def _check_not_cancelled(c: Clients, r: Run, seen: set[str]) -> None:
    if _cancels(c, r) - seen:
        raise RunError(f"run {r.name!r} was cancelled while this submit was running; nothing more was started")


# --- workers -----------------------------------------------------------------------------------------------


def worker_jobs(c: Clients, r: Run) -> list[dict[str, Any]]:
    """This run's worker jobs: found by name in every lane's job queue, kept only if their tags match the run."""
    found: dict[str, dict[str, Any]] = {}
    for lane in r.cfg.lanes.values():
        for page in c.batch.get_paginator("list_jobs").paginate(jobQueue=lane.job_queue, filters=[{"name": "JOB_NAME", "values": [f"{r.base}--*"]}]):
            for job in page.get("jobSummaryList", []):
                if ":" not in job["jobId"]:  # skip array children ("<parent>:<index>"); counted via the parent
                    found[job["jobId"]] = {"job_id": job["jobId"], "name": job.get("jobName"), "lane": lane.name}
    ids, out = list(found), []
    for i in range(0, len(ids), 100):
        for job in c.batch.describe_jobs(jobs=ids[i : i + 100]).get("jobs", []):
            if not r.owns(job.get("tags")):
                continue
            summary = (job.get("arrayProperties") or {}).get("statusSummary")
            out.append({**found[job["jobId"]], "status": job["status"], "children": {k: v for k, v in (summary or {job["status"]: 1}).items() if v}})
    return out


def active_workers(jobs: list[dict[str, Any]]) -> int:
    return sum(n for j in jobs for state, n in (j.get("children") or {}).items() if state in ACTIVE_JOB_STATES)


def start_workers(c: Clients, r: Run, queue: str, shape: dict[str, Any], workers: int) -> dict[str, Any]:
    cfg = r.cfg
    if not cfg.job_role_arn:
        raise RunError("config has no job_role_arn; workers would have no AWS permissions")
    if not 1 <= workers <= 10_000:
        raise RunError("workers must be between 1 and 10,000 (the AWS Batch array limit)")
    lane = cfg.lane(shape["lane"])
    if shape.get("gpus") and not lane.gpu:
        raise RunError(f"lane {lane.name!r} is not a GPU lane (set gpu = true for it in the stack); cannot run --gpus {shape['gpus']}")
    resources = [{"type": "VCPU", "value": f"{shape['vcpus']:g}"}, {"type": "MEMORY", "value": str(int(shape["memory"]))}]
    if shape.get("gpus"):
        resources.append({"type": "GPU", "value": str(int(shape["gpus"]))})
    env = {
        "SWEETSPOT_QUEUE_URL": queue,
        "SWEETSPOT_PREFIX": r.prefix,
        "SWEETSPOT_REQUIRE_OUTPUT": "1" if shape.get("require_output", True) else "0",
        "SWEETSPOT_IMAGE": shape["image"],
        "SWEETSPOT_LANE": lane.name,
        "SWEETSPOT_VCPUS": f"{shape['vcpus']:g}",
        "SWEETSPOT_MEMORY_MIB": str(int(shape["memory"])),
        "SWEETSPOT_GPUS": str(int(shape.get("gpus") or 0)),
        "AWS_DEFAULT_REGION": cfg.region,
        "PYTHONUNBUFFERED": "1",
    }
    container: dict[str, Any] = {
        "image": shape["image"],
        "command": ["sweetspot", "worker"],
        "jobRoleArn": cfg.job_role_arn,
        "resourceRequirements": resources,
        "environment": [{"name": k, "value": v} for k, v in env.items()],
    }
    if cfg.log_group:
        container["logConfiguration"] = {"logDriver": "awslogs", "options": {"awslogs-group": cfg.log_group, "awslogs-stream-prefix": r.name}}
    launch_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:6]
    jd = c.batch.register_job_definition(
        jobDefinitionName=r.base,
        type="container",
        containerProperties=container,
        # Replace workers lost to Spot reclaim (host gone, or the worker saw the notice and exited 75). Any other
        # worker exit is final; SQS redelivery retries the task itself.
        retryStrategy={
            "attempts": 5,
            "evaluateOnExit": [
                {"onStatusReason": "Host EC2*", "action": "RETRY"},
                {"onExitCode": "75", "action": "RETRY"},
                {"onReason": "*", "action": "EXIT"},
            ],
        },
        propagateTags=True,
        tags=r.tags,
    )
    kwargs: dict[str, Any] = {"jobName": f"{r.base}--{launch_id}", "jobQueue": lane.job_queue, "jobDefinition": jd["jobDefinitionArn"], "tags": r.tags}
    if workers > 1:
        kwargs["arrayProperties"] = {"size": workers}
    resp = c.batch.submit_job(**kwargs)
    launch = {"launch_id": launch_id, "at": protocol.now_iso(), "job_id": resp["jobId"], "workers": workers, "job_definition": jd["jobDefinitionArn"], **shape}
    protocol.put_json(c.store, r.uri(_record_key("launches")), launch)
    return launch


# --- submit ------------------------------------------------------------------------------------------------


def _claim(c: Clients, r: Run, uri: str, data: bytes, what: str) -> None:
    """Create uri with data, or accept it if it already holds exactly data (a concurrent or earlier submit)."""
    if not c.store.put_if_absent(uri, data) and c.store.get(uri) != data:
        raise RunError(f"run {r.name!r} already has a different {what}; task lists are fixed per run, so use a new run name")


def submit(
    c: Clients,
    cfg: Config,
    run: str,
    tasks_path: Path | None,
    *,
    image: str | None = None,
    vcpus: float | None = None,
    memory: int | None = None,
    gpus: int | None = None,
    workers: int | None = None,
    lane: str | None = None,
    max_attempts: int = 5,
    require_output: bool = True,
    retry: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Create or resume a run: claim its identity, enqueue what is not done, start workers.

    Safe to repeat. A run's task list is fixed when it is created; passing a different list is an error.
    Enqueueing happens when no enqueue wave has completed yet, or when the queue and DLQ are both empty while tasks
    remain (their messages were lost, e.g. after `cancel`). Tasks in the DLQ go back only with retry=True.
    """
    check_run_name(run)
    r = Run(cfg, run)
    seen_cancels = _cancels(c, r)  # first: any cancel after this point stops this submit
    tasks = read_tasks(tasks_path) if tasks_path else None
    ident = protocol.read_json(c.store, r.uri("run.json"))
    if ident is None and tasks is None:
        raise RunError(f"run {run!r} does not exist yet; pass a tasks file")
    ledger = tasks if tasks is not None else load_ledger(c, r)
    ledger_data = dumps_tasks(ledger).encode()
    ledger_sha = hashlib.sha256(ledger_data).hexdigest()
    if ident is not None and ledger_sha != ident["ledger_sha256"]:
        raise RunError(f"{tasks_path or 'tasks.jsonl'} differs from the task list run {run!r} was created with; task lists are fixed per run, so use a new run name")
    if c.store.exists(r.uri("READY")):
        raise RunError(f"run {run!r} is finished (READY exists)")

    launches = records(c, r, "launches") if ident else []
    last = launches[-1] if launches else {}
    shape = {
        "image": image or last.get("image"),
        "vcpus": vcpus or last.get("vcpus") or 1,
        "memory": memory or last.get("memory") or 1536,
        "gpus": gpus if gpus is not None else last.get("gpus", 0),
        "lane": cfg.lane(lane or last.get("lane")).name,
        "require_output": require_output,
    }
    done = done_ids(c, r) if ident else {}
    pending = [t for t in ledger if t["id"] not in done]
    warnings = []
    if last.get("image") and shape["image"] != last["image"]:
        warnings.append(f"image changed from {last['image']} to {shape['image']}: tasks already done keep their old outputs. " "If the new image changes results (not just a crash fix), start a new run instead.")
    plan: dict[str, Any] = {"run": run, "prefix": r.prefix, "new_run": ident is None, "tasks": len(ledger), "done": len(done), "pending": len(pending), **shape}
    if dry_run:
        return {**plan, "workers": workers if workers is not None else min(len(pending), 10), "warnings": warnings, "dry_run": True}
    if (workers is None or workers > 0) and pending and not shape["image"]:
        raise RunError("--image is required to start workers")
    if shape["gpus"] and not cfg.lane(str(shape["lane"])).gpu:
        raise RunError(f"lane {shape['lane']!r} is not a GPU lane (set gpu = true for it in the stack); cannot run --gpus {shape['gpus']}")

    if ident is None:
        _claim(c, r, r.uri("tasks.jsonl"), ledger_data, "task list")
        new_ident = {"run": run, "created_at": protocol.now_iso(), "prefix": r.prefix, "task_count": len(ledger), "ledger_sha256": ledger_sha}
        if not c.store.put_if_absent(r.uri("run.json"), _json_bytes(new_ident)):
            if (protocol.read_json(c.store, r.uri("run.json")) or {}).get("ledger_sha256") != ledger_sha:
                raise RunError(f"run {run!r} was just created by someone else with a different task list")
    queue, dlq = ensure_queues(c, r, max_attempts)
    _check_not_cancelled(c, r, seen_cancels)

    waves = records(c, r, "enqueues")
    counts, dlq_depth = queue_counts(c, queue), queue_counts(c, dlq).get("visible", 0)
    queue_empty = not any(counts.values())
    enqueued = redriven = 0
    notes = []
    if not pending:
        notes.append(f"every task is done; next: sweetspot finish {run}")
    elif not waves or not waves[-1].get("complete"):
        enqueued = _enqueue_wave(c, r, queue, pending)  # first enqueue, or finishing one that was interrupted
    else:
        if dlq_depth and retry:
            redriven = redrive(c, dlq, queue)
        elif dlq_depth:
            notes.append(f"{dlq_depth} task(s) failed {max_attempts} times and are in the DLQ: inspect `sweetspot logs {run} TASK_ID`, fix, then `sweetspot submit {run} --retry`")
        if queue_empty and not dlq_depth:
            enqueued = _enqueue_wave(c, r, queue, pending)  # their messages are gone (cancelled, expired)
        elif not queue_empty and not redriven:
            notes.append("the queue still holds this run's tasks; nothing re-enqueued")

    if workers is None:
        workers = min(len(pending), 10) if (enqueued or redriven or not queue_empty) else 0
    launch = None
    if workers > 0 and pending:
        if c.store.exists(r.uri("READY")):  # finished while we were enqueueing
            raise RunError(f"run {run!r} was finished concurrently; not starting workers")
        _check_not_cancelled(c, r, seen_cancels)
        launch = start_workers(c, r, queue, shape, workers)
    return {**plan, "enqueued": enqueued, "redriven": redriven, "dlq": dlq_depth, "launch": launch, "notes": notes, "warnings": warnings}


def _enqueue_wave(c: Clients, r: Run, queue: str, tasks: list[dict[str, Any]]) -> int:
    uri = r.uri(_record_key("enqueues"))
    wave = {"at": protocol.now_iso(), "count": len(tasks), "complete": False}
    protocol.put_json(c.store, uri, wave)
    n = enqueue(c, queue, tasks)
    protocol.put_json(c.store, uri, {**wave, "complete": True, "completed_at": protocol.now_iso()})
    return n


# --- status ------------------------------------------------------------------------------------------------


def status(c: Clients, cfg: Config, run: str, *, failures_shown: int = 5) -> dict[str, Any]:
    r = Run(cfg, run)
    ident = load_identity(c, r)
    total = int(ident["task_count"])
    done = done_ids(c, r)
    remaining = total - len(done)
    now = time.time()
    rate = sum(1 for t in done.values() if now - t <= 600) / 600

    latest_failure = {task_id: max(recs) for task_id, recs in failure_records(c, r).items() if task_id not in done}
    rows = []
    for _mtime, uri in sorted(latest_failure.values(), reverse=True):
        rec = protocol.read_json(c.store, uri) or {}
        if rec.get("status") == "interrupted":
            continue  # handed back by a stopping worker; not the task's fault
        rows.append({k: rec.get(k) for k in ("task_id", "attempt", "status", "error", "stderr_tail")})
        if len(rows) >= failures_shown:
            break

    queue, dlq = queue_url(c, r, r.queue_name), queue_url(c, r, r.dlq_name)
    counts = queue_counts(c, queue)
    dlq_depth = queue_counts(c, dlq).get("visible", 0)
    jobs = worker_jobs(c, r)
    active = active_workers(jobs)

    if c.store.exists(r.uri("READY")):
        state, hint = "finished", f"outputs: {r.uri('out')}/ (manifest: {r.uri('manifest.jsonl')})"
    elif remaining <= 0:
        state, hint = "complete", f"sweetspot finish {run}"
    elif active:
        state, hint = "running", (f"{dlq_depth} task(s) in the DLQ: sweetspot logs {run} TASK_ID" if dlq_depth else "")
    elif dlq_depth and not any(counts.values()):
        state, hint = "failed", f"{dlq_depth} task(s) failed every attempt: inspect `sweetspot logs {run} TASK_ID`, fix, then `sweetspot submit {run} --retry`"
    elif any(counts.values()):
        state, hint = "stalled", f"no workers but {sum(counts.values())} task(s) queued: sweetspot submit {run} --workers N"
    else:
        state, hint = "stalled", f"{remaining} task(s) neither done nor queued: sweetspot submit {run}"
    return {
        "run": run,
        "prefix": r.prefix,
        "state": state,
        "next": hint,
        "tasks": total,
        "done": len(done),
        "with_failed_attempts": len(latest_failure),
        "queue": counts,
        "dlq": dlq_depth,
        "active_workers": active,
        "worker_jobs": jobs,
        "done_per_minute": round(rate * 60, 2),
        "eta_minutes": round(remaining / rate / 60, 1) if rate and remaining > 0 else None,
        "recent_failures": rows,
    }


# --- finish / cancel ---------------------------------------------------------------------------------------


def _units(value: Any) -> float:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0 else 0.0


def finish(c: Clients, cfg: Config, run: str, *, keep_attempts: bool = False, verify_hashes: bool = False, threads: int = 32) -> dict[str, Any]:
    """Verify every task, repair interrupted promotions, publish manifest.jsonl + READY, release run resources.

    Publication is write-once: if manifest.jsonl or READY already exist they must match what this finish computed,
    otherwise nothing is written and an error explains why. verify_hashes reads every output back and checks it
    against the sha256 in its done marker (slow for big runs; sizes are always checked).
    """
    r = Run(cfg, run)
    ident = load_identity(c, r)
    ledger = load_ledger(c, r)
    if hashlib.sha256(dumps_tasks(ledger).encode()).hexdigest() != ident["ledger_sha256"]:
        raise RunError("tasks.jsonl does not match run.json; refusing to publish")
    done = done_ids(c, r)
    missing = [t["id"] for t in ledger if t["id"] not in done]
    if missing:
        return {"run": run, "complete": False, "tasks": len(ledger), "done": len(ledger) - len(missing), "missing": missing}

    present = {uri: size for uri, size, _ in c.store.list(r.uri("out") + "/")}
    staged = {uri for uri, _s, _m in c.store.list(r.uri("attempts") + "/")}
    published = c.store.exists(r.uri("READY"))

    def check(task: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """Make out/<id>/ match the marker. Before publication, copy the winner's staged files again while they
        exist (a size check cannot see same-length damage). Once READY exists, out/ is only ever checked."""
        marker = protocol.read_json(c.store, protocol.done_uri(r.prefix, task["id"]))
        protocol.check_marker(marker, task["id"], task_hash(task))
        want = {protocol.out_uri(r.prefix, task["id"], f["path"]): f for f in marker["files"]}
        sizes_ok = all(present.get(uri) == f["size"] for uri, f in want.items())
        has_staged = all(protocol.attempt_uri(r.prefix, task["id"], marker["attempt"], f"out/{f['path']}") in staged for f in marker["files"])
        copied = False
        if not published and has_staged:
            protocol.promote(c.store, r.prefix, marker)
            copied = True
        elif not sizes_ok:
            raise RunError(f"task {task['id']}: out/ does not match its done marker" + (" (run already published; not overwriting)" if published else " and the winning attempt's staged files are gone"))
        if copied:
            now = {uri: size for uri, size, _ in c.store.list(protocol.out_uri(r.prefix, task["id"]) + "/")}
            if any(now.get(uri) != f["size"] for uri, f in want.items()):
                raise RunError(f"task {task['id']}: outputs in out/ still do not match its done marker after copying")
        if verify_hashes:
            bad = [uri for uri, f in want.items() if c.store.sha256(uri) != f["sha256"]]
            if bad:
                raise RunError(f"task {task['id']}: sha256 of {bad[0]} does not match its done marker")
        return marker, copied and not sizes_ok

    with cf.ThreadPoolExecutor(max_workers=threads) as pool:
        checked = list(pool.map(check, ledger))
    lines = []
    for marker, _ in checked:
        files = [{"path": f["path"], "uri": protocol.out_uri(r.prefix, marker["task_id"], f["path"]), "size": f["size"], "sha256": f["sha256"]} for f in marker["files"]]
        image = (marker.get("worker") or {}).get("image")
        lines.append(json.dumps({"id": marker["task_id"], "files": files, "attempt": marker["attempt"], "image": image, "elapsed": marker.get("elapsed"), "units": marker.get("units")}, sort_keys=True))
    manifest = ("\n".join(lines) + "\n").encode()  # deterministic: ledger order, immutable markers
    images = Counter((m.get("worker") or {}).get("image") or "unknown" for m, _ in checked)
    ready = {
        "run": run,
        "ledger_sha256": ident["ledger_sha256"],
        "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
        "tasks": len(ledger),
        "files": sum(len(m["files"]) for m, _ in checked),
        "bytes": sum(f["size"] for m, _ in checked for f in m["files"]),
        "units": sum(_units(m.get("units")) for m, _ in checked) or None,
        "images": dict(images),
        "published_at": protocol.now_iso(),
    }
    existing = protocol.read_json(c.store, r.uri("READY"))
    restored_manifest = False
    if existing is None:
        if not c.store.put_if_absent(r.uri("manifest.jsonl"), manifest) and c.store.get(r.uri("manifest.jsonl")) != manifest:
            raise RunError("manifest.jsonl exists with different contents (was finish run with a different config prefix?); not publishing")
        if not c.store.put_if_absent(r.uri("READY"), _json_bytes(ready)):
            existing = protocol.read_json(c.store, r.uri("READY"))
    if existing is not None:
        if (existing.get("ledger_sha256"), existing.get("manifest_sha256")) != (ready["ledger_sha256"], ready["manifest_sha256"]):
            raise RunError("READY already exists for a different manifest; investigate before re-publishing")
        ready = existing
        if c.store.get(r.uri("manifest.jsonl")) != manifest:  # READY vouches for this exact content; put it back
            c.store.put(r.uri("manifest.jsonl"), manifest, "application/x-ndjson")
            restored_manifest = True

    cleanup = release(c, r)
    if not keep_attempts:
        root = r.uri("attempts") + "/"
        c.store.delete_many([uri for uri, _s, _m in c.store.list(root) if uri[len(root) :].split("/")[2:3] == ["out"]])
    warnings = [f"outputs come from {len(images)} different images: {dict(images)}"] if len(images) > 1 else []
    if restored_manifest:
        warnings.append("manifest.jsonl was missing or altered; restored it from the done markers")
    return {"run": run, "complete": True, "repaired_promotions": sum(1 for _, b in checked if b), "cleanup": cleanup, "warnings": warnings, **ready}


def release(c: Clients, r: Run) -> dict[str, Any]:
    """Stop the run's workers, then delete its queues and job definitions. Idempotent."""
    terminated = []
    for job in worker_jobs(c, r):
        if job["status"] in ACTIVE_JOB_STATES or any(s in ACTIVE_JOB_STATES for s in job.get("children", {})):
            c.batch.terminate_job(jobId=job["job_id"], reason="sweetspot: run released")
            terminated.append(job["job_id"])
    deleted = []
    for name in (r.queue_name, r.dlq_name):
        url = queue_url(c, r, name)
        if url:
            c.sqs.delete_queue(QueueUrl=url)
            deleted.append(name)
    deregistered = 0
    for page in c.batch.get_paginator("describe_job_definitions").paginate(jobDefinitionName=r.base, status="ACTIVE"):
        for jd in page.get("jobDefinitions", []):
            if r.owns(jd.get("tags")):
                c.batch.deregister_job_definition(jobDefinition=jd["jobDefinitionArn"])
                deregistered += 1
    return {"terminated_jobs": terminated, "deleted_queues": deleted, "deregistered_job_definitions": deregistered}


def cancel(c: Clients, cfg: Config, run: str) -> dict[str, Any]:
    """Stop a run. Outputs and markers stay; `sweetspot submit RUN` resumes it later."""
    r = Run(cfg, run)
    load_identity(c, r)
    protocol.put_json(c.store, r.uri("control", f"cancel-{time.time_ns()}-{uuid.uuid4().hex[:8]}.json"), {"at": protocol.now_iso()})
    return {"run": run, **release(c, r)}


def attempts(c: Clients, cfg: Config, run: str, task_id: str) -> list[dict[str, Any]]:
    """Every attempt of one task, oldest first: its summary, or its failure record if no summary was written."""
    r = Run(cfg, run)
    found: dict[str, dict[str, Any]] = {}
    for _mtime, uri in sorted(failure_records(c, r).get(task_id, [])):
        found[uri.rsplit("/", 1)[-1][: -len(".json")]] = protocol.read_json(c.store, uri) or {}
    root = r.uri("attempts", task_id) + "/"
    for uri, _s, _m in c.store.list(root):
        attempt, leaf = uri[len(root) :].split("/", 1)
        found.setdefault(attempt, {"attempt": attempt})
        if leaf == "summary.json":
            found[attempt].update(protocol.read_json(c.store, uri) or {})
        elif leaf in ("stdout.txt", "stderr.txt"):
            found[attempt].setdefault("logs", {})[leaf] = uri
    return [found[a] for a in sorted(found)]
