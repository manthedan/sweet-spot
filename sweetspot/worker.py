"""The SQS worker: lease a task, run it, commit it, repeat until the queue is drained.

Runs inside AWS Batch (one container per array child) but works anywhere with credentials, e.g. to lend a
workstation to a cloud run: `sweetspot worker --run NAME`.

Exit codes: 0 drained, 3 gave up after repeated infrastructure errors, 75 interrupted (Spot notice or SIGTERM;
the job definition's retry strategy replaces the worker).
"""

from __future__ import annotations

import json
import os
import signal
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from . import protocol
from .runner import run_task
from .store import Store
from .tasks import task_hash, validate

LEASE_SECONDS = 300  # visibility per lease; a dead worker's task reappears within this long
HEARTBEAT_SECONDS = 60
LEASE_JOIN_SECONDS = 5
POLL_SECONDS = 20
MAX_BACKOFF = 900
# Quiet time before the sweeper leaves without a complete ledger: longer than any message can stay invisible (a
# failure backoff plus a lease), so anything still in the queue must have been received during it.
SWEEPER_GRACE = MAX_BACKOFF + LEASE_SECONDS + 120
EXIT_GAVE_UP = 3
EXIT_INTERRUPTED = 75


def log(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, "at": protocol.now_iso(), **fields}, sort_keys=True, default=str), file=sys.stderr, flush=True)


class Lease:
    """Keeps one SQS message invisible from receipt until it is acknowledged or released."""

    def __init__(self, sqs: Any, queue_url: str, receipt: str, lease: int, every: int) -> None:
        self.sqs, self.queue_url, self.receipt, self.lease, self.every = sqs, queue_url, receipt, lease, every
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._beat, daemon=True)
        self._thread.start()

    def _beat(self) -> None:
        while not self._stop.wait(self.every):
            try:
                self.sqs.change_message_visibility(QueueUrl=self.queue_url, ReceiptHandle=self.receipt, VisibilityTimeout=self.lease)
            except Exception as exc:  # noqa: BLE001 - losing the lease only risks a duplicate attempt, which the marker absorbs
                log("lease_renew_failed", error=str(exc))

    def _halt(self) -> None:
        # Join so a renewal already in flight cannot land after (and undo) the release below. Bounded, because a
        # Spot shutdown has little time; if a renewal is stuck, the release may be undone and the task simply
        # reappears when that lease expires.
        self._stop.set()
        self._thread.join(LEASE_JOIN_SECONDS)

    def release(self, delay: int) -> None:
        """Hand the message back: visible again after `delay` seconds."""
        self._halt()
        try:
            self.sqs.change_message_visibility(QueueUrl=self.queue_url, ReceiptHandle=self.receipt, VisibilityTimeout=max(0, delay))
        except Exception as exc:  # noqa: BLE001 - worst case it reappears when the current lease expires
            log("lease_release_failed", error=str(exc))

    def ack(self) -> None:
        self._halt()
        self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=self.receipt)


def _watch_spot_notice(stop: threading.Event) -> None:
    """A Spot interruption notice usually arrives ~2 minutes before reclaim: stop early and hand the task back."""
    while not stop.wait(5):
        if protocol._imds("spot/instance-action"):
            log("spot_interruption_notice")
            stop.set()


def _queue_gone(exc: ClientError) -> bool:
    return "NonExistentQueue" in str(exc.response.get("Error", {}).get("Code", ""))


def run_worker(
    *,
    sqs: Any,
    queue_url: str,
    prefix: str,
    store: Store,
    work_dir: Path | None = None,
    idle_polls: int = 2,
    sweeper: bool = False,
    max_infra_errors: int = 5,
    max_tasks: int = 0,
    require_output: bool = True,
    lease: int = LEASE_SECONDS,
    heartbeat: int = HEARTBEAT_SECONDS,
    poll_seconds: int = POLL_SECONDS,
    sweeper_grace: float = SWEEPER_GRACE,
    lease_sqs: Any = None,
    stop: threading.Event | None = None,
) -> int:
    """Process messages until the queue stays empty. Returns a process exit code.

    The sweeper does not leave while messages are in flight or delayed elsewhere (running, backing off after a
    failure, or held by a worker that died): those come back, and someone has to be there to take them. SQS's
    counts can lag, so it leaves only once every task has a done marker, or after receiving nothing for longer than
    any message can stay invisible (sweeper_grace).

    lease_sqs: an SQS client with short timeouts for lease renewals and releases (defaults to sqs).
    """
    lease_sqs = lease_sqs or sqs
    quiet_since: float | None = None
    last_ledger_check = 0.0
    stop = stop or threading.Event()
    worker = protocol.worker_info()
    if worker.get("instance_type"):
        threading.Thread(target=_watch_spot_notice, args=(stop,), daemon=True).start()
    log("worker_started", queue_url=queue_url, prefix=prefix, sweeper=sweeper, **worker)
    idle = infra_errors = processed = 0
    while not stop.is_set():
        if max_tasks and processed >= max_tasks:
            break
        try:
            resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=poll_seconds, VisibilityTimeout=lease, MessageSystemAttributeNames=["ApproximateReceiveCount"])
        except ClientError as exc:
            if _queue_gone(exc):
                log("queue_deleted")  # the run was finished or cancelled
                return 0
            raise
        messages = resp.get("Messages") or []
        if not messages:
            if sweeper:
                if _outstanding(sqs, queue_url) > 0:
                    quiet_since = None
                    continue
                now = time.monotonic()
                if now - last_ledger_check >= 60:
                    last_ledger_check = now
                    if _all_done(store, prefix):
                        break
                quiet_since = quiet_since or now
                if now - quiet_since < sweeper_grace:
                    continue
                break
            idle += 1
            if idle >= idle_polls:
                break
            continue
        idle, quiet_since = 0, None
        msg = messages[0]
        receives = int(msg.get("Attributes", {}).get("ApproximateReceiveCount", "1"))
        lease_ = Lease(lease_sqs, queue_url, msg["ReceiptHandle"], lease, heartbeat)
        if stop.is_set():
            lease_.release(0)
            break
        try:
            task = validate(json.loads(msg["Body"]))
        except ValueError as exc:
            log("bad_message", error=str(exc), receives=receives, body=msg["Body"][:500])
            lease_.release(MAX_BACKOFF)  # bounces until the DLQ, where an operator can read it
            continue
        outcome = _handle(task, store=store, prefix=prefix, worker=worker, lease_=lease_, receives=receives, stop=stop, work_dir=work_dir, require_output=require_output)
        processed += 1
        if outcome == "interrupted":
            break
        if outcome == "infra_error":
            infra_errors += 1
            if infra_errors >= max_infra_errors:
                log("worker_giving_up", consecutive_infra_errors=infra_errors)
                return EXIT_GAVE_UP
        else:
            infra_errors = 0
    log("worker_exiting", processed=processed, interrupted=stop.is_set())
    return EXIT_INTERRUPTED if stop.is_set() else 0


def _all_done(store: Store, prefix: str) -> bool:
    """True when every task in the run's ledger has a done marker (run.json is absent outside `submit` runs)."""
    try:
        ident = protocol.read_json(store, f"{prefix.rstrip('/')}/run.json")
        if not isinstance(ident, dict) or not ident.get("task_count"):
            return False
        done = sum(1 for uri, _s, _m in store.list(f"{prefix.rstrip('/')}/done/") if uri.endswith(".json"))
        return done >= int(ident["task_count"])
    except Exception as exc:  # noqa: BLE001 - fall back to the quiet-period rule
        log("ledger_check_failed", error=str(exc))
        return False


def _outstanding(sqs: Any, queue_url: str) -> int:
    attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"])["Attributes"]
    return sum(int(attrs.get(k, 0)) for k in ("ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"))


def _handle(task: dict[str, Any], *, store: Store, prefix: str, worker: dict[str, Any], lease_: Lease, receives: int, stop: threading.Event, work_dir: Path | None, require_output: bool) -> str:
    """Returns committed | duplicate | skipped | failed | infra_error | interrupted."""
    task_id, thash = task["id"], task_hash(task)
    attempt = protocol.new_attempt_id()
    try:
        marker = protocol.read_json(store, protocol.done_uri(prefix, task_id))
        if marker is not None:
            protocol.check_marker(marker, task_id, thash)
            log("already_done", task_id=task_id)
            lease_.ack()
            return "skipped"
        log("task_started", task_id=task_id, attempt=attempt, receives=receives)
        with tempfile.TemporaryDirectory(prefix="sweetspot-", dir=work_dir) as tmp:
            result = run_task(task, store=store, task_dir=Path(tmp) / "task", stop=stop, require_output=require_output)
            if result.status == "interrupted":
                lease_.release(0)  # first, so another worker can start on it right away
                log("task_interrupted", task_id=task_id, attempt=attempt)
                result.error = "worker stopped (Spot notice or SIGTERM)"
                try:  # best effort while the host goes away; scout counts these seconds as replay
                    protocol.record_attempt(store, prefix, task, thash, attempt, result, worker, {"receives": receives})
                except Exception:  # noqa: BLE001
                    pass
                return "interrupted"
            outcome = protocol.record_attempt(store, prefix, task, thash, attempt, result, worker, {"receives": receives})
    except Exception as exc:  # noqa: BLE001 - storage/protocol errors fail this attempt, not the worker
        error = f"{type(exc).__name__}: {exc}"
        log("attempt_error", task_id=task_id, attempt=attempt, error=error)
        protocol.record_failure(store, prefix, task_id, attempt, "error", error, worker)
        lease_.release(_backoff(receives))
        return "infra_error"
    log("task_finished", task_id=task_id, attempt=attempt, outcome=outcome, status=result.status, elapsed=round(result.elapsed, 1), error=result.error)
    if outcome == "failed":
        lease_.release(_backoff(receives))
        # A command that ran and failed says nothing bad about this host; staging/spawn errors might.
        return "infra_error" if result.status == "error" else "failed"
    lease_.ack()
    return outcome


def _backoff(receives: int) -> int:
    return min(MAX_BACKOFF, 30 * 2 ** max(0, receives - 1))


def install_signal_handlers(stop: threading.Event) -> None:
    def handler(signum: int, _frame: Any) -> None:
        log("signal", signal=signal.Signals(signum).name)
        stop.set()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def is_sweeper() -> bool:
    """Array child 0 (or a non-array job) is the sweeper."""
    return os.environ.get("AWS_BATCH_JOB_ARRAY_INDEX", "0") == "0"
