from __future__ import annotations

import argparse
import time
from typing import Any, Callable

from .aws_batch import queue_depth, utc_stamp
from .batch_service import safe_active_worker_count
from .controller import choose_worker_top_up


PersistReconcilePhase = Callable[..., None]


def run_worker_reconciliation(
    args: argparse.Namespace,
    *,
    sqs: Any,
    batch: Any,
    target: Any,
    sent: int,
    submitted_for_reconcile: list[dict[str, Any]],
    previous_reconcile_phase: dict[str, Any],
    plan_worker_count: int,
    plan_messages_per_worker: int,
    job_name_prefix: str,
    overrides: dict[str, Any],
    persist_phase: PersistReconcilePhase,
    sleep_func: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Run bounded production worker reconciliation and persist every unsafe edge.

    The caller owns the surrounding run report shape; this service owns the
    reconciliation state machine and calls ``persist_phase`` before and after
    each mutation that could otherwise become ambiguous on crash/resume.
    """

    previous_reconcile_status = previous_reconcile_phase.get("status")
    if previous_reconcile_status in {"job_in_flight", "needs_review"}:
        raise SystemExit("existing run_state.json has ambiguous reconciliation worker submission progress; review Batch jobs before retrying to avoid duplicate workers")
    reconcile_until_drained = bool(getattr(args, "reconcile_until_drained", False))
    rounds = max(1, int(getattr(args, "reconcile_rounds", 1)))
    if getattr(args, "kickoff_only", False):
        return {"name": "reconcile_workers", "status": "skipped", "reason": "kickoff_only"}

    previous_rounds_completed = int(previous_reconcile_phase.get("rounds_completed", 0) or 0) if previous_reconcile_status in {"in_progress", "completed"} else 0
    previous_drained = bool(previous_reconcile_phase.get("drained"))
    previous_until_drained = bool(previous_reconcile_phase.get("until_drained"))
    if previous_reconcile_status == "in_progress":
        if previous_until_drained and not reconcile_until_drained:
            raise SystemExit("existing run_state.json has an in-progress drain-watch reconciliation; resume with --reconcile-until-drained")
        if previous_rounds_completed >= rounds:
            raise SystemExit("existing run_state.json has in-progress reconciliation beyond the requested --reconcile-rounds; resume with a larger round limit")
    if previous_reconcile_status == "completed" and (previous_drained or previous_rounds_completed >= rounds):
        reconcile_phase = dict(previous_reconcile_phase)
        reconcile_phase["resumed"] = True
        return reconcile_phase

    reconcile_submitted: list[dict[str, Any]] = list(previous_reconcile_phase.get("submitted", [])) if isinstance(previous_reconcile_phase.get("submitted"), list) else []
    decisions: list[dict[str, Any]] = list(previous_reconcile_phase.get("decisions", [])) if isinstance(previous_reconcile_phase.get("decisions"), list) else []
    start_round = previous_rounds_completed
    reconcile_stamp = str(previous_reconcile_phase.get("submission_stamp") or utc_stamp())
    reconcile_phase = {
        "name": "reconcile_workers",
        "status": "completed" if start_round >= rounds else "in_progress",
        "submission_stamp": reconcile_stamp,
        "rounds_completed": min(start_round, rounds),
        "target_workers": plan_worker_count,
        "submitted_count": len(reconcile_submitted),
        "submitted": reconcile_submitted,
        "decisions": decisions,
        "drained": previous_drained,
        "until_drained": reconcile_until_drained,
    }
    for round_index in range(start_round, rounds):
        observed_depth = queue_depth(sqs, target.sqs_queue_url)
        if getattr(args, "dedicated_run_queue", False):
            observed_backlog = max(
                0,
                min(
                    sent,
                    int(observed_depth.get("visible", 0)) + int(observed_depth.get("not_visible", 0)) + int(observed_depth.get("delayed", 0)),
                ),
            )
            backlog_signal = "dedicated_queue_depth"
        else:
            # SQS depth is queue-global and may include other runs.  Never turn
            # unrelated shared-queue messages into this run's backlog estimate.
            # Without a run-specific status artifact, the safe shared-queue
            # signal is unassigned run work implied by the persisted task count
            # minus submitted worker message capacity.
            submitted_capacity = (len(submitted_for_reconcile) + len(reconcile_submitted)) * plan_messages_per_worker
            observed_backlog = max(0, sent - submitted_capacity)
            backlog_signal = "task_count_minus_submitted_capacity"
        fallback_active = len(submitted_for_reconcile) + len(reconcile_submitted)
        observed_active, active_examples, active_warning = safe_active_worker_count(
            batch,
            job_queue=target.batch_job_queue,
            job_name_prefix=job_name_prefix,
            fallback=fallback_active,
        )
        drained = bool(getattr(args, "dedicated_run_queue", False) and observed_backlog <= 0 and observed_active <= 0)
        top_up = choose_worker_top_up(backlog=observed_backlog, active_workers=observed_active, target_workers=plan_worker_count)
        decision: dict[str, Any] = {
            "round": round_index,
            "queue_depth": observed_depth,
            "run_backlog_estimate": observed_backlog,
            "run_backlog_signal": backlog_signal,
            "active_matching_workers": observed_active,
            "active_examples": active_examples,
            "target_workers": plan_worker_count,
            "top_up_workers": top_up,
            "drained": drained,
        }
        if active_warning:
            decision["warning"] = active_warning
        if top_up:
            if top_up > 1:
                decision["top_up_limit_reason"] = "one_top_up_per_reconciliation_round_keeps_crash_resume_idempotent"
                top_up = 1
            decision["submitting_top_up_workers"] = top_up
            for top_up_index in range(top_up):
                global_top_up_index = len(reconcile_submitted)
                job_name = f"{job_name_prefix}-reconcile-{reconcile_stamp}-r{round_index:02d}-{global_top_up_index:04d}"
                in_flight_phase = {
                    "name": "reconcile_workers",
                    "status": "job_in_flight",
                    "submission_stamp": reconcile_stamp,
                    "rounds_completed": round_index,
                    "target_workers": plan_worker_count,
                    "submitted_count": len(reconcile_submitted),
                    "submitted": reconcile_submitted,
                    "decisions": decisions + [decision],
                    "in_flight_round": round_index,
                    "in_flight_top_up_index": top_up_index,
                    "in_flight_job_name": job_name,
                }
                persist_phase(
                    in_flight_phase,
                    report_status="worker_reconcile_job_in_flight",
                    next_actions=["A reconciliation top-up submit_job call may have reached Batch; if this controller stops here, review Batch jobs before retrying."],
                )
                top_up_kwargs: dict[str, Any] = {
                    "jobName": job_name,
                    "jobQueue": target.batch_job_queue,
                    "jobDefinition": target.job_definition,
                    "containerOverrides": overrides,
                }
                if args.retry_attempts is not None:
                    top_up_kwargs["retryStrategy"] = {"attempts": args.retry_attempts}
                try:
                    resp = batch.submit_job(**top_up_kwargs)
                except Exception as exc:
                    review_phase = {**in_flight_phase, "status": "needs_review", "submit_error": str(exc)}
                    persist_phase(
                        review_phase,
                        report_status="worker_reconcile_needs_review",
                        next_actions=["A reconciliation top-up submit_job call failed or is ambiguous; review Batch jobs before retrying."],
                    )
                    raise
                reconcile_submitted.append({"jobName": job_name, "jobId": resp.get("jobId"), "jobArn": resp.get("jobArn"), "round": round_index, "reason": "reconcile_top_up"})
                decision["submitted_top_up_workers"] = int(decision.get("submitted_top_up_workers", 0) or 0) + 1
                partial_phase = {
                    "name": "reconcile_workers",
                    "status": "in_progress",
                    "submission_stamp": reconcile_stamp,
                    "rounds_completed": round_index + 1,
                    "target_workers": plan_worker_count,
                    "submitted_count": len(reconcile_submitted),
                    "submitted": reconcile_submitted,
                    "decisions": decisions + [decision],
                    "drained": False,
                    "until_drained": reconcile_until_drained,
                }
                persist_phase(
                    partial_phase,
                    report_status="worker_reconcile_in_progress",
                    next_actions=["A reconciliation top-up worker was submitted and durably recorded; rerun the same command to continue with the next reconciliation round if this controller stops."],
                )
            decision["submitted_top_up_workers"] = top_up
        decisions.append(decision)
        phase_drained = drained or any(bool(d.get("drained")) for d in decisions)
        phase_status = "completed" if (round_index + 1 >= rounds or (reconcile_until_drained and drained)) else "in_progress"
        reconcile_phase = {
            "name": "reconcile_workers",
            "status": phase_status,
            "submission_stamp": reconcile_stamp,
            "rounds_completed": round_index + 1,
            "target_workers": plan_worker_count,
            "submitted_count": len(reconcile_submitted),
            "submitted": reconcile_submitted,
            "decisions": decisions,
            "drained": phase_drained,
            "until_drained": reconcile_until_drained,
        }
        if reconcile_until_drained and drained:
            reconcile_phase["stop_reason"] = "drained"
        elif reconcile_until_drained and round_index + 1 >= rounds:
            reconcile_phase["stop_reason"] = "round_limit_before_drain"
        persist_phase(
            reconcile_phase,
            report_status="worker_reconcile_in_progress" if reconcile_phase["status"] == "in_progress" else "worker_reconcile_complete",
            next_actions=["Worker reconciliation observed queue depth/active Batch jobs and submitted bounded top-ups when active capacity was below the Plan target."],
        )
        if reconcile_until_drained and drained:
            break
        if round_index + 1 < rounds and float(getattr(args, "reconcile_interval_seconds", 0.0)) > 0:
            sleep_func(float(args.reconcile_interval_seconds))
    if not decisions:
        reconcile_phase = {"name": "reconcile_workers", "status": "completed", "rounds_completed": 0, "submitted_count": 0, "submitted": [], "decisions": [], "until_drained": reconcile_until_drained}
    return reconcile_phase
