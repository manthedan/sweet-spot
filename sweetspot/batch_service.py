from __future__ import annotations

from typing import Any

from .aws_batch import active_jobs, desired_worker_count, utc_stamp
from .task_model import parse_allowed_s3_prefixes
from .worker import parse_redact_patterns


def safe_active_worker_count(batch: Any, *, job_queue: str, job_name_prefix: str, fallback: int) -> tuple[int, list[dict[str, Any]], dict[str, Any] | None]:
    try:
        active = active_jobs(batch, job_queue, job_name_prefix)
        return len(active), active[:20], None
    except Exception as exc:  # noqa: BLE001
        return fallback, [], {"code": "active_worker_observation_unavailable", "severity": "warning", "message": str(exc)}


def redact_env(env: list[dict[str, str]]) -> list[dict[str, str]]:
    return [{"name": str(x.get("name", "")), "value": "<redacted>"} for x in env]


def worker_overrides(
    *,
    sqs_queue_url: str,
    messages_per_worker: int,
    visibility_timeout: int,
    heartbeat_seconds: int,
    task_timeout_seconds: float,
    env: list[dict[str, str]],
    allowed_s3_prefixes: list[str] | tuple[str, ...] | None,
    log_tail_bytes: int | None = None,
    max_log_bytes: int | None = None,
    redact_regexes: list[str] | tuple[str, ...] | None = None,
    allow_legacy_done_markers: bool = False,
    vcpus: int | None = None,
    memory: int | None = None,
) -> dict[str, Any]:
    base_env = [
        {"name": "SWEETSPOT_SQS_QUEUE_URL", "value": sqs_queue_url},
        {"name": "SWEETSPOT_MAX_MESSAGES", "value": str(messages_per_worker)},
        {"name": "SWEETSPOT_VISIBILITY_TIMEOUT", "value": str(visibility_timeout)},
        {"name": "SWEETSPOT_HEARTBEAT_SECONDS", "value": str(heartbeat_seconds)},
        {"name": "SWEETSPOT_TASK_TIMEOUT_SECONDS", "value": str(task_timeout_seconds)},
    ]
    if vcpus is not None:
        base_env.append({"name": "SWEETSPOT_WORKER_VCPUS", "value": str(vcpus)})
    if memory is not None:
        base_env.append({"name": "SWEETSPOT_WORKER_MEMORY_MIB", "value": str(memory)})
    normalized_prefixes = parse_allowed_s3_prefixes(allowed_s3_prefixes)
    if normalized_prefixes:
        base_env.append({"name": "SWEETSPOT_ALLOWED_S3_PREFIXES", "value": ",".join(normalized_prefixes)})
    if log_tail_bytes is not None:
        base_env.append({"name": "SWEETSPOT_LOG_TAIL_BYTES", "value": str(log_tail_bytes)})
    if max_log_bytes is not None:
        base_env.append({"name": "SWEETSPOT_MAX_LOG_BYTES", "value": str(max_log_bytes)})
    if redact_regexes:
        parse_redact_patterns(redact_regexes)
        base_env.append({"name": "SWEETSPOT_REDACT_REGEXES", "value": "\n".join(redact_regexes)})
    if allow_legacy_done_markers:
        base_env.append({"name": "SWEETSPOT_ALLOW_LEGACY_DONE_MARKERS", "value": "1"})
    base_env.extend(env or [])
    overrides: dict[str, Any] = {"environment": base_env}
    if vcpus is not None:
        overrides["vcpus"] = vcpus
    if memory is not None:
        overrides["memory"] = memory
    return overrides


def submit_worker_jobs(
    batch,
    *,
    count: int,
    job_name_prefix: str,
    batch_job_queue: str,
    job_definition: str,
    overrides: dict[str, Any],
    retry_attempts: int | None,
) -> list[dict[str, Any]]:
    submitted = []
    stamp = utc_stamp()
    for i in range(count):
        job_name = f"{job_name_prefix}-{stamp}-{i:04d}"
        kwargs: dict[str, Any] = {
            "jobName": job_name,
            "jobQueue": batch_job_queue,
            "jobDefinition": job_definition,
            "containerOverrides": overrides,
        }
        if retry_attempts is not None:
            kwargs["retryStrategy"] = {"attempts": retry_attempts}
        resp = batch.submit_job(**kwargs)
        submitted.append({"jobName": job_name, "jobId": resp.get("jobId"), "jobArn": resp.get("jobArn")})
    return submitted


def supervisor_desired_workers(*, backlog: int, messages_per_worker: int, target_active_workers: int, max_active_workers: int, keep_full_pool: bool) -> int:
    if backlog <= 0 and not keep_full_pool:
        return 0
    desired = target_active_workers if keep_full_pool else min(target_active_workers, desired_worker_count(backlog, messages_per_worker, 0, target_active_workers))
    return min(desired, max_active_workers)
