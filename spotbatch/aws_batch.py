from __future__ import annotations

import math
import time
from typing import Any

ACTIVE_STATUSES = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]


def utc_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def queue_depth(sqs, queue_url: str) -> dict[str, int]:
    resp = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )
    attrs = resp.get("Attributes", {})
    return {
        "visible": int(attrs.get("ApproximateNumberOfMessages", 0)),
        "not_visible": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        "delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0)),
    }


def active_jobs(batch, job_queue: str, job_name_prefix: str) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for status in ACTIVE_STATUSES:
        paginator = batch.get_paginator("list_jobs")
        for page in paginator.paginate(jobQueue=job_queue, jobStatus=status):
            for job in page.get("jobSummaryList", []):
                if job_name_prefix and not str(job.get("jobName", "")).startswith(job_name_prefix):
                    continue
                jobs.append({
                    "jobId": job.get("jobId"),
                    "jobName": job.get("jobName"),
                    "status": status,
                    "createdAt": job.get("createdAt"),
                })
    return jobs


def desired_worker_count(backlog: int, messages_per_worker: int, min_workers: int, max_workers: int) -> int:
    raw = math.ceil(backlog / messages_per_worker) if backlog > 0 else 0
    if backlog > 0:
        raw = max(raw, min_workers)
    return min(raw, max_workers)
