#!/usr/bin/env python3
"""Dry-run/apply multi-region Spot worker lane submissions for SQS-backed jobs.

A lane is an AWS Batch queue/job definition in some region that reads from a central
(or regional) SQS queue. This script does not create infrastructure; it allocates a
bounded number of worker jobs across pre-existing lanes.

Example config:
{
  "sqs_queue_url": "https://sqs.us-west-2.amazonaws.com/ACCOUNT/sweetspot-work",
  "instance_types": ["c6i.large", "c6a.large", "m6i.large", "m6a.large"],
  "lanes": [
    {
      "name": "us-west-2-x86",
      "region": "us-west-2",
      "batch_job_queue": "arn:aws:batch:us-west-2:ACCOUNT:job-queue/sweetspot-cpu-spot-queue",
      "job_definition": "arn:aws:batch:us-west-2:ACCOUNT:job-definition/sweetspot-worker:1",
      "job_name_prefix": "my-run-worker",
      "max_workers": 128,
      "messages_per_worker": 4,
      "vcpus": 2,
      "memory": 4096,
      "min_placement_score": 7,
      "expected_total_cost_per_1m_units": 0.42
    }
  ]
}
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

ACTIVE_STATUSES = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]


def utc_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def queue_depth(sqs, queue_url: str) -> dict[str, int]:
    resp = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"],
    )
    attrs = resp.get("Attributes", {})
    return {
        "visible": int(attrs.get("ApproximateNumberOfMessages", 0)),
        "not_visible": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        "delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0)),
    }


def active_jobs(batch, job_queue: str, prefix: str) -> int:
    total = 0
    for status in ACTIVE_STATUSES:
        paginator = batch.get_paginator("list_jobs")
        for page in paginator.paginate(jobQueue=job_queue, jobStatus=status):
            total += sum(1 for j in page.get("jobSummaryList", []) if str(j.get("jobName", "")).startswith(prefix))
    return total


def placement_score(ec2_home, lane: dict[str, Any], instance_types: list[str], target_vcpus: int) -> int | None:
    if not instance_types or target_vcpus <= 0:
        return None
    try:
        resp = ec2_home.get_spot_placement_scores(
            InstanceTypes=instance_types,
            TargetCapacity=target_vcpus,
            TargetCapacityUnitType="vcpu",
            RegionNames=[lane["region"]],
        )
        vals = resp.get("SpotPlacementScores", [])
        return int(vals[0]["Score"]) if vals else None
    except ClientError:
        return None


def lane_expected_cost(lane: dict[str, Any]) -> float | None:
    raw = lane.get("expected_total_cost_per_1m_units", lane.get("expected_cost_per_1m_units"))
    if raw is None:
        return None
    try:
        cost = float(raw)
    except (TypeError, ValueError):
        return None
    return cost if cost >= 0 else None


def submit_jobs(batch, lane: dict[str, Any], sqs_queue_url: str, count: int, dry_run: bool) -> list[dict[str, str | None]]:
    if dry_run or count <= 0:
        return []
    env = [
        {"name": "SWEETSPOT_SQS_QUEUE_URL", "value": sqs_queue_url},
        {"name": "SWEETSPOT_MAX_MESSAGES", "value": str(lane.get("messages_per_worker", 1))},
    ]
    for k, v in (lane.get("env") or {}).items():
        env.append({"name": str(k), "value": str(v)})
    overrides: dict[str, Any] = {"environment": env}
    if lane.get("vcpus") is not None:
        overrides["vcpus"] = int(lane["vcpus"])
    if lane.get("memory") is not None:
        overrides["memory"] = int(lane["memory"])
    stamp = utc_stamp()
    out = []
    for i in range(count):
        name = f"{lane['job_name_prefix']}-{stamp}-{i:04d}"
        resp = batch.submit_job(
            jobName=name,
            jobQueue=lane["batch_job_queue"],
            jobDefinition=lane["job_definition"],
            containerOverrides=overrides,
        )
        out.append({"jobName": name, "jobId": resp.get("jobId"), "jobArn": resp.get("jobArn")})
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", type=Path, required=True)
    ap.add_argument("--profile")
    ap.add_argument("--home-region", default="us-west-2")
    ap.add_argument("--target-workers", type=int, help="Total desired active workers across lanes; default=sum lane max_workers")
    ap.add_argument("--include-not-visible", action="store_true")
    ap.add_argument("--submit", action="store_true")
    args = ap.parse_args()

    cfg = json.loads(args.config.read_text())
    lanes = cfg.get("lanes") or []
    if not lanes:
        raise SystemExit("config has no lanes")
    sqs_queue_url = cfg["sqs_queue_url"]
    session = boto3.Session(profile_name=args.profile)
    sqs_region = sqs_queue_url.split(".")[1] if "." in sqs_queue_url else args.home_region
    sqs = session.client("sqs", region_name=sqs_region)
    depth = queue_depth(sqs, sqs_queue_url)
    backlog = depth["visible"] + (depth["not_visible"] if args.include_not_visible else 0)
    target_workers = args.target_workers if args.target_workers is not None else sum(int(l.get("max_workers", 0)) for l in lanes)
    target_workers = min(target_workers, math.ceil(backlog / max(1, min(int(l.get("messages_per_worker", 1)) for l in lanes)))) if backlog else 0

    ec2_home = session.client("ec2", region_name=args.home_region)
    instance_types = cfg.get("instance_types") or []
    scored_lanes = []
    for index, lane in enumerate(lanes):
        batch = session.client("batch", region_name=lane["region"])
        active = active_jobs(batch, lane["batch_job_queue"], lane["job_name_prefix"])
        max_workers = int(lane.get("max_workers", 0))
        lane_target_vcpus = max_workers * int(lane.get("vcpus", 2))
        score = placement_score(ec2_home, lane, instance_types, lane_target_vcpus)
        min_score = int(lane.get("min_placement_score", 0))
        allow_unknown_score = bool(lane.get("allow_unknown_placement_score", False))
        eligible = (score is not None and score >= min_score) or (score is None and (min_score <= 0 or allow_unknown_score))
        expected_cost = lane_expected_cost(lane)
        scored_lanes.append(
            {"index": index, "lane": lane, "batch": batch, "active": active, "score": score, "min_score": min_score, "allow_unknown_score": allow_unknown_score, "eligible": eligible, "expected_cost": expected_cost}
        )
    scored_lanes.sort(key=lambda x: (not bool(x["eligible"]), x["expected_cost"] is None, x["expected_cost"] if x["expected_cost"] is not None else math.inf, -(x["score"] or -1), x["index"]))

    total_active = sum(int(x["active"]) for x in scored_lanes)
    remaining = max(0, target_workers - total_active)
    lane_reports = []
    for allocation_index, scored in enumerate(scored_lanes):
        lane = scored["lane"]
        batch = scored["batch"]
        active = int(scored["active"])
        max_workers = int(lane.get("max_workers", 0))
        score = scored["score"]
        min_score = scored["min_score"]
        eligible = bool(scored["eligible"])
        lane_capacity = max(0, max_workers - active)
        to_submit = min(lane_capacity, remaining) if eligible else 0
        desired_for_lane = active + to_submit
        remaining = max(0, remaining - to_submit)
        submitted = submit_jobs(batch, lane, sqs_queue_url, to_submit, dry_run=not args.submit)
        lane_reports.append(
            {
                "name": lane.get("name"),
                "region": lane["region"],
                "placement_score": score,
                "min_placement_score": min_score,
                "allocation_order": allocation_index,
                "expected_total_cost_per_1m_units": scored["expected_cost"],
                "eligible": eligible,
                "allow_unknown_placement_score": bool(scored.get("allow_unknown_score")),
                "active": active,
                "max_workers": max_workers,
                "desired_for_lane": desired_for_lane,
                "to_submit": to_submit,
                "submitted_count": len(submitted),
                "submitted": submitted[:20],
            }
        )

    print(
        json.dumps(
            {
                "schema": "sweetspot.lane_manager.v1",
                "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "submit": bool(args.submit),
                "queue_depth": depth,
                "backlog_used_for_sizing": backlog,
                "target_workers": target_workers,
                "active_workers_before_submit": total_active,
                "remaining_unallocated": remaining,
                "lanes": lane_reports,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
