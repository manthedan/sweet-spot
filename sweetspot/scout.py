#!/usr/bin/env python3
"""Rank AWS Spot regions/instance pools for AWS Batch retryable CPU workers.

This combines three signals:
- EC2 Spot placement score for requested vCPU targets.
- Recent Spot price history by instance type/AZ.
- Optional observed throughput/retry/discard telemetry from worker summary JSON.

Ranking uses expected total cost per useful unit: Spot compute plus replay,
startup overhead, and caller-supplied non-compute costs.

It does not submit jobs or mutate AWS resources.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from .cost_model import expected_cost_per_1m_units

X86_CPU_TYPES = [
    # Include the smallest practical EC2 Spot shapes first.  For 2 GiB
    # instances, Batch/ECS workers must reserve less than full host memory
    # (for example 1536 MiB) because host memory is not fully schedulable.
    "c7a.medium",
    "c5.large",
    "c5.xlarge",
    "c5.2xlarge",
    "c5.4xlarge",
    "c5a.large",
    "c5a.xlarge",
    "c5a.2xlarge",
    "c5a.4xlarge",
    "c6i.large",
    "c6i.xlarge",
    "c6i.2xlarge",
    "c6i.4xlarge",
    "c6a.large",
    "c6a.xlarge",
    "c6a.2xlarge",
    "c6a.4xlarge",
    "c7i.large",
    "c7i.xlarge",
    "c7i.2xlarge",
    "c7i.4xlarge",
    "c7a.large",
    "c7a.xlarge",
    "c7a.2xlarge",
    "c7a.4xlarge",
    "c8i.large",
    "c8i.xlarge",
    "c8i.2xlarge",
    "c8i.4xlarge",
    "c8a.large",
    "c8a.xlarge",
    "c8a.2xlarge",
    "c8a.4xlarge",
    "m5.large",
    "m5.xlarge",
    "m5.2xlarge",
    "m5.4xlarge",
    "m5a.large",
    "m5a.xlarge",
    "m5a.2xlarge",
    "m5a.4xlarge",
    "m6i.large",
    "m6i.xlarge",
    "m6i.2xlarge",
    "m6i.4xlarge",
    "m6a.large",
    "m6a.xlarge",
    "m6a.2xlarge",
    "m6a.4xlarge",
    "m7i.large",
    "m7i.xlarge",
    "m7i.2xlarge",
    "m7i.4xlarge",
    "m7a.large",
    "m7a.xlarge",
    "m7a.2xlarge",
    "m7a.4xlarge",
    "m8i.large",
    "m8i.xlarge",
    "m8i.2xlarge",
    "m8i.4xlarge",
    "m8a.large",
    "m8a.xlarge",
    "m8a.2xlarge",
    "m8a.4xlarge",
]

ARM_CPU_TYPES = [
    # Keep Graviton medium shapes in the default ARM/mixed scout universe so
    # canary-backed planning can discover cheap 1 vCPU lanes instead of
    # defaulting to larger "safe" instances.
    "c7g.medium",
    "c6g.medium",
    "c6g.large",
    "c6g.xlarge",
    "c6g.2xlarge",
    "c6g.4xlarge",
    "c7g.large",
    "c7g.xlarge",
    "c7g.2xlarge",
    "c7g.4xlarge",
    "c8g.large",
    "c8g.xlarge",
    "c8g.2xlarge",
    "c8g.4xlarge",
    "m6g.large",
    "m6g.xlarge",
    "m6g.2xlarge",
    "m6g.4xlarge",
    "m7g.large",
    "m7g.xlarge",
    "m7g.2xlarge",
    "m7g.4xlarge",
    "m8g.large",
    "m8g.xlarge",
    "m8g.2xlarge",
    "m8g.4xlarge",
]

SMALLEST_CPU_TYPES = ["c7a.medium", "c7g.medium", "c6g.medium"]

PRESETS = {
    "smallest": SMALLEST_CPU_TYPES,
    "x86": X86_CPU_TYPES,
    "arm": ARM_CPU_TYPES,
    "mixed": X86_CPU_TYPES + ARM_CPU_TYPES,
}

SCOUT_REASON_CODES: dict[str, str] = {
    "cost_components_not_provided": "One or more optional non-compute cost components were left at zero and may be omitted from the estimate.",
    "replay_fraction_lower_bound_only": "Receive-count telemetry proves at least one replay, but discarded compute was not observed; replay cost is a lower bound unless overridden.",
    "replay_fraction_partially_observed": "Discarded compute telemetry was observed, but receive-count telemetry proves additional replay attempts may be missing; replay cost is a lower bound unless overridden.",
    "replay_fraction_unobserved": "Replay/interruption overhead was not observed and defaults to zero unless explicitly overridden.",
    "throughput_default_used": "No observed throughput summaries were provided, so default units/sec was used.",
}


def parse_s3_uri(uri: str) -> tuple[str, str]:
    p = urlparse(uri)
    if p.scheme != "s3" or not p.netloc:
        raise ValueError(f"not an s3 uri: {uri}")
    return p.netloc, p.path.lstrip("/")


def chunks(xs: list[str], n: int) -> Iterable[list[str]]:
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


def pct(values: list[float], q: float) -> float:
    if not values:
        return math.nan
    values = sorted(values)
    idx = min(len(values) - 1, max(0, round((len(values) - 1) * q)))
    return values[idx]


def nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed < 0:
        return None
    return int(parsed)


def auto_regions(session: boto3.Session, home_region: str) -> list[str]:
    ec2 = session.client("ec2", region_name=home_region)
    regs = ec2.describe_regions(AllRegions=True)["Regions"]
    return sorted(r["RegionName"] for r in regs if r.get("OptInStatus") in (None, "opt-in-not-required", "opted-in"))


def latest_spot_prices(ec2, instance_types: list[str], start: dt.datetime, end: dt.datetime) -> dict[tuple[str, str], float]:
    latest: dict[tuple[str, str], tuple[float, dt.datetime]] = {}
    # API accepts up to many types, but chunk to survive region-specific unsupported new families.
    for part in chunks(instance_types, 20):
        try:
            paginator = ec2.get_paginator("describe_spot_price_history")
            for page in paginator.paginate(
                InstanceTypes=part,
                ProductDescriptions=["Linux/UNIX"],
                StartTime=start,
                EndTime=end,
                PaginationConfig={"MaxItems": 1200},
            ):
                for x in page.get("SpotPriceHistory", []):
                    key = (x["InstanceType"], x["AvailabilityZone"])
                    val = (float(x["SpotPrice"]), x["Timestamp"])
                    if key not in latest or val[1] > latest[key][1]:
                        latest[key] = val
        except ClientError as exc:
            # Some newer families are not in all regions yet; try individual fallbacks.
            if exc.response.get("Error", {}).get("Code") not in {"InvalidParameterValue", "InvalidInstanceType"}:
                raise
            for it in part:
                try:
                    for page in ec2.get_paginator("describe_spot_price_history").paginate(
                        InstanceTypes=[it],
                        ProductDescriptions=["Linux/UNIX"],
                        StartTime=start,
                        EndTime=end,
                        PaginationConfig={"MaxItems": 200},
                    ):
                        for x in page.get("SpotPriceHistory", []):
                            key = (x["InstanceType"], x["AvailabilityZone"])
                            val = (float(x["SpotPrice"]), x["Timestamp"])
                            if key not in latest or val[1] > latest[key][1]:
                                latest[key] = val
                except ClientError:
                    continue
    return {k: v[0] for k, v in latest.items()}


def placement_scores(ec2_home, regions: list[str], instance_types: list[str], target_vcpus: list[int]) -> dict[int, dict[str, int]]:
    out: dict[int, dict[str, int]] = {cap: {} for cap in target_vcpus}
    for cap in target_vcpus:
        for region_chunk in chunks(regions, 10):
            try:
                resp = ec2_home.get_spot_placement_scores(
                    InstanceTypes=instance_types,
                    TargetCapacity=cap,
                    TargetCapacityUnitType="vcpu",
                    RegionNames=region_chunk,
                )
                for row in resp.get("SpotPlacementScores", []):
                    out[cap][row["Region"]] = row["Score"]
            except ClientError as exc:
                print(f"WARN placement score failed cap={cap} regions={region_chunk}: {exc.response.get('Error', {}).get('Code')}: {exc.response.get('Error', {}).get('Message')}", file=sys.stderr)
    return out


def s3_get_text(s3, uri: str) -> str:
    bucket, key = parse_s3_uri(uri)
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    if key.endswith(".gz"):
        body = gzip.decompress(body)
    return body.decode("utf-8")


def _looks_like_summary_path(path: str) -> bool:
    return path.endswith(".summary.json") or path.endswith("/summary.json")


def iter_summary_jsons(session: boto3.Session, refs: list[str], max_files: int) -> Iterable[dict[str, Any]]:
    seen = 0
    s3_clients: dict[str, Any] = {}
    for ref in refs:
        if seen >= max_files:
            return
        if ref.startswith("s3://"):
            bucket, prefix = parse_s3_uri(ref)
            s3 = s3_clients.setdefault(bucket, session.client("s3"))
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not _looks_like_summary_path(key):
                        continue
                    try:
                        yield json.loads(s3_get_text(s3, f"s3://{bucket}/{key}"))
                        seen += 1
                    except Exception as exc:  # noqa: BLE001
                        print(f"WARN failed summary {key}: {exc}", file=sys.stderr)
                    if seen >= max_files:
                        return
        else:
            p = Path(ref)
            paths = [p] if p.is_file() else sorted(path for path in p.rglob("*.json") if _looks_like_summary_path(path.as_posix()))
            for path in paths:
                if seen >= max_files:
                    return
                try:
                    yield json.loads(path.read_text())
                    seen += 1
                except Exception as exc:  # noqa: BLE001
                    print(f"WARN failed summary {path}: {exc}", file=sys.stderr)


def observed_perf(session: boto3.Session, refs: list[str], max_files: int) -> dict[str, Any]:
    by_type: dict[str, list[float]] = defaultdict(list)
    all_vals: list[float] = []
    attempts = 0
    retry_attempts = 0
    receive_count_observed_attempts = 0
    max_receive_count_by_task: dict[str, int] = {}
    discarded_compute_attempts_by_task: dict[str, int] = defaultdict(int)
    useful_compute = 0.0
    discarded_compute = 0.0
    discarded_compute_observed_attempts = 0
    bytes_transferred = 0.0
    for js in iter_summary_jsons(session, refs, max_files):
        attempts += 1
        telemetry_raw = js.get("telemetry")
        telemetry: dict[str, Any] = telemetry_raw if isinstance(telemetry_raw, dict) else {}
        returncode = js.get("returncode")
        successful_useful_attempt = returncode in (None, 0) and not js.get("timed_out") and not js.get("framework_error") and js.get("commit_status") != "lost"
        lps = js.get("units_per_s") or js.get("completed_units_per_s") or js.get("labels_per_s") or telemetry.get("units_per_second")
        completed_units = telemetry.get("completed_units")
        useful_sec = telemetry.get("useful_compute_seconds")
        if (not isinstance(lps, (int, float)) or lps <= 0) and isinstance(completed_units, (int, float)) and isinstance(useful_sec, (int, float)) and useful_sec > 0:
            lps = float(completed_units) / float(useful_sec)
        if successful_useful_attempt and isinstance(lps, (int, float)) and lps > 0:
            all_vals.append(float(lps))
            inst = telemetry.get("instance_type") or ((js.get("worker_metadata") or {}).get("ec2") or {}).get("instanceType")
            if inst:
                by_type[str(inst)].append(float(lps))
        task_identity = str(js.get("task_hash") or js.get("task_id") or js.get("done_s3") or js.get("summary_s3") or f"unknown-attempt-{attempts}")
        receive_count = nonnegative_int(telemetry.get("receive_count"))
        receive_count_replayed = False
        if receive_count is not None and receive_count >= 1:
            receive_count_observed_attempts += 1
            receive_count_replayed = receive_count > 1
            max_receive_count_by_task[task_identity] = max(receive_count, max_receive_count_by_task.get(task_identity, 0))
        if telemetry.get("retry") or receive_count_replayed:
            retry_attempts += 1
        if successful_useful_attempt and isinstance(useful_sec, (int, float)):
            useful_compute += max(0.0, float(useful_sec))
        discarded = telemetry.get("discarded_compute_seconds")
        if isinstance(discarded, (int, float)):
            discarded_nonnegative = max(0.0, float(discarded))
            discarded_compute += discarded_nonnegative
            if discarded_nonnegative > 0:
                discarded_compute_observed_attempts += 1
                discarded_compute_attempts_by_task[task_identity] += 1
        transferred = telemetry.get("bytes_transferred")
        if isinstance(transferred, (int, float)):
            bytes_transferred += max(0.0, float(transferred))
    observed_replay_fraction = discarded_compute / useful_compute if useful_compute > 0 else 0.0
    receive_count_replay_lower_bound = sum(max(0, count - 1) for count in max_receive_count_by_task.values())
    receive_count_missing_discarded_attempts = sum(max(0, max(0, receive_count - 1) - discarded_compute_attempts_by_task.get(task_identity, 0)) for task_identity, receive_count in max_receive_count_by_task.items())
    replay_observation_status = "unobserved"
    if attempts:
        if receive_count_missing_discarded_attempts > 0:
            replay_observation_status = "discarded_compute_partial_receive_count_lower_bound" if discarded_compute > 0 else "receive_count_lower_bound_only"
        elif discarded_compute > 0:
            replay_observation_status = "discarded_compute_observed"
        else:
            replay_observation_status = "no_replay_observed"
    return {
        "global_median_units_per_s": statistics.median(all_vals) if all_vals else None,
        "by_instance_type": {k: {"n": len(v), "median_units_per_s": statistics.median(v)} for k, v in sorted(by_type.items())},
        "count": len(all_vals),
        "attempt_count": attempts,
        "retry_fraction": retry_attempts / attempts if attempts else 0.0,
        "receive_count_observed_attempts": receive_count_observed_attempts,
        "receive_count_replay_lower_bound": receive_count_replay_lower_bound,
        "receive_count_replay_lower_bound_fraction": receive_count_replay_lower_bound / attempts if attempts else 0.0,
        "receive_count_missing_discarded_attempts": receive_count_missing_discarded_attempts,
        "replay_observation_status": replay_observation_status,
        "discarded_compute_observed_attempts": discarded_compute_observed_attempts,
        "discarded_compute_seconds": discarded_compute,
        "useful_compute_seconds": useful_compute,
        "observed_replay_fraction": observed_replay_fraction,
        "bytes_transferred": bytes_transferred,
    }


def omitted_cost_components(args: argparse.Namespace) -> list[str]:
    out = []
    if args.extra_cost_per_1m_units == 0:
        out.append("extra_external_costs")
    if args.cross_region_gb_per_1m_units == 0:
        out.append("cross_region_transfer")
    if args.nat_gb_per_1m_units == 0:
        out.append("nat_or_private_link_data_processing")
    if args.cloudwatch_log_gb_per_1m_units == 0:
        out.append("cloudwatch_log_ingest")
    if args.s3_storage_gb_month_per_1m_units == 0:
        out.append("s3_storage")
    return out


def scout_reasons(args: argparse.Namespace, obs: dict[str, Any]) -> list[dict[str, Any]]:
    reasons: list[dict[str, Any]] = []
    if not obs.get("count"):
        reasons.append(
            {
                "code": "throughput_default_used",
                "severity": "warning",
                "message": SCOUT_REASON_CODES["throughput_default_used"],
            }
        )
    if args.expected_replay_fraction is None and not obs.get("useful_compute_seconds"):
        reasons.append(
            {
                "code": "replay_fraction_unobserved",
                "severity": "warning",
                "message": SCOUT_REASON_CODES["replay_fraction_unobserved"],
            }
        )
    if args.expected_replay_fraction is None:
        if obs.get("replay_observation_status") == "receive_count_lower_bound_only":
            reasons.append(
                {
                    "code": "replay_fraction_lower_bound_only",
                    "severity": "warning",
                    "message": SCOUT_REASON_CODES["replay_fraction_lower_bound_only"],
                    "receive_count_replay_lower_bound": obs.get("receive_count_replay_lower_bound"),
                }
            )
        elif obs.get("replay_observation_status") == "discarded_compute_partial_receive_count_lower_bound":
            reasons.append(
                {
                    "code": "replay_fraction_partially_observed",
                    "severity": "warning",
                    "message": SCOUT_REASON_CODES["replay_fraction_partially_observed"],
                    "discarded_compute_observed_attempts": obs.get("discarded_compute_observed_attempts"),
                    "receive_count_missing_discarded_attempts": obs.get("receive_count_missing_discarded_attempts"),
                    "receive_count_replay_lower_bound": obs.get("receive_count_replay_lower_bound"),
                }
            )
    omitted = omitted_cost_components(args)
    if omitted:
        reasons.append(
            {
                "code": "cost_components_not_provided",
                "severity": "info",
                "message": SCOUT_REASON_CODES["cost_components_not_provided"],
                "components": omitted,
            }
        )
    return reasons


def noncompute_cost_per_1m_units(args: argparse.Namespace, *, bucket_local: bool | None) -> float:
    transfer_gb = max(0.0, args.cross_region_gb_per_1m_units)
    if bucket_local is True:
        transfer_gb = 0.0
    return (
        max(0.0, args.extra_cost_per_1m_units)
        + transfer_gb * max(0.0, args.cross_region_cost_per_gb)
        + max(0.0, args.nat_gb_per_1m_units) * max(0.0, args.nat_cost_per_gb)
        + max(0.0, args.cloudwatch_log_gb_per_1m_units) * max(0.0, args.cloudwatch_log_cost_per_gb)
        + max(0.0, args.s3_storage_gb_month_per_1m_units) * max(0.0, args.s3_storage_cost_per_gb_month)
    )


def instance_vcpus(ec2, instance_types: list[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for part in chunks(instance_types, 100):
        try:
            resp = ec2.describe_instance_types(InstanceTypes=part)
            for it in resp.get("InstanceTypes", []):
                out[it["InstanceType"]] = int(it["VCpuInfo"]["DefaultVCpus"])
        except ClientError:
            for name in part:
                try:
                    resp = ec2.describe_instance_types(InstanceTypes=[name])
                    for it in resp.get("InstanceTypes", []):
                        out[it["InstanceType"]] = int(it["VCpuInfo"]["DefaultVCpus"])
                except ClientError:
                    continue
    return out


def instance_memory_mib(ec2, instance_types: list[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for part in chunks(instance_types, 100):
        try:
            resp = ec2.describe_instance_types(InstanceTypes=part)
            for it in resp.get("InstanceTypes", []):
                out[it["InstanceType"]] = int(it["MemoryInfo"]["SizeInMiB"])
        except ClientError:
            for name in part:
                try:
                    resp = ec2.describe_instance_types(InstanceTypes=[name])
                    for it in resp.get("InstanceTypes", []):
                        out[it["InstanceType"]] = int(it["MemoryInfo"]["SizeInMiB"])
                except ClientError:
                    continue
    return out


def main(argv: list[str] | None = None, *, prog: str | None = None) -> int:
    ap = argparse.ArgumentParser(prog=prog, description=__doc__)
    ap.add_argument("--profile", default=None)
    ap.add_argument("--home-region", default="us-west-2", help="Endpoint used for DescribeRegions and placement score calls")
    ap.add_argument("--regions", nargs="*", help="Regions to compare; default is all opted-in regions")
    ap.add_argument("--preset", choices=sorted(PRESETS), default="x86")
    ap.add_argument("--instance-types", nargs="*", help="Override preset instance types")
    ap.add_argument("--target-vcpus", nargs="+", type=int, default=[256, 512])
    ap.add_argument("--hours", type=int, default=8)
    ap.add_argument("--bucket", default="", help="Optional canonical data bucket for locality annotation")
    ap.add_argument("--worker-vcpus", type=int, default=2)
    ap.add_argument("--worker-memory", "--worker-memory-mib", dest="worker_memory_mib", type=int, default=0, help="Optional worker memory request in MiB for memory-aware instance packing")
    ap.add_argument("--instance-memory-reserve-mib", type=int, default=512, help="Conservative per-instance memory reserve subtracted before Batch worker packing")
    ap.add_argument("--default-units-per-s", type=float, default=27.0, help="Fallback units/sec for one worker job")
    ap.add_argument("--observed-summaries", nargs="*", default=[], help="Local dirs/files or s3:// prefixes containing *.summary.json or attempt-scoped */summary.json files")
    ap.add_argument("--max-observed-files", type=int, default=5000)
    ap.add_argument("--top-instance-rows", type=int, default=20)
    ap.add_argument("--expected-replay-fraction", type=float, default=None, help="Override observed discarded/useful compute fraction for retries/interruption replay")
    ap.add_argument("--startup-overhead-seconds", type=float, default=0.0, help="Expected worker startup/queue overhead amortized per task")
    ap.add_argument("--useful-task-seconds", type=float, default=3600.0, help="Expected useful task duration used to amortize startup overhead")
    ap.add_argument("--extra-cost-per-1m-units", type=float, default=0.0, help="Known extra non-compute cost per 1M useful units")
    ap.add_argument("--cross-region-gb-per-1m-units", type=float, default=0.0)
    ap.add_argument("--cross-region-cost-per-gb", type=float, default=0.02)
    ap.add_argument("--nat-gb-per-1m-units", type=float, default=0.0)
    ap.add_argument("--nat-cost-per-gb", type=float, default=0.045)
    ap.add_argument("--cloudwatch-log-gb-per-1m-units", type=float, default=0.0)
    ap.add_argument("--cloudwatch-log-cost-per-gb", type=float, default=0.50)
    ap.add_argument("--s3-storage-gb-month-per-1m-units", type=float, default=0.0)
    ap.add_argument("--s3-storage-cost-per-gb-month", type=float, default=0.023)
    ap.add_argument("--format", choices=["json", "table"], default="json", help="Output format; JSON stdout is the default for agent workflows")
    ap.add_argument("--json-out", type=Path, help="Optional path to also write the JSON report")
    args = ap.parse_args(argv)
    if args.worker_vcpus <= 0:
        ap.error("--worker-vcpus must be greater than 0")
    if args.worker_memory_mib < 0:
        ap.error("--worker-memory must be greater than or equal to 0")
    if args.instance_memory_reserve_mib < 0:
        ap.error("--instance-memory-reserve-mib must be greater than or equal to 0")

    session = boto3.Session(profile_name=args.profile)
    regions = args.regions or auto_regions(session, args.home_region)
    instance_types = args.instance_types or PRESETS[args.preset]
    now = dt.datetime.now(dt.timezone.utc)
    start = now - dt.timedelta(hours=args.hours)
    ec2_home = session.client("ec2", region_name=args.home_region)

    bucket_region = None
    if args.bucket:
        try:
            loc = session.client("s3", region_name=args.home_region).get_bucket_location(Bucket=args.bucket).get("LocationConstraint")
            bucket_region = loc or "us-east-1"
        except Exception as exc:  # noqa: BLE001
            print(f"WARN bucket location failed: {exc}", file=sys.stderr)

    scores = placement_scores(ec2_home, regions, instance_types, args.target_vcpus)
    obs = observed_perf(session, args.observed_summaries, args.max_observed_files) if args.observed_summaries else {"count": 0, "by_instance_type": {}, "global_median_units_per_s": None}
    fallback_lps = obs.get("global_median_units_per_s") or args.default_units_per_s
    replay_fraction = args.expected_replay_fraction if args.expected_replay_fraction is not None else float(obs.get("observed_replay_fraction") or 0.0)

    region_rows = []
    instance_rows = []
    for region in regions:
        ec2 = session.client("ec2", region_name=region)
        try:
            price_map = latest_spot_prices(ec2, instance_types, start, now)
            priced_instance_types = sorted({it for it, _az in price_map})
            vcpu_map = instance_vcpus(ec2, priced_instance_types)
            memory_map = instance_memory_mib(ec2, priced_instance_types) if args.worker_memory_mib > 0 else {}
        except Exception as exc:  # noqa: BLE001
            region_rows.append({"region": region, "ok": False, "error": repr(exc)})
            continue
        if not price_map:
            region_rows.append({"region": region, "ok": False, "error": "no spot price history"})
            continue
        prices = list(price_map.values())
        per_vcpu = [p / max(1, vcpu_map.get(it, 2)) for (it, _az), p in price_map.items()]
        cap0 = args.target_vcpus[-1]
        score = scores.get(cap0, {}).get(region)
        same_bucket = bucket_region == region if bucket_region else None
        row = {
            "region": region,
            "ok": True,
            "bucket_local": same_bucket,
            "placement_scores": {str(cap): scores.get(cap, {}).get(region) for cap in args.target_vcpus},
            "pools": len(price_map),
            "min_per_vcpu": min(per_vcpu),
            "p25_per_vcpu": pct(per_vcpu, 0.25),
            "median_per_vcpu": statistics.median(per_vcpu),
            "min_hourly": min(prices),
            "median_hourly": statistics.median(prices),
            "noncompute_cost_per_1m_units": noncompute_cost_per_1m_units(args, bucket_local=same_bucket),
        }
        region_rows.append(row)
        for (it, az), price in price_map.items():
            vcpus = vcpu_map.get(it)
            if not vcpus:
                continue
            obs_it = (obs.get("by_instance_type") or {}).get(it) or {}
            lps = obs_it.get("median_units_per_s") or fallback_lps
            vcpu_fit_workers = vcpus // args.worker_vcpus
            if vcpu_fit_workers <= 0:
                continue
            memory_mib = memory_map.get(it)
            if args.worker_memory_mib > 0 and memory_mib is None:
                continue
            memory_available_mib = max(0, memory_mib - args.instance_memory_reserve_mib) if memory_mib is not None else None
            memory_fit_workers = memory_available_mib // args.worker_memory_mib if memory_available_mib is not None and args.worker_memory_mib > 0 else None
            packed_workers = min(vcpu_fit_workers, memory_fit_workers) if memory_fit_workers is not None else vcpu_fit_workers
            if packed_workers <= 0:
                continue
            packing_limiter = "memory" if memory_fit_workers is not None and memory_fit_workers < vcpu_fit_workers else "vcpu"
            units_per_hour = lps * packed_workers * 3600.0
            compute_cost_per_1m = (price / units_per_hour) * 1_000_000.0 if units_per_hour > 0 else math.nan
            noncompute_per_1m = noncompute_cost_per_1m_units(args, bucket_local=same_bucket)
            total_cost_per_1m = expected_cost_per_1m_units(
                hourly_price=price,
                units_per_hour=units_per_hour,
                replay_fraction=replay_fraction,
                startup_overhead_seconds=args.startup_overhead_seconds,
                useful_task_seconds=args.useful_task_seconds,
                noncompute_per_1m=noncompute_per_1m,
            )
            instance_rows.append(
                {
                    "region": region,
                    "az": az,
                    "instance_type": it,
                    "vcpus": vcpus,
                    "memory_mib": memory_mib,
                    "memory_available_mib": memory_available_mib,
                    "spot_hourly": price,
                    "spot_per_vcpu": price / vcpus,
                    "configuration_placement_score": score,
                    "placement_score_scope": "region_instance_type_configuration",
                    "bucket_local": same_bucket,
                    "packed_workers": packed_workers,
                    "vcpu_fit_workers": vcpu_fit_workers,
                    "memory_fit_workers": memory_fit_workers,
                    "packing_limiter": packing_limiter,
                    "units_per_s_per_worker": lps,
                    "observed_n": obs_it.get("n", 0),
                    "estimated_compute_cost_per_1m_units": compute_cost_per_1m,
                    "expected_total_cost_per_1m_units": total_cost_per_1m,
                    "noncompute_cost_per_1m_units": noncompute_per_1m,
                    "expected_replay_fraction": replay_fraction,
                }
            )

    ok_regions = [r for r in region_rows if r.get("ok")]
    cap0 = args.target_vcpus[-1]

    def region_placement_score(row: dict[str, Any]) -> float:
        scores = row.get("placement_scores")
        if not isinstance(scores, dict):
            return -1.0
        return float(scores.get(str(cap0)) or -1)

    ok_regions.sort(key=lambda r: (-region_placement_score(r), r["median_per_vcpu"], not bool(r.get("bucket_local"))))
    instance_rows.sort(key=lambda r: (r["expected_total_cost_per_1m_units"], -(r.get("configuration_placement_score") or -1), not bool(r.get("bucket_local"))))

    report = {
        "schema": "sweetspot.scout.v1",
        "checked_at": now.isoformat(),
        "reasons": scout_reasons(args, obs),
        "home_region": args.home_region,
        "bucket": args.bucket,
        "bucket_region": bucket_region,
        "preset": args.preset,
        "instance_types": instance_types,
        "target_vcpus": args.target_vcpus,
        "worker_vcpus": args.worker_vcpus,
        "worker_memory_mib": args.worker_memory_mib,
        "instance_memory_reserve_mib": args.instance_memory_reserve_mib,
        "observed_perf": obs,
        "cost_model": {
            "expected_replay_fraction": replay_fraction,
            "startup_overhead_seconds": args.startup_overhead_seconds,
            "useful_task_seconds": args.useful_task_seconds,
            "extra_cost_per_1m_units": args.extra_cost_per_1m_units,
            "cross_region_gb_per_1m_units": args.cross_region_gb_per_1m_units,
            "cross_region_cost_per_gb": args.cross_region_cost_per_gb,
            "nat_gb_per_1m_units": args.nat_gb_per_1m_units,
            "nat_cost_per_gb": args.nat_cost_per_gb,
            "cloudwatch_log_gb_per_1m_units": args.cloudwatch_log_gb_per_1m_units,
            "cloudwatch_log_cost_per_gb": args.cloudwatch_log_cost_per_gb,
            "s3_storage_gb_month_per_1m_units": args.s3_storage_gb_month_per_1m_units,
            "s3_storage_cost_per_gb_month": args.s3_storage_cost_per_gb_month,
            "omitted_cost_components": omitted_cost_components(args),
        },
        "regions": region_rows,
        "top_instance_pools": instance_rows[: max(0, args.top_instance_rows)],
    }
    report_json = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(report_json)

    if args.format == "json":
        print(report_json, end="")
        return 0

    print(f"checked_at: {now.isoformat()}")
    print(f"bucket_region: {bucket_region}")
    print(f"preset: {args.preset}  target_vcpus: {args.target_vcpus}  observed_summaries: {obs.get('count', 0)}")
    print("\nREGION RANK")
    print("region           local score  pools  min$/vcpu med$/vcpu")
    for r in ok_regions[:20]:
        placement_scores_obj = r.get("placement_scores")
        score = placement_scores_obj.get(str(cap0)) if isinstance(placement_scores_obj, dict) else None
        print(f"{r['region']:15s} {str(r.get('bucket_local')):5s} {str(score):>5s} {r['pools']:6d}  ${r['min_per_vcpu']:.4f}   ${r['median_per_vcpu']:.4f}")
    print("\nTOP INSTANCE POOLS BY EXPECTED TOTAL $/1M UNITS")
    print("region           az              type          cfg_score  $/hr    vcpu memAvail workers limit  units/s compute$/1M total$/1M")
    for r in instance_rows[: args.top_instance_rows]:
        print(
            f"{r['region']:15s} {r['az']:15s} {r['instance_type']:13s} {str(r.get('configuration_placement_score')):>9s}  ${r['spot_hourly']:.4f} {r['vcpus']:5d} {str(r.get('memory_available_mib')):>8s} {r['packed_workers']:7d} {r['packing_limiter']:6s} {r['units_per_s_per_worker']:8.2f} ${r['estimated_compute_cost_per_1m_units']:.3f} ${r['expected_total_cost_per_1m_units']:.3f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
