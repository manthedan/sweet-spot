#!/usr/bin/env python3
"""Rank AWS Spot regions/instance pools for AWS Batch retryable CPU workers.

This combines three signals:
- EC2 Spot placement score for requested vCPU targets.
- Recent Spot price history by instance type/AZ.
- Optional observed completed-units/sec from worker summary JSON.

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

X86_CPU_TYPES = [
    "c5.large", "c5.xlarge", "c5.2xlarge", "c5.4xlarge",
    "c5a.large", "c5a.xlarge", "c5a.2xlarge", "c5a.4xlarge",
    "c6i.large", "c6i.xlarge", "c6i.2xlarge", "c6i.4xlarge",
    "c6a.large", "c6a.xlarge", "c6a.2xlarge", "c6a.4xlarge",
    "c7i.large", "c7i.xlarge", "c7i.2xlarge", "c7i.4xlarge",
    "c7a.large", "c7a.xlarge", "c7a.2xlarge", "c7a.4xlarge",
    "c8i.large", "c8i.xlarge", "c8i.2xlarge", "c8i.4xlarge",
    "c8a.large", "c8a.xlarge", "c8a.2xlarge", "c8a.4xlarge",
    "m5.large", "m5.xlarge", "m5.2xlarge", "m5.4xlarge",
    "m5a.large", "m5a.xlarge", "m5a.2xlarge", "m5a.4xlarge",
    "m6i.large", "m6i.xlarge", "m6i.2xlarge", "m6i.4xlarge",
    "m6a.large", "m6a.xlarge", "m6a.2xlarge", "m6a.4xlarge",
    "m7i.large", "m7i.xlarge", "m7i.2xlarge", "m7i.4xlarge",
    "m7a.large", "m7a.xlarge", "m7a.2xlarge", "m7a.4xlarge",
    "m8i.large", "m8i.xlarge", "m8i.2xlarge", "m8i.4xlarge",
    "m8a.large", "m8a.xlarge", "m8a.2xlarge", "m8a.4xlarge",
]

ARM_CPU_TYPES = [
    "c6g.large", "c6g.xlarge", "c6g.2xlarge", "c6g.4xlarge",
    "c7g.large", "c7g.xlarge", "c7g.2xlarge", "c7g.4xlarge",
    "c8g.large", "c8g.xlarge", "c8g.2xlarge", "c8g.4xlarge",
    "m6g.large", "m6g.xlarge", "m6g.2xlarge", "m6g.4xlarge",
    "m7g.large", "m7g.xlarge", "m7g.2xlarge", "m7g.4xlarge",
    "m8g.large", "m8g.xlarge", "m8g.2xlarge", "m8g.4xlarge",
]

PRESETS = {
    "x86": X86_CPU_TYPES,
    "arm": ARM_CPU_TYPES,
    "mixed": X86_CPU_TYPES + ARM_CPU_TYPES,
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
                    if not key.endswith(".summary.json"):
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
            paths = [p] if p.is_file() else sorted(p.rglob("*.summary.json"))
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
    for js in iter_summary_jsons(session, refs, max_files):
        lps = js.get("units_per_s") or js.get("completed_units_per_s") or js.get("labels_per_s")
        if not isinstance(lps, (int, float)) or lps <= 0:
            continue
        all_vals.append(float(lps))
        inst = (((js.get("worker_metadata") or {}).get("ec2") or {}).get("instanceType"))
        if inst:
            by_type[str(inst)].append(float(lps))
    return {
        "global_median_units_per_s": statistics.median(all_vals) if all_vals else None,
        "by_instance_type": {k: {"n": len(v), "median_units_per_s": statistics.median(v)} for k, v in sorted(by_type.items())},
        "count": len(all_vals),
    }


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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--profile", default=None)
    ap.add_argument("--home-region", default="us-west-2", help="Endpoint used for DescribeRegions and placement score calls")
    ap.add_argument("--regions", nargs="*", help="Regions to compare; default is all opted-in regions")
    ap.add_argument("--preset", choices=sorted(PRESETS), default="x86")
    ap.add_argument("--instance-types", nargs="*", help="Override preset instance types")
    ap.add_argument("--target-vcpus", nargs="+", type=int, default=[256, 512])
    ap.add_argument("--hours", type=int, default=8)
    ap.add_argument("--bucket", default="", help="Optional canonical data bucket for locality annotation")
    ap.add_argument("--worker-vcpus", type=int, default=2)
    ap.add_argument("--default-units-per-s", type=float, default=27.0, help="Fallback units/sec for one worker job")
    ap.add_argument("--observed-summaries", nargs="*", default=[], help="Local dirs/files or s3:// prefixes containing *.summary.json")
    ap.add_argument("--max-observed-files", type=int, default=5000)
    ap.add_argument("--top-instance-rows", type=int, default=20)
    ap.add_argument("--json-out", type=Path)
    args = ap.parse_args()

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

    region_rows = []
    instance_rows = []
    for region in regions:
        ec2 = session.client("ec2", region_name=region)
        try:
            price_map = latest_spot_prices(ec2, instance_types, start, now)
            vcpu_map = instance_vcpus(ec2, sorted({it for it, _az in price_map}))
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
        }
        region_rows.append(row)
        for (it, az), price in price_map.items():
            vcpus = vcpu_map.get(it)
            if not vcpus:
                continue
            obs_it = (obs.get("by_instance_type") or {}).get(it) or {}
            lps = obs_it.get("median_units_per_s") or fallback_lps
            packed_workers = max(1, vcpus // args.worker_vcpus)
            units_per_hour = lps * packed_workers * 3600.0
            cost_per_1m = (price / units_per_hour) * 1_000_000.0 if units_per_hour > 0 else math.nan
            instance_rows.append({
                "region": region,
                "az": az,
                "instance_type": it,
                "vcpus": vcpus,
                "spot_hourly": price,
                "spot_per_vcpu": price / vcpus,
                "placement_score": score,
                "bucket_local": same_bucket,
                "packed_workers": packed_workers,
                "units_per_s_per_worker": lps,
                "observed_n": obs_it.get("n", 0),
                "estimated_cost_per_1m_units": cost_per_1m,
            })

    ok_regions = [r for r in region_rows if r.get("ok")]
    cap0 = args.target_vcpus[-1]
    ok_regions.sort(key=lambda r: (-(r["placement_scores"].get(str(cap0)) or -1), r["median_per_vcpu"], not bool(r.get("bucket_local"))))
    instance_rows.sort(key=lambda r: (-(r.get("placement_score") or -1), r["estimated_cost_per_1m_units"], not bool(r.get("bucket_local"))))

    report = {
        "schema": "spotbatch.spot_scout.v1",
        "checked_at": now.isoformat(),
        "home_region": args.home_region,
        "bucket": args.bucket,
        "bucket_region": bucket_region,
        "preset": args.preset,
        "instance_types": instance_types,
        "target_vcpus": args.target_vcpus,
        "worker_vcpus": args.worker_vcpus,
        "observed_perf": obs,
        "regions": region_rows,
        "top_instance_pools": instance_rows[: max(0, args.top_instance_rows)],
    }
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")

    print(f"checked_at: {now.isoformat()}")
    print(f"bucket_region: {bucket_region}")
    print(f"preset: {args.preset}  target_vcpus: {args.target_vcpus}  observed_summaries: {obs.get('count', 0)}")
    print("\nREGION RANK")
    print("region           local score  pools  min$/vcpu med$/vcpu")
    for r in ok_regions[:20]:
        score = r["placement_scores"].get(str(cap0))
        print(f"{r['region']:15s} {str(r.get('bucket_local')):5s} {str(score):>5s} {r['pools']:6d}  ${r['min_per_vcpu']:.4f}   ${r['median_per_vcpu']:.4f}")
    print("\nTOP INSTANCE POOLS BY ESTIMATED $/1M UNITS")
    print("region           az              type          score  $/hr    vcpu workers units/s  $/1M")
    for r in instance_rows[: args.top_instance_rows]:
        print(f"{r['region']:15s} {r['az']:15s} {r['instance_type']:13s} {str(r.get('placement_score')):>5s}  ${r['spot_hourly']:.4f} {r['vcpus']:5d} {r['packed_workers']:7d} {r['units_per_s_per_worker']:8.2f} ${r['estimated_cost_per_1m_units']:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
