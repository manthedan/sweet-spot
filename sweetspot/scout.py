"""Spot compute estimates for a worker shape; throughput comes only from matching run telemetry."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from datetime import datetime, timedelta, timezone
from itertools import islice
from typing import Any

from .config import Config
from .store import Store, join

Group = tuple[str, str, int, int, int, str]


def add_arguments(p: argparse.ArgumentParser) -> None:
    p.add_argument("--region")
    p.add_argument("--instance-types", nargs="+", help="Space or comma separated EC2 types; defaults to configured lanes")
    p.add_argument("--lane")
    p.add_argument("--arch", choices=("x86_64", "arm64"))
    p.add_argument("--vcpus", type=int, default=None, help="Worker vCPUs; defaults to run markers, otherwise 1")
    p.add_argument("--memory", type=int, default=None, help="Worker memory in MiB; defaults to run markers, otherwise 1536")
    p.add_argument("--gpus", type=int, default=None, help="Worker GPUs; defaults to run markers, otherwise 0")
    p.add_argument("--instance-memory-reserve-mib", type=int, default=512, help="Estimated OS/ECS headroom per host; 512 MiB is conservative for small instances")
    p.add_argument("--hours", type=float, default=24)
    p.add_argument("--run", help="Use this run's measured throughput, separately for each image and shape")
    p.add_argument("--max-markers", type=int, default=2000)
    p.add_argument("--by-az", action="store_true", help="Show each priced AZ in the human table; JSON always includes every AZ")
    p.add_argument("--json", action="store_true")


def _arch(value: str) -> str:
    return {"aarch64": "arm64", "amd64": "x86_64"}.get(value, value)


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        return None
    return float(value)


def _shape(record: dict[str, Any]) -> tuple[int, int, int] | None:
    w = record.get("worker")
    if not isinstance(w, dict):
        return None
    cpu, mem, gpu = _number(w.get("vcpus")), _number(w.get("memory_mib")), _number(w.get("gpus", 0))
    if not cpu or not mem or gpu is None or not all(value.is_integer() for value in (cpu, mem, gpu)):
        return None
    return int(cpu), int(mem), int(gpu)


def _group(record: dict[str, Any]) -> Group | None:
    shape = _shape(record)
    if shape is None:
        return None
    w = record["worker"]
    kind, arch, image = w.get("instance_type"), w.get("arch"), w.get("image")
    if not isinstance(kind, str) or not kind or not isinstance(arch, str) or not isinstance(image, str) or not image:
        return None
    if _arch(arch) not in ("x86_64", "arm64"):
        return None
    return kind, _arch(arch), *shape, image


def observations(store: Store, prefix: str, limit: int, *, types: set[str] | None = None, arch: str | None = None, lane: str | None = None) -> tuple[dict[Group, dict[str, Any]], dict[str, Any]]:
    """Sample markers in listing order; replay covers attempts for those same sampled task IDs.

    Missing/best-effort summaries and tasks with no marker make replay a lower bound. Never use host cpus
    as the worker allocation, or pool measurements from different images or worker shapes.
    """
    groups: dict[Group, dict[str, Any]] = {}
    shapes: set[tuple[int, int, int]] = set()
    sampled: list[str] = []
    skipped = 0
    filtered = 0

    def selected(record: dict[str, Any]) -> bool:
        w = record.get("worker")
        if not isinstance(w, dict):
            return types is None and arch is None and lane is None
        # Older markers lack lane; type/arch remain usable, but cannot distinguish shared lanes.
        kind, observed_arch = w.get("instance_type"), w.get("arch")
        return (types is None or isinstance(kind, str) and kind in types) and (arch is None or isinstance(observed_arch, str) and _arch(observed_arch) == arch) and (lane is None or w.get("lane") in (None, "", lane))

    def read(uri: str) -> dict[str, Any] | None:
        nonlocal skipped
        try:
            data = store.get(uri)
            obj = json.loads(data) if data is not None else None
            if isinstance(obj, dict):
                return obj
        except (ValueError, UnicodeError):
            pass
        skipped += 1
        return None

    def bucket(key: Group) -> dict[str, Any]:
        return groups.setdefault(key, {"rates": [], "committed_seconds": 0.0, "discarded_seconds": 0.0})

    markers = (uri for uri, _, _ in store.list(join(prefix, "done") + "/") if uri.endswith(".json"))
    count = 0
    for uri in islice(markers, limit):
        count += 1
        marker = read(uri)
        if marker is None:
            continue
        # Derive the directory from the marker's key, never an unchecked task_id inside JSON.
        sampled.append(uri.rsplit("/", 1)[-1][:-5])
        if not selected(marker):
            filtered += 1
            continue
        shape = _shape(marker)
        if shape is not None:
            shapes.add(shape)
        key, elapsed, units = _group(marker), _number(marker.get("elapsed")), _number(marker.get("units"))
        if key is None or not elapsed:
            skipped += 1
            continue
        group = bucket(key)
        group["committed_seconds"] += elapsed
        if units is not None:
            group["rates"].append(units / elapsed)

    for task_id in sampled:
        for uri, _, _ in store.list(join(prefix, "attempts", task_id) + "/"):
            if not uri.endswith("/summary.json"):
                continue
            summary = read(uri)
            if summary is None or summary.get("outcome") not in {"duplicate", "failed", "interrupted"}:
                continue
            if not selected(summary):
                filtered += 1
                continue
            key, elapsed = _group(summary), _number(summary.get("elapsed"))
            if key is None or elapsed is None:
                skipped += 1
                continue
            bucket(key)["discarded_seconds"] += elapsed
    return groups, {
        "markers_sampled": count,
        "marker_limit_reached": count == limit,
        "skipped_records": skipped,
        "filtered_records": filtered,
        "replay_scope": "attempt summaries for sampled completed tasks; observed lower bound",
        "shapes": [{"vcpus": cpu, "memory_mib": mem, "gpus": gpu} for cpu, mem, gpu in sorted(shapes)],
    }


def capacity(ec2: Any, types: list[str], hours: float) -> tuple[dict[str, Any], dict[tuple[str, str], dict[str, Any]]]:
    specs: dict[str, Any] = {}
    prices: dict[tuple[str, str], dict[str, Any]] = {}
    now = datetime.now(timezone.utc)
    for offset in range(0, len(types), 100):
        part = types[offset : offset + 100]
        for page in ec2.get_paginator("describe_instance_types").paginate(InstanceTypes=part):
            for spec in page.get("InstanceTypes", []):
                specs[spec["InstanceType"]] = spec
        for page in ec2.get_paginator("describe_spot_price_history").paginate(
            InstanceTypes=part,
            ProductDescriptions=["Linux/UNIX"],
            StartTime=now - timedelta(hours=hours),
            EndTime=now,
        ):
            for price in page.get("SpotPriceHistory", []):
                key = price["InstanceType"], price["AvailabilityZone"]
                if key not in prices or price["Timestamp"] > prices[key]["Timestamp"]:
                    prices[key] = price
    return specs, prices


def run(args: argparse.Namespace, cfg: Config | None, store: Store, session: Any) -> int:
    region = args.region or (cfg.region if cfg else None)
    if not region or (args.run or args.lane) and cfg is None:
        print("scout: supply --region or config; --run and --lane require config", file=sys.stderr)
        return 2
    if (
        any(value is not None and value <= 0 for value in (args.vcpus, args.memory))
        or args.gpus is not None
        and args.gpus < 0
        or args.instance_memory_reserve_mib < 0
        or not 0 < args.hours <= 90 * 24
        or args.max_markers <= 0
    ):
        print("scout: positive vCPUs/memory/marker limit, nonnegative GPUs/reserve, and --hours in (0, 2160] required", file=sys.stderr)
        return 2
    lanes = [cfg.lane(args.lane)] if args.lane and cfg else list(cfg.lanes.values()) if cfg else []
    lanes = [lane for lane in lanes if not args.arch or lane.arch == args.arch]
    allowed = {kind for lane in lanes for kind in lane.instance_types}
    types = {kind for token in (args.instance_types or []) for kind in token.split(",") if kind} or allowed
    if args.lane:
        types &= allowed
    if not types:
        print("scout: no instance types match; supply --instance-types or configured lanes", file=sys.stderr)
        return 2
    selected_arch = args.arch or (cfg.lane(args.lane).arch if args.lane and cfg else None)
    groups, sampling = observations(store, cfg.run_prefix(args.run), args.max_markers, types=types, arch=selected_arch, lane=args.lane) if args.run and cfg else ({}, {})
    shapes = sampling.get("shapes", [])
    vcpus, memory_mib, gpus = args.vcpus, args.memory, args.gpus
    if vcpus is None or memory_mib is None or gpus is None:
        candidates = [shape for shape in shapes if all(value is None or shape[field] == value for field, value in (("vcpus", vcpus), ("memory_mib", memory_mib), ("gpus", gpus)))] if len(shapes) > 1 else shapes
        if len(shapes) > 1 and len(candidates) != 1:
            choices = ", ".join(f"{shape['vcpus']} vCPU / {shape['memory_mib']} MiB / {shape['gpus']} GPU" for shape in shapes)
            print(f"scout: sampled matching markers have multiple worker shapes: {choices}; specify --vcpus and --memory and --gpus", file=sys.stderr)
            return 2
        default_shape = candidates[0] if candidates else {"vcpus": 1, "memory_mib": 1536, "gpus": 0}
        vcpus = vcpus if vcpus is not None else default_shape["vcpus"]
        memory_mib = memory_mib if memory_mib is not None else default_shape["memory_mib"]
        gpus = gpus if gpus is not None else default_shape["gpus"]
    specs, prices = capacity(session.client("ec2", region_name=region), sorted(types), args.hours)
    rows = []
    for (kind, az), price in prices.items():
        spec = specs.get(kind)
        if spec is None:
            continue
        cpus, memory = spec["VCpuInfo"]["DefaultVCpus"], spec["MemoryInfo"]["SizeInMiB"]
        host_gpus = sum(gpu["Count"] for gpu in spec.get("GpuInfo", {}).get("Gpus", []))
        arches = [_arch(a) for a in spec["ProcessorInfo"]["SupportedArchitectures"]]
        for arch in arches:
            if arch not in ("x86_64", "arm64") or args.arch and arch != args.arch:
                continue
            if args.lane and cfg and arch != cfg.lane(args.lane).arch:
                continue
            packing = min(cpus // vcpus, max(0, memory - args.instance_memory_reserve_mib) // memory_mib)
            if gpus:
                packing = min(packing, host_gpus // gpus)
                if not packing:
                    continue
            hourly = float(price["SpotPrice"])
            matches: list[tuple[Group | None, dict[str, Any] | None]] = [(key, g) for key, g in groups.items() if key[:5] == (kind, arch, vcpus, memory_mib, gpus) and g["rates"]]
            for key, group in matches or [(None, None)]:
                rate = statistics.median(group["rates"]) if group else None
                replay = group["discarded_seconds"] / group["committed_seconds"] if group else None
                worker_hour = hourly / packing if packing else None
                rows.append(
                    {
                        "instance_type": kind,
                        "az": az,
                        "arch": arch,
                        "image": key[5] if key else None,
                        "vcpus": vcpus,
                        "memory_mib": memory_mib,
                        "gpus": gpus,
                        "host_vcpus": cpus,
                        "host_memory_mib": memory,
                        "host_gpus": host_gpus,
                        "packed_workers": packing,
                        "spot_hour_usd": hourly,
                        "price_timestamp": price["Timestamp"].isoformat(),
                        "vcpu_hour_usd": hourly / cpus,
                        "worker_hour_usd": worker_hour,
                        "observations": len(group["rates"]) if group else 0,
                        "units_per_second_per_worker": rate,
                        "replay_fraction": replay,
                        "committed_seconds": group["committed_seconds"] if group else None,
                        "discarded_seconds": group["discarded_seconds"] if group else None,
                        "usd_per_1m_units": worker_hour * 1e6 * (1 + replay) / (rate * 3600) if worker_hour is not None and rate and replay is not None else None,
                    }
                )

    def ranking(row: dict[str, Any]) -> tuple[Any, ...]:
        return (row["usd_per_1m_units"] is None, row["usd_per_1m_units"] or 0, row["worker_hour_usd"] is None, row["worker_hour_usd"] or 0, row["instance_type"], row["az"], row["image"] or "")

    rows.sort(key=ranking)
    notes = [
        "Estimates: latest Linux/UNIX Spot prices, full worker packing, steady-state compute only.",
        "Excludes startup, idle capacity, storage, network and logs; AZ capacity is not guaranteed.",
        "Throughput is the median task units/elapsed for the exact type, architecture, worker shape and image.",
        "Replay is an observed lower bound: best-effort summaries and tasks without done markers can be missing.",
    ]
    report = {
        "estimate": True,
        "region": region,
        "run": args.run,
        "instance_memory_reserve_mib": args.instance_memory_reserve_mib,
        "notes": notes,
        "sampling": sampling,
        "unpriced_instance_types": sorted(types - {key[0] for key in prices}),
        "rows": rows,
    }
    if args.json:
        print(json.dumps(report, indent=2, allow_nan=False))
    else:
        table_rows = rows
        if not args.by_az:
            pools: dict[tuple[str, str, str | None], list[dict[str, Any]]] = {}
            for row in rows:
                pools.setdefault((row["instance_type"], row["arch"], row["image"]), []).append(row)
            table_rows = sorted([{**min(pool, key=lambda row: (row["spot_hour_usd"], row["az"])), "azs_priced": len({row["az"] for row in pool})} for pool in pools.values()], key=ranking)
        print(f"Spot compute estimates ({region}); worker {vcpus} vCPU / {memory_mib} MiB / {gpus} GPU; host reserve {args.instance_memory_reserve_mib} MiB")
        az_heading = "AZ" if args.by_az else "CHEAPEST AZ"
        count_heading = "" if args.by_az else f" {'AZS':>3}"
        print(f"{'TYPE':15} {az_heading:16}{count_heading} {'ARCH':7} {'PACK':>4} {'$/vCPU-h':>9} {'$/worker-h':>10} {'units/s':>9} {'replay':>8} {'$/1M':>10} {'N':>5} IMAGE")
        for row in table_rows:

            def fmt(field: str) -> str:
                value = row[field]
                return "-" if value is None else f"{value:.4g}"

            count = "" if args.by_az else f" {row['azs_priced']:3}"
            print(
                f"{row['instance_type']:15} {row['az']:16}{count} {row['arch']:7} {row['packed_workers']:4} "
                f"{fmt('vcpu_hour_usd'):>9} {fmt('worker_hour_usd'):>10} {fmt('units_per_second_per_worker'):>9} "
                f"{fmt('replay_fraction'):>8} {fmt('usd_per_1m_units'):>10} {row['observations']:5} {row['image'] or '-'}"
            )
        if not rows:
            print("No matching Spot pools found.")
        if report["unpriced_instance_types"]:
            print("No price: " + ", ".join(report["unpriced_instance_types"]))
        for note in notes[1:]:
            print(note)
        if sampling:
            print(f"Sampled {sampling['markers_sampled']} markers (limit {args.max_markers}); skipped {sampling['skipped_records']} records.")
    return 0 if rows else 1
