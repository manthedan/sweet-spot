"""sweetspot command line."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

from . import __version__, config, runs, tasks
from .store import Store, join

EXAMPLES = """\
examples:
  sweetspot tasks --inputs shards.txt -- python label.py --in {input} --out {out}/labels.jsonl > tasks.jsonl
  sweetspot local tasks.jsonl --limit 2               # try a couple of tasks on this machine
  sweetspot submit sf-d10 tasks.jsonl --image ECR_IMAGE --workers 100
  sweetspot status sf-d10
  sweetspot logs sf-d10 shard_0007
  sweetspot submit sf-d10 --retry                     # after fixing a failure
  sweetspot finish sf-d10                             # verify, publish manifest + READY, clean up
"""


def _print(obj: Any, as_json: bool, human: str) -> None:
    print(json.dumps(obj, indent=2, sort_keys=True, default=str) if as_json else human)


def _clients(args: argparse.Namespace) -> tuple[config.Config, runs.Clients]:
    cfg = config.load(args.config)
    assert cfg is not None
    return cfg, runs.Clients.from_config(cfg, args.profile)


# --- tasks / local -----------------------------------------------------------------------------------------


def cmd_tasks(args: argparse.Namespace) -> int:
    template = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not template:
        raise SystemExit("give the command template after --")
    inputs = None
    if args.inputs:
        text = sys.stdin.read() if args.inputs == "-" else Path(args.inputs).read_text()
        inputs = [line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#")]
    elif args.inputs_from:
        rx = re.compile(args.match) if args.match else None
        inputs = [uri for uri, _s, _m in Store().list(args.inputs_from) if not rx or rx.search(uri)]
        if not inputs:
            raise SystemExit(f"no objects under {args.inputs_from}" + (f" matching {args.match!r}" if args.match else ""))
    env = dict(kv.split("=", 1) for kv in args.env or [])
    extra: dict[str, Any] = {}
    if args.cwd:
        extra["cwd"] = args.cwd
    if args.outputs:
        extra["outputs"] = args.outputs
    if args.units is not None:
        extra["units"] = int(args.units) if float(args.units).is_integer() else args.units
    out = open(args.output, "w") if args.output else sys.stdout
    n = 0
    for task in tasks.generate(template, inputs=inputs, count=args.count, chunks=args.chunks, timeout=args.timeout, env=env or None, extra=extra):
        out.write(json.dumps(task, sort_keys=True) + "\n")
        n += 1
    if args.output:
        out.close()
        print(f"wrote {n} tasks to {args.output}", file=sys.stderr)
    return 0


def cmd_local(args: argparse.Namespace) -> int:
    from .local import run_local

    all_tasks = tasks.read_tasks(args.tasks)
    selected = [t for t in all_tasks if not args.only or t["id"] in args.only][: args.limit or None]
    if not selected:
        raise SystemExit("no tasks selected")
    prefix = args.prefix or str(Path("sweetspot-local") / args.tasks.stem)
    print(f"running {len(selected)} of {len(all_tasks)} task(s) with -j {args.jobs}; results under {prefix}", file=sys.stderr)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    counts = run_local(selected, ledger=all_tasks, prefix=prefix, store=Store(), jobs=args.jobs, require_output=not args.allow_empty_output, stop=stop)
    print(json.dumps(counts))
    print(f"outputs: {join(prefix, 'out')}/", file=sys.stderr)
    return 0 if counts["failed"] == 0 and counts["interrupted"] == 0 else 1


# --- cloud runs --------------------------------------------------------------------------------------------


def cmd_submit(args: argparse.Namespace) -> int:
    cfg, c = _clients(args)
    report = runs.submit(
        c,
        cfg,
        args.run,
        args.tasks,
        image=args.image,
        vcpus=args.vcpus,
        memory=args.memory,
        gpus=args.gpus,
        workers=args.workers,
        lane=args.lane,
        max_attempts=args.max_attempts,
        require_output=not args.allow_empty_output,
        retry=args.retry,
        dry_run=args.dry_run,
    )
    if args.json:
        _print(report, True, "")
        return 0
    lines = [f"run {report['run']}: {report['done']}/{report['tasks']} done, {report['pending']} pending  ({report['prefix']})"]
    if report.get("dry_run"):
        lines.append(f"dry run: would start {report['workers']} worker(s) of {report['vcpus']:g} vCPU / {report['memory']} MiB on lane {report['lane']} with {report['image']}")
    else:
        if report.get("enqueued"):
            lines.append(f"enqueued {report['enqueued']} task(s)")
        if report.get("redriven"):
            lines.append(f"moved {report['redriven']} failed task(s) from the DLQ back to the queue")
        if report.get("launch"):
            la = report["launch"]
            lines.append(f"started {la['workers']} worker(s) ({la['vcpus']:g} vCPU / {la['memory']} MiB, lane {la['lane']}) as Batch job {la['job_id']}")
        lines += report.get("notes", [])
        lines.append(f"next: sweetspot status {report['run']}")
    lines += [f"warning: {w}" for w in report.get("warnings", [])]
    print("\n".join(lines))
    return 0


def _status_text(st: dict[str, Any]) -> str:
    q = st["queue"]
    lines = [
        f"run {st['run']}  [{st['state']}]  {st['done']}/{st['tasks']} done" + (f", {st['with_failed_attempts']} with failed attempts" if st["with_failed_attempts"] else ""),
        f"  queue: {q.get('visible', 0)} waiting, {q.get('in_flight', 0)} running/backing off, {q.get('delayed', 0)} delayed; DLQ {st['dlq']}" if q else "  queue: (none)",
        f"  workers: {st['active_workers']} active" + "".join(f"\n    {j['name']}: " + ", ".join(f"{k.lower()} {v}" for k, v in sorted(j.get("children", {}).items())) for j in st["worker_jobs"][:5]),
    ]
    if st["done_per_minute"]:
        lines.append(f"  rate: {st['done_per_minute']:g} tasks/min over the last 10 min" + (f", ETA {st['eta_minutes']:g} min" if st["eta_minutes"] else ""))
    for f in st["recent_failures"]:
        tail = (f.get("stderr_tail") or "").strip().splitlines()[-1:] or [""]
        lines.append(f"  failed {f['task_id']} ({f['status']}): {f['error']}" + (f" | {tail[0][:160]}" if tail[0] else ""))
    if st["next"]:
        lines.append(f"next: {st['next']}")
    return "\n".join(lines)


def cmd_status(args: argparse.Namespace) -> int:
    cfg, c = _clients(args)
    while True:
        st = runs.status(c, cfg, args.run)
        _print(st, args.json, _status_text(st))
        if not args.watch or st["state"] in ("finished", "complete", "failed", "stalled"):
            return 0
        time.sleep(args.watch)
        print()


def cmd_logs(args: argparse.Namespace) -> int:
    cfg, c = _clients(args)
    found = runs.attempts(c, cfg, args.run, args.task)
    if args.json:
        _print(found, True, "")
        return 0
    if not found:
        print(f"no attempts recorded for {args.task}")
        return 1
    for a in found:
        worker = a.get("worker") or {}
        where = " ".join(str(worker[k]) for k in ("instance_type", "az", "host") if worker.get(k))
        print(f"== attempt {a.get('attempt')}  {a.get('outcome') or a.get('status')}  {a.get('elapsed', '?')}s  {where}".rstrip())
        if a.get("error"):
            print(f"   error: {a['error']}")
    last = found[-1]
    for name in ("stdout.txt", "stderr.txt"):
        uri = (last.get("logs") or {}).get(name)
        text = (c.store.get(uri) or b"").decode("utf-8", "replace") if uri else last.get(name.replace(".txt", "_tail"), "")
        if text:
            shown = text if args.full else text[-4000:]
            print(f"--- {name} of {last.get('attempt')}" + ("" if args.full or len(text) <= 4000 else " (last 4000 chars; --full for all)"))
            print(shown.rstrip())
    return 0


def cmd_finish(args: argparse.Namespace) -> int:
    cfg, c = _clients(args)
    report = runs.finish(c, cfg, args.run, keep_attempts=args.keep_attempts, verify_hashes=args.verify_hashes)
    if args.json:
        _print(report, True, "")
    elif not report["complete"]:
        missing = report["missing"]
        print(f"run {args.run} is not complete: {report['done']}/{report['tasks']} done; missing {', '.join(missing[:10])}" + (" ..." if len(missing) > 10 else ""))
        print(f"next: sweetspot status {args.run}")
    else:
        prefix = cfg.run_prefix(args.run)
        print(f"run {args.run} published: {report['tasks']} tasks, {report['files']} files, {report['bytes'] / 1e9:.2f} GB")
        print(f"  outputs:  {prefix}/out/<task id>/")
        print(f"  manifest: {prefix}/manifest.jsonl")
        if report["repaired_promotions"]:
            print(f"  repaired {report['repaired_promotions']} interrupted output promotion(s)")
        for w in report.get("warnings", []):
            print(f"warning: {w}")
    return 0 if report["complete"] else 1


def cmd_cancel(args: argparse.Namespace) -> int:
    cfg, c = _clients(args)
    report = runs.cancel(c, cfg, args.run)
    _print(
        report, args.json, f"run {args.run} stopped: terminated {len(report['terminated_jobs'])} worker job(s), deleted {len(report['deleted_queues'])} queue(s). Outputs are kept; `sweetspot submit {args.run}` resumes."
    )
    return 0


def cmd_scout(args: argparse.Namespace) -> int:
    from . import scout

    cfg = config.load(args.config, required=False)
    import boto3

    session = boto3.Session(profile_name=args.profile, region_name=getattr(args, "region", None) or (cfg.region if cfg else None))
    return scout.run(args, cfg, Store(session=session), session)


def cmd_worker(args: argparse.Namespace) -> int:
    import boto3

    from . import worker

    from botocore.config import Config as BotoConfig

    queue, prefix = os.environ.get("SWEETSPOT_QUEUE_URL"), os.environ.get("SWEETSPOT_PREFIX")
    if args.run:
        cfg = config.load(args.config)
        assert cfg is not None
        session = boto3.Session(profile_name=args.profile, region_name=cfg.region)
        r = runs.Run(cfg, args.run)
        queue, prefix = runs.queue_url(runs.Clients(Store(session=session), session.client("sqs"), None), r, r.queue_name), r.prefix
        if not queue:
            raise SystemExit(f"run {args.run} has no queue (finished, cancelled, or never submitted)")
    else:
        session = boto3.Session(profile_name=args.profile)
    # Lease renewals must fail fast: a stuck call would delay handing the task back during a Spot shutdown.
    lease_sqs = session.client("sqs", config=BotoConfig(connect_timeout=3, read_timeout=4, retries={"mode": "standard", "total_max_attempts": 2}))
    if not queue or not prefix:
        raise SystemExit("set SWEETSPOT_QUEUE_URL and SWEETSPOT_PREFIX, or pass --run NAME")
    stop = threading.Event()
    worker.install_signal_handlers(stop)
    return worker.run_worker(
        sqs=session.client("sqs"),
        queue_url=queue,
        prefix=prefix,
        store=Store(session=session),
        work_dir=Path(args.work_dir).resolve() if args.work_dir else None,
        sweeper=worker.is_sweeper() and not args.run,
        require_output=os.environ.get("SWEETSPOT_REQUIRE_OUTPUT", "1") != "0",
        max_tasks=args.max_tasks,
        lease_sqs=lease_sqs,
        stop=stop,
    )


# --- parser ------------------------------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="sweetspot", description="Run many independent tasks on AWS Batch Spot, durably and cheaply.", epilog=EXAMPLES, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--version", action="version", version=f"sweetspot {__version__}")
    base = argparse.ArgumentParser(add_help=False)
    base.add_argument("--config", help="stack config (default: $SWEETSPOT_CONFIG or ./sweetspot.json)")
    base.add_argument("--profile", help="AWS profile")
    common = argparse.ArgumentParser(add_help=False, parents=[base])
    common.add_argument("--json", action="store_true", help="machine-readable output")
    sub = ap.add_subparsers(dest="cmd", required=True, metavar="COMMAND")

    p = sub.add_parser("tasks", help="generate tasks.jsonl from a command template", description=tasks.generate.__doc__)
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--inputs", help="file listing one input URI per line ('-' for stdin): one task per input")
    src.add_argument("--inputs-from", metavar="PREFIX", help="list a s3:// prefix or local directory: one task per object")
    src.add_argument("--count", type=int, help="N tasks with no inputs; use {i} in the template")
    p.add_argument("--match", help="regex filter for --inputs-from")
    p.add_argument("--chunks", type=int, default=1, help="split each input into K tasks; use {chunk} and {chunks}")
    p.add_argument("--timeout", type=float, help="per-task timeout in seconds (default 3600)")
    p.add_argument("--units", type=float, help="work units per task, for throughput and cost telemetry")
    p.add_argument("--cwd", help="working directory for the command (default: the task scratch directory)")
    p.add_argument("--outputs", nargs="+", metavar="GLOB", help="keep only these files from {out}")
    p.add_argument("--env", action="append", metavar="K=V", help="environment variable for every task")
    p.add_argument("-o", "--output", help="write here instead of stdout")
    p.add_argument("command", nargs=argparse.REMAINDER, help="-- COMMAND TEMPLATE...")
    p.set_defaults(fn=cmd_tasks)

    p = sub.add_parser("local", help="run tasks on this machine (same runner and commit protocol as the cloud)")
    p.add_argument("tasks", type=Path)
    p.add_argument("--prefix", help="where results go: a directory or s3:// prefix (default ./sweetspot-local/<tasks file stem>)")
    p.add_argument("-j", "--jobs", type=int, default=1, help="tasks to run in parallel")
    p.add_argument("--limit", type=int, help="run at most N tasks")
    p.add_argument("--only", nargs="+", metavar="ID", help="run only these task ids")
    p.add_argument("--allow-empty-output", action="store_true", help="accept tasks that write no files")
    p.set_defaults(fn=cmd_local)

    p = sub.add_parser("submit", parents=[common], help="create or resume a run on AWS Batch")
    p.add_argument("run", help="run name: lowercase letters, digits and single hyphens")
    p.add_argument("tasks", nargs="?", type=Path, help="tasks.jsonl (required the first time)")
    p.add_argument("--image", help="worker image (must contain the sweetspot CLI); default: the run's last image")
    p.add_argument("--vcpus", type=float, help="vCPUs per worker (default 1)")
    p.add_argument("--memory", type=int, help="MiB per worker (default 1536; leave headroom below instance memory)")
    p.add_argument("--gpus", type=int, help="GPUs per worker")
    p.add_argument("--workers", type=int, help="workers to start (default min(pending tasks, 10); 0 to only enqueue)")
    p.add_argument("--lane", help="compute lane from the stack config")
    p.add_argument("--max-attempts", type=int, default=5, help="deliveries per task before it goes to the DLQ")
    p.add_argument("--retry", action="store_true", help="re-enqueue tasks that failed (after you fixed the cause)")
    p.add_argument("--allow-empty-output", action="store_true", help="accept tasks that write no files")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(fn=cmd_submit)

    p = sub.add_parser("status", parents=[common], help="progress, failures, workers, and what to do next")
    p.add_argument("run")
    p.add_argument("--watch", type=float, metavar="SECONDS", help="refresh until the run stops making progress")
    p.set_defaults(fn=cmd_status)

    p = sub.add_parser("logs", parents=[common], help="every attempt of one task, and the latest attempt's output")
    p.add_argument("run")
    p.add_argument("task")
    p.add_argument("--full", action="store_true", help="print complete logs instead of the tail")
    p.set_defaults(fn=cmd_logs)

    p = sub.add_parser("finish", parents=[common], help="verify, publish manifest.jsonl + READY, release queues and workers")
    p.add_argument("run")
    p.add_argument("--keep-attempts", action="store_true", help="keep staged attempt outputs (default: delete after publishing)")
    p.add_argument("--verify-hashes", action="store_true", help="read every output back and check its sha256 (slow for big runs)")
    p.set_defaults(fn=cmd_finish)

    p = sub.add_parser("cancel", parents=[common], help="stop workers and delete the run's queues (outputs stay)")
    p.add_argument("run")
    p.set_defaults(fn=cmd_cancel)

    p = sub.add_parser("scout", parents=[base], help="rank Spot instance types by price and, with --run, by $ per unit")
    try:
        from . import scout

        scout.add_arguments(p)
    except ImportError:
        pass
    p.set_defaults(fn=cmd_scout)

    p = sub.add_parser("worker", parents=[common], help="process a run's queue (container entrypoint; also works on your own machine)")
    p.add_argument("--run", help="join this run (otherwise SWEETSPOT_QUEUE_URL / SWEETSPOT_PREFIX)")
    p.add_argument("--work-dir", help="scratch directory for task files (default: system temp)")
    p.add_argument("--max-tasks", type=int, default=0, help="exit after N tasks (0 = until drained)")
    p.set_defaults(fn=cmd_worker)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.fn(args) or 0)
    except (config.ConfigError, runs.RunError, ValueError) as exc:
        print(f"sweetspot: error: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
