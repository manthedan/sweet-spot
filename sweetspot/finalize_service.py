from __future__ import annotations

import argparse
import concurrent.futures as cf
import contextlib
import io
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

from .aws_batch import iso_now
from .s3util import parse_s3_uri, s3_delete, s3_download_text, s3_exists, s3_join, s3_upload_file, s3_upload_text
from .task_model import default_done_s3, task_hash
from .worker import validate_done_marker


FINALIZER_DEFAULT_MAX_INLINE_OUTPUTS = 1000
FINALIZER_FUTURE_BUFFER_MULTIPLIER = 4


AwsClientFactory = Callable[[argparse.Namespace, str], Any]
JsonlIteratorFactory = Callable[[Path], Iterator[dict[str, Any]]]
CheckTaskFn = Callable[..., dict[str, Any]]
FinalizeFn = Callable[[argparse.Namespace], int]


def build_integrated_finalizer_args(args: argparse.Namespace, *, spec: dict[str, Any], tasks_path: Path, artifact_dir: Path) -> argparse.Namespace:
    return argparse.Namespace(
        allow_incomplete_ready=bool(getattr(args, "finalize_allow_incomplete_ready", False)),
        allow_legacy_done_markers=bool(getattr(args, "allow_legacy_done_markers", False)),
        artifact_dir=artifact_dir,
        dry_run=bool(getattr(args, "finalize_dry_run", False)),
        max_inline_outputs=int(getattr(args, "finalize_max_inline_outputs", FINALIZER_DEFAULT_MAX_INLINE_OUTPUTS)),
        output_prefix=str(spec["output_prefix"]),
        preload_s3_prefix=list(getattr(args, "finalize_preload_s3_prefix", None) or []),
        profile=getattr(args, "profile", None),
        progress_interval=int(getattr(args, "finalize_progress_interval", 0)),
        publish_ready=bool(getattr(args, "finalize_publish_ready", False)),
        ready_key=str(getattr(args, "finalize_ready_key", "READY")),
        region=getattr(args, "region", None),
        require_complete=False,
        run_id=str(spec["run_id"]),
        tasks_jsonl=tasks_path,
        tasks_s3=None,
        upload=bool(getattr(args, "finalize_upload", False)),
        use_listing_index=bool(getattr(args, "finalize_use_listing_index", False)),
        workers=int(getattr(args, "finalize_workers", 32)),
        write_repair_jsonl=None,
    )


def run_integrated_finalizer(
    args: argparse.Namespace,
    *,
    spec: dict[str, Any],
    tasks_path: Path,
    artifact_dir: Path,
    finalizer_func: FinalizeFn,
) -> tuple[int, dict[str, Any]]:
    finalizer_args = build_integrated_finalizer_args(args, spec=spec, tasks_path=tasks_path, artifact_dir=artifact_dir)
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        rc = finalizer_func(finalizer_args)
    try:
        report = json.loads(out.getvalue())
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"integrated finalizer did not emit JSON: {out.getvalue()!r}") from exc
    if not isinstance(report, dict):
        raise RuntimeError("integrated finalizer did not emit a JSON object")
    return rc, report


def iter_s3_jsonl(s3, uri: str) -> Iterator[dict[str, Any]]:
    bucket, key = parse_s3_uri(uri)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    if hasattr(body, "iter_lines"):
        lines = body.iter_lines()
    else:
        lines = body.read().splitlines()
    for line_no, line in enumerate(lines, start=1):
        if isinstance(line, bytes):
            line = line.decode("utf-8")
        line = str(line).strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSONL at {uri}:{line_no}") from exc
        if not isinstance(obj, dict):
            raise ValueError(f"task at {uri}:{line_no} is not an object")
        yield obj


def iter_tasks_for_finalizer(args: argparse.Namespace, s3, *, iter_jsonl: JsonlIteratorFactory) -> Iterator[dict[str, Any]]:
    if args.tasks_jsonl:
        yield from iter_jsonl(args.tasks_jsonl)
        return
    tasks_s3 = args.tasks_s3 or s3_join(args.output_prefix, "manifests", "tasks.jsonl")
    yield from iter_s3_jsonl(s3, tasks_s3)


class S3ExistenceIndex:
    def __init__(self, s3, prefixes: Iterable[str]) -> None:
        self.s3 = s3
        self.prefixes: list[tuple[str, str]] = []
        self.keys_by_bucket: dict[str, set[str]] = {}
        for uri in prefixes:
            if not uri:
                continue
            bucket, key = parse_s3_uri(uri)
            key = key.rstrip("/") + "/" if key else ""
            self.prefixes.append((bucket, key))

    def load(self) -> None:
        for bucket, prefix in self.prefixes:
            keys = self.keys_by_bucket.setdefault(bucket, set())
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj.get("Key")
                    if key is not None:
                        keys.add(str(key))

    def indexed_prefixes(self) -> list[str]:
        return [f"s3://{bucket}/{prefix}" for bucket, prefix in self.prefixes]

    def exists(self, uri: str) -> bool:
        bucket, key = parse_s3_uri(uri)
        for indexed_bucket, indexed_prefix in self.prefixes:
            if bucket == indexed_bucket and key.startswith(indexed_prefix):
                return key in self.keys_by_bucket.get(bucket, set())
        return s3_exists(self.s3, uri)


def finalizer_existence_index(args: argparse.Namespace, s3) -> S3ExistenceIndex | None:
    prefixes = list(getattr(args, "preload_s3_prefix", None) or [])
    if getattr(args, "use_listing_index", False):
        prefixes.extend(
            [
                s3_join(args.output_prefix, "done"),
                s3_join(args.output_prefix, "shards"),
                s3_join(args.output_prefix, "summaries"),
            ]
        )
    if not prefixes:
        return None
    index = S3ExistenceIndex(s3, prefixes)
    index.load()
    return index


def s3_exists_indexed(s3, uri: str, existence_index: S3ExistenceIndex | None) -> bool:
    return existence_index.exists(uri) if existence_index else s3_exists(s3, uri)


def repair_done_marker_candidates(s3, canonical_done_s3: str) -> Iterator[str]:
    bucket, key = parse_s3_uri(canonical_done_s3)
    prefix = key + ".repair-"
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            candidate_key = obj.get("Key")
            if candidate_key:
                yield f"s3://{bucket}/{candidate_key}"


def repair_task_candidates_for_marker_validation(task: dict[str, Any], repair_done_s3: str) -> Iterator[dict[str, Any]]:
    """Yield task payload variants that could have produced a repair marker.

    Current repair tasks include sweetspot_repair_reason, which participates in
    the full task hash. Older repair tasks did not, so validate both forms.
    """
    base = dict(task)
    base["done_s3"] = repair_done_s3
    yield base
    for reason in ("invalid_done_marker", "missing_output", "output_without_done", "incomplete"):
        with_reason = dict(base)
        with_reason["sweetspot_repair_reason"] = reason
        yield with_reason


def valid_repair_done_marker(s3, task: dict[str, Any], canonical_done_s3: str, *, allow_legacy_done_markers: bool = False) -> tuple[str, dict[str, Any]] | None:
    for repair_done_s3 in repair_done_marker_candidates(s3, canonical_done_s3):
        repair_marker = done_marker_for_task(s3, task, repair_done_s3, None)
        if repair_marker is None or repair_marker.get("_sweetspot_marker_parse_error"):
            continue
        for repair_task in repair_task_candidates_for_marker_validation(task, repair_done_s3):
            try:
                validate_done_marker(s3, repair_task, repair_marker, task_hash(repair_task), allow_legacy_done_markers=allow_legacy_done_markers)
            except ValueError:
                continue
            return repair_done_s3, repair_marker
    return None


def read_tasks_for_finalizer(args: argparse.Namespace, s3, *, iter_jsonl: JsonlIteratorFactory) -> list[dict[str, Any]]:
    return list(iter_tasks_for_finalizer(args, s3, iter_jsonl=iter_jsonl))


def done_marker_for_task(s3, task: dict[str, Any], done_s3: str, existence_index: S3ExistenceIndex | None = None) -> dict[str, Any] | None:
    if not s3_exists_indexed(s3, done_s3, existence_index):
        return None
    try:
        marker = json.loads(s3_download_text(s3, done_s3))
    except json.JSONDecodeError as exc:
        return {"_sweetspot_marker_parse_error": f"done marker is not valid JSON: {exc}"}
    if not isinstance(marker, dict):
        return {"_sweetspot_marker_parse_error": f"done marker is not an object: {done_s3}"}
    return marker


def check_task(s3, task: dict[str, Any], existence_index: S3ExistenceIndex | None = None, *, allow_legacy_done_markers: bool = False) -> dict[str, Any]:
    logical_output_s3 = str(task.get("output_s3") or "")
    summary_s3 = str(task.get("summary_s3") or "")
    done_s3 = default_done_s3(task)
    marker = done_marker_for_task(s3, task, done_s3, existence_index)
    marker_validation_error = None
    if marker is not None:
        marker_validation_error = marker.get("_sweetspot_marker_parse_error")
        if not marker_validation_error:
            try:
                # validate_done_marker verifies schema/run/task/hash and, for
                # v2, HEADs the immutable attempt output to check size/SHA
                # metadata. Validation failures are status/repair inputs, not
                # finalizer crashes.
                validate_done_marker(s3, task, marker, task_hash(task), allow_legacy_done_markers=allow_legacy_done_markers)
            except ValueError as exc:
                marker_validation_error = str(exc)
    if marker is not None and marker_validation_error:
        repair_candidate = valid_repair_done_marker(s3, task, done_s3, allow_legacy_done_markers=allow_legacy_done_markers)
        if repair_candidate is not None:
            done_s3, marker = repair_candidate
            marker_validation_error = None
    done_exists = marker is not None
    marker_valid = marker is not None and marker_validation_error is None
    output_s3 = logical_output_s3
    output_exists = False if marker_validation_error else (s3_exists_indexed(s3, logical_output_s3, existence_index) if logical_output_s3 else False)
    summary_exists = s3_exists_indexed(s3, summary_s3, existence_index) if summary_s3 else False
    if marker and isinstance(marker.get("output"), dict):
        output_s3 = str(marker["output"].get("uri") or logical_output_s3)
        # Keep an explicit existence check here even though v2 validation
        # already verified metadata, so the status record reflects current S3
        # availability and listing-index decisions.
        output_exists = False if marker_validation_error else s3_exists_indexed(s3, output_s3, existence_index)
    if marker and marker.get("attempt_summary_s3"):
        summary_s3 = str(marker.get("attempt_summary_s3"))
        summary_exists = s3_exists_indexed(s3, summary_s3, existence_index)
    state = "done" if marker_valid else ("invalid_done_marker" if done_exists else "incomplete")
    if done_exists and logical_output_s3 and not output_exists:
        state = "missing_output"
    elif output_exists and not marker_valid:
        state = "output_without_done"
    return {
        "task_id": task.get("task_id"),
        "output_s3": output_s3,
        "logical_output_s3": logical_output_s3,
        "summary_s3": summary_s3,
        "done_s3": done_s3,
        "done_exists": done_exists,
        "marker_valid": marker_valid,
        "output_exists": output_exists,
        "summary_exists": summary_exists,
        "state": state,
        "marker_validation_error": marker_validation_error,
    }


def repair_task_for_record(task: dict[str, Any], record: dict[str, Any], repair_suffix: str) -> dict[str, Any]:
    repair = dict(task)
    if record["done_exists"] and (record["state"] == "missing_output" or not record.get("marker_valid", False)):
        # Existing invalid/incomplete done markers make normal workers collide
        # on the canonical marker. Keep the original output_s3 so missing
        # objects are regenerated, but write the repair completion marker
        # elsewhere; the next finalize validates the original output location.
        repair["done_s3"] = str(record["done_s3"]) + f".repair-{repair_suffix}"
    return repair


def run_finalizer(
    args: argparse.Namespace,
    *,
    aws_client: AwsClientFactory,
    iter_jsonl: JsonlIteratorFactory,
    check_task_fn: CheckTaskFn = check_task,
) -> int:
    dry_run = bool(getattr(args, "dry_run", False))
    requested_upload = bool(args.upload)
    effective_upload = requested_upload and not dry_run
    if args.publish_ready and not (requested_upload or dry_run):
        raise SystemExit("--publish-ready requires --upload unless --dry-run is set")
    args.ready_key = str(args.ready_key).strip("/")
    reserved_ready_keys = {"manifests/final_manifest.json", "manifests/repair_tasks.jsonl", "manifests/task_status.jsonl", "manifests/outputs.jsonl"}
    if args.publish_ready and (not args.ready_key or args.ready_key in reserved_ready_keys):
        raise SystemExit("--ready-key must not be empty or collide with SweetSpot manifest paths")
    if args.workers <= 0:
        raise SystemExit("--workers must be positive")
    s3 = aws_client(args, "s3")
    existence_index = finalizer_existence_index(args, s3)
    artifact_dir = args.artifact_dir or Path("artifacts") / args.run_id / "finalizer"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    final_path = artifact_dir / "final_manifest.json"
    repair_path = args.write_repair_jsonl or artifact_dir / "repair_tasks.jsonl"
    status_path = artifact_dir / "task_status.jsonl"
    outputs_path = artifact_dir / "outputs.jsonl"
    final_s3 = s3_join(args.output_prefix, "manifests", "final_manifest.json")
    repair_s3 = s3_join(args.output_prefix, "manifests", "repair_tasks.jsonl")
    status_s3 = s3_join(args.output_prefix, "manifests", "task_status.jsonl")
    outputs_s3 = s3_join(args.output_prefix, "manifests", "outputs.jsonl")
    ready_s3 = s3_join(args.output_prefix, args.ready_key)

    counts: Counter[str] = Counter()
    seen_task_ids: dict[str, int] = {}
    checked = submitted = 0
    max_inline_outputs = max(0, int(getattr(args, "max_inline_outputs", FINALIZER_DEFAULT_MAX_INLINE_OUTPUTS)))
    inline_outputs: list[str] = []
    missing_task_ids: list[Any] = []
    output_without_done_task_ids: list[Any] = []
    missing_output_task_ids: list[Any] = []
    repair_suffix = str(time.time_ns())
    pending: dict[cf.Future, tuple[int, dict[str, Any]]] = {}
    ready_records: dict[int, tuple[dict[str, Any], dict[str, Any]]] = {}
    next_to_emit = 0
    buffer_limit = max(1, args.workers * FINALIZER_FUTURE_BUFFER_MULTIPLIER)

    def remember(xs: list[Any], value: Any) -> None:
        if len(xs) < 1000:
            xs.append(value)

    def process_record(task: dict[str, Any], record: dict[str, Any], status_f, repair_f, outputs_f) -> None:
        nonlocal checked
        checked += 1
        counts["task"] += 1
        if record["done_exists"]:
            counts["done_marker"] += 1
        if record.get("marker_validation_error"):
            counts["invalid_marker"] += 1
        if record["marker_valid"]:
            counts["done"] += 1
        if record["output_exists"]:
            counts["output"] += 1
        if record["summary_exists"]:
            counts["summary"] += 1
        is_missing_done = not record["marker_valid"]
        is_missing_output = bool(record["output_s3"] and not record["output_exists"])
        is_missing = is_missing_done or is_missing_output
        if record["state"] == "output_without_done":
            counts["output_without_done"] += 1
            remember(output_without_done_task_ids, record["task_id"])
        if is_missing_done:
            counts["missing_done"] += 1
        if is_missing_output:
            counts["missing_output"] += 1
            remember(missing_output_task_ids, record["task_id"])
        if is_missing:
            counts["missing"] += 1
            remember(missing_task_ids, record["task_id"])
            repair = repair_task_for_record(task, record, repair_suffix)
            repair_f.write(json.dumps(repair, sort_keys=True) + "\n")
        if record["marker_valid"] and (not record["output_s3"] or record["output_exists"]):
            output_uri = record["output_s3"]
            counts["output_manifest"] += 1
            if len(inline_outputs) < max_inline_outputs:
                inline_outputs.append(output_uri)
            outputs_f.write(json.dumps({"task_id": record["task_id"], "output_s3": output_uri}, sort_keys=True) + "\n")
        status_f.write(json.dumps(record, sort_keys=True) + "\n")
        if args.progress_interval and checked % args.progress_interval == 0:
            print(f"sweetspot finalize progress: checked={checked}", file=sys.stderr)

    def drain(done_futures: set[cf.Future], status_f, repair_f, outputs_f) -> None:
        nonlocal next_to_emit
        for fut in done_futures:
            index, task = pending.pop(fut)
            ready_records[index] = (task, fut.result())
        while next_to_emit in ready_records:
            task, record = ready_records.pop(next_to_emit)
            process_record(task, record, status_f, repair_f, outputs_f)
            next_to_emit += 1

    with (
        status_path.open("w", encoding="utf-8") as status_f,
        repair_path.open("w", encoding="utf-8") as repair_f,
        outputs_path.open("w", encoding="utf-8") as outputs_f,
        cf.ThreadPoolExecutor(max_workers=args.workers) as ex,
    ):
        for line_no, task in enumerate(iter_tasks_for_finalizer(args, s3, iter_jsonl=iter_jsonl), start=1):
            task_id = str(task.get("task_id") or "")
            if task_id in seen_task_ids:
                raise SystemExit(f"duplicate task_id values in finalizer tasks: {task_id!r} at lines {seen_task_ids[task_id]} and {line_no}")
            if task_id:
                seen_task_ids[task_id] = line_no
            pending[ex.submit(check_task_fn, s3, task, existence_index, allow_legacy_done_markers=bool(getattr(args, "allow_legacy_done_markers", False)))] = (submitted, task)
            submitted += 1
            while len(pending) + len(ready_records) >= buffer_limit and pending:
                done_futures, _ = cf.wait(pending.keys(), return_when=cf.FIRST_COMPLETED)
                drain(done_futures, status_f, repair_f, outputs_f)
        while pending:
            done_futures, _ = cf.wait(pending.keys(), return_when=cf.FIRST_COMPLETED)
            drain(done_futures, status_f, repair_f, outputs_f)
    if args.progress_interval and checked and checked % args.progress_interval != 0:
        print(f"sweetspot finalize progress: checked={checked}", file=sys.stderr)

    final_manifest = {
        "schema": "sweetspot.final_manifest.v1",
        "run_id": args.run_id,
        "finalized_at": iso_now(),
        "output_prefix": args.output_prefix.rstrip("/"),
        "task_count": counts["task"],
        "done_count": counts["done"],
        "done_marker_count": counts["done_marker"],
        "invalid_marker_count": counts["invalid_marker"],
        "output_count": counts["output"],
        "summary_count": counts["summary"],
        "missing_count": counts["missing"],
        "missing_done_count": counts["missing_done"],
        "output_without_done_count": counts["output_without_done"],
        "missing_output_count": counts["missing_output"],
        "complete": counts["missing"] == 0,
        "missing_task_ids": missing_task_ids,
        "output_without_done_task_ids": output_without_done_task_ids,
        "missing_output_task_ids": missing_output_task_ids,
        "outputs": inline_outputs,
        "outputs_truncated": counts["output_manifest"] > len(inline_outputs),
        "outputs_manifest": str(outputs_path),
        "outputs_manifest_s3": outputs_s3 if effective_upload else None,
        "task_status": str(status_path),
        "task_status_s3": status_s3 if effective_upload else None,
        "repair_task_count": counts["missing"],
        "final_manifest_s3": final_s3 if effective_upload else None,
        "repair_tasks_s3": repair_s3 if effective_upload and counts["missing"] else None,
        "ready_s3": ready_s3 if args.publish_ready and effective_upload else None,
        "dry_run": dry_run,
        "would_upload": requested_upload,
        "would_publish_ready": bool(args.publish_ready and (counts["missing"] == 0 or args.allow_incomplete_ready)),
        "would_final_manifest_s3": final_s3 if requested_upload else None,
        "would_repair_tasks_s3": repair_s3 if requested_upload and counts["missing"] else None,
        "would_task_status_s3": status_s3 if requested_upload else None,
        "would_outputs_manifest_s3": outputs_s3 if requested_upload else None,
        "would_ready_s3": ready_s3 if args.publish_ready else None,
        "existence_index_prefixes": existence_index.indexed_prefixes() if existence_index else [],
    }
    if submitted != checked:
        raise RuntimeError(f"finalizer internal error: submitted {submitted}, checked {checked}")
    if args.publish_ready and not final_manifest["complete"] and not args.allow_incomplete_ready:
        final_manifest["ready_s3"] = None

    final_path.write_text(json.dumps(final_manifest, indent=2, sort_keys=True) + "\n")

    if effective_upload:
        if args.publish_ready:
            s3_delete(s3, ready_s3)
        s3_upload_text(s3, json.dumps(final_manifest, indent=2, sort_keys=True) + "\n", final_s3)
        s3_upload_file(s3, status_path, status_s3, "application/jsonl")
        s3_upload_file(s3, outputs_path, outputs_s3, "application/jsonl")
        if counts["missing"]:
            s3_upload_file(s3, repair_path, repair_s3, "application/jsonl")

    if args.publish_ready and not final_manifest["complete"] and not args.allow_incomplete_ready:
        print(
            json.dumps(
                {
                    **{k: final_manifest[k] for k in ["schema", "run_id", "task_count", "done_count", "output_count", "summary_count", "missing_count", "missing_output_count", "output_without_done_count", "complete"]},
                    "final_manifest": str(final_path),
                    "repair_tasks": str(repair_path),
                    "task_status": str(status_path),
                    "outputs_manifest": str(outputs_path),
                    "final_manifest_s3": final_s3 if effective_upload else None,
                    "ready_s3": None,
                    "dry_run": dry_run,
                    "refused_ready": True,
                    "would_final_manifest_s3": final_s3 if requested_upload else None,
                    "would_ready_s3": ready_s3 if args.publish_ready else None,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2

    if effective_upload and args.publish_ready:
        ready = {"schema": "sweetspot.ready_marker.v1", "run_id": args.run_id, "ready_at": iso_now(), "final_manifest_s3": final_s3, "complete": final_manifest["complete"]}
        s3_upload_text(s3, json.dumps(ready, indent=2, sort_keys=True) + "\n", ready_s3)
    print(
        json.dumps(
            {
                **{k: final_manifest[k] for k in ["schema", "run_id", "task_count", "done_count", "output_count", "summary_count", "missing_count", "missing_output_count", "output_without_done_count", "complete"]},
                "final_manifest": str(final_path),
                "repair_tasks": str(repair_path),
                "task_status": str(status_path),
                "outputs_manifest": str(outputs_path),
                "final_manifest_s3": final_s3 if effective_upload else None,
                "ready_s3": ready_s3 if args.publish_ready and effective_upload else None,
                "dry_run": dry_run,
                "would_final_manifest_s3": final_s3 if requested_upload else None,
                "would_ready_s3": ready_s3 if args.publish_ready else None,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 2 if args.require_complete and not final_manifest["complete"] else 0
