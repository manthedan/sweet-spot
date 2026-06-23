from __future__ import annotations

import hashlib
import json
import math
import re
from typing import Any, Iterable, Sequence

from .s3util import parse_s3_uri


TASK_SCHEMA_V1 = "sweetspot.task.v1"
RESERVED_TASK_ENV_PREFIXES = ("SWEETSPOT_", "AWS_", "ECS_")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/+\-]{0,255}$")
S3_URI_START_RE = re.compile(r"s3://")
S3_TOKEN_END_RE = re.compile(r"[\s'\"<>]")
S3_FIELD_NAMES = {"input_s3", "output_s3", "summary_s3", "done_s3"}


def _embedded_s3_uri_candidates(s: str) -> Iterable[str]:
    # S3 keys may legally contain punctuation such as comma, semicolon, colon,
    # and parentheses. For embedded command strings, split only on delimiters
    # that cannot be part of a shell/JSON token (whitespace, quotes, angle
    # brackets), and emit a candidate for every s3:// occurrence so adjacent
    # URI lists still validate each URI independently.
    for match in S3_URI_START_RE.finditer(s):
        rest = s[match.start() :]
        end_match = S3_TOKEN_END_RE.search(rest)
        yield rest[: end_match.start()] if end_match else rest


def _require_id(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"task requires non-empty string {field}")
    if any(ord(ch) < 32 for ch in value):
        raise ValueError(f"task {field} contains control characters")
    if not SAFE_ID_RE.fullmatch(value):
        raise ValueError(f"task {field} contains unsupported characters or is too long")
    return value


def validate_timeout_seconds(raw: Any, default_timeout_seconds: float, *, max_timeout_seconds: float) -> float:
    raw = default_timeout_seconds if raw is None else raw
    try:
        timeout = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("timeout_seconds must be a positive number") from exc
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout_seconds must be a positive finite number")
    if timeout > max_timeout_seconds:
        raise ValueError(f"timeout_seconds must be <= {max_timeout_seconds:g}s to stay below the SQS 12h visibility limit")
    return timeout


def task_env_overrides(task: dict[str, Any]) -> dict[str, str]:
    raw = task.get("env") or {}
    if not isinstance(raw, dict):
        raise ValueError("task env must be an object mapping string keys to values")
    out: dict[str, str] = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not k:
            raise ValueError("task env keys must be non-empty strings")
        if any(ord(ch) < 32 for ch in k):
            raise ValueError(f"task env key {k!r} contains control characters")
        if any(k.startswith(prefix) for prefix in RESERVED_TASK_ENV_PREFIXES):
            raise ValueError(f"task env key {k!r} uses a reserved prefix")
        if isinstance(v, (dict, list)):
            raise ValueError(f"task env value for {k!r} must be scalar")
        out[k] = str(v)
    return out


def default_done_s3(task: dict[str, Any]) -> str:
    if task.get("done_s3"):
        return str(task["done_s3"])
    output = str(task.get("output_s3") or "")
    if not output:
        raise ValueError("task needs done_s3 or output_s3")
    return output.replace("/shards/", "/done/") + ".done.json"


def task_hash(task: dict[str, Any]) -> str:
    stable = dict(task)
    stable["done_s3"] = default_done_s3(task)
    payload = json.dumps(stable, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def iter_s3_uris(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield from _embedded_s3_uri_candidates(value)
    elif isinstance(value, dict):
        for v in value.values():
            yield from iter_s3_uris(v)
    elif isinstance(value, list):
        for v in value:
            yield from iter_s3_uris(v)


def parse_allowed_s3_prefixes(prefixes: str | Sequence[str] | None) -> tuple[str, ...]:
    if prefixes is None:
        return ()
    raw_items: list[str]
    if isinstance(prefixes, str):
        raw_items = [p for p in re.split(r"[,\s]+", prefixes) if p]
    else:
        raw_items = []
        for item in prefixes:
            raw_items.extend(p for p in re.split(r"[,\s]+", str(item)) if p)
    normalized: list[str] = []
    for prefix in raw_items:
        bucket, key = parse_s3_uri(prefix)
        key = key.strip("/")
        normalized.append(f"s3://{bucket}/{key}" if key else f"s3://{bucket}/")
    return tuple(dict.fromkeys(normalized))


def _s3_uri_allowed(uri: str, allowed_prefixes: Sequence[str]) -> bool:
    if not allowed_prefixes:
        return True
    bucket, key = parse_s3_uri(uri)
    for prefix in allowed_prefixes:
        allowed_bucket, allowed_key = parse_s3_uri(prefix)
        allowed_key = allowed_key.strip("/")
        if bucket != allowed_bucket:
            continue
        if not allowed_key or key.startswith(allowed_key.rstrip("/") + "/"):
            return True
    return False


def validate_task_s3_prefixes(task: dict[str, Any], allowed_s3_prefixes: Sequence[str] | None) -> None:
    allowed = parse_allowed_s3_prefixes(allowed_s3_prefixes)
    uris = set(iter_s3_uris(task))
    try:
        uris.add(default_done_s3(task))
    except ValueError:
        pass
    for uri in sorted(uris):
        # Validate syntax even when no allow-list is configured.
        parse_s3_uri(uri)
        if allowed and not _s3_uri_allowed(uri, allowed):
            raise ValueError(f"task S3 URI {uri!r} is outside allowed prefixes: {', '.join(allowed)}")


def _validate_command(command: Any) -> list[str]:
    if not isinstance(command, list) or not command:
        raise ValueError("task requires command: list[str]")
    if len(command) > 256:
        raise ValueError("task command has too many arguments")
    out: list[str] = []
    for arg in command:
        if not isinstance(arg, str):
            raise ValueError("task requires command: list[str]")
        if "\x00" in arg:
            raise ValueError("task command arguments must not contain NUL bytes")
        if len(arg) > 8192:
            raise ValueError("task command argument is too long")
        out.append(arg)
    return out


def validate_task_model(
    task: dict[str, Any],
    *,
    default_timeout_seconds: float,
    max_timeout_seconds: float,
    allowed_s3_prefixes: Sequence[str] | None = None,
) -> None:
    if not isinstance(task, dict):
        raise ValueError("task must be a JSON object")
    if task.get("schema") != TASK_SCHEMA_V1:
        raise ValueError(f"task schema must be {TASK_SCHEMA_V1!r}")
    _require_id(task.get("run_id"), "run_id")
    _require_id(task.get("task_id"), "task_id")
    _validate_command(task.get("command"))
    validate_timeout_seconds(task.get("timeout_seconds"), default_timeout_seconds, max_timeout_seconds=max_timeout_seconds)
    task_env_overrides(task)
    default_done_s3(task)
    for field in S3_FIELD_NAMES:
        if task.get(field):
            if not isinstance(task[field], str):
                raise ValueError(f"task {field} must be an S3 URI string")
            parse_s3_uri(task[field])
    if task.get("job_type") is not None and (not isinstance(task.get("job_type"), str) or not task.get("job_type")):
        raise ValueError("task job_type must be a non-empty string when present")
    validate_task_s3_prefixes(task, allowed_s3_prefixes)
