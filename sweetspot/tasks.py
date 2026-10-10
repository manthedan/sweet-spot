"""Task format, validation, and the `sweetspot tasks` generator.

A task is one JSON object per line:

    {"id": "shard_0007",
     "command": ["python", "label.py", "--input", "{in}/shard.jsonl.zst", "--out", "{out}/labels.jsonl.zst"],
     "inputs": {"shard.jsonl.zst": "s3://bucket/data/shard_0007.jsonl.zst"},
     "env": {"DEPTH": "10"},
     "timeout": 3600,
     "cwd": "/work",                    # optional; default is the task's scratch directory
     "outputs": ["*.jsonl.zst"],        # optional glob allowlist of files under {out} to keep
     "units": 25000}                    # optional work units, for throughput/cost telemetry

At run time `{in}`, `{out}` and `{id}` in command arguments (and cwd) are replaced with the task's input
directory, output directory and id. Nothing else in the command is interpreted, and there is no escaping (to pass
a literal "{in}", put it in an env variable).
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Iterator

TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$")
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
RUNTIME_PLACEHOLDER_RE = re.compile(r"\{(in|out|id)\}")
KNOWN_KEYS = {"id", "command", "inputs", "env", "timeout", "cwd", "outputs", "units"}
MAX_TIMEOUT = 11 * 3600  # must stay below the 12h SQS visibility ceiling


def validate(raw: Any, *, where: str = "task") -> dict[str, Any]:
    """Return the normalized task, or raise ValueError naming what is wrong."""
    if not isinstance(raw, dict):
        raise ValueError(f"{where}: must be a JSON object")
    unknown = set(raw) - KNOWN_KEYS
    if unknown:
        raise ValueError(f"{where}: unknown keys {sorted(unknown)} (allowed: {sorted(KNOWN_KEYS)})")
    task_id = raw.get("id")
    if not isinstance(task_id, str) or not TASK_ID_RE.fullmatch(task_id):
        raise ValueError(f"{where}: id must match {TASK_ID_RE.pattern}, got {task_id!r}")
    where = f"task {task_id}"
    command = raw.get("command")
    if not isinstance(command, list) or not command or not all(isinstance(a, str) for a in command):
        raise ValueError(f"{where}: command must be a non-empty list of strings")
    if any("\x00" in a for a in command):
        raise ValueError(f"{where}: command contains a NUL byte")
    inputs = raw.get("inputs") or {}
    if not isinstance(inputs, dict):
        raise ValueError(f"{where}: inputs must map local file names to URIs")
    for name, uri in inputs.items():
        _check_input_name(name, where)
        if not isinstance(uri, str) or not uri:
            raise ValueError(f"{where}: input {name!r} must be a URI string")
    env = raw.get("env") or {}
    if not isinstance(env, dict):
        raise ValueError(f"{where}: env must be an object")
    for key, value in env.items():
        if not isinstance(key, str) or not ENV_KEY_RE.fullmatch(key) or key.startswith(("SWEETSPOT_", "AWS_")):
            raise ValueError(f"{where}: env key {key!r} is invalid or reserved")
        if isinstance(value, (dict, list)) or value is None:
            raise ValueError(f"{where}: env value for {key!r} must be a scalar")
        if "\x00" in str(value):
            raise ValueError(f"{where}: env value for {key!r} contains a NUL byte")
    timeout = raw.get("timeout")
    if timeout is not None and (isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= MAX_TIMEOUT):
        raise ValueError(f"{where}: timeout must be a number of seconds in (0, {MAX_TIMEOUT}]")
    cwd = raw.get("cwd")
    if cwd is not None and (not isinstance(cwd, str) or not cwd or "\x00" in cwd):
        raise ValueError(f"{where}: cwd must be a non-empty path string")
    outputs = raw.get("outputs")
    if outputs is not None and (not isinstance(outputs, list) or not outputs or not all(isinstance(g, str) and g for g in outputs)):
        raise ValueError(f"{where}: outputs must be a non-empty list of glob patterns")
    units = raw.get("units")
    if units is not None and (isinstance(units, bool) or not isinstance(units, (int, float)) or units < 0):
        raise ValueError(f"{where}: units must be a non-negative number")
    out: dict[str, Any] = {"id": task_id, "command": list(command)}
    if inputs:
        out["inputs"] = dict(sorted(inputs.items()))
    if env:
        out["env"] = {k: str(v) for k, v in sorted(env.items())}
    if timeout is not None:
        out["timeout"] = timeout
    if cwd is not None:
        out["cwd"] = cwd
    if outputs is not None:
        out["outputs"] = list(outputs)
    if units is not None:
        out["units"] = units
    return out


def _check_input_name(name: Any, where: str) -> None:
    if not isinstance(name, str) or not name:
        raise ValueError(f"{where}: input names must be non-empty strings")
    p = PurePosixPath(name)
    if p.is_absolute() or ".." in p.parts or "\\" in name or "\x00" in name:
        raise ValueError(f"{where}: input name {name!r} must be a relative path inside the input directory")


def task_hash(task: dict[str, Any]) -> str:
    canonical = json.dumps(task, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def expand(arg: str, *, in_dir: str, out_dir: str, task_id: str) -> str:
    values = {"in": in_dir, "out": out_dir, "id": task_id}
    return RUNTIME_PLACEHOLDER_RE.sub(lambda m: values[m.group(1)], arg)


def read_tasks(path: Path) -> list[dict[str, Any]]:
    """Read and validate a tasks.jsonl file; ids must be unique."""
    tasks: list[dict[str, Any]] = []
    seen: set[str] = set()
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_no}: invalid JSON: {exc}") from exc
            task = validate(raw, where=f"{path}:{line_no}")
            if task["id"] in seen:
                raise ValueError(f"{path}:{line_no}: duplicate task id {task['id']!r}")
            seen.add(task["id"])
            tasks.append(task)
    if not tasks:
        raise ValueError(f"{path}: no tasks")
    return tasks


def dumps_tasks(tasks: Iterable[dict[str, Any]]) -> str:
    return "".join(json.dumps(t, sort_keys=True, separators=(",", ":")) + "\n" for t in tasks)


# --- generator -------------------------------------------------------------------------------------------

GEN_PLACEHOLDER_RE = re.compile(r"\{(input|name|stem|i|chunk|chunks)\}")
_COMPRESSION_SUFFIXES = (".zst", ".gz", ".bz2", ".xz", ".lz4")


def _stem(name: str) -> str:
    for suffix in _COMPRESSION_SUFFIXES:
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    return name.rsplit(".", 1)[0] if "." in name else name


def _safe_id(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("._-")[:180] or "task"


def generate(
    template: list[str],
    *,
    inputs: list[str] | None = None,
    count: int | None = None,
    chunks: int = 1,
    timeout: float | None = None,
    env: dict[str, str] | None = None,
    extra: dict[str, Any] | None = None,
) -> Iterator[dict[str, Any]]:
    """Expand a command template into tasks: one per input URI (or per index when count is given), times chunks.

    Generation-time placeholders: {input} (the staged input path, i.e. {in}/<name>), {name}, {stem}, {i},
    {chunk}, {chunks}. Run-time placeholders {in}, {out}, {id} are passed through untouched.
    """
    if (inputs is None) == (count is None):
        raise ValueError("give exactly one of inputs or count")
    if chunks < 1:
        raise ValueError("chunks must be >= 1")
    items: list[str | None] = list(inputs) if inputs is not None else [None] * int(count or 0)
    # Only URLs have query strings; S3 keys and local paths may contain "?" and "#" literally.
    names = [PurePosixPath(u.split("?", 1)[0].split("#", 1)[0] if u.startswith(("http://", "https://")) else u).name if u else "" for u in items]
    stems = [_stem(n) for n in names]
    if inputs is not None and len(set(stems)) != len(stems):
        dupes = sorted({s for s in stems if stems.count(s) > 1})[:5]
        raise ValueError(f"input file names must have unique stems to derive task ids; duplicates: {dupes}")
    width = max(4, len(str(max(len(items) - 1, 0))))
    ids = [_safe_id(stems[i]) if uri else f"task-{i:0{width}d}" for i, uri in enumerate(items)]
    clashes = sorted({x for x in ids if ids.count(x) > 1})
    if clashes:
        raise ValueError(f"input names map to the same task id after sanitizing: {clashes[:5]}")
    for i, uri in enumerate(items):
        base_id = ids[i]
        for chunk in range(chunks):
            values = {"input": f"{{in}}/{names[i]}", "name": names[i], "stem": stems[i], "i": str(i), "chunk": str(chunk), "chunks": str(chunks)}
            if uri is None and any("{input}" in a or "{name}" in a or "{stem}" in a for a in template):
                raise ValueError("{input}/{name}/{stem} need --inputs")
            command = [GEN_PLACEHOLDER_RE.sub(lambda m: values[m.group(1)], arg) for arg in template]
            task: dict[str, Any] = {"id": base_id if chunks == 1 else f"{base_id}-c{chunk:0{len(str(chunks - 1))}d}", "command": command}
            if uri:
                task["inputs"] = {names[i]: uri}
            if env:
                task["env"] = env
            if timeout is not None:
                task["timeout"] = timeout
            task.update(extra or {})
            yield validate(task)
