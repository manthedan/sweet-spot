"""Stack configuration: what `tofu output -raw sweetspot_config > sweetspot.json` produces.

    {
      "region": "us-west-2",
      "prefix": "s3://my-bucket/sweetspot",       # runs live under <prefix>/<run>/
      "name": "sweetspot",                        # AWS resource name prefix (queues, job definitions)
      "job_role_arn": "arn:aws:iam::123456789012:role/sweetspot-worker",
      "log_group": "/aws/batch/sweetspot",
      "lanes": {"x86": {"job_queue": "sweetspot-x86", "arch": "x86_64", "instance_types": ["c7a.medium"]},
                "arm": {"job_queue": "sweetspot-arm", "arch": "arm64", "instance_types": ["c7g.medium"]}},
      "default_lane": "x86"
    }

Looked up from --config, then $SWEETSPOT_CONFIG, then ./sweetspot.json.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ARCHES = {"x86_64", "arm64"}


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Lane:
    name: str
    job_queue: str
    arch: str = "x86_64"
    instance_types: tuple[str, ...] = ()
    gpu: bool = False


@dataclass(frozen=True)
class Config:
    region: str
    prefix: str
    name: str = "sweetspot"
    job_role_arn: str = ""
    log_group: str = ""
    lanes: dict[str, Lane] = field(default_factory=dict)
    default_lane: str = ""
    source: str = ""

    def lane(self, name: str | None) -> Lane:
        key = name or self.default_lane or (next(iter(self.lanes)) if len(self.lanes) == 1 else "")
        if not key:
            raise ConfigError(f"choose a lane with --lane ({', '.join(self.lanes) or 'none configured'})")
        if key not in self.lanes:
            raise ConfigError(f"unknown lane {key!r}; configured: {', '.join(self.lanes) or 'none'}")
        return self.lanes[key]

    def run_prefix(self, run: str) -> str:
        return f"{self.prefix.rstrip('/')}/{run}"


def parse(data: Any, source: str = "") -> Config:
    if not isinstance(data, dict):
        raise ConfigError(f"{source}: config must be a JSON object")
    for key in ("region", "prefix"):
        if not isinstance(data.get(key), str) or not data[key]:
            raise ConfigError(f"{source}: missing {key!r}")
    lanes: dict[str, Lane] = {}
    for lane_name, raw in (data.get("lanes") or {}).items():
        if not isinstance(raw, dict) or not raw.get("job_queue"):
            raise ConfigError(f"{source}: lane {lane_name!r} needs a job_queue")
        arch = raw.get("arch", "x86_64")
        if arch not in ARCHES:
            raise ConfigError(f"{source}: lane {lane_name!r} arch must be one of {sorted(ARCHES)}")
        lanes[lane_name] = Lane(lane_name, raw["job_queue"], arch, tuple(raw.get("instance_types") or ()), bool(raw.get("gpu", False)))
    default_lane = data.get("default_lane") or ""
    if default_lane and default_lane not in lanes:
        raise ConfigError(f"{source}: default_lane {default_lane!r} is not a configured lane")
    return Config(
        region=data["region"],
        prefix=data["prefix"].rstrip("/"),
        name=data.get("name") or "sweetspot",
        job_role_arn=data.get("job_role_arn") or "",
        log_group=data.get("log_group") or "",
        lanes=lanes,
        default_lane=default_lane,
        source=source,
    )


def load(path: str | Path | None = None, *, required: bool = True) -> Config | None:
    candidates = [Path(p) for p in (path, os.environ.get("SWEETSPOT_CONFIG")) if p] or [Path("sweetspot.json")]
    for candidate in candidates:
        if candidate.is_file():
            try:
                data = json.loads(candidate.read_text())
            except ValueError as exc:
                raise ConfigError(f"{candidate}: invalid JSON: {exc}") from exc
            return parse(data, str(candidate))
        if path or os.environ.get("SWEETSPOT_CONFIG"):
            raise ConfigError(f"config file not found: {candidate}")
    if required:
        raise ConfigError("no sweetspot.json found; create one with `tofu output -raw sweetspot_config > sweetspot.json` (see infra/opentofu) or pass --config")
    return None
