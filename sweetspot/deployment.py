from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .planner import PLAN_SCHEMA_V1, PlannerSpecError, validate_plan
from .s3util import parse_s3_uri


DEPLOYMENT_SCHEMA_V1 = "sweetspot.deployment.v1"
_ARCHITECTURES = {"x86_64", "arm64"}
_SQS_RE = re.compile(r"^https://sqs[.]([a-z0-9-]+)[.]amazonaws[.]com(?:[.]cn)?/(\d{12})/[^/]+$")
_ECR_RE = re.compile(r"^(?P<account>\d{12})[.]dkr[.]ecr[.](?P<region>[a-z0-9-]+)[.]amazonaws[.]com(?:[.]cn)?/(?P<repo>[^@:]+)(?P<suffix>[:@].+)?$")
_CANARY_ROUTE_RE = re.compile(r"^(?P<arch>x86_64|arm64)-(?P<vcpus>[1-9][0-9]*)vcpu-(?P<memory>[1-9][0-9]*)mib$")


def _job_definition_revision_pinned(value: str) -> bool:
    # AWS Batch accepts either name:revision or full job-definition ARN ending
    # in :name:revision. Requiring the numeric revision avoids preflighting one
    # active revision and submitting another latest-active revision.
    return bool(re.search(r":\d+$", value))


@dataclass(frozen=True)
class DeploymentTarget:
    region: str
    architecture: str
    sqs_queue_url: str
    dlq_url: str | None
    batch_job_queue: str
    job_definition: str
    image: str | None = None

    def as_dict(self) -> dict[str, Any]:
        out = {
            "region": self.region,
            "architecture": self.architecture,
            "sqs_queue_url": self.sqs_queue_url,
            "batch_job_queue": self.batch_job_queue,
            "job_definition": self.job_definition,
        }
        if self.dlq_url:
            out["dlq_url"] = self.dlq_url
        if self.image:
            out["image"] = self.image
        return out


@dataclass(frozen=True)
class ManifestIdentity:
    uri: str
    bucket: str
    key: str
    version_id: str | None = None
    etag: str | None = None
    size_bytes: int | None = None
    last_modified: str | None = None
    local_path: str | None = None
    local_sha256: str | None = None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"uri": self.uri, "bucket": self.bucket, "key": self.key}
        if self.version_id is not None:
            out["version_id"] = self.version_id
        if self.etag is not None:
            out["etag"] = self.etag
        if self.size_bytes is not None:
            out["size_bytes"] = self.size_bytes
        if self.last_modified is not None:
            out["last_modified"] = self.last_modified
        if self.local_path is not None:
            out["local_path"] = self.local_path
        if self.local_sha256 is not None:
            out["local_sha256"] = self.local_sha256
        return out


def load_deployment(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise PlannerSpecError(f"failed to read deployment registry {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise PlannerSpecError(f"invalid JSON in deployment registry {path}: {exc}") from exc
    return validate_deployment(data)


def _validate_execution_route(raw_route: Any, *, context: str, require_queue: bool = False, region: str | None = None, production_queue_url: str | None = None) -> None:
    if not isinstance(raw_route, dict):
        raise PlannerSpecError(f"{context} must be an object")
    if require_queue:
        queue_url = raw_route.get("sqs_queue_url")
        if not isinstance(queue_url, str) or not queue_url:
            raise PlannerSpecError(f"{context} must include sqs_queue_url")
        queue_region = sqs_queue_region(queue_url)
        if region and queue_region and queue_region != region:
            raise PlannerSpecError(f"{context} has sqs_queue_url in region {queue_region!r}")
        if production_queue_url and queue_url == production_queue_url:
            raise PlannerSpecError(f"{context} sqs_queue_url must be isolated from the production queue")
        dlq_url = raw_route.get("dlq_url")
        if dlq_url is not None:
            if not isinstance(dlq_url, str) or not dlq_url:
                raise PlannerSpecError(f"{context} dlq_url must be a non-empty string")
            dlq_region = sqs_queue_region(dlq_url)
            if region and dlq_region and dlq_region != region:
                raise PlannerSpecError(f"{context} has dlq_url in region {dlq_region!r}")
    for key in ("batch_job_queue", "job_definition"):
        if not isinstance(raw_route.get(key), str) or not raw_route.get(key):
            raise PlannerSpecError(f"{context} must include {key}")
    job_definition = str(raw_route["job_definition"])
    if not _job_definition_revision_pinned(job_definition):
        raise PlannerSpecError(f"{context} job_definition must pin an explicit numeric revision")
    image = raw_route.get("image")
    if not isinstance(image, str) or not image:
        raise PlannerSpecError(f"{context} must include digest-pinned image")
    if image_digest(image) is None:
        raise PlannerSpecError(f"{context} image must be digest-pinned")


def validate_deployment(registry: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(registry, dict):
        raise PlannerSpecError("deployment registry must be a JSON object")
    if registry.get("schema") != DEPLOYMENT_SCHEMA_V1:
        raise PlannerSpecError(f"deployment registry schema must be {DEPLOYMENT_SCHEMA_V1!r}")
    regions = registry.get("regions")
    if not isinstance(regions, dict) or not regions:
        raise PlannerSpecError("deployment registry must include non-empty regions object")
    for region, raw_region in regions.items():
        if not isinstance(region, str) or not region:
            raise PlannerSpecError("deployment region names must be non-empty strings")
        if not isinstance(raw_region, dict):
            raise PlannerSpecError(f"deployment region {region!r} must be an object")
        queue_url = raw_region.get("sqs_queue_url")
        if not isinstance(queue_url, str) or not queue_url:
            raise PlannerSpecError(f"deployment region {region!r} must include sqs_queue_url")
        queue_region = sqs_queue_region(queue_url)
        if queue_region and queue_region != region:
            raise PlannerSpecError(f"deployment region {region!r} has sqs_queue_url in region {queue_region!r}")
        dlq_url = raw_region.get("dlq_url")
        if dlq_url is not None:
            if not isinstance(dlq_url, str) or not dlq_url:
                raise PlannerSpecError(f"deployment region {region!r} dlq_url must be a non-empty string")
            dlq_region = sqs_queue_region(dlq_url)
            if dlq_region and dlq_region != region:
                raise PlannerSpecError(f"deployment region {region!r} has dlq_url in region {dlq_region!r}")
        architectures = raw_region.get("architectures")
        if not isinstance(architectures, dict) or not architectures:
            raise PlannerSpecError(f"deployment region {region!r} must include architectures object")
        for arch, raw_arch in architectures.items():
            if arch not in _ARCHITECTURES:
                raise PlannerSpecError(f"unsupported deployment architecture {arch!r}")
            _validate_execution_route(raw_arch, context=f"deployment target {region}/{arch}")
        canary_routes = raw_region.get("canary_routes")
        if canary_routes is not None:
            if not isinstance(canary_routes, dict) or not canary_routes:
                raise PlannerSpecError(f"deployment region {region!r} canary_routes must be a non-empty object when present")
            seen_canary_queues: dict[str, str] = {}
            for route_key, raw_route in canary_routes.items():
                match = _CANARY_ROUTE_RE.fullmatch(str(route_key))
                if not match:
                    raise PlannerSpecError(f"deployment canary route {region}/{route_key!r} must be keyed as ARCH-Nvcpu-Nmib")
                if match.group("arch") not in architectures:
                    raise PlannerSpecError(f"deployment canary route {region}/{route_key!r} references an architecture without a deployment target")
                _validate_execution_route(raw_route, context=f"deployment canary route {region}/{route_key}", require_queue=True, region=region, production_queue_url=queue_url)
                route_queue = str(raw_route["sqs_queue_url"])
                if route_queue in seen_canary_queues:
                    raise PlannerSpecError(f"deployment canary routes {seen_canary_queues[route_queue]!r} and {route_key!r} share sqs_queue_url; each candidate needs an isolated queue")
                seen_canary_queues[route_queue] = str(route_key)
    return registry


def deployment_target_for(registry: dict[str, Any], *, region: str, architecture: str) -> DeploymentTarget:
    registry = validate_deployment(registry)
    regions = registry["regions"]
    raw_region = regions.get(region)
    if not isinstance(raw_region, dict):
        raise PlannerSpecError(f"deployment registry has no target for region {region!r}")
    raw_arch = (raw_region.get("architectures") or {}).get(architecture)
    if not isinstance(raw_arch, dict):
        raise PlannerSpecError(f"deployment registry has no target for architecture {architecture!r} in {region!r}")
    return DeploymentTarget(
        region=region,
        architecture=architecture,
        sqs_queue_url=str(raw_region["sqs_queue_url"]),
        dlq_url=str(raw_region["dlq_url"]) if raw_region.get("dlq_url") else None,
        batch_job_queue=str(raw_arch["batch_job_queue"]),
        job_definition=str(raw_arch["job_definition"]),
        image=str(raw_arch["image"]) if raw_arch.get("image") else None,
    )


def select_deployment_target(plan: dict[str, Any], registry: dict[str, Any]) -> DeploymentTarget:
    validate_plan(plan)
    if plan.get("schema") != PLAN_SCHEMA_V1:
        raise PlannerSpecError("invalid plan schema")
    if plan.get("status") != "ready":
        raise PlannerSpecError("deployment target can only be selected for a ready Plan")
    selected = plan.get("selected")
    if not isinstance(selected, dict):
        raise PlannerSpecError("ready Plan must include selected settings")
    region = str(selected.get("region") or "")
    architecture = str(selected.get("architecture") or "")
    if not region or not architecture:
        raise PlannerSpecError("ready Plan selected settings must include region and architecture")
    return deployment_target_for(registry, region=region, architecture=architecture)


def canary_route_key(*, architecture: str, vcpus: int, memory_mib: int) -> str:
    return f"{architecture}-{int(vcpus)}vcpu-{int(memory_mib)}mib"


def select_canary_deployment_region(job_spec: dict[str, Any], registry: dict[str, Any]) -> str:
    registry = validate_deployment(registry)
    regions = registry["regions"]
    constraints = job_spec.get("constraints") if isinstance(job_spec.get("constraints"), dict) else {}
    allowed_regions = constraints.get("regions") if isinstance(constraints, dict) else None
    candidates = list(regions.keys())
    if isinstance(allowed_regions, list) and allowed_regions:
        allowed = {str(region) for region in allowed_regions}
        candidates = [region for region in candidates if region in allowed]
    if len(candidates) != 1:
        raise PlannerSpecError("controller canary apply requires exactly one deployment region via constraints.regions or a single-region deployment registry")
    return str(candidates[0])


def canary_deployment_target_for(registry: dict[str, Any], *, region: str, task: dict[str, Any]) -> DeploymentTarget:
    registry = validate_deployment(registry)
    raw_input = task.get("input")
    input_obj: dict[str, Any] = raw_input if isinstance(raw_input, dict) else {}
    architecture = str(input_obj.get("candidate_architecture") or "")
    raw_vcpus = input_obj.get("candidate_vcpus")
    raw_memory_mib = input_obj.get("candidate_memory_mib")
    try:
        if raw_vcpus is None or raw_memory_mib is None:
            raise TypeError("missing candidate vCPU/memory")
        vcpus = int(raw_vcpus)
        memory_mib = int(raw_memory_mib)
    except (TypeError, ValueError) as exc:
        raise PlannerSpecError(f"canary task {task.get('task_id')!r} is missing candidate vCPU/memory metadata") from exc
    route_key = canary_route_key(architecture=architecture, vcpus=vcpus, memory_mib=memory_mib)
    raw_region = registry["regions"].get(region)
    if not isinstance(raw_region, dict):
        raise PlannerSpecError(f"deployment registry has no target for region {region!r}")
    routes = raw_region.get("canary_routes")
    if not isinstance(routes, dict):
        raise PlannerSpecError(f"deployment registry region {region!r} has no safe canary_routes")
    raw_route = routes.get(route_key)
    if not isinstance(raw_route, dict):
        raise PlannerSpecError(f"deployment registry has no canary route for {region}/{route_key}")
    return DeploymentTarget(
        region=region,
        architecture=architecture,
        sqs_queue_url=str(raw_route["sqs_queue_url"]),
        dlq_url=str(raw_route["dlq_url"]) if raw_route.get("dlq_url") else None,
        batch_job_queue=str(raw_route["batch_job_queue"]),
        job_definition=str(raw_route["job_definition"]),
        image=str(raw_route["image"]),
    )


def sqs_queue_region(queue_url: str) -> str | None:
    m = _SQS_RE.match(queue_url)
    return m.group(1) if m else None


def image_digest(image: str) -> str | None:
    if "@sha256:" not in image:
        return None
    digest = image.rsplit("@sha256:", 1)[1]
    return digest.lower() if re.fullmatch(r"[0-9a-fA-F]{64}", digest) else None


def image_region(image: str) -> str | None:
    m = _ECR_RE.match(image)
    return m.group("region") if m else None


def validate_target_matches_job(*, job_spec: dict[str, Any], target: DeploymentTarget) -> list[dict[str, Any]]:
    """Return non-blocking warnings after enforcing safe target/job bindings."""

    warnings: list[dict[str, Any]] = []
    job_image = str(job_spec.get("image") or "")
    if target.image:
        target_digest = image_digest(target.image)
        job_digest = image_digest(job_image)
        if target_digest and job_digest and target_digest != job_digest:
            raise PlannerSpecError("deployment target image digest does not match JobSpec image digest")
        if target_digest is None or job_digest is None:
            if target.image != job_image:
                raise PlannerSpecError("deployment target image does not match JobSpec image; use digest-pinned images for production")
            warnings.append({"code": "image_digest_missing", "severity": "warning", "message": "JobSpec/deployment image is tag-pinned rather than digest-pinned."})
    job_region = image_region(job_image)
    if job_region and job_region != target.region:
        warnings.append({"code": "image_region_differs", "severity": "warning", "message": "JobSpec image registry region differs from selected execution region."})
    queue_region = sqs_queue_region(target.sqs_queue_url)
    if queue_region and queue_region != target.region:
        raise PlannerSpecError("deployment target SQS queue region does not match Plan region")
    return warnings


def _remote_sha256_from_head(head: dict[str, Any]) -> str | None:
    metadata = head.get("Metadata") if isinstance(head.get("Metadata"), dict) else {}
    for key in ("sha256", "checksum-sha256", "sweetspot-sha256"):
        value = metadata.get(key) if isinstance(metadata, dict) else None
        if isinstance(value, str) and re.fullmatch(r"[0-9a-fA-F]{64}", value.strip()):
            return value.strip().lower()
    checksum = head.get("ChecksumSHA256")
    if isinstance(checksum, str) and checksum.strip():
        try:
            return base64.b64decode(checksum.strip(), validate=True).hex()
        except (ValueError, binascii.Error):
            return None
    return None


def _md5_file(path: Path) -> str:
    h = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def validate_local_manifest_matches_head(*, local_path: Path, local_sha256: str, head: dict[str, Any]) -> None:
    """Fail closed unless the local manifest is verifiably the S3 object that was HEADed."""

    remote_size = head.get("ContentLength")
    if remote_size is not None and int(remote_size) != local_path.stat().st_size:
        raise PlannerSpecError("local input manifest size does not match deployment S3 manifest")
    remote_sha256 = _remote_sha256_from_head(head)
    if remote_sha256 is not None:
        if remote_sha256 != local_sha256.lower():
            raise PlannerSpecError("local input manifest SHA256 does not match deployment S3 manifest")
        return
    etag = str(head.get("ETag") or "").strip('"').lower()
    if re.fullmatch(r"[0-9a-f]{32}", etag):
        if _md5_file(local_path) != etag:
            raise PlannerSpecError("local input manifest MD5/ETag does not match deployment S3 manifest")
        return
    raise PlannerSpecError("deployment S3 manifest must expose SHA256 metadata/checksum or a single-part ETag to verify --input-manifest-jsonl")


def manifest_identity_from_head(uri: str, head: dict[str, Any], *, local_path: Path | None = None, local_sha256: str | None = None) -> ManifestIdentity:
    bucket, key = parse_s3_uri(uri)
    last_modified = head.get("LastModified")
    if last_modified is None:
        last_modified_text = None
    elif hasattr(last_modified, "isoformat"):
        last_modified_text = last_modified.isoformat()
    else:
        last_modified_text = str(last_modified)
    return ManifestIdentity(
        uri=uri,
        bucket=bucket,
        key=key,
        version_id=head.get("VersionId"),
        etag=str(head.get("ETag")).strip('"') if head.get("ETag") is not None else None,
        size_bytes=int(head["ContentLength"]) if head.get("ContentLength") is not None else None,
        last_modified=last_modified_text,
        local_path=str(local_path) if local_path else None,
        local_sha256=local_sha256,
    )


def local_manifest_identity(uri: str, *, local_path: Path | None = None, local_sha256: str | None = None) -> ManifestIdentity:
    bucket, key = parse_s3_uri(uri)
    return ManifestIdentity(uri=uri, bucket=bucket, key=key, local_path=str(local_path) if local_path else None, local_sha256=local_sha256)
