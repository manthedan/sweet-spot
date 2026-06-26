from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml


SETUP_SCHEMA_V1 = "sweetspot.project.v1"

SWEETSPOT_DIR = ".sweetspot"
SWEETSPOT_CONFIG = "sweetspot.yaml"
SWEETSPOT_DOC = "SWEETSPOT.md"
JOB_SPEC = "job.json"
DEPLOYMENT_TEMPLATE = "deployment.template.json"
WORKER_DIR = "worker"
WORKER_NOTES = "worker/README.md"
WORKER_SCAFFOLD = "worker/worker.py"
INFRA_VARS_STUB = "infra/terraform.tfvars.json"
NEXT_STEPS = "next_steps.md"

SWEETSPOT_CONFIG_PATH = f"{SWEETSPOT_DIR}/{SWEETSPOT_CONFIG}"
SWEETSPOT_DOC_PATH = f"{SWEETSPOT_DIR}/{SWEETSPOT_DOC}"
JOB_SPEC_PATH = f"{SWEETSPOT_DIR}/{JOB_SPEC}"
DEPLOYMENT_TEMPLATE_PATH = f"{SWEETSPOT_DIR}/{DEPLOYMENT_TEMPLATE}"
WORKER_NOTES_PATH = f"{SWEETSPOT_DIR}/{WORKER_NOTES}"
WORKER_SCAFFOLD_PATH = f"{SWEETSPOT_DIR}/{WORKER_SCAFFOLD}"
INFRA_VARS_STUB_PATH = f"{SWEETSPOT_DIR}/{INFRA_VARS_STUB}"
NEXT_STEPS_PATH = f"{SWEETSPOT_DIR}/{NEXT_STEPS}"

LAYOUT_FILES = {
    "config": SWEETSPOT_CONFIG_PATH,
    "doc": SWEETSPOT_DOC_PATH,
    "job": JOB_SPEC_PATH,
    "deployment_template": DEPLOYMENT_TEMPLATE_PATH,
    "worker_notes": WORKER_NOTES_PATH,
    "worker_scaffold": WORKER_SCAFFOLD_PATH,
    "infra_vars_stub": INFRA_VARS_STUB_PATH,
    "next_steps": NEXT_STEPS_PATH,
}

ARCHITECTURES = {"x86_64", "arm64"}
AWS_AUTH_METHODS = {"profile", "sso", "env", "role"}
_SECRET_KEY_RE = re.compile(r"(access.?key|secret|session.?token|password|credential|token|private.?key)", re.IGNORECASE)
_AWS_ACCESS_KEY_ID_RE = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")
_BEARER_TOKEN_RE = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{16,}\b", re.IGNORECASE)
_PRIVATE_KEY_RE = re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")
_REGION_RE = re.compile(r"^[a-z]{2}-[a-z]+-\d$|^[a-z]{2}-gov-[a-z]+-\d$")
_PLACEHOLDER_RE = re.compile(r"(<[^>]+>|TODO|TBD|REPLACE_ME|changeme)", re.IGNORECASE)


class SetupSpecError(ValueError):
    """Configuration validation error with a machine-renderable field path."""

    def __init__(self, field_path: str, message: str):
        self.field_path = field_path
        self.message = message
        super().__init__(f"{field_path}: {message}")


@dataclass(frozen=True)
class SecretFinding:
    path: str
    code: str
    severity: str
    message: str


@dataclass(frozen=True)
class ProjectInfo:
    name: str
    description: str = ""


@dataclass(frozen=True)
class WorkloadIntent:
    input_manifest: str
    output_prefix: str
    command: tuple[str, ...]
    architecture: str


@dataclass(frozen=True)
class AwsAuthIntent:
    region: str
    method: str
    profile: str | None = None
    role_arn: str | None = None


@dataclass(frozen=True)
class BootstrapLayout:
    job: str = JOB_SPEC_PATH
    deployment_template: str = DEPLOYMENT_TEMPLATE_PATH
    worker_notes: str = WORKER_NOTES_PATH
    worker_scaffold: str = WORKER_SCAFFOLD_PATH
    infra_vars_stub: str = INFRA_VARS_STUB_PATH
    next_steps: str = NEXT_STEPS_PATH


@dataclass(frozen=True)
class SweetSpotProject:
    schema: str
    project: ProjectInfo
    workload: WorkloadIntent
    aws: AwsAuthIntent
    bootstrap: BootstrapLayout


def load_setup(path: str | Path) -> SweetSpotProject:
    with Path(path).open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return validate_setup(data)


def dump_setup(config: SweetSpotProject) -> str:
    return yaml.safe_dump(setup_to_dict(config), sort_keys=False)


def setup_to_dict(config: SweetSpotProject) -> dict[str, Any]:
    return {
        "schema": config.schema,
        "project": {"name": config.project.name, "description": config.project.description},
        "workload": {
            "input_manifest": config.workload.input_manifest,
            "output_prefix": config.workload.output_prefix,
            "command": list(config.workload.command),
            "architecture": config.workload.architecture,
        },
        "aws": {
            "region": config.aws.region,
            "auth": _without_none({"method": config.aws.method, "profile": config.aws.profile, "role_arn": config.aws.role_arn}),
        },
        "bootstrap": {
            "job": config.bootstrap.job,
            "deployment_template": config.bootstrap.deployment_template,
            "worker_notes": config.bootstrap.worker_notes,
            "worker_scaffold": config.bootstrap.worker_scaffold,
            "infra_vars_stub": config.bootstrap.infra_vars_stub,
            "next_steps": config.bootstrap.next_steps,
        },
    }


def validate_setup(data: Any) -> SweetSpotProject:
    if not isinstance(data, dict):
        raise SetupSpecError("$", "setup document must be a mapping")
    secret_findings = scan_for_secrets(data)
    if secret_findings:
        finding = secret_findings[0]
        raise SetupSpecError(finding.path, f"{finding.code}: {finding.message}")

    schema = _required_str(data, "schema")
    if schema != SETUP_SCHEMA_V1:
        raise SetupSpecError("schema", f"must be {SETUP_SCHEMA_V1!r}")

    _required_mapping(data, "project")
    project = ProjectInfo(name=_required_str(data, "project.name"), description=_optional_str(data, "project.description", default=""))

    _required_mapping(data, "workload")
    input_manifest = _required_s3_uri(data, "workload.input_manifest")
    output_prefix = _required_s3_uri(data, "workload.output_prefix")
    command = _required_command(data, "workload.command")
    architecture = _required_str(data, "workload.architecture")
    if architecture not in ARCHITECTURES:
        raise SetupSpecError("workload.architecture", f"must be one of {sorted(ARCHITECTURES)}")
    workload = WorkloadIntent(input_manifest=input_manifest, output_prefix=output_prefix, command=command, architecture=architecture)

    _required_mapping(data, "aws")
    region = _required_str(data, "aws.region")
    if not _REGION_RE.match(region):
        raise SetupSpecError("aws.region", "must look like an AWS region, for example us-west-2")
    _required_mapping(data, "aws.auth")
    method = _required_str(data, "aws.auth.method")
    if method not in AWS_AUTH_METHODS:
        raise SetupSpecError("aws.auth.method", f"must be one of {sorted(AWS_AUTH_METHODS)}")
    profile = _optional_str(data, "aws.auth.profile", default=None)
    role_arn = _optional_str(data, "aws.auth.role_arn", default=None)
    if method == "profile" and not profile:
        raise SetupSpecError("aws.auth.profile", "is required when aws.auth.method is 'profile'")
    if method == "role" and not role_arn:
        raise SetupSpecError("aws.auth.role_arn", "is required when aws.auth.method is 'role'")
    aws = AwsAuthIntent(region=region, method=method, profile=profile, role_arn=role_arn)

    bootstrap_data = data.get("bootstrap", {})
    if bootstrap_data is None:
        bootstrap_data = {}
    if not isinstance(bootstrap_data, dict):
        raise SetupSpecError("bootstrap", "must be a mapping when provided")
    bootstrap = BootstrapLayout(
        job=_layout_path(data, "bootstrap.job", JOB_SPEC_PATH),
        deployment_template=_layout_path(data, "bootstrap.deployment_template", DEPLOYMENT_TEMPLATE_PATH),
        worker_notes=_layout_path(data, "bootstrap.worker_notes", WORKER_NOTES_PATH),
        worker_scaffold=_layout_path(data, "bootstrap.worker_scaffold", WORKER_SCAFFOLD_PATH),
        infra_vars_stub=_layout_path(data, "bootstrap.infra_vars_stub", INFRA_VARS_STUB_PATH),
        next_steps=_layout_path(data, "bootstrap.next_steps", NEXT_STEPS_PATH),
    )

    return SweetSpotProject(schema=schema, project=project, workload=workload, aws=aws, bootstrap=bootstrap)


def scan_for_secrets(value: Any, field_path: str = "$") -> tuple[SecretFinding, ...]:
    """Return sanitized findings for secret-like keys or scalar values.

    The scanner is pure and deterministic so CLI init and project-doctor flows can
    reuse the same no-secret contract without risking diagnostic leakage.
    """
    findings: list[SecretFinding] = []
    _scan_for_secrets(value, field_path, findings)
    return tuple(findings)


def _scan_for_secrets(value: Any, field_path: str, findings: list[SecretFinding]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            key_text = str(key)
            child_path = f"{field_path}.{key_text}" if field_path != "$" else key_text
            if _SECRET_KEY_RE.search(key_text):
                findings.append(
                    SecretFinding(
                        path=child_path,
                        code="secret_key_name",
                        severity="error",
                        message="field name is not allowed; store auth intent references only",
                    )
                )
            _scan_for_secrets(child, child_path, findings)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _scan_for_secrets(child, f"{field_path}[{index}]", findings)
    elif isinstance(value, str):
        if _AWS_ACCESS_KEY_ID_RE.search(value):
            findings.append(
                SecretFinding(
                    path=field_path,
                    code="secret_value_aws_access_key_id",
                    severity="error",
                    message="value looks like an AWS access key id; store auth intent references only",
                )
            )
        if _BEARER_TOKEN_RE.search(value):
            findings.append(
                SecretFinding(
                    path=field_path,
                    code="secret_value_bearer_token",
                    severity="error",
                    message="value looks like a bearer token; store auth intent references only",
                )
            )
        if _PRIVATE_KEY_RE.search(value):
            findings.append(
                SecretFinding(
                    path=field_path,
                    code="secret_value_private_key",
                    severity="error",
                    message="value looks like a private key; store auth intent references only",
                )
            )


def _without_none(values: dict[str, str | None]) -> dict[str, str]:
    return {key: value for key, value in values.items() if value is not None}


def _required_mapping(data: dict[str, Any], field_path: str) -> dict[str, Any]:
    value = _lookup(data, field_path)
    if not isinstance(value, dict):
        raise SetupSpecError(field_path, "is required and must be a mapping")
    return value


def _required_str(data: dict[str, Any], field_path: str) -> str:
    value = _lookup(data, field_path)
    if not isinstance(value, str) or not value.strip():
        raise SetupSpecError(field_path, "is required and must be a non-empty string")
    return value.strip()


def _optional_str(data: dict[str, Any], field_path: str, *, default: str | None) -> str | None:
    value = _lookup(data, field_path, missing=default)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise SetupSpecError(field_path, "must be a non-empty string when provided")
    return value.strip()


def _required_s3_uri(data: dict[str, Any], field_path: str) -> str:
    value = _required_str(data, field_path)
    parsed = urlparse(value)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise SetupSpecError(field_path, "must be an s3://bucket/key URI")
    return value


def _required_command(data: dict[str, Any], field_path: str) -> tuple[str, ...]:
    value = _lookup(data, field_path)
    if not isinstance(value, list) or not value:
        raise SetupSpecError(field_path, "is required and must be a non-empty list of command tokens")
    tokens: list[str] = []
    for index, token in enumerate(value):
        if not isinstance(token, str) or not token.strip():
            raise SetupSpecError(f"{field_path}[{index}]", "must be a non-empty string")
        tokens.append(token.strip())
    return tuple(tokens)


def _layout_path(data: dict[str, Any], field_path: str, default: str) -> str:
    value = _optional_str(data, field_path, default=default)
    assert value is not None
    if Path(value).is_absolute() or ".." in Path(value).parts:
        raise SetupSpecError(field_path, "must be a contained relative .sweetspot path")
    if not value.startswith(f"{SWEETSPOT_DIR}/"):
        raise SetupSpecError(field_path, f"must be under {SWEETSPOT_DIR}/")
    if _PLACEHOLDER_RE.search(value):
        raise SetupSpecError(field_path, "must be resolved to a concrete bootstrap artifact path")
    return value


def _lookup(data: dict[str, Any], field_path: str, *, missing: Any = None) -> Any:
    current: Any = data
    parts = field_path.split(".")
    if parts and parts[0] in {"$", "project", "workload", "aws", "bootstrap", "schema"}:
        pass
    for part in parts:
        if part == "$":
            continue
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return missing
    return current
