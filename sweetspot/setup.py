from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

from .planner import JOB_SPEC_SCHEMA_V1, PlannerSpecError, load_job_spec, validate_job_spec


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

DOCTOR_SCHEMA_V1 = "sweetspot.project.doctor.v1"
BOOTSTRAP_INTENT_SCHEMA_V1 = "sweetspot.bootstrap.intent.v1"
BOOTSTRAP_STATUS_SCHEMA_V1 = "sweetspot.bootstrap.status.v1"
BOOTSTRAP_INTENT_STATUSES = {"ready", "incomplete", "invalid"}
BOOTSTRAP_STATUS_STATUSES = {"ready", "incomplete", "invalid"}
DOCTOR_STATUSES = {"pass", "warning", "fail"}
DOCTOR_SEVERITIES = {"info", "warning", "error"}

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
_AWS_SECRET_ACCESS_KEY_RE = re.compile(r"(?<![A-Za-z0-9/+=])[A-Za-z0-9/+=]{40}(?![A-Za-z0-9/+=])")
_BEARER_TOKEN_RE = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{16,}\b", re.IGNORECASE)
_PRIVATE_KEY_RE = re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")
_REGION_RE = re.compile(r"^[a-z]{2}-[a-z]+-\d$|^[a-z]{2}-gov-[a-z]+-\d$")
_PLACEHOLDER_RE = re.compile(r"(<[^>]+>|TODO|TBD|REPLACE_ME|changeme)", re.IGNORECASE)
_MISSING = object()


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


@dataclass(frozen=True)
class BootstrapResourceNames:
    project_slug: str
    job_definition: str
    job_queue: str
    container_image: str
    input_bucket: str
    output_bucket: str
    output_prefix: str


@dataclass(frozen=True)
class BootstrapIntentError:
    field_path: str
    code: str
    message: str


@dataclass(frozen=True)
class BootstrapIntent:
    schema: str
    status: str
    project_name: str | None
    region: str | None
    auth_method: str | None
    auth_reference: str | None
    backend: str
    resource_names: BootstrapResourceNames | None
    missing_inputs: tuple[str, ...]
    errors: tuple[BootstrapIntentError, ...]


@dataclass(frozen=True)
class BootstrapStatus:
    schema: str
    status: str
    ok: bool
    project_dir: str
    root_dir: str
    intent: dict[str, Any]
    validation_findings: tuple[dict[str, str], ...]
    generated_artifacts: tuple[dict[str, str], ...]
    next_actions: tuple[str, ...]


def load_setup(path: str | Path) -> SweetSpotProject:
    with Path(path).open("r", encoding="utf-8") as fh:
        try:
            data = yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            raise SetupSpecError("$", "setup document must be valid YAML") from exc
    return validate_setup(data)


def dump_setup(config: SweetSpotProject) -> str:
    return yaml.safe_dump(setup_to_dict(config), sort_keys=False)


def starter_job_spec(config: SweetSpotProject | dict[str, Any]) -> dict[str, Any]:
    """Return the deterministic starter JobSpec generated by init."""
    project = _ensure_valid_project(config)
    job_spec: dict[str, Any] = {
        "schema": JOB_SPEC_SCHEMA_V1,
        "run_id": f"{_safe_slug(project.project.name)}-starter-run",
        "image": "public.ecr.aws/sweetspot/worker:starter",
        "command": list(project.workload.command),
        "input_manifest": project.workload.input_manifest,
        "output_prefix": project.workload.output_prefix,
        "constraints": {
            "max_cost_usd": 50,
            "deadline_hours": 6,
            "completion_fraction": 1.0,
            "architectures": [project.workload.architecture],
            "regions": [project.aws.region],
        },
        "validation": {"output_check": "done_marker"},
    }
    return validate_job_spec(job_spec)


def render_starter_job_spec(config: SweetSpotProject | dict[str, Any]) -> str:
    """Render deterministic starter JobSpec JSON with stable key ordering."""
    return json.dumps(starter_job_spec(config), sort_keys=True, indent=2) + "\n"


def render_deployment_template(config: SweetSpotProject | dict[str, Any]) -> str:
    """Render a review-only deployment placeholder with stable key ordering."""
    project = _ensure_valid_project(config)
    template = {
        "aws": {
            "auth": {"method": project.aws.method, "reference": _auth_reference(project.aws)},
            "region": project.aws.region,
        },
        "notes": [
            "TODO: review and replace placeholder queue, job definition, and image values before any deployment work.",
            "Review-only template; sweetspot init does not create AWS resources.",
            "Keep auth values outside generated files; store references only.",
        ],
        "project": {"description": project.project.description, "name": project.project.name},
        "ready_to_deploy": False,
        "resources": {
            "batch": {
                "job_definition": f"TODO-{_safe_slug(project.project.name)}-{project.workload.architecture}-job-definition",
                "job_queue": f"TODO-{_safe_slug(project.project.name)}-{project.workload.architecture}-job-queue",
            },
            "container": {"architecture": project.workload.architecture, "image": "TODO-public-or-private-image-uri"},
            "sqs": {
                "dead_letter_queue_url": "TODO-queue-url-after-infra-review",
                "queue_url": "TODO-queue-url-after-infra-review",
            },
        },
        "schema": "sweetspot.deployment.template.v1",
        "status": "template-review-only",
        "workload": {
            "command": list(project.workload.command),
            "input_manifest": project.workload.input_manifest,
            "output_prefix": project.workload.output_prefix,
        },
    }
    return json.dumps(template, sort_keys=True, indent=2) + "\n"


def render_worker_notes(config: SweetSpotProject | dict[str, Any]) -> str:
    """Render deterministic worker scaffold notes."""
    project = _ensure_valid_project(config)
    command = " ".join(project.workload.command)
    return (
        f"# Worker scaffold for {project.project.name}\n\n"
        "This review-only scaffold helps you adapt your workload command before later deployment/bootstrap slices. "
        "It is not deployed by `sweetspot init` and it does not create AWS resources.\n\n"
        "## Workload intent\n\n"
        f"- Region: `{project.aws.region}`\n"
        f"- Auth method: `{project.aws.method}`\n"
        f"- Auth reference: `{_auth_reference(project.aws)}`\n"
        f"- Architecture: `{project.workload.architecture}`\n"
        f"- Command: `{command}`\n"
        f"- Input manifest: `{project.workload.input_manifest}`\n"
        f"- Output prefix: `{project.workload.output_prefix}`\n\n"
        "## TODO before deployment\n\n"
        "- Replace the scaffold body in `worker.py` with your workload logic.\n"
        "- Keep auth values outside this directory; reference profiles, roles, SSO, or process environment only.\n"
        "- Produce the `done_marker` output expected by the starter job spec validation contract.\n"
    )


def render_worker_scaffold(config: SweetSpotProject | dict[str, Any]) -> str:
    """Render a deterministic Python worker scaffold."""
    project = _ensure_valid_project(config)
    command = json.dumps(list(project.workload.command), sort_keys=True)
    return (
        '"""Review-only SweetSpot worker scaffold.\n\n'
        "Generated by sweetspot init for local customization. This file is not deployed\n"
        "and does not create AWS resources. Keep auth values outside source files.\n"
        '"""\n\n'
        "from __future__ import annotations\n\n"
        "import json\n"
        "from pathlib import Path\n\n\n"
        f"PROJECT_NAME = {project.project.name!r}\n"
        f"AWS_REGION = {project.aws.region!r}\n"
        f"AUTH_METHOD = {project.aws.method!r}\n"
        f"AUTH_REFERENCE = {_auth_reference(project.aws)!r}\n"
        f"ARCHITECTURE = {project.workload.architecture!r}\n"
        f"WORKLOAD_COMMAND = {command}\n"
        f"INPUT_MANIFEST = {project.workload.input_manifest!r}\n"
        f"OUTPUT_PREFIX = {project.workload.output_prefix!r}\n\n\n"
        "def main() -> None:\n"
        "    summary = {\n"
        '        "architecture": ARCHITECTURE,\n'
        '        "auth_method": AUTH_METHOD,\n'
        '        "auth_reference": AUTH_REFERENCE,\n'
        '        "input_manifest": INPUT_MANIFEST,\n'
        '        "output_prefix": OUTPUT_PREFIX,\n'
        '        "project": PROJECT_NAME,\n'
        '        "region": AWS_REGION,\n'
        '        "workload_command": WORKLOAD_COMMAND,\n'
        "    }\n"
        '    Path("sweetspot-worker-scaffold-summary.json").write_text(json.dumps(summary, sort_keys=True, indent=2) + "\\n", encoding="utf-8")\n'
        '    Path("done_marker").write_text("review-only scaffold completed\\n", encoding="utf-8")\n\n\n'
        'if __name__ == "__main__":\n'
        "    main()\n"
    )


def render_infra_vars_stub(config: SweetSpotProject | dict[str, Any]) -> str:
    """Render review-only Terraform variable placeholders with stable key ordering."""
    project = _ensure_valid_project(config)
    vars_stub = {
        "architecture": project.workload.architecture,
        "auth_method": project.aws.method,
        "auth_reference": _auth_reference(project.aws),
        "input_manifest": project.workload.input_manifest,
        "output_prefix": project.workload.output_prefix,
        "project_name": project.project.name,
        "ready_for_apply": False,
        "region": project.aws.region,
        "review_status": "template-review-only",
        "todo": [
            "Review and replace placeholders before running terraform.",
            "Do not store auth values in tfvars; use external AWS configuration.",
            "sweetspot init does not create AWS resources.",
        ],
    }
    return json.dumps(vars_stub, sort_keys=True, indent=2) + "\n"


def render_next_steps(config: SweetSpotProject | dict[str, Any]) -> str:
    """Render deterministic next-step guidance for the starter bundle."""
    project = _ensure_valid_project(config)
    return (
        f"# Next steps for {project.project.name}\n\n"
        "The `.sweetspot/` starter bundle is ready for review and customization only. "
        "No AWS resources have been created, no deployment has run, and no secrets or AWS auth values are stored here.\n\n"
        "1. Review `.sweetspot/job.json` and confirm the starter constraints match your workload.\n"
        "2. Customize `.sweetspot/worker/worker.py` for the command shown in the setup document.\n"
        "3. Treat `.sweetspot/deployment.template.json` and `.sweetspot/infra/terraform.tfvars.json` as TODO templates, not deployable infrastructure.\n"
        "4. Keep AWS auth values outside the repository; use the configured auth reference only.\n"
        "5. Run later SweetSpot planning/doctor commands after replacing placeholders.\n\n"
        "## Starter context\n\n"
        f"- Region: `{project.aws.region}`\n"
        f"- Auth method: `{project.aws.method}`\n"
        f"- Auth reference: `{_auth_reference(project.aws)}`\n"
        f"- Architecture: `{project.workload.architecture}`\n"
    )


def render_sweetspot_doc(config: SweetSpotProject) -> str:
    """Render deterministic human/agent setup context for a SweetSpot project."""
    project = _ensure_valid_project(config)
    auth_reference = _auth_reference(project.aws)
    command = " ".join(project.workload.command)
    bootstrap_rows = [
        ("Job spec", project.bootstrap.job),
        ("Deployment template", project.bootstrap.deployment_template),
        ("Worker notes", project.bootstrap.worker_notes),
        ("Worker scaffold", project.bootstrap.worker_scaffold),
        ("Infra vars stub", project.bootstrap.infra_vars_stub),
        ("Next steps", project.bootstrap.next_steps),
    ]
    bootstrap_lines = "\n".join(f"- {label}: `{path}`" for label, path in bootstrap_rows)
    description = project.project.description or "No description provided."
    return (
        f"# SweetSpot Project: {project.project.name}\n\n"
        "## Setup Status\n\n"
        "This directory has been initialized with SweetSpot project context and a starter bundle for review/customization. "
        "No AWS resources have been created, and deployment/worker/infra starter artifacts are TODO templates, not deployable infrastructure. "
        "No secrets or AWS auth values are stored in these files.\n\n"
        "## Project\n\n"
        f"- Name: `{project.project.name}`\n"
        f"- Description: {description}\n\n"
        "## Workload Intent\n\n"
        f"- Input manifest: `{project.workload.input_manifest}`\n"
        f"- Output prefix: `{project.workload.output_prefix}`\n"
        f"- Command: `{command}`\n"
        f"- Architecture: `{project.workload.architecture}`\n\n"
        "## AWS Auth Intent\n\n"
        f"- Region: `{project.aws.region}`\n"
        f"- Method: `{project.aws.method}`\n"
        f"- Auth reference: `{auth_reference}`\n"
        "- Secret policy: reference auth by profile, role, SSO, or process environment only; "
        "do not paste secret values here.\n\n"
        "## Bootstrap Artifact Placeholders\n\n"
        "The following paths define the starter bundle layout written by init for review/customization before later bootstrap/runtime slices:\n\n"
        f"{bootstrap_lines}\n"
    )


def write_project_context(config: SweetSpotProject | dict[str, Any], project_dir: Path, *, overwrite: bool = False) -> list[Path]:
    """Write setup context plus the deterministic starter bundle for a project.

    All generated content is validated before any destination is written. Existing
    destinations fail closed unless overwrite=True.
    """
    project = _ensure_valid_project(config)
    base_dir = Path(project_dir)
    config_path = base_dir / SWEETSPOT_CONFIG_PATH
    doc_path = base_dir / SWEETSPOT_DOC_PATH
    job_path = base_dir / project.bootstrap.job
    deployment_path = base_dir / project.bootstrap.deployment_template
    worker_notes_path = base_dir / project.bootstrap.worker_notes
    worker_scaffold_path = base_dir / project.bootstrap.worker_scaffold
    infra_vars_path = base_dir / project.bootstrap.infra_vars_stub
    next_steps_path = base_dir / project.bootstrap.next_steps
    destinations = [config_path, doc_path, job_path, deployment_path, worker_notes_path, worker_scaffold_path, infra_vars_path, next_steps_path]

    rendered_files = {
        config_path: dump_setup(project),
        doc_path: render_sweetspot_doc(project),
        job_path: render_starter_job_spec(project),
        deployment_path: render_deployment_template(project),
        worker_notes_path: render_worker_notes(project),
        worker_scaffold_path: render_worker_scaffold(project),
        infra_vars_path: render_infra_vars_stub(project),
        next_steps_path: render_next_steps(project),
    }

    generated_parent_conflicts = _generated_parent_path_conflicts(destinations)
    if generated_parent_conflicts:
        relative_conflicts = ", ".join(_display_path(path, base_dir) for path in generated_parent_conflicts)
        raise FileExistsError(f"SweetSpot generated artifact paths overlap with generated parent paths: {relative_conflicts}")

    symlink_conflicts = _symlink_path_conflicts(destinations, base_dir)
    if symlink_conflicts:
        relative_conflicts = ", ".join(_display_path(path, base_dir) for path in symlink_conflicts)
        raise FileExistsError(f"SweetSpot project context paths must not contain symlinks: {relative_conflicts}")

    non_file_destination_conflicts = [path for path in destinations if path.exists() and not path.is_file()]
    if non_file_destination_conflicts:
        relative_conflicts = ", ".join(_display_path(path, base_dir) for path in non_file_destination_conflicts)
        raise FileExistsError(f"SweetSpot project context file paths are not regular files: {relative_conflicts}")

    parent_conflicts = _existing_parent_file_conflicts(destinations, base_dir)
    if parent_conflicts:
        relative_conflicts = ", ".join(_display_path(path, base_dir) for path in parent_conflicts)
        raise FileExistsError(f"SweetSpot project context parent paths are files, not directories: {relative_conflicts}")

    if not overwrite:
        conflicts = [path for path in destinations if path.exists()]
        if conflicts:
            relative_conflicts = ", ".join(_display_path(path, base_dir) for path in conflicts)
            raise FileExistsError(f"SweetSpot project context files already exist: {relative_conflicts}; pass overwrite=True to replace them")

    for path, text in rendered_files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return destinations


def _generated_parent_path_conflicts(destinations: list[Path]) -> list[Path]:
    destination_set = set(destinations)
    conflicts: list[Path] = []
    for path in destinations:
        for parent in path.parents:
            if parent in destination_set:
                conflicts.append(parent)
                break
    return sorted(set(conflicts))


def _symlink_path_conflicts(destinations: list[Path], base_dir: Path) -> list[Path]:
    conflicts: list[Path] = []
    for path in destinations:
        current = path
        while True:
            if current.is_symlink():
                conflicts.append(current)
                break
            if current == base_dir or current == current.parent:
                break
            current = current.parent
    return sorted(set(conflicts))


def _existing_parent_file_conflicts(destinations: list[Path], base_dir: Path) -> list[Path]:
    conflicts: list[Path] = []
    for path in destinations:
        parent = path.parent
        while True:
            if parent.exists() and not parent.is_dir():
                conflicts.append(parent)
                break
            if parent == base_dir or parent == parent.parent:
                break
            parent = parent.parent
    return sorted(set(conflicts))


def bootstrap_intent_from_setup(config: SweetSpotProject | dict[str, Any]) -> BootstrapIntent:
    """Derive the local bootstrap intent from setup config without external calls.

    The result is intentionally limited to sanitized references and deterministic
    resource naming hints so doctor/bootstrap planning can report readiness before
    any AWS SDK or OpenTofu dependency is imported. Dict inputs are scanned for
    secret-like material before schema validation so diagnostics can fail closed
    without echoing values.
    """
    if isinstance(config, SweetSpotProject):
        return _bootstrap_intent_report(project=_ensure_valid_project(config), errors=[])

    validation_errors = _bootstrap_intent_validation_errors(config)
    if validation_errors:
        return _bootstrap_intent_report(project=None, errors=validation_errors)

    try:
        project = validate_setup(config)
    except SetupSpecError as exc:
        return _bootstrap_intent_report(project=None, errors=[BootstrapIntentError(field_path=exc.field_path, code="invalid_setup", message=exc.message)])
    return _bootstrap_intent_report(project=project, errors=[])


def load_bootstrap_intent(path: str | Path) -> BootstrapIntent:
    """Load `.sweetspot/sweetspot.yaml` and return a structured local intent report.

    Filesystem and YAML/config failures are captured as sanitized intent errors
    instead of being raised, making the shape safe for project doctor and future
    failure-recovery flows. No network, subprocess, AWS SDK, or OpenTofu work is
    performed.
    """
    setup_path = Path(path)
    errors: list[BootstrapIntentError] = []
    try:
        with setup_path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except FileNotFoundError:
        errors.append(BootstrapIntentError(field_path=SWEETSPOT_CONFIG_PATH, code="missing_setup_config", message="setup config is missing"))
        return _bootstrap_intent_report(project=None, errors=errors)
    except IsADirectoryError:
        errors.append(BootstrapIntentError(field_path=SWEETSPOT_CONFIG_PATH, code="invalid_setup_config", message="setup config is not a regular file"))
        return _bootstrap_intent_report(project=None, errors=errors)
    except PermissionError as exc:
        errors.append(BootstrapIntentError(field_path=SWEETSPOT_CONFIG_PATH, code="unreadable_setup_config", message=_sanitize_error_message(exc)))
        return _bootstrap_intent_report(project=None, errors=errors)
    except OSError as exc:
        errors.append(BootstrapIntentError(field_path=SWEETSPOT_CONFIG_PATH, code="unreadable_setup_config", message=_sanitize_error_message(exc)))
        return _bootstrap_intent_report(project=None, errors=errors)
    except yaml.YAMLError as exc:
        errors.append(BootstrapIntentError(field_path=SWEETSPOT_CONFIG_PATH, code="invalid_setup_config", message=_sanitize_error_message(exc)))
        return _bootstrap_intent_report(project=None, errors=errors)

    return bootstrap_intent_from_setup(data)


def bootstrap_intent_to_dict(intent: BootstrapIntent) -> dict[str, Any]:
    """Return a JSON/YAML-safe bootstrap intent report."""
    if intent.status not in BOOTSTRAP_INTENT_STATUSES:
        raise ValueError(f"unknown bootstrap intent status: {intent.status}")
    resource_names = None
    if intent.resource_names is not None:
        resource_names = {
            "project_slug": intent.resource_names.project_slug,
            "job_definition": intent.resource_names.job_definition,
            "job_queue": intent.resource_names.job_queue,
            "container_image": intent.resource_names.container_image,
            "input_bucket": intent.resource_names.input_bucket,
            "output_bucket": intent.resource_names.output_bucket,
            "output_prefix": intent.resource_names.output_prefix,
        }
    return {
        "schema": intent.schema,
        "status": intent.status,
        "project_name": intent.project_name,
        "region": intent.region,
        "auth": {"method": intent.auth_method, "reference": intent.auth_reference},
        "backend": intent.backend,
        "resource_names": resource_names,
        "missing_inputs": list(intent.missing_inputs),
        "errors": [{"field_path": error.field_path, "code": error.code, "message": error.message} for error in intent.errors],
    }


def render_bootstrap_intent(config: SweetSpotProject | dict[str, Any]) -> str:
    """Render a stable, sanitized bootstrap intent report as JSON."""
    return json.dumps(bootstrap_intent_to_dict(bootstrap_intent_from_setup(config)), sort_keys=True, indent=2) + "\n"


def bootstrap_status_for_project(project_dir: str | Path) -> dict[str, Any]:
    """Return a deterministic local bootstrap status report for a generated project.

    The status report is a planning/doctor surface only: it reads local files,
    composes the sanitized bootstrap intent, checks generated artifact paths, and
    emits next actions without importing AWS/OpenTofu clients, running
    subprocesses, or writing project files.
    """
    requested_dir = Path(project_dir).expanduser()
    sweetspot_dir = _resolve_doctor_sweetspot_dir(requested_dir)
    root_dir = sweetspot_dir.parent
    config_path = sweetspot_dir / SWEETSPOT_CONFIG
    intent = load_bootstrap_intent(config_path)
    intent_report = bootstrap_intent_to_dict(intent)
    setup_config: SweetSpotProject | None = None
    if config_path.exists() and config_path.is_file() and intent.status != "invalid":
        try:
            setup_config = load_setup(config_path)
        except (OSError, SetupSpecError):
            setup_config = None

    artifact_paths = _doctor_artifact_paths(root_dir, setup_config)
    generated_artifacts = tuple(_bootstrap_artifact_status(name, path, root_dir) for name, path in sorted(artifact_paths.items()))
    validation_findings = tuple({"field_path": error.field_path, "code": error.code, "severity": "error", "message": error.message} for error in intent.errors)
    artifact_failures = [artifact for artifact in generated_artifacts if artifact["status"] != "present"]
    has_only_missing_inputs = bool(intent.errors) and all(error.code == "missing_bootstrap_input" for error in intent.errors)
    status = "invalid" if intent.status == "invalid" and not has_only_missing_inputs else "incomplete" if intent.missing_inputs or artifact_failures else "ready"
    if status not in BOOTSTRAP_STATUS_STATUSES:
        raise ValueError(f"unknown bootstrap status: {status}")
    report = BootstrapStatus(
        schema=BOOTSTRAP_STATUS_SCHEMA_V1,
        status=status,
        ok=status == "ready",
        project_dir=sweetspot_dir.as_posix(),
        root_dir=root_dir.as_posix(),
        intent=intent_report,
        validation_findings=validation_findings,
        generated_artifacts=generated_artifacts,
        next_actions=tuple(_bootstrap_next_actions(status, intent, artifact_failures)),
    )
    return bootstrap_status_to_dict(report)


def bootstrap_status_to_dict(status: BootstrapStatus) -> dict[str, Any]:
    """Return a JSON-safe local bootstrap status report."""
    return {
        "schema": status.schema,
        "status": status.status,
        "ok": status.ok,
        "project_dir": status.project_dir,
        "root_dir": status.root_dir,
        "intent": status.intent,
        "validation_findings": list(status.validation_findings),
        "generated_artifacts": list(status.generated_artifacts),
        "next_actions": list(status.next_actions),
    }


def render_bootstrap_status(project_dir: str | Path) -> str:
    """Render the local bootstrap status report as stable JSON."""
    return json.dumps(bootstrap_status_for_project(project_dir), sort_keys=True, indent=2) + "\n"


def _bootstrap_artifact_status(name: str, path: Path, root_dir: Path) -> dict[str, str]:
    if path.exists() and path.is_file():
        status = "present"
    elif path.exists():
        status = "invalid"
    else:
        status = "missing"
    return {"name": name, "path": _display_path(path, root_dir), "status": status}


def _bootstrap_next_actions(status: str, intent: BootstrapIntent, artifact_failures: list[dict[str, str]]) -> list[str]:
    actions: list[str] = []
    if status == "invalid":
        actions.append("Fix .sweetspot/sweetspot.yaml until the bootstrap intent validates without sanitized errors.")
    if intent.missing_inputs:
        fields = ", ".join(intent.missing_inputs)
        actions.append(f"Provide missing bootstrap inputs in .sweetspot/sweetspot.yaml: {fields}.")
    if artifact_failures:
        paths = ", ".join(artifact["path"] for artifact in artifact_failures)
        actions.append(f"Regenerate or restore generated bootstrap artifacts before planning: {paths}.")
    if not actions:
        actions.append("Review generated artifacts and placeholders before any future AWS bootstrap apply step.")
        actions.append("Keep AWS credentials outside .sweetspot; use the reported auth reference only.")
    return actions


def _bootstrap_intent_report(project: SweetSpotProject | None, errors: list[BootstrapIntentError]) -> BootstrapIntent:
    missing_inputs: list[str] = []
    if project is None:
        missing_inputs.extend(error.field_path for error in errors if error.code == "missing_bootstrap_input")
        if not missing_inputs:
            missing_inputs.extend(["project.name", "aws.region", "aws.auth", "workload.input_manifest", "workload.output_prefix"])
        return BootstrapIntent(
            schema=BOOTSTRAP_INTENT_SCHEMA_V1,
            status="invalid" if errors else "incomplete",
            project_name=None,
            region=None,
            auth_method=None,
            auth_reference=None,
            backend="opentofu-local-template",
            resource_names=None,
            missing_inputs=tuple(sorted(set(missing_inputs))),
            errors=tuple(errors),
        )

    missing_inputs.extend(_bootstrap_missing_inputs(project))
    status = "invalid" if errors else "incomplete" if missing_inputs else "ready"
    input_uri = urlparse(project.workload.input_manifest)
    output_uri = urlparse(project.workload.output_prefix)
    slug = _safe_slug(project.project.name)
    resource_names = BootstrapResourceNames(
        project_slug=slug,
        job_definition=f"{slug}-{project.workload.architecture}-job-definition",
        job_queue=f"{slug}-{project.workload.architecture}-job-queue",
        container_image=f"{slug}-{project.workload.architecture}-worker",
        input_bucket=input_uri.netloc,
        output_bucket=output_uri.netloc,
        output_prefix=output_uri.path.strip("/"),
    )
    return BootstrapIntent(
        schema=BOOTSTRAP_INTENT_SCHEMA_V1,
        status=status,
        project_name=project.project.name,
        region=project.aws.region,
        auth_method=project.aws.method,
        auth_reference=_auth_reference(project.aws),
        backend="opentofu-local-template",
        resource_names=resource_names,
        missing_inputs=tuple(sorted(set(missing_inputs))),
        errors=tuple(errors),
    )


def _bootstrap_missing_inputs(project: SweetSpotProject) -> list[str]:
    missing: list[str] = []
    if not project.project.name:
        missing.append("project.name")
    if not project.aws.region:
        missing.append("aws.region")
    if project.aws.method == "profile" and not project.aws.profile:
        missing.append("aws.auth.profile")
    if project.aws.method == "role" and not project.aws.role_arn:
        missing.append("aws.auth.role_arn")
    if not project.workload.input_manifest:
        missing.append("workload.input_manifest")
    if not project.workload.output_prefix:
        missing.append("workload.output_prefix")
    return missing


_BOOTSTRAP_REQUIRED_STRINGS = (
    "schema",
    "project.name",
    "workload.input_manifest",
    "workload.output_prefix",
    "workload.architecture",
    "aws.region",
    "aws.auth.method",
)
_BOOTSTRAP_REQUIRED_MAPPINGS = ("project", "workload", "aws", "aws.auth")


def _bootstrap_intent_validation_errors(data: Any) -> list[BootstrapIntentError]:
    if not isinstance(data, dict):
        return [BootstrapIntentError(field_path="$", code="invalid_setup", message="setup document must be a mapping")]

    secret_findings = scan_for_secrets(data)
    if secret_findings:
        return [BootstrapIntentError(field_path=finding.path, code=finding.code, message=finding.message) for finding in secret_findings]

    errors: list[BootstrapIntentError] = []
    for field_path in _BOOTSTRAP_REQUIRED_MAPPINGS:
        value = _lookup(data, field_path, missing=_MISSING)
        if not isinstance(value, dict):
            errors.append(_bootstrap_missing_error(field_path, "set this mapping in .sweetspot/sweetspot.yaml"))
    for field_path in _BOOTSTRAP_REQUIRED_STRINGS:
        value = _lookup(data, field_path, missing=_MISSING)
        if not isinstance(value, str) or not value.strip():
            errors.append(_bootstrap_missing_error(field_path, "set a non-empty value in .sweetspot/sweetspot.yaml"))

    method = _lookup(data, "aws.auth.method", missing=_MISSING)
    if isinstance(method, str):
        method = method.strip()
        if method == "profile":
            profile = _lookup(data, "aws.auth.profile", missing=_MISSING)
            if not isinstance(profile, str) or not profile.strip():
                errors.append(_bootstrap_missing_error("aws.auth.profile", "set a profile name or choose a different aws.auth.method"))
        if method == "role":
            role_arn = _lookup(data, "aws.auth.role_arn", missing=_MISSING)
            if not isinstance(role_arn, str) or not role_arn.strip():
                errors.append(_bootstrap_missing_error("aws.auth.role_arn", "set a role ARN reference or choose a different aws.auth.method"))

    return errors


def _bootstrap_missing_error(field_path: str, remediation: str) -> BootstrapIntentError:
    return BootstrapIntentError(field_path=field_path, code="missing_bootstrap_input", message=f"is required for bootstrap intent; {remediation}")


def doctor_project(project_dir: Path) -> dict[str, Any]:
    """Return a deterministic, read-only diagnostic report for a generated `.sweetspot/` bundle.

    `project_dir` is expected to be the contained `.sweetspot/` directory.  As a
    convenience for humans, a project root that already contains `.sweetspot/` is
    also accepted.  The helper performs local file reads only; it never writes
    project context, imports AWS SDKs, or contacts AWS.
    """
    requested_dir = Path(project_dir).expanduser()
    sweetspot_dir = _resolve_doctor_sweetspot_dir(requested_dir)
    root_dir = sweetspot_dir.parent
    checks: list[dict[str, Any]] = []

    config_path = sweetspot_dir / SWEETSPOT_CONFIG
    setup_config: SweetSpotProject | None = None
    setup_data: Any = None
    if not config_path.exists():
        checks.append(_doctor_check("setup_config", "fail", config_path, [_doctor_finding("missing_setup_config", "error", "setup config is missing", config_path, root_dir)]))
    else:
        try:
            setup_data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            setup_config = validate_setup(setup_data)
        except (OSError, yaml.YAMLError, SetupSpecError) as exc:
            checks.append(_doctor_check("setup_config", "fail", config_path, [_doctor_finding("invalid_setup_config", "error", _sanitize_error_message(exc), config_path, root_dir)]))
        else:
            checks.append(_doctor_check("setup_config", "pass", config_path, []))

    artifact_paths = _doctor_artifact_paths(root_dir, setup_config)
    missing_artifacts = [path for path in artifact_paths.values() if not path.exists()]
    non_file_artifacts = [path for path in artifact_paths.values() if path.exists() and not path.is_file()]
    artifact_findings = [
        *[_doctor_finding("missing_generated_artifact", "error", "generated artifact is missing", path, root_dir) for path in missing_artifacts],
        *[_doctor_finding("invalid_generated_artifact", "error", "generated artifact is not a regular file", path, root_dir) for path in non_file_artifacts],
    ]
    if artifact_findings:
        checks.append(_doctor_check("generated_artifacts", "fail", sweetspot_dir, artifact_findings))
    else:
        checks.append(_doctor_check("generated_artifacts", "pass", sweetspot_dir, []))

    job_path = artifact_paths["job"]
    if job_path.exists() and job_path.is_file():
        try:
            load_job_spec(job_path)
        except (OSError, PlannerSpecError) as exc:
            checks.append(_doctor_check("planner_job", "fail", job_path, [_doctor_finding("planner_incompatible_job", "error", _sanitize_error_message(exc), job_path, root_dir)]))
        else:
            checks.append(_doctor_check("planner_job", "pass", job_path, []))
    elif job_path.exists():
        checks.append(_doctor_check("planner_job", "fail", job_path, [_doctor_finding("invalid_job_artifact", "error", "job artifact is not a regular file", job_path, root_dir)]))
    else:
        checks.append(_doctor_check("planner_job", "fail", job_path, [_doctor_finding("missing_job_artifact", "error", "job artifact is missing", job_path, root_dir)]))

    scannable_paths = _existing_doctor_scan_paths(config_path, artifact_paths)
    secret_findings = _doctor_secret_findings(scannable_paths, root_dir)
    checks.append(_doctor_check("secret_scan", "fail" if secret_findings else "pass", sweetspot_dir, secret_findings))

    placeholder_findings = _doctor_placeholder_findings(scannable_paths, root_dir)
    checks.append(_doctor_check("placeholder_review", "warning" if placeholder_findings else "pass", sweetspot_dir, placeholder_findings))

    return _doctor_report(sweetspot_dir, root_dir, checks)


def _resolve_doctor_sweetspot_dir(path: Path) -> Path:
    if path.name == SWEETSPOT_DIR:
        return path.resolve()
    contained = path / SWEETSPOT_DIR
    if contained.exists():
        return contained.resolve()
    return path.resolve()


def _doctor_artifact_paths(root_dir: Path, setup_config: SweetSpotProject | None) -> dict[str, Path]:
    bootstrap = setup_config.bootstrap if setup_config is not None else BootstrapLayout()
    return {
        "config": root_dir / SWEETSPOT_CONFIG_PATH,
        "doc": root_dir / SWEETSPOT_DOC_PATH,
        "job": root_dir / bootstrap.job,
        "deployment_template": root_dir / bootstrap.deployment_template,
        "worker_notes": root_dir / bootstrap.worker_notes,
        "worker_scaffold": root_dir / bootstrap.worker_scaffold,
        "infra_vars_stub": root_dir / bootstrap.infra_vars_stub,
        "next_steps": root_dir / bootstrap.next_steps,
    }


def _existing_doctor_scan_paths(config_path: Path, artifact_paths: dict[str, Path]) -> list[Path]:
    paths = [config_path, *artifact_paths.values()]
    unique: dict[str, Path] = {}
    for path in paths:
        if path.exists() and path.is_file():
            unique[path.resolve().as_posix()] = path
    return [unique[key] for key in sorted(unique)]


def _doctor_secret_findings(paths: list[Path], root_dir: Path) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for path in paths:
        value = _doctor_load_scannable_value(path)
        for finding in scan_for_secrets(value):
            findings.append(
                {
                    "path": f"{_display_path(path, root_dir)}:{finding.path}",
                    "code": finding.code,
                    "severity": finding.severity,
                    "message": finding.message,
                }
            )
    return sorted(findings, key=lambda item: (item["path"], item["code"], item["severity"]))


def _doctor_load_scannable_value(path: Path) -> Any:
    text = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    try:
        if suffix == ".json":
            return json.loads(text)
        if suffix in {".yaml", ".yml"}:
            parsed = yaml.safe_load(text)
            return parsed if parsed is not None else ""
    except (json.JSONDecodeError, yaml.YAMLError):
        return text
    return text


def _doctor_placeholder_findings(paths: list[Path], root_dir: Path) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        if _PLACEHOLDER_RE.search(text):
            findings.append(_doctor_finding("review_placeholder", "warning", "review-only placeholder material is present", path, root_dir))
    return sorted(findings, key=lambda item: (item["path"], item["code"], item["severity"]))


def _doctor_check(name: str, status: str, path: Path, findings: list[dict[str, str]]) -> dict[str, Any]:
    if status not in DOCTOR_STATUSES:
        raise ValueError(f"unknown doctor check status: {status}")
    return {"name": name, "status": status, "path": path.as_posix(), "findings": findings}


def _doctor_finding(code: str, severity: str, message: str, path: Path, root_dir: Path) -> dict[str, str]:
    if severity not in DOCTOR_SEVERITIES:
        raise ValueError(f"unknown doctor finding severity: {severity}")
    return {"path": _display_path(path, root_dir), "code": code, "severity": severity, "message": message}


def _doctor_report(sweetspot_dir: Path, root_dir: Path, checks: list[dict[str, Any]]) -> dict[str, Any]:
    findings = [finding for check in checks for finding in check["findings"]]
    error_count = sum(1 for finding in findings if finding["severity"] == "error")
    warning_count = sum(1 for finding in findings if finding["severity"] == "warning")
    info_count = sum(1 for finding in findings if finding["severity"] == "info")
    failed_checks = sum(1 for check in checks if check["status"] == "fail")
    warning_checks = sum(1 for check in checks if check["status"] == "warning")
    passed_checks = sum(1 for check in checks if check["status"] == "pass")
    status = "fail" if failed_checks else "warning" if warning_checks or warning_count else "pass"
    return {
        "schema": DOCTOR_SCHEMA_V1,
        "ok": status != "fail",
        "status": status,
        "project_dir": sweetspot_dir.as_posix(),
        "root_dir": root_dir.as_posix(),
        "summary": {
            "checks": {"pass": passed_checks, "warning": warning_checks, "fail": failed_checks},
            "findings": {"error": error_count, "warning": warning_count, "info": info_count},
            "total_checks": len(checks),
            "total_findings": len(findings),
        },
        "checks": checks,
    }


def _sanitize_error_message(exc: BaseException) -> str:
    message = str(exc).splitlines()[0] if str(exc) else exc.__class__.__name__
    for pattern in (_AWS_ACCESS_KEY_ID_RE, _AWS_SECRET_ACCESS_KEY_RE, _BEARER_TOKEN_RE, _PRIVATE_KEY_RE):
        message = pattern.sub("[redacted]", message)
    return message


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
    project_description = _optional_str(data, "project.description", default="")
    assert project_description is not None
    project = ProjectInfo(name=_required_str(data, "project.name"), description=project_description)

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
    _validate_bootstrap_layout_collisions(bootstrap)

    return SweetSpotProject(schema=schema, project=project, workload=workload, aws=aws, bootstrap=bootstrap)


def _validate_bootstrap_layout_collisions(bootstrap: BootstrapLayout) -> None:
    seen = {
        SWEETSPOT_CONFIG_PATH: "generated setup config",
        SWEETSPOT_DOC_PATH: "generated project doc",
    }
    for field_path, path in (
        ("bootstrap.job", bootstrap.job),
        ("bootstrap.deployment_template", bootstrap.deployment_template),
        ("bootstrap.worker_notes", bootstrap.worker_notes),
        ("bootstrap.worker_scaffold", bootstrap.worker_scaffold),
        ("bootstrap.infra_vars_stub", bootstrap.infra_vars_stub),
        ("bootstrap.next_steps", bootstrap.next_steps),
    ):
        existing = seen.get(path)
        if existing is not None:
            raise SetupSpecError(field_path, f"must not collide with {existing} at {path}")
        seen[path] = field_path


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
            if key_text != "confirmation_token" and _SECRET_KEY_RE.search(key_text):
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
        if _AWS_SECRET_ACCESS_KEY_RE.search(value):
            findings.append(
                SecretFinding(
                    path=field_path,
                    code="secret_value_aws_secret_access_key",
                    severity="error",
                    message="value looks like an AWS secret access key; store auth intent references only",
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


def _ensure_valid_project(config: SweetSpotProject | dict[str, Any]) -> SweetSpotProject:
    if isinstance(config, SweetSpotProject):
        return validate_setup(setup_to_dict(config))
    return validate_setup(config)


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_-]+", "-", value.strip().lower()).strip("-_")
    slug = re.sub(r"[-_]{2,}", "-", slug)
    return slug or "sweetspot-project"


def _auth_reference(auth: AwsAuthIntent) -> str:
    if auth.method == "profile":
        return auth.profile or "profile reference required"
    if auth.method == "role":
        return auth.role_arn or "role ARN reference required"
    if auth.method == "sso":
        return "AWS SSO session configured outside SweetSpot"
    if auth.method == "env":
        return "AWS environment variables supplied outside SweetSpot"
    return "credential reference configured outside SweetSpot"


def _display_path(path: Path, base_dir: Path) -> str:
    try:
        return path.relative_to(base_dir).as_posix()
    except ValueError:
        return path.as_posix()


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
    value = _lookup(data, field_path, missing=_MISSING)
    if value is _MISSING:
        return default
    if value is None:
        raise SetupSpecError(field_path, "must be a non-empty string when provided")
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
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise SetupSpecError(field_path, "must be a contained relative .sweetspot path")
    normalized = path.as_posix()
    if not normalized.startswith(f"{SWEETSPOT_DIR}/"):
        raise SetupSpecError(field_path, f"must be under {SWEETSPOT_DIR}/")
    if _PLACEHOLDER_RE.search(normalized):
        raise SetupSpecError(field_path, "must be resolved to a concrete bootstrap artifact path")
    return normalized


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
