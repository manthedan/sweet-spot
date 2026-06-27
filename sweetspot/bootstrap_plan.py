from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .setup import BOOTSTRAP_INTENT_STATUSES, BootstrapIntent, bootstrap_intent_to_dict, load_bootstrap_intent


BOOTSTRAP_PLAN_SCHEMA_V1 = "sweetspot.bootstrap.plan.v1"
BOOTSTRAP_PLAN_STATUSES = {"ready", "incomplete", "invalid"}
DEPLOYMENT_SCHEMA_V1 = "sweetspot.deployment.v1"

_SECRET_KEY_RE = re.compile(r"(access.?key|secret|session.?token|password|credential|token|private.?key)", re.IGNORECASE)
_SECRET_VALUE_RE = re.compile(
    r"(AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|aws_secret_access_key|BEGIN [A-Z ]*PRIVATE KEY|Bearer\s+[A-Za-z0-9._~+/=-]+|[A-Za-z0-9/+]{40,})",
    re.IGNORECASE,
)
_SLUG_RE = re.compile(r"[^a-z0-9-]+")


def render_bootstrap_plan(project_dir: str | Path) -> dict[str, Any]:
    """Return a pure, JSON-safe bootstrap infrastructure plan.

    The renderer only reads the local SweetSpot setup intent through
    ``load_bootstrap_intent``. It does not call AWS, OpenTofu, subprocesses, or
    mutate files. Failures from the setup layer are represented as ``invalid`` or
    ``incomplete`` findings so the plan artifact remains reviewable.
    """

    root = Path(project_dir)
    intent = load_bootstrap_intent(root / ".sweetspot" / "sweetspot.yaml")
    plan_status = _plan_status(intent)
    findings = _findings_for_intent(intent)
    derived = _derived_names(intent)
    inventory = _resource_inventory(intent, derived) if plan_status == "ready" else []
    expected_deployment = _expected_deployment(intent, derived) if plan_status == "ready" else None
    generated_artifacts = _generated_artifacts(plan_status)

    return _redact_secrets(
        {
            "schema": BOOTSTRAP_PLAN_SCHEMA_V1,
            "status": plan_status,
            "project_dir": str(root),
            "intent": bootstrap_intent_to_dict(intent),
            "findings": findings,
            "generated_artifacts": generated_artifacts,
            "resource_inventory": inventory,
            "expected_deployment": expected_deployment,
            "command_attempts": [],
            "stderr_summary": [],
            "next_actions": _next_actions(intent, findings),
        }
    )


def load_bootstrap_plan(project_dir: str | Path) -> dict[str, Any]:
    """Alias for callers that want an explicit load/render verb."""

    return render_bootstrap_plan(project_dir)


def _plan_status(intent: BootstrapIntent) -> str:
    if intent.status == "ready":
        return "ready"
    if intent.missing_inputs and intent.errors and all(error.code == "missing_bootstrap_input" for error in intent.errors):
        return "incomplete"
    if intent.status == "incomplete":
        return "incomplete"
    if intent.status not in BOOTSTRAP_INTENT_STATUSES or intent.status not in BOOTSTRAP_PLAN_STATUSES:
        return "invalid"
    return intent.status


def _findings_for_intent(intent: BootstrapIntent) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for missing in intent.missing_inputs:
        findings.append(
            {
                "code": "missing_input",
                "severity": "error" if intent.status != "ready" else "warning",
                "field_path": missing,
                "message": f"Bootstrap setup is missing {missing}.",
            }
        )
    for error in intent.errors:
        findings.append(
            {
                "code": error.code,
                "severity": "error",
                "field_path": error.field_path,
                "message": error.message,
            }
        )
    if not findings and intent.status == "ready":
        findings.append(
            {
                "code": "intent_ready",
                "severity": "info",
                "field_path": "$",
                "message": "Bootstrap intent contains enough local information to render reviewable infrastructure resources.",
            }
        )
    return findings


def _generated_artifacts(plan_status: str) -> list[dict[str, str]]:
    artifacts = [
        ("opentofu_main", ".sweetspot/infra/main.tf"),
        ("opentofu_variables", ".sweetspot/infra/variables.tf"),
        ("opentofu_outputs", ".sweetspot/infra/outputs.tf"),
        ("opentofu_tfvars_stub", ".sweetspot/infra/terraform.tfvars.json"),
        ("bootstrap_plan", ".sweetspot/bootstrap-plan.json"),
        ("deployment_template", ".sweetspot/deployment.template.json"),
    ]
    status = "planned" if plan_status == "ready" else "blocked"
    return [{"name": name, "path": path, "status": status} for name, path in artifacts]


def _resource_inventory(intent: BootstrapIntent, names: dict[str, str]) -> list[dict[str, Any]]:
    region = intent.region or "${var.aws_region}"
    auth_reference = intent.auth_reference or "local AWS auth reference"
    output_bucket = names["output_bucket"]
    input_bucket = names["input_bucket"]
    return [
        {
            "group": "iam",
            "resources": [
                _resource("aws_iam_role", "operator_role", names["operator_role"], region="global", purpose="OpenTofu bootstrap operator role", auth_reference=auth_reference),
                _resource("aws_iam_role", "worker_task_role", names["worker_task_role"], region="global", purpose="AWS Batch worker task role", auth_reference=auth_reference),
                _resource("aws_iam_role_policy", "worker_task_policy", f"{names['worker_task_role']}-policy", region="global", purpose="Least-privilege access to input, output, queue, and logs"),
            ],
        },
        {
            "group": "batch",
            "resources": [
                _resource("aws_batch_compute_environment", "compute_environment", names["compute_environment"], region=region, purpose="Managed Spot compute environment"),
                _resource("aws_batch_job_queue", "job_queue", names["job_queue"], region=region, purpose="AWS Batch queue for SweetSpot worker jobs"),
                _resource("aws_batch_job_definition", "job_definition", names["job_definition"], region=region, purpose="Container job definition for the selected workload architecture"),
            ],
        },
        {
            "group": "sqs",
            "resources": [
                _resource("aws_sqs_queue", "queue", names["sqs_queue"], region=region, purpose="Primary work queue"),
                _resource("aws_sqs_queue", "dead_letter_queue", names["dead_letter_queue"], region=region, purpose="Dead-letter queue for failed work items"),
            ],
        },
        {
            "group": "ecr",
            "resources": [
                _resource("aws_ecr_repository", "worker_repository", names["ecr_repository"], region=region, purpose="Worker image repository"),
            ],
        },
        {
            "group": "s3",
            "resources": [
                _resource("aws_s3_bucket", "input_bucket", input_bucket, region=region, purpose="Existing input manifest bucket reference", mode="reference"),
                _resource("aws_s3_bucket", "output_bucket", output_bucket, region=region, purpose="Output bucket reference for completed runs", mode="reference"),
            ],
        },
        {
            "group": "logs",
            "resources": [
                _resource("aws_cloudwatch_log_group", "worker_log_group", names["log_group"], region=region, purpose="AWS Batch worker log group"),
            ],
        },
        {
            "group": "outputs",
            "resources": [
                {"type": "output", "name": key, "value": value}
                for key, value in sorted(_deployment_outputs(names).items())
            ],
        },
    ]


def _resource(resource_type: str, logical_name: str, name: str, **extra: str) -> dict[str, str]:
    out = {"type": resource_type, "logical_name": logical_name, "name": name}
    out.update({key: value for key, value in extra.items() if value})
    return out


def _expected_deployment(intent: BootstrapIntent, names: dict[str, str]) -> dict[str, Any]:
    region = intent.region or "${var.aws_region}"
    architecture = names["architecture"]
    return {
        "schema": DEPLOYMENT_SCHEMA_V1,
        "regions": {
            region: {
                "sqs_queue_url": "${output.sqs_queue_url}",
                "dlq_url": "${output.dlq_url}",
                "architectures": {
                    architecture: {
                        "batch_job_queue": names["job_queue"],
                        "job_definition": f"{names['job_definition']}:1",
                        "image": "${output.worker_image_digest}",
                    }
                },
            }
        },
    }


def _deployment_outputs(names: dict[str, str]) -> dict[str, str]:
    return {
        "batch_compute_environment": names["compute_environment"],
        "batch_job_queue": names["job_queue"],
        "batch_job_definition": f"{names['job_definition']}:1",
        "dlq_url": "${aws_sqs_queue.dead_letter_queue.url}",
        "ecr_repository_url": "${aws_ecr_repository.worker_repository.repository_url}",
        "log_group": names["log_group"],
        "operator_role_arn": "${aws_iam_role.operator_role.arn}",
        "sqs_queue_url": "${aws_sqs_queue.queue.url}",
        "worker_image_digest": "${aws_ecr_repository.worker_repository.repository_url}@sha256:${var.worker_image_sha256}",
        "worker_task_role_arn": "${aws_iam_role.worker_task_role.arn}",
    }


def _derived_names(intent: BootstrapIntent) -> dict[str, str]:
    resource_names = intent.resource_names
    project_slug = _slug(intent.project_name or (resource_names.project_slug if resource_names else "sweetspot"))
    architecture = "x86_64"
    input_bucket = "${var.input_bucket}"
    output_bucket = "${var.output_bucket}"
    output_prefix = "${var.output_prefix}"
    job_definition = f"{project_slug}-worker-job"
    job_queue = f"{project_slug}-worker-queue"
    container_image = f"${{aws_ecr_repository.worker_repository.repository_url}}@sha256:${{var.worker_image_sha256}}"
    if resource_names is not None:
        architecture = _architecture_from_name(resource_names.job_definition) or _architecture_from_name(resource_names.job_queue) or architecture
        input_bucket = resource_names.input_bucket or input_bucket
        output_bucket = resource_names.output_bucket or output_bucket
        output_prefix = resource_names.output_prefix or output_prefix
        job_definition = resource_names.job_definition or job_definition
        job_queue = resource_names.job_queue or job_queue
        container_image = resource_names.container_image or container_image
    return {
        "project_slug": project_slug,
        "architecture": architecture,
        "input_bucket": input_bucket,
        "output_bucket": output_bucket,
        "output_prefix": output_prefix,
        "job_definition": job_definition,
        "job_queue": job_queue,
        "container_image": container_image,
        "ecr_repository": f"{project_slug}-worker",
        "log_group": f"/aws/batch/sweetspot/{project_slug}",
        "compute_environment": f"{project_slug}-{architecture}-compute",
        "operator_role": f"{project_slug}-bootstrap-operator-role",
        "worker_task_role": f"{project_slug}-worker-task-role",
        "sqs_queue": f"{project_slug}-work-queue",
        "dead_letter_queue": f"{project_slug}-work-dlq",
    }


def _architecture_from_name(value: str) -> str | None:
    if "arm64" in value or "-arm-" in value:
        return "arm64"
    if "x86_64" in value or "-x86-" in value:
        return "x86_64"
    return None


def _slug(value: str) -> str:
    slug = _SLUG_RE.sub("-", value.lower()).strip("-")
    slug = re.sub(r"-+", "-", slug)
    return slug or "sweetspot"


def _next_actions(intent: BootstrapIntent, findings: list[dict[str, str]]) -> list[str]:
    if intent.status == "ready":
        return [
            "Review resource_inventory and expected_deployment before generating OpenTofu files.",
            "Provide account-specific tfvars such as AWS account id and worker image digest outside the plan artifact.",
            "Run the later OpenTofu render/validate task; this renderer intentionally applies nothing.",
        ]
    if findings:
        return ["Fix the listed bootstrap intent findings in .sweetspot/sweetspot.yaml, then render the plan again."]
    return ["Create .sweetspot/sweetspot.yaml with `sweetspot init` or an equivalent reviewed setup file."]


def _redact_secrets(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): ("[redacted]" if _SECRET_KEY_RE.search(str(key)) else _redact_secrets(item)) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_secrets(item) for item in value]
    if isinstance(value, tuple):
        return [_redact_secrets(item) for item in value]
    if isinstance(value, str):
        return _SECRET_VALUE_RE.sub("[redacted]", value)
    return value
