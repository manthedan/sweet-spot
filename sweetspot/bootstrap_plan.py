from __future__ import annotations

import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any, Callable

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


def render_opentofu_bootstrap_plan(
    project_dir: str | Path,
    *,
    validate: bool = False,
    command_runner: Callable[..., Any] | None = None,
    tofu_executable: str = "tofu",
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    """Render deterministic OpenTofu bootstrap review files and optional validation status."""

    root = Path(project_dir)
    plan = render_bootstrap_plan(root)
    if plan["status"] == "ready":
        files = _opentofu_files(plan)
        if plan.get("expected_deployment") is not None:
            files[".sweetspot/deployment.template.json"] = _json_text(plan["expected_deployment"])
        _write_generated_files(root, files)
        plan["generated_artifacts"] = _rendered_artifacts(plan["generated_artifacts"], files)
        plan["next_actions"] = [
            "Review .sweetspot/infra/ and .sweetspot/bootstrap-plan.json before running any infrastructure command.",
            "Run `tofu init` and `tofu validate` locally from .sweetspot/infra when ready; this task never runs apply.",
            "Fill account-specific tfvars such as aws_account_id and worker_image_sha256 outside source control before applying manually.",
        ]

    plan["opentofu"] = {
        "status": "not_requested" if not validate else "not_run",
        "executable": None,
        "version": None,
        "working_dir": str(root / ".sweetspot" / "infra"),
    }
    plan["command_attempts"] = []
    plan["stderr_summary"] = []
    if validate and plan["status"] == "ready":
        _run_opentofu_validation(plan, root / ".sweetspot" / "infra", command_runner or _default_command_runner, tofu_executable, timeout_seconds)
    elif validate:
        plan["opentofu"]["status"] = "blocked"
        plan["findings"].append(
            {
                "code": "opentofu_validation_blocked",
                "severity": "error",
                "field_path": "$.status",
                "message": "OpenTofu validation was not attempted because the bootstrap plan is not ready.",
            }
        )
    if plan["status"] == "ready":
        plan["confirmation_token"] = _confirmation_token_for_plan(plan)
        _write_json(root / ".sweetspot" / "bootstrap-plan.json", plan)
    return _redact_secrets(plan)


def _write_generated_files(root: Path, files: dict[str, str]) -> None:
    for relpath, content in sorted(files.items()):
        path = root / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def _json_text(value: Any) -> str:
    return json.dumps(_redact_secrets(value), indent=2, sort_keys=True) + "\n"


def _confirmation_token_for_plan(plan: dict[str, Any]) -> str:
    identity = dict(plan)
    identity.pop("confirmation_token", None)
    payload = json.dumps(_redact_secrets(identity), sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    return f"apply:{digest[:16]}"


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_text(value), encoding="utf-8")


def _rendered_artifacts(artifacts: list[dict[str, str]], files: dict[str, str]) -> list[dict[str, str]]:
    rendered_paths = set(files) | {".sweetspot/bootstrap-plan.json"}
    existing = {artifact["path"] for artifact in artifacts}
    out: list[dict[str, str]] = []
    for artifact in artifacts:
        updated = dict(artifact)
        relpath = updated.get("path")
        if relpath in rendered_paths:
            updated["status"] = "rendered"
            if relpath in files:
                updated["sha256"] = hashlib.sha256(files[relpath].encode("utf-8")).hexdigest()
        out.append(updated)
    for relpath in sorted(rendered_paths - existing):
        item = {"name": relpath.replace(".sweetspot/", "").replace("/", "_").replace(".", "_"), "path": relpath, "status": "rendered"}
        if relpath in files:
            item["sha256"] = hashlib.sha256(files[relpath].encode("utf-8")).hexdigest()
        out.append(item)
    return out


def _opentofu_files(plan: dict[str, Any]) -> dict[str, str]:
    names = _inventory_by_logical_name(plan.get("resource_inventory", []))
    resource_names = plan.get("intent", {}).get("resource_names") if isinstance(plan.get("intent"), dict) else None
    resource_names = resource_names if isinstance(resource_names, dict) else {}
    auth = plan["intent"].get("auth") if isinstance(plan["intent"].get("auth"), dict) else {}
    auth_method = str(auth.get("method") or "env")
    auth_reference = str(auth.get("reference") or "")
    tfvars = {
        "aws_account_id": "000000000000",
        "aws_region": plan["intent"].get("region") or "us-east-1",
        "aws_profile": auth_reference if auth_method in {"profile", "sso"} and auth_reference and not auth_reference.startswith("AWS SSO session") else "",
        "aws_role_arn": auth_reference if auth_method == "role" else "",
        "input_bucket": names.get("input_bucket", {}).get("name", "replace-me-input-bucket"),
        "output_bucket": names.get("output_bucket", {}).get("name", "replace-me-output-bucket"),
        "output_prefix": resource_names.get("output_prefix") or "runs/",
        "worker_image_sha256": "replace-with-worker-image-digest",
    }
    return {
        ".sweetspot/infra/versions.tf": _versions_tf(auth_method),
        ".sweetspot/infra/variables.tf": _variables_tf(),
        ".sweetspot/infra/main.tf": _main_tf(names),
        ".sweetspot/infra/outputs.tf": _outputs_tf(),
        ".sweetspot/infra/terraform.tfvars.json": json.dumps(tfvars, indent=2, sort_keys=True) + "\n",
    }


def _inventory_by_logical_name(inventory: Any) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    if not isinstance(inventory, list):
        return out
    for group in inventory:
        resources = group.get("resources") if isinstance(group, dict) else None
        if not isinstance(resources, list):
            continue
        for resource in resources:
            if isinstance(resource, dict) and isinstance(resource.get("logical_name"), str):
                out[str(resource["logical_name"])] = {str(k): str(v) for k, v in resource.items() if isinstance(v, (str, int, float))}
    return out


def _versions_tf(auth_method: str = "env") -> str:
    provider_lines = ['provider "aws" {', "  region = var.aws_region"]
    if auth_method in {"profile", "sso"}:
        provider_lines.append('  profile = var.aws_profile != "" ? var.aws_profile : null')
    if auth_method == "role":
        provider_lines.extend(["  assume_role {", "    role_arn = var.aws_role_arn", "  }"])
    provider_lines.append("}")
    provider = "\n".join(provider_lines)
    return f"""terraform {{
  required_version = ">= 1.6.0"

  required_providers {{
    aws = {{
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }}
  }}
}}

{provider}
"""


def _variables_tf() -> str:
    return """variable "aws_account_id" {
  description = "AWS account id used for ARN construction in reviewable outputs."
  type        = string
}

variable "aws_region" {
  description = "AWS region where bootstrap resources will be created."
  type        = string
}

variable "aws_profile" {
  description = "Optional AWS shared config profile reviewed for bootstrap apply."
  type        = string
  default     = ""
}

variable "aws_role_arn" {
  description = "Optional AWS role ARN reviewed for bootstrap apply."
  type        = string
  default     = ""
}

variable "input_bucket" {
  description = "Existing S3 bucket containing input manifests."
  type        = string
}

variable "output_bucket" {
  description = "S3 bucket or bucket reference for SweetSpot output."
  type        = string
}

variable "output_prefix" {
  description = "S3 prefix for SweetSpot run output."
  type        = string
}

variable "worker_image_sha256" {
  description = "Digest for the worker image that will be submitted to AWS Batch."
  type        = string
}
"""


def _main_tf(names: dict[str, dict[str, str]]) -> str:
    def name(logical: str, fallback: str) -> str:
        return names.get(logical, {}).get("name", fallback)

    return f"""data "aws_vpc" "default" {{
  default = true
}}

data "aws_subnets" "default" {{
  filter {{
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }}
}}

data "aws_security_group" "default" {{
  name   = "default"
  vpc_id = data.aws_vpc.default.id
}}

data "aws_iam_policy_document" "batch_assume_role" {{
  statement {{
    actions = ["sts:AssumeRole"]
    principals {{
      type        = "Service"
      identifiers = ["batch.amazonaws.com"]
    }}
  }}
}}

data "aws_iam_policy_document" "task_assume_role" {{
  statement {{
    actions = ["sts:AssumeRole"]
    principals {{
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }}
  }}
}}

data "aws_iam_policy_document" "ec2_assume_role" {{
  statement {{
    actions = ["sts:AssumeRole"]
    principals {{
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }}
  }}
}}

resource "aws_iam_role" "operator_role" {{
  name               = "{name('operator_role', 'sweetspot-bootstrap-operator-role')}"
  assume_role_policy = data.aws_iam_policy_document.batch_assume_role.json
}}

resource "aws_iam_role" "batch_service_role" {{
  name               = "{name('compute_environment', 'sweetspot-compute')}-batch-service-role"
  assume_role_policy = data.aws_iam_policy_document.batch_assume_role.json
}}

resource "aws_iam_role_policy_attachment" "batch_service_role" {{
  role       = aws_iam_role.batch_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}}

resource "aws_iam_role" "worker_task_role" {{
  name               = "{name('worker_task_role', 'sweetspot-worker-task-role')}"
  assume_role_policy = data.aws_iam_policy_document.task_assume_role.json
}}

resource "aws_iam_role" "ecs_instance_role" {{
  name               = "{name('compute_environment', 'sweetspot-compute')}-ecs-instance-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}}

resource "aws_iam_role_policy_attachment" "ecs_instance_role" {{
  role       = aws_iam_role.ecs_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}}

resource "aws_iam_instance_profile" "ecs_instance_profile" {{
  name = "{name('compute_environment', 'sweetspot-compute')}-ecs-instance-profile"
  role = aws_iam_role.ecs_instance_role.name
}}

resource "aws_iam_role_policy" "worker_task_policy" {{
  name = "{name('worker_task_role', 'sweetspot-worker-task-role')}-policy"
  role = aws_iam_role.worker_task_role.id
  policy = jsonencode({{
    Version = "2012-10-17"
    Statement = [
      {{ Effect = "Allow", Action = ["s3:GetObject", "s3:ListBucket"], Resource = ["arn:aws:s3:::${{var.input_bucket}}", "arn:aws:s3:::${{var.input_bucket}}/*"] }},
      {{ Effect = "Allow", Action = ["s3:PutObject", "s3:GetObject"], Resource = ["arn:aws:s3:::${{var.output_bucket}}/${{var.output_prefix}}*"] }},
      {{ Effect = "Allow", Action = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:SendMessage"], Resource = [aws_sqs_queue.queue.arn, aws_sqs_queue.dead_letter_queue.arn] }},
      {{ Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = ["${{aws_cloudwatch_log_group.worker_log_group.arn}}:*"] }}
    ]
  }})
}}

resource "aws_batch_compute_environment" "compute_environment" {{
  compute_environment_name = "{name('compute_environment', 'sweetspot-compute')}"
  type                     = "MANAGED"
  state                    = "ENABLED"
  compute_resources {{
    type                = "EC2"
    allocation_strategy = "BEST_FIT_PROGRESSIVE"
    min_vcpus           = 0
    max_vcpus           = 16
    instance_role       = aws_iam_instance_profile.ecs_instance_profile.arn
    instance_type       = ["optimal"]
    subnets             = data.aws_subnets.default.ids
    security_group_ids  = [data.aws_security_group.default.id]
  }}
  service_role = aws_iam_role.batch_service_role.arn
  depends_on   = [aws_iam_role_policy_attachment.batch_service_role, aws_iam_role_policy_attachment.ecs_instance_role]
}}

resource "aws_batch_job_queue" "job_queue" {{
  name     = "{name('job_queue', 'sweetspot-worker-queue')}"
  state    = "ENABLED"
  priority = 1
  compute_environment_order {{
    order               = 1
    compute_environment = aws_batch_compute_environment.compute_environment.arn
  }}
}}

resource "aws_batch_job_definition" "job_definition" {{
  name = "{name('job_definition', 'sweetspot-worker-job')}"
  type = "container"
  container_properties = jsonencode({{ image = "${{aws_ecr_repository.worker_repository.repository_url}}@sha256:${{var.worker_image_sha256}}", jobRoleArn = aws_iam_role.worker_task_role.arn, vcpus = 1, memory = 2048 }})
}}

resource "aws_sqs_queue" "dead_letter_queue" {{
  name = "{name('dead_letter_queue', 'sweetspot-work-dlq')}"
}}

resource "aws_sqs_queue" "queue" {{
  name = "{name('queue', 'sweetspot-work-queue')}"
  redrive_policy = jsonencode({{ deadLetterTargetArn = aws_sqs_queue.dead_letter_queue.arn, maxReceiveCount = 3 }})
}}

resource "aws_ecr_repository" "worker_repository" {{
  name                 = "{name('worker_repository', 'sweetspot-worker')}"
  image_tag_mutability = "IMMUTABLE"
}}

resource "aws_cloudwatch_log_group" "worker_log_group" {{
  name              = "{name('worker_log_group', '/aws/batch/sweetspot')}"
  retention_in_days = 14
}}
"""


def _outputs_tf() -> str:
    return """output "batch_compute_environment" { value = aws_batch_compute_environment.compute_environment.name }
output "batch_job_queue" { value = aws_batch_job_queue.job_queue.name }
output "batch_job_definition" { value = aws_batch_job_definition.job_definition.name }
output "dlq_url" { value = aws_sqs_queue.dead_letter_queue.url }
output "ecr_repository_url" { value = aws_ecr_repository.worker_repository.repository_url }
output "log_group" { value = aws_cloudwatch_log_group.worker_log_group.name }
output "operator_role_arn" { value = aws_iam_role.operator_role.arn }
output "sqs_queue_url" { value = aws_sqs_queue.queue.url }
output "worker_image_digest" { value = "${aws_ecr_repository.worker_repository.repository_url}@sha256:${var.worker_image_sha256}" }
output "worker_task_role_arn" { value = aws_iam_role.worker_task_role.arn }
"""


def _default_command_runner(command: list[str], *, cwd: Path, timeout_seconds: int = 30) -> dict[str, Any]:
    completed = subprocess.run(command, cwd=cwd, timeout=timeout_seconds, check=False, text=True, capture_output=True)
    return {"returncode": completed.returncode, "stdout": completed.stdout, "stderr": completed.stderr, "executable": command[0]}


def _run_opentofu_validation(plan: dict[str, Any], cwd: Path, runner: Callable[..., Any], executable: str, timeout_seconds: int) -> None:
    version = _attempt_command(plan, runner, [executable, "version"], cwd, timeout_seconds, unavailable_status="tofu_unavailable")
    if version is None:
        plan["opentofu"]["status"] = "tofu_unavailable"
        plan["findings"].append(
            {"code": "tofu_unavailable", "severity": "warning", "field_path": "$.opentofu.executable", "message": "OpenTofu executable was not available; review artifacts were rendered but local validation was skipped."}
        )
        plan["next_actions"].append("Install OpenTofu or provide a command runner, then rerun validation from .sweetspot/infra.")
        return
    plan["opentofu"]["executable"] = version.get("executable") or executable
    plan["opentofu"]["version"] = _first_line(version.get("stdout"))
    init = _attempt_command(plan, runner, [executable, "init", "-backend=false", "-input=false", "-no-color"], cwd, timeout_seconds, unavailable_status="validation_failed")
    if init is None or int(init.get("returncode", 1)) != 0:
        plan["opentofu"]["status"] = "validation_failed"
        plan["findings"].append(
            {"code": "opentofu_init_failed", "severity": "error", "field_path": "$.opentofu.status", "message": "OpenTofu init -backend=false returned a nonzero status; inspect command_attempts and stderr_summary."}
        )
        plan["next_actions"].append("Fix the rendered OpenTofu init findings before validation or apply.")
        return
    validation = _attempt_command(plan, runner, [executable, "validate", "-no-color"], cwd, timeout_seconds, unavailable_status="validation_failed")
    if validation is not None and int(validation.get("returncode", 1)) == 0:
        plan["opentofu"]["status"] = "validation_passed"
        plan["next_actions"].append("OpenTofu validate passed locally; apply remains a manual, out-of-band action.")
    else:
        plan["opentofu"]["status"] = "validation_failed"
        plan["findings"].append(
            {"code": "opentofu_validation_failed", "severity": "error", "field_path": "$.opentofu.status", "message": "OpenTofu validation returned a nonzero status; inspect command_attempts and stderr_summary."}
        )
        plan["next_actions"].append("Fix the rendered OpenTofu validation findings before any manual apply.")


def _attempt_command(plan: dict[str, Any], runner: Callable[..., Any], command: list[str], cwd: Path, timeout_seconds: int, *, unavailable_status: str) -> dict[str, Any] | None:
    _assert_safe_tofu_command(command)
    attempt: dict[str, Any] = {"command": command[:], "cwd": str(cwd), "status": "attempted", "exit_code": None, "stdout_summary": "", "stderr_summary": ""}
    try:
        result = _normalize_command_result(runner(command, cwd=cwd, timeout_seconds=timeout_seconds), command)
    except (FileNotFoundError, PermissionError, OSError) as exc:
        attempt.update({"status": unavailable_status, "stderr_summary": _sanitize_output(str(exc))})
        plan["command_attempts"].append(attempt)
        if attempt["stderr_summary"]:
            plan["stderr_summary"].append(attempt["stderr_summary"])
        return None
    except subprocess.TimeoutExpired as exc:
        attempt.update({"status": "timeout", "stderr_summary": _sanitize_output(str(exc))})
        plan["command_attempts"].append(attempt)
        plan["stderr_summary"].append(attempt["stderr_summary"])
        return {"returncode": 124, "stdout": "", "stderr": str(exc), "executable": command[0]}
    exit_code = int(result.get("returncode", 1))
    attempt.update(
        {
            "status": "passed" if exit_code == 0 else "failed",
            "exit_code": exit_code,
            "stdout_summary": _sanitize_output(str(result.get("stdout") or "")),
            "stderr_summary": _sanitize_output(str(result.get("stderr") or "")),
        }
    )
    plan["command_attempts"].append(attempt)
    if attempt["stderr_summary"]:
        plan["stderr_summary"].append(attempt["stderr_summary"])
    return result


def _normalize_command_result(raw: Any, command: list[str]) -> dict[str, Any]:
    if isinstance(raw, dict):
        return {"returncode": raw.get("returncode", raw.get("exit_code", 1)), "stdout": raw.get("stdout", ""), "stderr": raw.get("stderr", ""), "executable": raw.get("executable", command[0])}
    return {"returncode": getattr(raw, "returncode", 1), "stdout": getattr(raw, "stdout", ""), "stderr": getattr(raw, "stderr", ""), "executable": command[0]}


def _assert_safe_tofu_command(command: list[str]) -> None:
    forbidden = {"apply", "destroy", "import", "taint", "untaint", "state", "force-unlock"}
    if any(str(part).lower() in forbidden for part in command):
        raise ValueError(f"forbidden OpenTofu mutation command: {' '.join(command)}")


def _sanitize_output(value: str, *, max_chars: int = 500) -> str:
    redacted = _redact_secrets(value.replace("\r", ""))
    if not isinstance(redacted, str):
        redacted = str(redacted)
    redacted = "\n".join(line.rstrip() for line in redacted.splitlines() if line.strip())
    if len(redacted) > max_chars:
        return redacted[: max_chars - 15].rstrip() + "... [truncated]"
    return redacted


def _first_line(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    for line in value.splitlines():
        if line.strip():
            return _sanitize_output(line.strip(), max_chars=120)
    return None


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
            "resources": [{"type": "output", "name": key, "value": value} for key, value in sorted(_deployment_outputs(names).items())],
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
    project_slug = resource_names.project_slug if resource_names and resource_names.project_slug else _slug(intent.project_name or "sweetspot")
    architecture = "x86_64"
    input_bucket = "${var.input_bucket}"
    output_bucket = "${var.output_bucket}"
    output_prefix = "${var.output_prefix}"
    job_definition = f"{project_slug}-worker-job"
    job_queue = f"{project_slug}-worker-queue"
    container_image = "${aws_ecr_repository.worker_repository.repository_url}@sha256:${var.worker_image_sha256}"
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
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if key_text == "sha256" and isinstance(item, str) and re.fullmatch(r"[A-Fa-f0-9]{64}", item):
                redacted[key_text] = item
            elif key_text == "confirmation_token" and isinstance(item, str) and re.fullmatch(r"apply:[0-9a-f]{16}", item):
                redacted[key_text] = item
            elif _SECRET_KEY_RE.search(key_text):
                redacted[key_text] = "[redacted]"
            else:
                redacted[key_text] = _redact_secrets(item)
        return redacted
    if isinstance(value, list):
        return [_redact_secrets(item) for item in value]
    if isinstance(value, tuple):
        return [_redact_secrets(item) for item in value]
    if isinstance(value, str):
        return _SECRET_VALUE_RE.sub("[redacted]", value)
    return value
