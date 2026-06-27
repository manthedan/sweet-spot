from __future__ import annotations

import copy
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any, Callable

from .bootstrap_plan import BOOTSTRAP_PLAN_SCHEMA_V1, DEPLOYMENT_SCHEMA_V1
from .deployment import validate_deployment
from .planner import PlannerSpecError


BOOTSTRAP_APPLY_SCHEMA_V1 = "sweetspot.bootstrap.apply.v1"
BOOTSTRAP_STATE_PATH = Path(".sweetspot/bootstrap/state.json")
BOOTSTRAP_FAILURE_PATH = Path(".sweetspot/bootstrap/failure.json")
BOOTSTRAP_PLAN_PATH = Path(".sweetspot/bootstrap-plan.json")
DEPLOYMENT_OUTPUT_PATH = Path(".sweetspot/deployment.json")
_INFRA_DIR = Path(".sweetspot/infra")
_SECRET_KEY_RE = re.compile(r"(access.?key|secret|session.?token|password|credential|token|private.?key)", re.IGNORECASE)
_SECRET_VALUE_RE = re.compile(
    r"(AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|aws_secret_access_key|BEGIN [A-Z ]*PRIVATE KEY|Bearer\s+[A-Za-z0-9._~+/=-]+|[A-Za-z0-9/+]{40,})",
    re.IGNORECASE,
)
_ACCOUNT_RE = re.compile(r"\b\d{12}\b")
_ARN_RE = re.compile(r"\barn:aws(?:-[a-z]+)*:[^\s\"'<>]+")
_REQUEST_ID_RE = re.compile(r"(?i)\b(?:request(?:id| id)?|x-amz-request-id|host id)[:= ]+[A-Za-z0-9/+=._:-]+")
_PROFILE_CONTEXT_RE = re.compile(r"(?i)(?:profile|role|session)(?: name)?\s*['\"]([^'\"]+)['\"]")
_PERMISSION_RE = re.compile(r"(access.?denied|unauthorized|forbidden|permission|not authorized)", re.IGNORECASE)

CommandRunner = Callable[..., dict[str, Any]]


class BootstrapApplyError(RuntimeError):
    """Raised when a bootstrap apply command cannot produce safe deployment outputs."""

    def __init__(
        self,
        category: str,
        message: str,
        *,
        command_summaries: list[dict[str, Any]] | None = None,
        output_completeness: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.category = category
        self.command_summaries = command_summaries or []
        self.output_completeness = output_completeness


def bootstrap_apply_confirmation_token(plan_path: Path) -> str:
    """Return the exact human confirmation token for a reviewed bootstrap plan."""

    digest = hashlib.sha256(_confirmation_identity_bytes(plan_path)).hexdigest()
    return f"apply:{digest[:16]}"


def _confirmation_identity_bytes(plan_path: Path) -> bytes:
    data = plan_path.read_bytes()
    try:
        plan = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return data
    if not isinstance(plan, dict):
        return data
    plan = copy.deepcopy(plan)
    plan.pop("confirmation_token", None)
    return json.dumps(plan, sort_keys=True, separators=(",", ":")).encode("utf-8")


def apply_bootstrap_plan(
    project_dir: Path,
    *,
    confirmation: str | None,
    command_runner: CommandRunner | None = None,
    timeout_seconds: int = 300,
    tofu_executable: str = "tofu",
) -> dict[str, Any]:
    """Guard and apply the reviewed bootstrap plan without requiring live AWS in tests."""

    project_dir = Path(project_dir)
    runner = command_runner or _subprocess_runner
    plan_path = project_dir / BOOTSTRAP_PLAN_PATH
    plan, reviewed_plan, plan_error = _load_reviewed_plan(plan_path)
    expected_token = _expected_token(plan_path)
    confirmation_status = _confirmation_status(confirmation, expected_token)
    command_summaries: list[dict[str, Any]] = []

    output_completeness = _output_completeness(project_dir, plan if isinstance(plan, dict) else None)
    block = _blocking_reason(plan, reviewed_plan, plan_error, output_completeness, confirmation_status)
    if block is not None:
        outcome = _diagnostic(
            status="blocked",
            category=block[0],
            message=block[1],
            reviewed_plan=reviewed_plan,
            confirmation=confirmation_status,
            output_completeness=output_completeness,
            command_summaries=[],
            recovery_hints=_recovery_hints(block[0]),
        )
        _persist_state_and_failure(project_dir, outcome)
        return outcome

    applying = _diagnostic(
        status="applying",
        category="apply_started",
        message="Bootstrap apply guard passed; invoking OpenTofu apply runner.",
        reviewed_plan=reviewed_plan,
        confirmation=confirmation_status,
        output_completeness=output_completeness,
        command_summaries=[],
        recovery_hints=[],
    )
    _write_bootstrap_json(project_dir / BOOTSTRAP_STATE_PATH, applying)

    try:
        deployment = _run_apply_and_extract_deployment(
            project_dir,
            runner,
            timeout_seconds=timeout_seconds,
            command_summaries=command_summaries,
            plan=plan,
            tofu_executable=tofu_executable,
        )
        _write_deployment_json(project_dir / DEPLOYMENT_OUTPUT_PATH, deployment)
        _remove_bootstrap_failure(project_dir)
        output_completeness = _output_completeness(project_dir, plan, deployment_written=True, outputs=deployment.get("bootstrap_outputs"))
        outcome = _diagnostic(
            status="output_written",
            category="applied",
            message="Bootstrap apply succeeded and deployment outputs were written.",
            reviewed_plan=reviewed_plan,
            confirmation=confirmation_status,
            output_completeness=output_completeness,
            command_summaries=command_summaries,
            recovery_hints=[],
        )
        _write_bootstrap_json(project_dir / BOOTSTRAP_STATE_PATH, outcome)
        return outcome
    except BootstrapApplyError as exc:
        category = exc.category
        summaries = exc.command_summaries or command_summaries
        outcome = _diagnostic(
            status="failed",
            category=category,
            message=str(exc),
            reviewed_plan=reviewed_plan,
            confirmation=confirmation_status,
            output_completeness=exc.output_completeness or output_completeness,
            command_summaries=summaries,
            recovery_hints=_recovery_hints(category),
        )
        _persist_state_and_failure(project_dir, outcome)
        return outcome


def _load_reviewed_plan(plan_path: Path) -> tuple[dict[str, Any] | None, dict[str, Any], str | None]:
    if not plan_path.exists():
        return None, {"status": "missing", "path": str(BOOTSTRAP_PLAN_PATH)}, "missing_reviewed_plan"
    try:
        raw = json.loads(plan_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return None, {"status": "invalid", "path": str(BOOTSTRAP_PLAN_PATH), "error": sanitize_message(str(exc))}, "invalid_reviewed_plan"
    except OSError as exc:
        return None, {"status": "unreadable", "path": str(BOOTSTRAP_PLAN_PATH), "error": sanitize_message(str(exc))}, "invalid_reviewed_plan"
    if not isinstance(raw, dict):
        return None, {"status": "invalid", "path": str(BOOTSTRAP_PLAN_PATH), "error": "reviewed plan must be a JSON object"}, "invalid_reviewed_plan"
    identity = _plan_identity(plan_path)
    return raw, {"status": str(raw.get("status") or "unknown"), "path": str(BOOTSTRAP_PLAN_PATH), **identity}, None


def _plan_identity(plan_path: Path) -> dict[str, Any]:
    data = plan_path.read_bytes()
    return {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data), "confirmation_token": bootstrap_apply_confirmation_token(plan_path)}


def _expected_token(plan_path: Path) -> str | None:
    if not plan_path.exists():
        return None
    try:
        return bootstrap_apply_confirmation_token(plan_path)
    except OSError:
        return None


def _confirmation_status(confirmation: str | None, expected: str | None) -> dict[str, Any]:
    if expected is None:
        return {"status": "not_required"}
    if not confirmation:
        return {"status": "missing", "expected": expected}
    if confirmation != expected:
        return {"status": "mismatched", "expected": expected}
    return {"status": "accepted", "expected": expected}


def _blocking_reason(
    plan: dict[str, Any] | None,
    reviewed_plan: dict[str, Any],
    plan_error: str | None,
    output_completeness: dict[str, Any],
    confirmation: dict[str, Any],
) -> tuple[str, str] | None:
    if plan_error == "missing_reviewed_plan":
        return "missing_reviewed_plan", "Reviewed bootstrap plan artifact is missing."
    if plan_error is not None or plan is None or plan.get("schema") != BOOTSTRAP_PLAN_SCHEMA_V1:
        return "invalid_reviewed_plan", "Reviewed bootstrap plan is invalid or has an unexpected schema."
    if plan.get("status") != "ready":
        return "reviewed_plan_not_ready", "Reviewed bootstrap plan is not ready for apply."
    if _has_blocking_findings(plan):
        return "blocking_plan_finding", "Reviewed bootstrap plan contains blocking findings."
    if not output_completeness.get("required_artifacts_present", False):
        return "missing_generated_artifact", "Required generated infrastructure artifacts are missing."
    if output_completeness.get("drifted"):
        return "generated_artifact_drift", "Generated infrastructure artifacts changed after plan review."
    if output_completeness.get("input_errors"):
        return "unresolved_apply_input", "Mutable OpenTofu inputs still contain unresolved or invalid apply values."
    if confirmation.get("status") == "missing":
        return "confirmation_missing", "Exact apply confirmation token is required before AWS mutation."
    if confirmation.get("status") == "mismatched":
        return "confirmation_mismatched", "Apply confirmation token does not match the reviewed plan identity."
    return None


def _has_blocking_findings(plan: dict[str, Any]) -> bool:
    findings = plan.get("findings")
    if not isinstance(findings, list):
        return True
    for finding in findings:
        if not isinstance(finding, dict):
            return True
        severity = str(finding.get("severity") or "").lower()
        if severity in {"error", "critical", "blocking"}:
            return True
    return False


def _output_completeness(
    project_dir: Path,
    plan: dict[str, Any] | None,
    *,
    deployment_written: bool | None = None,
    outputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    required = _required_generated_paths(plan)
    missing = [path for path in required if not (project_dir / path).is_file()]
    drifted = _artifact_digest_mismatches(project_dir, plan)
    if deployment_written is None:
        deployment_written = (project_dir / DEPLOYMENT_OUTPUT_PATH).is_file()
    output_keys = sorted(outputs) if isinstance(outputs, dict) else []
    missing_outputs = _missing_required_outputs(outputs)
    input_errors = _tfvars_input_errors(project_dir)
    complete = not missing and not drifted and not input_errors and deployment_written and not missing_outputs
    return {
        "complete": complete,
        "required_artifacts_present": not missing,
        "deployment_output_written": bool(deployment_written),
        "required": [str(path) for path in required],
        "missing": [str(path) for path in missing],
        "drifted": drifted,
        "input_errors": input_errors,
        "required_outputs": _required_opentofu_outputs(),
        "present_outputs": output_keys,
        "missing_outputs": missing_outputs,
    }


def _tfvars_immutable_mismatches(project_dir: Path, plan: dict[str, Any] | None) -> list[dict[str, str]]:
    if not isinstance(plan, dict):
        return []
    tfvars_path = project_dir / ".sweetspot/infra/terraform.tfvars.json"
    if not tfvars_path.is_file():
        return []
    try:
        data = json.loads(tfvars_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []
    if not isinstance(data, dict):
        return []
    raw_intent = plan.get("intent")
    intent: dict[str, Any] = raw_intent if isinstance(raw_intent, dict) else {}
    raw_resource_names = intent.get("resource_names")
    resource_names: dict[str, Any] = raw_resource_names if isinstance(raw_resource_names, dict) else {}
    raw_auth = intent.get("auth")
    auth: dict[str, Any] = raw_auth if isinstance(raw_auth, dict) else {}
    auth_method = str(auth.get("method") or "env")
    auth_reference = str(auth.get("reference") or "")
    expected = {
        "aws_region": intent.get("region"),
        "aws_profile": auth_reference if auth_method in {"profile", "sso"} and auth_reference and not auth_reference.startswith("AWS SSO session") else "",
        "aws_role_arn": auth_reference if auth_method == "role" else "",
        "input_bucket": resource_names.get("input_bucket"),
        "output_bucket": resource_names.get("output_bucket"),
        "output_prefix": resource_names.get("output_prefix"),
    }
    drifted: list[dict[str, str]] = []
    for field, expected_value in expected.items():
        if expected_value is None:
            continue
        actual_value = data.get(field)
        if actual_value != expected_value:
            drifted.append(
                {
                    "path": ".sweetspot/infra/terraform.tfvars.json",
                    "field": field,
                    "reason": "immutable_tfvars_field_changed",
                    "expected": str(expected_value),
                    "actual": str(actual_value),
                }
            )
    return drifted


def _tfvars_input_errors(project_dir: Path) -> list[dict[str, str]]:
    tfvars_path = project_dir / ".sweetspot/infra/terraform.tfvars.json"
    if not tfvars_path.exists():
        return []
    try:
        data = json.loads(tfvars_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        return [{"path": ".sweetspot/infra/terraform.tfvars.json", "field": "$", "reason": sanitize_message(str(exc))}]
    if not isinstance(data, dict):
        return [{"path": ".sweetspot/infra/terraform.tfvars.json", "field": "$", "reason": "tfvars must be a JSON object"}]
    digest = data.get("worker_image_sha256")
    if not isinstance(digest, str) or not re.fullmatch(r"[A-Fa-f0-9]{64}", digest):
        return [
            {
                "path": ".sweetspot/infra/terraform.tfvars.json",
                "field": "worker_image_sha256",
                "reason": "must be replaced with a 64-character worker image sha256 digest before apply",
            }
        ]
    return []


def _artifact_digest_mismatches(project_dir: Path, plan: dict[str, Any] | None) -> list[dict[str, str]]:
    if not isinstance(plan, dict):
        return []
    drifted: list[dict[str, str]] = []
    for artifact in plan.get("generated_artifacts") or []:
        if not isinstance(artifact, dict) or artifact.get("status") != "rendered":
            continue
        raw_path = artifact.get("path")
        if not isinstance(raw_path, str) or not raw_path:
            continue
        candidate = Path(raw_path)
        if candidate.is_absolute() or ".." in candidate.parts:
            continue
        if not (candidate == Path(".sweetspot/deployment.template.json") or candidate.is_relative_to(_INFRA_DIR)):
            continue
        if candidate == Path(".sweetspot/infra/terraform.tfvars.json"):
            drifted.extend(_tfvars_immutable_mismatches(project_dir, plan))
            continue
        expected = artifact.get("sha256")
        if not isinstance(expected, str) or not expected:
            drifted.append({"path": str(candidate), "reason": "missing_review_digest"})
            continue
        path = project_dir / candidate
        if not path.is_file():
            continue
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected:
            drifted.append({"path": str(candidate), "reason": "sha256_mismatch", "expected_sha256": expected, "actual_sha256": actual})
    return drifted


def _required_generated_paths(plan: dict[str, Any] | None) -> list[Path]:
    fallback = [
        Path(".sweetspot/infra/main.tf"),
        Path(".sweetspot/infra/variables.tf"),
        Path(".sweetspot/infra/outputs.tf"),
        Path(".sweetspot/infra/terraform.tfvars.json"),
        Path(".sweetspot/deployment.template.json"),
    ]
    if not isinstance(plan, dict):
        return fallback
    paths: list[Path] = []
    for artifact in plan.get("generated_artifacts") or []:
        if not isinstance(artifact, dict):
            continue
        if artifact.get("status") != "rendered":
            continue
        raw_path = artifact.get("path")
        if not isinstance(raw_path, str) or not raw_path:
            continue
        candidate = Path(raw_path)
        if candidate.is_absolute() or ".." in candidate.parts or not str(candidate).startswith(".sweetspot/"):
            paths.append(Path(".sweetspot/invalid-generated-artifact-path"))
            continue
        if candidate == BOOTSTRAP_PLAN_PATH:
            continue
        if candidate == DEPLOYMENT_OUTPUT_PATH:
            continue
        if candidate == Path(".sweetspot/deployment.template.json") or candidate.is_relative_to(_INFRA_DIR):
            paths.append(candidate)
    return paths or fallback


def _run_apply_and_extract_deployment(
    project_dir: Path,
    runner: CommandRunner,
    *,
    timeout_seconds: int,
    command_summaries: list[dict[str, Any]],
    plan: dict[str, Any] | None,
    tofu_executable: str,
) -> dict[str, Any]:
    infra_dir = project_dir / _INFRA_DIR
    executable = tofu_executable or "tofu"
    init_result = _invoke_runner(runner, [executable, "init", "-backend=false", "-input=false", "-no-color"], cwd=infra_dir, timeout_seconds=timeout_seconds)
    command_summaries.append(_command_summary(f"{executable} init -backend=false", init_result))
    if int(init_result.get("returncode", 1)) != 0:
        message = _join_command_message(init_result)
        raise BootstrapApplyError("init_failed", f"OpenTofu init failed: {sanitize_message(message)}", command_summaries=command_summaries)

    apply_result = _invoke_runner(runner, [executable, "apply", "-auto-approve"], cwd=infra_dir, timeout_seconds=timeout_seconds)
    command_summaries.append(_command_summary(f"{executable} apply", apply_result))
    if int(apply_result.get("returncode", 1)) != 0:
        message = _join_command_message(apply_result)
        category = "missing_permission" if _PERMISSION_RE.search(message) else "apply_failed"
        raise BootstrapApplyError(category, f"OpenTofu apply failed: {sanitize_message(message)}", command_summaries=command_summaries)

    output_result = _invoke_runner(runner, [executable, "output", "-json"], cwd=infra_dir, timeout_seconds=timeout_seconds)
    command_summaries.append(_command_summary(f"{executable} output -json", output_result))
    if int(output_result.get("returncode", 1)) != 0:
        message = _join_command_message(output_result)
        raise BootstrapApplyError("output_extraction_failed", f"OpenTofu output extraction failed: {sanitize_message(message)}", command_summaries=command_summaries)
    try:
        outputs = json.loads(str(output_result.get("stdout") or "{}"))
    except json.JSONDecodeError as exc:
        raise BootstrapApplyError("output_extraction_failed", f"OpenTofu output JSON was malformed: {sanitize_message(str(exc))}", command_summaries=command_summaries) from exc
    try:
        deployment = _deployment_from_outputs(plan, outputs)
    except (TypeError, ValueError, KeyError) as exc:
        output_completeness = _output_completeness(project_dir, plan, deployment_written=False, outputs=outputs if isinstance(outputs, dict) else None)
        raise BootstrapApplyError(
            "output_extraction_failed",
            f"Deployment outputs were incomplete: {sanitize_message(str(exc))}",
            command_summaries=command_summaries,
            output_completeness=output_completeness,
        ) from exc
    return deployment


def _invoke_runner(runner: CommandRunner, command: list[str], *, cwd: Path, timeout_seconds: int) -> dict[str, Any]:
    try:
        result = runner(command, cwd=cwd, timeout_seconds=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        return {"returncode": 124, "stdout": exc.stdout or "", "stderr": exc.stderr or f"command timed out after {timeout_seconds}s", "executable": command[0]}
    except OSError as exc:
        return {"returncode": 127, "stdout": "", "stderr": str(exc), "executable": command[0]}
    except Exception as exc:  # runner seam failures should persist diagnostics, not leak raw exceptions.
        return {"returncode": 1, "stdout": "", "stderr": str(exc), "executable": command[0]}
    if not isinstance(result, dict):
        return {"returncode": 1, "stdout": "", "stderr": "command runner returned a non-object result", "executable": command[0]}
    normalized = dict(result)
    normalized.setdefault("returncode", 0)
    normalized.setdefault("stdout", "")
    normalized.setdefault("stderr", "")
    normalized.setdefault("executable", command[0])
    return normalized


def _subprocess_runner(command: list[str], *, cwd: Path, timeout_seconds: int = 300) -> dict[str, Any]:
    proc = subprocess.run(command, cwd=str(cwd), timeout=timeout_seconds, text=True, capture_output=True, check=False)
    return {"returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr, "executable": command[0]}


def _deployment_from_outputs(plan: dict[str, Any] | None, outputs: Any) -> dict[str, Any]:
    if not isinstance(outputs, dict):
        raise TypeError("output JSON must be an object")
    values = {key: _output_value(value) for key, value in outputs.items()}
    missing_outputs = _missing_required_outputs(values)
    if missing_outputs:
        raise ValueError(f"missing required OpenTofu outputs: {', '.join(missing_outputs)}")
    if isinstance(outputs.get("deployment"), dict):
        deployment_value = _output_value(outputs["deployment"])
        if not isinstance(deployment_value, dict):
            raise ValueError("deployment output must be an object")
        deployment = copy.deepcopy(deployment_value)
    else:
        if not isinstance(plan, dict) or not isinstance(plan.get("expected_deployment"), dict):
            raise ValueError("reviewed plan is missing expected_deployment template")
        deployment = _replace_output_placeholders(copy.deepcopy(plan["expected_deployment"]), values)
        _apply_named_outputs_to_deployment(deployment, values)
    deployment["bootstrap_outputs"] = {key: values[key] for key in _required_opentofu_outputs()}
    if deployment.get("schema") != DEPLOYMENT_SCHEMA_V1:
        raise ValueError("deployment output has unexpected schema")
    if _contains_placeholder(deployment):
        raise ValueError("deployment output still contains unresolved placeholders")
    try:
        return validate_deployment(deployment)
    except PlannerSpecError as exc:
        raise ValueError(str(exc)) from exc


def _required_opentofu_outputs() -> list[str]:
    return [
        "batch_compute_environment",
        "batch_job_queue",
        "batch_job_definition",
        "dlq_url",
        "ecr_repository_url",
        "log_group",
        "operator_role_arn",
        "sqs_queue_url",
        "worker_image_digest",
        "worker_task_role_arn",
    ]


def _missing_required_outputs(outputs: dict[str, Any] | None) -> list[str]:
    if not isinstance(outputs, dict):
        return _required_opentofu_outputs()
    missing: list[str] = []
    for key in _required_opentofu_outputs():
        value = outputs.get(key)
        if value is None or value == "":
            missing.append(key)
    return missing


def _apply_named_outputs_to_deployment(deployment: dict[str, Any], values: dict[str, Any]) -> None:
    regions = deployment.get("regions")
    if not isinstance(regions, dict):
        return
    for raw_region in regions.values():
        if not isinstance(raw_region, dict):
            continue
        raw_region["sqs_queue_url"] = values["sqs_queue_url"]
        raw_region["dlq_url"] = values["dlq_url"]
        architectures = raw_region.get("architectures")
        if not isinstance(architectures, dict):
            continue
        for raw_arch in architectures.values():
            if not isinstance(raw_arch, dict):
                continue
            raw_arch["batch_job_queue"] = values["batch_job_queue"]
            raw_arch["job_definition"] = values["batch_job_definition"]
            raw_arch["image"] = values["worker_image_digest"]


def _output_value(raw: Any) -> Any:
    if isinstance(raw, dict) and "value" in raw:
        return raw["value"]
    return raw


def _replace_output_placeholders(value: Any, outputs: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {key: _replace_output_placeholders(child, outputs) for key, child in value.items()}
    if isinstance(value, list):
        return [_replace_output_placeholders(child, outputs) for child in value]
    if isinstance(value, str):
        match = re.fullmatch(r"[$][{]output[.]([A-Za-z0-9_-]+)[}]", value)
        if match:
            name = match.group(1)
            if name not in outputs:
                raise KeyError(name)
            return outputs[name]
    return value


def _contains_placeholder(value: Any) -> bool:
    if isinstance(value, dict):
        return any(_contains_placeholder(child) for child in value.values())
    if isinstance(value, list):
        return any(_contains_placeholder(child) for child in value)
    return isinstance(value, str) and "${output." in value


def _command_summary(label: str, result: dict[str, Any]) -> dict[str, Any]:
    return {
        "command": label,
        "executable": sanitize_message(str(result.get("executable") or "")),
        "returncode": int(result.get("returncode", 1)),
        "stdout_summary": summarize_text(result.get("stdout")),
        "stderr_summary": summarize_text(result.get("stderr")),
    }


def _join_command_message(result: dict[str, Any]) -> str:
    return "\n".join(str(result.get(key) or "") for key in ("stdout", "stderr")).strip() or "command failed without output"


def _diagnostic(
    *,
    status: str,
    category: str,
    message: str,
    reviewed_plan: dict[str, Any],
    confirmation: dict[str, Any],
    output_completeness: dict[str, Any],
    command_summaries: list[dict[str, Any]],
    recovery_hints: list[str],
) -> dict[str, Any]:
    return _sanitize_obj(
        {
            "schema": BOOTSTRAP_APPLY_SCHEMA_V1,
            "status": status,
            "category": category,
            "message": message,
            "recovery_hints": recovery_hints,
            "reviewed_plan": reviewed_plan,
            "confirmation": confirmation,
            "output_completeness": output_completeness,
            "command_summaries": command_summaries,
        }
    )


def _recovery_hints(category: str) -> list[str]:
    hints = {
        "missing_reviewed_plan": ["Run and review the bootstrap plan before applying."],
        "invalid_reviewed_plan": ["Regenerate the bootstrap plan artifact and review it before applying."],
        "reviewed_plan_not_ready": ["Resolve plan findings until the bootstrap plan status is ready."],
        "blocking_plan_finding": ["Resolve blocking findings in the reviewed bootstrap plan before applying."],
        "missing_generated_artifact": ["Regenerate missing .sweetspot infrastructure artifacts before applying."],
        "generated_artifact_drift": ["Regenerate and rereview the bootstrap plan after any immutable .sweetspot/infra or deployment template changes, then use the new confirmation token."],
        "unresolved_apply_input": ["Replace .sweetspot/infra/terraform.tfvars.json worker_image_sha256 with the reviewed 64-character image digest before applying."],
        "init_failed": ["Run `tofu init -backend=false` from .sweetspot/infra and fix initialization errors before retrying guarded apply."],
        "confirmation_missing": ["Pass the exact apply:<plan-hash> confirmation token shown in the reviewed plan diagnostics."],
        "confirmation_mismatched": ["Re-read the reviewed plan and use its current exact confirmation token."],
        "missing_permission": ["Verify AWS/OpenTofu credentials and IAM permissions, then retry with a freshly reviewed plan."],
        "apply_failed": ["Inspect sanitized command summaries, fix the OpenTofu apply failure, then retry."],
        "output_extraction_failed": ["Ensure OpenTofu outputs include all deployment fields required by .sweetspot/deployment.json."],
    }
    return hints.get(category, ["Inspect bootstrap state and regenerate the reviewed plan before retrying."])


def _persist_state_and_failure(project_dir: Path, outcome: dict[str, Any]) -> None:
    _write_bootstrap_json(project_dir / BOOTSTRAP_STATE_PATH, outcome)
    _write_bootstrap_json(project_dir / BOOTSTRAP_FAILURE_PATH, outcome)


def _write_bootstrap_json(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(_sanitize_obj(report), indent=2, sort_keys=True) + "\n"
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(path)


def _write_deployment_json(path: Path, deployment: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(deployment, indent=2, sort_keys=True) + "\n"
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(path)


def _remove_bootstrap_failure(project_dir: Path) -> None:
    try:
        (project_dir / BOOTSTRAP_FAILURE_PATH).unlink()
    except FileNotFoundError:
        return


def summarize_text(value: Any, *, limit: int = 500) -> str:
    text = sanitize_message(str(value or ""))
    text = "\n".join(line.strip() for line in text.splitlines() if line.strip())
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def sanitize_message(value: str) -> str:
    redacted = _REQUEST_ID_RE.sub("[REDACTED_AWS_REQUEST]", value)
    redacted = _ARN_RE.sub("[REDACTED_ARN]", redacted)
    redacted = _ACCOUNT_RE.sub("[REDACTED_ACCOUNT_ID]", redacted)
    redacted = _PROFILE_CONTEXT_RE.sub(lambda match: match.group(0).replace(match.group(1), "[REDACTED_AUTH_REFERENCE]"), redacted)
    return _SECRET_VALUE_RE.sub("[REDACTED]", redacted)


def _sanitize_obj(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, child in value.items():
            if _SECRET_KEY_RE.search(str(key)):
                sanitized[key] = "[REDACTED]"
            else:
                sanitized[key] = _sanitize_obj(child)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_obj(child) for child in value]
    if isinstance(value, str):
        return sanitize_message(value)
    return value
