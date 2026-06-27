from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .bootstrap_apply import BOOTSTRAP_FAILURE_PATH, BOOTSTRAP_PLAN_PATH, BOOTSTRAP_STATE_PATH, DEPLOYMENT_OUTPUT_PATH, sanitize_message
from .bootstrap_plan import BOOTSTRAP_PLAN_SCHEMA_V1
from .deployment import load_deployment
from .planner import PlannerSpecError
from .setup import bootstrap_status_for_project


BOOTSTRAP_DOCTOR_SCHEMA_V1 = "sweetspot.bootstrap.doctor.v1"
_CLASSIFICATIONS = {"not_started", "planned", "applied", "drift_error", "missing_permission"}
_SECRET_KEY_TOKENS = ("access", "secret", "session", "token", "password", "credential", "private")


def classify_bootstrap_lifecycle(project_dir: str | Path, aws_diagnostics: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a non-mutating bootstrap lifecycle report for a project directory.

    The doctor only reads local SweetSpot artifacts and optional caller-injected AWS
    diagnostics. It never renders plans, invokes OpenTofu/AWS, starts subprocesses,
    or writes recovery artifacts.
    """

    return build_bootstrap_doctor_report(project_dir, aws_diagnostics=aws_diagnostics)


def build_bootstrap_doctor_report(project_dir: str | Path, aws_diagnostics: dict[str, Any] | None = None) -> dict[str, Any]:
    root = _project_root(Path(project_dir).expanduser())
    evidence: list[dict[str, Any]] = []
    next_actions: list[str] = []
    local_setup = _local_setup_status(root, evidence, next_actions)

    plan = _read_json_artifact(root / BOOTSTRAP_PLAN_PATH, BOOTSTRAP_PLAN_PATH, "bootstrap_plan", evidence)
    state = _read_json_artifact(root / BOOTSTRAP_STATE_PATH, BOOTSTRAP_STATE_PATH, "bootstrap_state", evidence)
    failure = _read_json_artifact(root / BOOTSTRAP_FAILURE_PATH, BOOTSTRAP_FAILURE_PATH, "bootstrap_failure", evidence)
    deployment_status = _deployment_status(root, evidence)

    local_status = {
        "setup": local_setup,
        "plan": _plan_status(plan, evidence),
        "apply": _apply_status(state),
        "failure": _failure_status(failure),
        "deployment": deployment_status,
    }
    if local_status["apply"] == "output_written" and local_status["deployment"] == "missing":
        local_status["deployment"] = "invalid"
        evidence.append(
            {
                "code": "deployment_missing",
                "severity": "error",
                "source": "deployment",
                "path": str(DEPLOYMENT_OUTPUT_PATH),
                "message": "Apply state reports output_written but deployment output is missing.",
            }
        )

    sanitized_aws = _sanitize_obj(aws_diagnostics) if aws_diagnostics is not None else None
    if _is_missing_permission_aws(sanitized_aws):
        aws_category = sanitized_aws.get("category") if isinstance(sanitized_aws, dict) else None
        evidence.append(
            {
                "code": "aws_missing_permission",
                "severity": "error",
                "source": "aws_diagnostics",
                "message": "Injected AWS diagnostics report missing permissions.",
                "category": str(aws_category or "missing_permission"),
            }
        )
    elif isinstance(sanitized_aws, dict) and sanitized_aws.get("status") == "blocked":
        evidence.append(
            {
                "code": "aws_diagnostics_blocked",
                "severity": "error",
                "source": "aws_diagnostics",
                "message": "Opt-in AWS diagnostics were blocked before live bootstrap status could be confirmed.",
            }
        )

    classification = _classify(local_status=local_status, plan=plan, state=state, failure=failure, aws_diagnostics=sanitized_aws, evidence=evidence)
    if classification not in _CLASSIFICATIONS:
        raise ValueError(f"unknown bootstrap lifecycle classification: {classification}")

    next_actions.extend(_next_actions_for(classification, local_status, plan=plan, failure=failure, aws_diagnostics=sanitized_aws))
    report: dict[str, Any] = {
        "schema": BOOTSTRAP_DOCTOR_SCHEMA_V1,
        "classification": classification,
        "status": _status_for(classification),
        "exit_code": 2 if classification in {"drift_error", "missing_permission"} else 0,
        "local_status": local_status,
        "evidence": evidence,
        "next_actions": _dedupe(next_actions) or ["Inspect local SweetSpot bootstrap artifacts before retrying."],
    }
    if sanitized_aws is not None:
        report["aws_diagnostics"] = sanitized_aws
    return _sanitize_obj(report)


def _project_root(requested_dir: Path) -> Path:
    return requested_dir.parent if requested_dir.name == ".sweetspot" else requested_dir


def _local_setup_status(root: Path, evidence: list[dict[str, Any]], next_actions: list[str]) -> str:
    try:
        local_report = bootstrap_status_for_project(root)
    except Exception as exc:  # local setup diagnostics should classify, not crash the doctor.
        evidence.append(
            {
                "code": "local_setup_unreadable",
                "severity": "error",
                "source": "local_setup",
                "message": sanitize_message(str(exc)),
            }
        )
        return "invalid"

    status = str(local_report.get("status") or "missing")
    next_actions.extend(str(action) for action in local_report.get("next_actions") or [] if action)
    if status == "ready":
        evidence.append({"code": "local_setup_ready", "severity": "info", "source": "local_setup", "message": "Local SweetSpot setup artifacts are ready."})
        return "ready"
    sweetspot_dir = root / ".sweetspot"
    if not sweetspot_dir.exists():
        evidence.append({"code": "sweetspot_state_missing", "severity": "warning", "source": "local_setup", "path": ".sweetspot", "message": "No local SweetSpot state directory was found."})
        return "missing"
    severity = "error" if status == "invalid" else "warning"
    evidence.append({"code": f"local_setup_{status}", "severity": severity, "source": "local_setup", "message": f"Local SweetSpot setup status is {status}."})
    return status if status in {"ready", "incomplete", "invalid"} else "invalid"


def _read_json_artifact(path: Path, rel_path: Path, prefix: str, evidence: list[dict[str, Any]]) -> dict[str, Any]:
    if not path.exists():
        return {"status": "missing", "path": str(rel_path), "data": None}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        evidence.append(_artifact_error(prefix, "malformed", rel_path, str(exc)))
        return {"status": "malformed", "path": str(rel_path), "data": None}
    except OSError as exc:
        evidence.append(_artifact_error(prefix, "unreadable", rel_path, str(exc)))
        return {"status": "unreadable", "path": str(rel_path), "data": None}
    if not isinstance(data, dict):
        evidence.append(_artifact_error(prefix, "invalid", rel_path, "artifact must be a JSON object"))
        return {"status": "invalid", "path": str(rel_path), "data": None}
    return {"status": "present", "path": str(rel_path), "data": _sanitize_obj(data)}


def _artifact_error(prefix: str, suffix: str, rel_path: Path, message: str) -> dict[str, Any]:
    return {
        "code": f"{prefix}_{suffix}",
        "severity": "error",
        "source": prefix,
        "path": str(rel_path),
        "message": sanitize_message(message),
    }


def _plan_status(plan: dict[str, Any], evidence: list[dict[str, Any]]) -> str:
    if plan["status"] != "present":
        if plan["status"] == "missing":
            return "missing"
        return "invalid"
    data = plan["data"]
    if data.get("schema") != BOOTSTRAP_PLAN_SCHEMA_V1:
        evidence.append(_artifact_error("bootstrap_plan", "invalid", BOOTSTRAP_PLAN_PATH, "reviewed plan has an unexpected schema"))
        return "invalid"
    status = str(data.get("status") or "unknown")
    if status == "ready":
        evidence.append(
            {
                "code": "bootstrap_plan_ready",
                "severity": "info",
                "source": "bootstrap_plan",
                "path": str(BOOTSTRAP_PLAN_PATH),
                "message": "Reviewed bootstrap plan is ready for apply.",
            }
        )
        return "ready"
    severity = "error" if status in {"invalid", "error"} else "warning"
    evidence.append(
        {
            "code": "bootstrap_plan_not_ready",
            "severity": severity,
            "source": "bootstrap_plan",
            "path": str(BOOTSTRAP_PLAN_PATH),
            "message": f"Reviewed bootstrap plan status is {status}.",
        }
    )
    return status


def _apply_status(state: dict[str, Any]) -> str:
    if state["status"] != "present":
        return state["status"]
    return str((state["data"] or {}).get("status") or "unknown")


def _failure_status(failure: dict[str, Any]) -> str:
    if failure["status"] != "present":
        return failure["status"]
    data = failure["data"] or {}
    return str(data.get("category") or data.get("status") or "unknown")


def _deployment_status(root: Path, evidence: list[dict[str, Any]]) -> str:
    path = root / DEPLOYMENT_OUTPUT_PATH
    if not path.exists():
        return "missing"
    try:
        load_deployment(path)
    except PlannerSpecError as exc:
        evidence.append(_artifact_error("deployment", "invalid", DEPLOYMENT_OUTPUT_PATH, str(exc)))
        return "invalid"
    evidence.append(
        {
            "code": "deployment_output_valid",
            "severity": "info",
            "source": "deployment",
            "path": str(DEPLOYMENT_OUTPUT_PATH),
            "message": "Deployment output is present and validates against the deployment contract.",
        }
    )
    return "valid"


def _classify(
    *,
    local_status: dict[str, str],
    plan: dict[str, Any],
    state: dict[str, Any],
    failure: dict[str, Any],
    aws_diagnostics: dict[str, Any] | None,
    evidence: list[dict[str, Any]],
) -> str:
    if local_status["apply"] == "output_written" and local_status["deployment"] == "valid":
        return "applied"

    if _is_missing_permission_failure(failure):
        raw_failure_data = failure.get("data")
        failure_data: dict[str, Any] = raw_failure_data if isinstance(raw_failure_data, dict) else {}
        evidence_item: dict[str, Any] = {
            "code": "bootstrap_missing_permission",
            "severity": "error",
            "source": "bootstrap_failure",
            "path": str(BOOTSTRAP_FAILURE_PATH),
            "message": "Bootstrap apply failure diagnostics indicate missing permissions.",
        }
        diagnostic_message = failure_data.get("message")
        if isinstance(diagnostic_message, str) and diagnostic_message:
            evidence_item["diagnostic_message"] = diagnostic_message
        command_summaries = failure_data.get("command_summaries")
        if isinstance(command_summaries, list) and command_summaries:
            evidence_item["command_summaries"] = command_summaries
        evidence.append(evidence_item)
        return "missing_permission"
    if _is_missing_permission_aws(aws_diagnostics):
        return "missing_permission"

    error_codes = [item for item in evidence if item.get("severity") == "error"]
    if error_codes:
        return "drift_error"

    if local_status["apply"] == "output_written":
        if local_status["deployment"] == "valid":
            return "applied"
        return "drift_error"
    if local_status["apply"] in {"failed", "applying"}:
        evidence.append(
            {
                "code": f"bootstrap_apply_{local_status['apply']}",
                "severity": "error",
                "source": "bootstrap_state",
                "path": str(BOOTSTRAP_STATE_PATH),
                "message": f"Bootstrap apply state is {local_status['apply']}; inspect state/failure artifacts before retrying.",
            }
        )
        return "drift_error"

    if local_status["plan"] == "ready" and local_status["setup"] == "ready":
        return "planned"
    return "not_started"


def _is_missing_permission_failure(failure: dict[str, Any]) -> bool:
    if failure.get("status") != "present":
        return False
    data = failure.get("data")
    if not isinstance(data, dict):
        return False
    return str(data.get("category") or "").lower() == "missing_permission"


def _is_missing_permission_aws(aws_diagnostics: dict[str, Any] | None) -> bool:
    if not isinstance(aws_diagnostics, dict):
        return False
    if str(aws_diagnostics.get("category") or "").lower() == "missing_permission":
        return True
    permission_classifications = {"access_denied", "simulation_denied", "simulation_unavailable", "missing_permission", "unauthorized"}
    for check in aws_diagnostics.get("checks") or []:
        if not isinstance(check, dict):
            continue
        status = str(check.get("status") or "").lower()
        if status not in {"fail", "warn"}:
            continue
        raw_details = check.get("details")
        details: dict[str, Any] = raw_details if isinstance(raw_details, dict) else {}
        raw_error = check.get("error")
        error: dict[str, Any] = raw_error if isinstance(raw_error, dict) else {}
        classifications = {
            str(details.get("classification") or "").lower(),
            str(error.get("classification") or "").lower(),
            str(error.get("type") or "").lower(),
        }
        if details.get("missing_permission") or classifications & permission_classifications:
            return True
    return False


def _status_for(classification: str) -> str:
    if classification == "applied":
        return "ok"
    if classification in {"drift_error", "missing_permission"}:
        return "error"
    return "action_required"


def _next_actions_for(
    classification: str,
    local_status: dict[str, str],
    *,
    plan: dict[str, Any],
    failure: dict[str, Any],
    aws_diagnostics: dict[str, Any] | None,
) -> list[str]:
    if classification == "applied":
        return ["Proceed with downstream worker-container handoff using the validated deployment output."]
    if classification == "missing_permission":
        hints: list[str] = []
        raw_failure_data = failure.get("data")
        failure_data: dict[str, Any] = raw_failure_data if isinstance(raw_failure_data, dict) else {}
        hints.extend(str(item) for item in failure_data.get("recovery_hints") or [] if item)
        if isinstance(aws_diagnostics, dict):
            hints.extend(str(item) for item in aws_diagnostics.get("recovery_hints") or [] if item)
        return hints or ["Verify AWS credentials and IAM permissions, then retry apply with a freshly reviewed plan."]
    if classification == "drift_error":
        return ["Inspect sanitized evidence, regenerate bootstrap artifacts if needed, and retry from the last safe lifecycle step."]
    if classification == "planned":
        raw_plan_data = plan.get("data")
        data: dict[str, Any] = raw_plan_data if isinstance(raw_plan_data, dict) else {}
        token = data.get("confirmation_token") or data.get("apply_confirmation_token")
        action = "Review the ready bootstrap plan, then run the explicit apply command when prepared."
        if token:
            action = f"Review the ready bootstrap plan, then apply with confirmation token {token}."
        return [action]
    if local_status.get("setup") == "missing":
        return ["Run SweetSpot setup to create local .sweetspot bootstrap intent artifacts."]
    return ["Resolve local setup or bootstrap plan findings before applying infrastructure."]


def _dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = sanitize_message(str(value)).strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _sanitize_obj(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, child in value.items():
            key_text = str(key).lower()
            if key_text == "confirmation_token" and isinstance(child, str) and child.startswith("apply:"):
                sanitized[key] = child
            elif any(token in key_text for token in _SECRET_KEY_TOKENS):
                sanitized[key] = "[REDACTED]"
            else:
                sanitized[key] = _sanitize_obj(child)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_obj(child) for child in value]
    if isinstance(value, str):
        return sanitize_message(value)
    return value
