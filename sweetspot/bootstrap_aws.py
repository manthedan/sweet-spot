from __future__ import annotations

import re
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

try:  # pragma: no cover - exercised when botocore is installed.
    from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError, ProfileNotFound
except (ImportError, ModuleNotFoundError):  # pragma: no cover - local fallback is covered in this environment.

    class ClientError(Exception):  # type: ignore[no-redef]
        def __init__(self, error_response: Mapping[str, Any], operation_name: str):
            super().__init__(str(error_response.get("Error", {}).get("Message", "ClientError")))
            self.response = dict(error_response)
            self.operation_name = operation_name

    class NoCredentialsError(Exception):  # type: ignore[no-redef]
        pass

    class PartialCredentialsError(Exception):  # type: ignore[no-redef]
        def __init__(self, provider: str, cred_var: str):
            super().__init__(f"partial credentials for {provider}: {cred_var}")
            self.provider = provider
            self.cred_var = cred_var

    class ProfileNotFound(Exception):  # type: ignore[no-redef]
        def __init__(self, profile: str):
            super().__init__(f"The config profile ({profile}) could not be found")
            self.profile = profile


AWS_DIAGNOSTICS_SCHEMA_V1 = "sweetspot.bootstrap.aws_diagnostics.v1"
SWEETSPOT_CONFIG_PATH = ".sweetspot/sweetspot.yaml"

DEFAULT_REQUIRED_ACTIONS = (
    "sts:GetCallerIdentity",
    "iam:SimulatePrincipalPolicy",
    "iam:CreateRole",
    "iam:CreatePolicy",
    "iam:PutRolePolicy",
    "iam:AttachRolePolicy",
    "iam:CreateInstanceProfile",
    "iam:AddRoleToInstanceProfile",
    "iam:PassRole",
    "ec2:DescribeVpcs",
    "ec2:DescribeSubnets",
    "ec2:DescribeSecurityGroups",
    "batch:CreateComputeEnvironment",
    "batch:CreateJobQueue",
    "batch:RegisterJobDefinition",
    "batch:SubmitJob",
    "batch:DescribeJobs",
    "sqs:CreateQueue",
    "sqs:SetQueueAttributes",
    "sqs:GetQueueAttributes",
    "sqs:ReceiveMessage",
    "sqs:DeleteMessage",
    "s3:GetObject",
    "s3:PutObject",
    "ecr:CreateRepository",
    "ecr:DescribeRepositories",
    "logs:CreateLogGroup",
    "logs:PutRetentionPolicy",
)
SUPPORTED_AUTH_METHODS = {"env", "profile", "role", "sso"}
_ACCOUNT_RE = re.compile(r"\b\d{12}\b")
_ARN_RE = re.compile(r"\barn:aws(?:-[a-z]+)*:[^\s\"'<>]+")
_ACCESS_KEY_RE = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")
_SECRET_LIKE_RE = re.compile(r"(?i)\b(?:secret|token|password|credential|access[_-]?key)\b[\w .:=/-]*")
_REQUEST_ID_RE = re.compile(r"(?i)\b(?:request(?:id| id)?|x-amz-request-id|host id)[:= ]+[A-Za-z0-9/+=._:-]+")
_PROFILE_CONTEXT_RE = re.compile(r"(?i)(?:profile|role|session)(?: name)?\s*['\"]([^'\"]+)['\"]")
_QUOTED_VALUE_RE = re.compile(r"['\"][^'\"]{2,}['\"]")


def diagnose_bootstrap_aws(
    project_dir: str | Path | None = None,
    intent: Any | None = None,
    *,
    session_factory: Callable[..., Any] | None = None,
    sts_client_factory: Callable[[Any, str | None], Any] | None = None,
    iam_client_factory: Callable[[Any, str | None], Any] | None = None,
    required_actions: tuple[str, ...] = DEFAULT_REQUIRED_ACTIONS,
) -> dict[str, Any]:
    """Return a deterministic, sanitized read-only AWS bootstrap diagnostic report.

    The only live AWS calls are STS ``get_caller_identity`` and best-effort IAM
    ``simulate_principal_policy``. All AWS construction is injectable so unit
    tests can exercise the contract without credentials or network access.
    """
    intent_dict = _load_intent(project_dir=project_dir, intent=intent)
    region = _redact(intent_dict.get("region")) if intent_dict.get("region") else None
    auth_method = intent_dict.get("auth_method")
    auth_reference = intent_dict.get("auth_reference")
    if auth_method == "sso" and isinstance(auth_reference, str) and auth_reference.startswith("AWS SSO session"):
        auth_reference = None
    actions = tuple(required_actions)
    if auth_method == "role" and "sts:AssumeRole" not in actions:
        actions = ("sts:AssumeRole", *actions)
    checks: list[dict[str, Any]] = []
    redactions: set[str] = set()
    caller_identity: dict[str, Any] | None = None

    report: dict[str, Any] = {
        "schema": AWS_DIAGNOSTICS_SCHEMA_V1,
        "ok": False,
        "status": "unknown",
        "region": region,
        "auth": {
            "method": _redact(auth_method),
            "reference": _redact_auth_reference(auth_reference, redactions) if auth_reference else None,
            "supported": auth_method in SUPPORTED_AUTH_METHODS,
        },
        "caller_identity": None,
        "checks": checks,
        "required_actions": actions,
        "redactions": [],
    }

    if not region:
        checks.append(_check("region", "fail", "error", details={"classification": "missing_region"}))
        return _finalize(report, redactions)

    if not auth_method:
        checks.append(_check("auth", "fail", "error", details={"classification": "missing_auth_method"}))
        return _finalize(report, redactions)

    if auth_method not in SUPPORTED_AUTH_METHODS:
        checks.append(
            _check(
                "auth",
                "fail",
                "error",
                details={"classification": "unsupported_auth", "method": _redact(auth_method)},
            )
        )
        checks.append(_check("sts_get_caller_identity", "skipped", "info", details={"reason": "unsupported_auth"}))
        checks.append(_check("iam_simulate_principal_policy", "skipped", "info", details={"reason": "unsupported_auth"}))
        return _finalize(report, redactions)

    if auth_method in {"profile", "role", "sso"} and not auth_reference:
        checks.append(
            _check(
                "auth",
                "fail",
                "error",
                details={"classification": "incomplete_auth", "method": _redact(auth_method)},
            )
        )
        return _finalize(report, redactions)

    session_kwargs: dict[str, Any] = {"region_name": region}
    if auth_method in {"profile", "sso"} and auth_reference:
        session_kwargs["profile_name"] = auth_reference

    try:
        session = _default_session_factory(**session_kwargs) if session_factory is None else session_factory(**session_kwargs)
        if auth_method == "role":
            base_sts = _client(session, "sts", region, sts_client_factory)
            assumed = base_sts.assume_role(RoleArn=auth_reference, RoleSessionName="sweetspot-bootstrap-diagnostics")
            credentials = assumed.get("Credentials") if isinstance(assumed, Mapping) else None
            if not isinstance(credentials, Mapping):
                raise RuntimeError("assume_role response did not contain credentials")
            assumed_kwargs = {
                "region_name": region,
                "aws_access_key_id": credentials.get("AccessKeyId"),
                "aws_secret_access_key": credentials.get("SecretAccessKey"),
                "aws_session_token": credentials.get("SessionToken"),
            }
            session = _default_session_factory(**assumed_kwargs) if session_factory is None else session_factory(**assumed_kwargs)
        checks.append(
            _check(
                "auth",
                "pass",
                "info",
                details={"classification": "configured", "method": _redact(auth_method)},
            )
        )
    except Exception as exc:  # construction can fail before client calls, especially profiles
        classified = _classify_exception(exc)
        checks.append(_check("auth", "fail", "error", details={"classification": classified}, error=_safe_error(exc)))
        return _finalize(report, redactions)

    try:
        sts = _client(session, "sts", region, sts_client_factory)
        identity = sts.get_caller_identity()
        caller_identity = _sanitize_identity(identity, redactions)
        report["caller_identity"] = caller_identity
        checks.append(
            _check(
                "sts_get_caller_identity",
                "pass",
                "info",
                details={"classification": "identity_available", "caller_identity": caller_identity},
            )
        )
    except Exception as exc:
        classified = _classify_exception(exc)
        checks.append(
            _check(
                "sts_get_caller_identity",
                "fail",
                _severity_for_classification(classified),
                details={"classification": classified},
                error=_safe_error(exc),
            )
        )
        return _finalize(report, redactions)

    raw_arn = _mapping_get(identity, "Arn")
    if not raw_arn:
        checks.append(
            _check(
                "iam_simulate_principal_policy",
                "skipped",
                "warning",
                details={"classification": "simulation_skipped", "reason": "missing_principal_arn"},
            )
        )
        return _finalize(report, redactions)

    try:
        iam = _client(session, "iam", region, iam_client_factory)
        response = iam.simulate_principal_policy(PolicySourceArn=raw_arn, ActionNames=list(actions))
        evaluation = tuple(_simulation_results(response))
        denied = tuple(item for item in evaluation if item.get("decision") != "allowed")
        checks.append(
            _check(
                "iam_simulate_principal_policy",
                "pass" if not denied else "warn",
                "info" if not denied else "warning",
                details={
                    "classification": "simulation_allowed" if not denied else "simulation_denied",
                    "evaluations": evaluation,
                },
            )
        )
    except Exception as exc:
        classified = _classify_exception(exc)
        if classified == "access_denied":
            classified = "simulation_unavailable"
        checks.append(
            _check(
                "iam_simulate_principal_policy",
                "warn",
                "warning",
                details={
                    "classification": classified,
                    "missing_permission": "iam:SimulatePrincipalPolicy" if classified == "simulation_unavailable" else None,
                },
                error=_safe_error(exc),
            )
        )

    return _finalize(report, redactions)


def _load_intent(project_dir: str | Path | None, intent: Any | None) -> dict[str, Any]:
    if intent is None and project_dir is None:
        raise ValueError("project_dir or intent is required")
    if intent is None:
        if project_dir is None:
            raise ValueError("project_dir or intent is required")
        setup_path = Path(project_dir) / SWEETSPOT_CONFIG_PATH
        intent = _setup_mapping_from_yaml(setup_path)
    elif isinstance(intent, Mapping) and "aws" in intent:
        intent = _intent_from_setup_mapping(intent)
    elif not isinstance(intent, Mapping) and not hasattr(intent, "auth_method"):
        raise TypeError("intent must be a BootstrapIntent-compatible object or setup dict")
    if is_dataclass(intent) and not isinstance(intent, type):
        return asdict(intent)
    if isinstance(intent, Mapping):
        return dict(intent)
    return dict(vars(intent))


def _default_session_factory(**kwargs: Any) -> Any:
    import boto3

    return boto3.Session(**kwargs)


def _client(session: Any, service: str, region: str | None, factory: Callable[[Any, str | None], Any] | None) -> Any:
    if factory is not None:
        return factory(session, region)
    return session.client(service, region_name=region)


def _check(
    name: str,
    status: str,
    severity: str,
    *,
    details: Mapping[str, Any] | None = None,
    error: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"name": name, "status": status, "severity": severity, "details": _sanitize(details or {})}
    if error is not None:
        payload["error"] = _sanitize(error)
    return payload


def _finalize(report: dict[str, Any], redactions: set[str]) -> dict[str, Any]:
    statuses = [check["status"] for check in report["checks"]]
    has_fail = any(status == "fail" for status in statuses)
    has_warn = any(status == "warn" for status in statuses)
    report["ok"] = bool(statuses) and not has_fail
    report["status"] = "ready" if report["ok"] and not has_warn else "warning" if report["ok"] else "blocked"
    report["redactions"] = tuple(sorted(redactions | _detect_redaction_markers(report)))
    return _sanitize(report)


def _sanitize_identity(identity: Mapping[str, Any], redactions: set[str]) -> dict[str, Any]:
    return {
        "account": _redact(_mapping_get(identity, "Account"), redactions),
        "arn": _redact(_mapping_get(identity, "Arn"), redactions),
        "user_id": _redact_user_id(_mapping_get(identity, "UserId"), redactions),
    }


def _simulation_results(response: Mapping[str, Any]) -> list[dict[str, str]]:
    results = []
    for item in response.get("EvaluationResults", ()) or ():
        action = str(item.get("EvalActionName", "unknown"))
        decision = str(item.get("EvalDecision", "unknown")).lower()
        results.append({"action": action, "decision": "allowed" if decision == "allowed" else decision})
    return results


def _classify_exception(exc: BaseException) -> str:
    if isinstance(exc, NoCredentialsError):
        return "missing_credentials"
    if isinstance(exc, PartialCredentialsError):
        return "partial_credentials"
    if isinstance(exc, ProfileNotFound):
        return "profile_not_found"
    if isinstance(exc, ClientError):
        code = str(exc.response.get("Error", {}).get("Code", "")).lower()
        message = str(exc.response.get("Error", {}).get("Message", "")).lower()
        text = f"{code} {message}"
        if "accessdenied" in code or "access denied" in message or "unauthorized" in text:
            return "access_denied"
        if "throttl" in text or "toomanyrequests" in code:
            return "throttled"
        if "endpoint" in text or "could not connect" in text or "connection" in text:
            return "endpoint_unavailable"
        return "client_error"
    return "unknown_exception"


def _severity_for_classification(classification: str) -> str:
    if classification in {"throttled", "endpoint_unavailable"}:
        return "warning"
    return "error"


def _safe_error(exc: BaseException) -> dict[str, str]:
    classification = _classify_exception(exc)
    if isinstance(exc, ClientError):
        code = str(exc.response.get("Error", {}).get("Code", "ClientError"))
    else:
        code = exc.__class__.__name__
    return {"classification": classification, "type": _redact(code), "message": "[REDACTED_AWS_ERROR]"}


def _sanitize(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(_redact(key)): _sanitize(item) for key, item in value.items() if item is not None}
    if isinstance(value, (tuple, list)):
        return tuple(_sanitize(item) for item in value)
    if isinstance(value, str):
        return _redact(value)
    return value


def _setup_mapping_from_yaml(path: Path) -> dict[str, Any]:
    import yaml

    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, Mapping):
        raise ValueError("setup document must be a mapping")
    return _intent_from_setup_mapping(data)


def _intent_from_setup_mapping(setup: Mapping[str, Any]) -> dict[str, Any]:
    aws = setup.get("aws") or {}
    auth = aws.get("auth") or {}
    method = auth.get("method")
    reference = auth.get("profile") or auth.get("sso_profile") or auth.get("sso_session") or auth.get("role_arn")
    return {
        "schema": "sweetspot.bootstrap.intent.v1",
        "status": "ready",
        "project_name": (setup.get("project") or {}).get("name"),
        "region": aws.get("region"),
        "auth_method": method,
        "auth_reference": reference,
        "backend": "local",
        "resource_names": None,
        "missing_inputs": (),
        "errors": (),
    }


def _redact_auth_reference(value: Any, redactions: set[str]) -> str:
    redactions.add("profile_or_role_name")
    return "[REDACTED_AUTH_REFERENCE]"


def _redact_user_id(value: Any, redactions: set[str]) -> str | None:
    if value is None:
        return None
    redactions.add("principal_user_id")
    return "[REDACTED_USER_ID]"


def _redact(value: Any, redactions: set[str] | None = None) -> str:
    text = "" if value is None else str(value)
    markers = redactions if redactions is not None else set()
    if _ACCOUNT_RE.search(text):
        markers.add("account_id")
        text = _ACCOUNT_RE.sub("[REDACTED_ACCOUNT_ID]", text)
    if _ARN_RE.search(text):
        markers.add("arn")
        text = _ARN_RE.sub("[REDACTED_ARN]", text)
    if _ACCESS_KEY_RE.search(text):
        markers.add("access_key_id")
        text = _ACCESS_KEY_RE.sub("[REDACTED_ACCESS_KEY_ID]", text)
    if _SECRET_LIKE_RE.search(text):
        markers.add("secret_like")
        text = _SECRET_LIKE_RE.sub("[REDACTED_SECRET_LIKE]", text)
    if _REQUEST_ID_RE.search(text):
        markers.add("request_id")
        text = _REQUEST_ID_RE.sub("[REDACTED_REQUEST_ID]", text)
    if _PROFILE_CONTEXT_RE.search(text):
        markers.add("profile_or_role_name")
        text = _PROFILE_CONTEXT_RE.sub(lambda m: m.group(0).replace(m.group(1), "[REDACTED_NAME]"), text)
    if _QUOTED_VALUE_RE.search(text) and any(word in text.lower() for word in ("profile", "role", "session")):
        markers.add("profile_or_role_name")
        text = _QUOTED_VALUE_RE.sub("'[REDACTED_NAME]'", text)
    return text


def _detect_redaction_markers(value: Any) -> set[str]:
    serialized = repr(value)
    markers = set()
    if "[REDACTED_ACCOUNT_ID]" in serialized:
        markers.add("account_id")
    if "[REDACTED_ARN]" in serialized:
        markers.add("arn")
    if "[REDACTED_AWS_ERROR]" in serialized:
        markers.add("aws_error_message")
    if "[REDACTED_REQUEST_ID]" in serialized:
        markers.add("request_id")
    if "[REDACTED_SECRET" in serialized or "[REDACTED_ACCESS_KEY_ID]" in serialized:
        markers.add("secret_like")
    if "[REDACTED_NAME]" in serialized or "[REDACTED_AUTH_REFERENCE]" in serialized:
        markers.add("profile_or_role_name")
    if "[REDACTED_USER_ID]" in serialized:
        markers.add("principal_user_id")
    return markers


def _mapping_get(mapping: Mapping[str, Any], key: str) -> Any:
    try:
        return mapping.get(key)
    except AttributeError:
        return None
