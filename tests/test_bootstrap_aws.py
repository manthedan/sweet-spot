from __future__ import annotations

import json
import unittest

from sweetspot.bootstrap_aws import (
    AWS_DIAGNOSTICS_SCHEMA_V1,
    ClientError,
    NoCredentialsError,
    PartialCredentialsError,
    ProfileNotFound,
    diagnose_bootstrap_aws,
)


RAW_ACCOUNT = "123456789012"
RAW_ARN = f"arn:aws:iam::{RAW_ACCOUNT}:role/AdminSecretRole"
RAW_PROFILE = "prod-admin-profile"
RAW_REQUEST_ID = "req-1234567890abcdef"
RAW_ACCESS_KEY = "AKIAABCDEFGHIJKLMNOP"


def intent(**overrides):
    base = {
        "schema": "sweetspot.bootstrap.intent.v1",
        "status": "ready",
        "project_name": "example",
        "region": "us-west-2",
        "auth_method": "env",
        "auth_reference": None,
        "backend": "local",
        "resource_names": None,
        "missing_inputs": (),
        "errors": (),
    }
    base.update(overrides)
    return base


class FakeSession:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.clients = {}

    def client(self, service, region_name=None):
        if service not in self.clients:
            raise AssertionError(f"unexpected client {service}")
        return self.clients[service]


class FakeSTS:
    def __init__(self, response=None, exc=None):
        self.response = response or {"Account": RAW_ACCOUNT, "Arn": RAW_ARN, "UserId": "AROAABCDEFGHIJKLMNOP:raw-session"}
        self.exc = exc
        self.calls = 0
        self.assume_role_calls = []

    def assume_role(self, **kwargs):
        self.assume_role_calls.append(kwargs)
        if self.exc:
            raise self.exc
        return {"Credentials": {"AccessKeyId": "ASIAABCDEFGHIJKLMNOP", "SecretAccessKey": "secret", "SessionToken": "token"}}

    def get_caller_identity(self):
        self.calls += 1
        if self.exc:
            raise self.exc
        return self.response


class FakeIAM:
    def __init__(self, response=None, exc=None):
        self.response = response or {
            "EvaluationResults": [
                {"EvalActionName": "sts:GetCallerIdentity", "EvalDecision": "allowed"},
                {"EvalActionName": "iam:SimulatePrincipalPolicy", "EvalDecision": "allowed"},
            ]
        }
        self.exc = exc
        self.calls = []

    def simulate_principal_policy(self, **kwargs):
        self.calls.append(kwargs)
        if self.exc:
            raise self.exc
        return self.response


def session_factory_with(sts=None, iam=None, captured=None):
    def factory(**kwargs):
        session = FakeSession(**kwargs)
        session.clients["sts"] = sts or FakeSTS()
        session.clients["iam"] = iam or FakeIAM()
        if captured is not None:
            captured.append(session)
        return session

    return factory


def client_error(code, message="raw AWS error", operation="Operation"):
    return ClientError(
        {
            "Error": {"Code": code, "Message": message},
            "ResponseMetadata": {"RequestId": RAW_REQUEST_ID},
        },
        operation,
    )


def assert_no_raw_identifiers(testcase, report):
    text = json.dumps(report, sort_keys=True)
    for raw in (RAW_ACCOUNT, RAW_ARN, RAW_PROFILE, RAW_REQUEST_ID, RAW_ACCESS_KEY, "AdminSecretRole", "raw-session"):
        testcase.assertNotIn(raw, text)
    testcase.assertNotIn("raw AWS error", text)


class BootstrapAwsDiagnosticsTests(unittest.TestCase):
    def test_success_returns_versioned_sanitized_schema_and_simulation_allowed(self):
        sessions = []
        report = diagnose_bootstrap_aws(
            intent=intent(auth_method="profile", auth_reference=RAW_PROFILE),
            session_factory=session_factory_with(captured=sessions),
        )

        self.assertEqual(AWS_DIAGNOSTICS_SCHEMA_V1, report["schema"])
        self.assertTrue(report["ok"])
        self.assertEqual("ready", report["status"])
        self.assertEqual("us-west-2", report["region"])
        self.assertEqual("profile", report["auth"]["method"])
        self.assertEqual("[REDACTED_AUTH_REFERENCE]", report["auth"]["reference"])
        self.assertEqual("[REDACTED_ACCOUNT_ID]", report["caller_identity"]["account"])
        self.assertEqual("[REDACTED_ARN]", report["caller_identity"]["arn"])
        self.assertEqual(["us-west-2", RAW_PROFILE], [sessions[0].kwargs["region_name"], sessions[0].kwargs["profile_name"]])
        self.assertNotIn("sts:AssumeRole", report["required_actions"])
        checks = {check["name"]: check for check in report["checks"]}
        self.assertEqual("pass", checks["sts_get_caller_identity"]["status"])
        self.assertEqual("simulation_allowed", checks["iam_simulate_principal_policy"]["details"]["classification"])
        assert_no_raw_identifiers(self, report)

    def test_env_auth_omits_profile_name_when_constructing_session(self):
        sessions = []
        report = diagnose_bootstrap_aws(intent=intent(), session_factory=session_factory_with(captured=sessions))
        self.assertTrue(report["ok"])
        self.assertNotIn("profile_name", sessions[0].kwargs)

    def test_sso_auth_uses_configured_profile_reference(self):
        sessions = []
        report = diagnose_bootstrap_aws(intent=intent(auth_method="sso", auth_reference=RAW_PROFILE), session_factory=session_factory_with(captured=sessions))
        self.assertTrue(report["ok"])
        self.assertEqual("sso", report["auth"]["method"])
        self.assertEqual(RAW_PROFILE, sessions[0].kwargs["profile_name"])

    def test_sso_auth_without_reference_is_incomplete(self):
        report = diagnose_bootstrap_aws(intent=intent(auth_method="sso", auth_reference=None), session_factory=lambda **kwargs: None)
        self.assertFalse(report["ok"])
        self.assertEqual("incomplete_auth", report["checks"][0]["details"]["classification"])

    def test_role_auth_assumes_role_before_identity_checks(self):
        sessions = []
        base_sts = FakeSTS()
        report = diagnose_bootstrap_aws(
            intent=intent(auth_method="role", auth_reference=RAW_ARN),
            session_factory=session_factory_with(sts=base_sts, captured=sessions),
        )

        self.assertTrue(report["ok"])
        self.assertEqual("role", report["auth"]["method"])
        self.assertEqual("[REDACTED_AUTH_REFERENCE]", report["auth"]["reference"])
        self.assertIn("sts:AssumeRole", report["required_actions"])
        self.assertEqual(RAW_ARN, base_sts.assume_role_calls[0]["RoleArn"])
        self.assertNotIn("profile_name", sessions[0].kwargs)
        self.assertIn("aws_access_key_id", sessions[1].kwargs)
        assert_no_raw_identifiers(self, report)

    def test_missing_credentials_from_sts_is_structured_failure(self):
        report = diagnose_bootstrap_aws(
            intent=intent(),
            session_factory=session_factory_with(sts=FakeSTS(exc=NoCredentialsError())),
        )
        self.assertFalse(report["ok"])
        self.assertEqual("blocked", report["status"])
        sts_check = next(check for check in report["checks"] if check["name"] == "sts_get_caller_identity")
        self.assertEqual("fail", sts_check["status"])
        self.assertEqual("missing_credentials", sts_check["details"]["classification"])
        self.assertEqual("[REDACTED_AWS_ERROR]", sts_check["error"]["message"])

    def test_partial_credentials_and_profile_not_found_are_classified_without_raw_profile(self):
        partial = diagnose_bootstrap_aws(
            intent=intent(),
            session_factory=session_factory_with(sts=FakeSTS(exc=PartialCredentialsError(provider="env", cred_var="aws_secret_access_key"))),
        )
        partial_check = next(check for check in partial["checks"] if check["name"] == "sts_get_caller_identity")
        self.assertEqual("partial_credentials", partial_check["details"]["classification"])

        missing_profile = diagnose_bootstrap_aws(
            intent=intent(auth_method="profile", auth_reference=RAW_PROFILE),
            session_factory=lambda **kwargs: (_ for _ in ()).throw(ProfileNotFound(profile=RAW_PROFILE)),
        )
        auth_check = next(check for check in missing_profile["checks"] if check["name"] == "auth")
        self.assertEqual("profile_not_found", auth_check["details"]["classification"])
        assert_no_raw_identifiers(self, missing_profile)

    def test_sts_access_denied_throttled_endpoint_and_unknown_errors_are_classified(self):
        cases = [
            (client_error("AccessDenied", f"Access denied for {RAW_ARN} request id {RAW_REQUEST_ID}"), "access_denied"),
            (client_error("ThrottlingException", "Rate exceeded"), "throttled"),
            (client_error("EndpointConnectionError", "Could not connect to endpoint"), "endpoint_unavailable"),
            (RuntimeError(f"boom {RAW_ACCOUNT} {RAW_ACCESS_KEY}"), "unknown_exception"),
        ]
        for exc, expected in cases:
            with self.subTest(expected=expected):
                report = diagnose_bootstrap_aws(intent=intent(), session_factory=session_factory_with(sts=FakeSTS(exc=exc)))
                sts_check = next(check for check in report["checks"] if check["name"] == "sts_get_caller_identity")
                self.assertEqual(expected, sts_check["details"]["classification"])
                assert_no_raw_identifiers(self, report)

    def test_iam_simulation_denied_is_warning_not_blocker(self):
        iam = FakeIAM(
            response={
                "EvaluationResults": [
                    {"EvalActionName": "batch:SubmitJob", "EvalDecision": "explicitDeny"},
                    {"EvalActionName": "s3:GetObject", "EvalDecision": "allowed"},
                ]
            }
        )
        report = diagnose_bootstrap_aws(intent=intent(), session_factory=session_factory_with(iam=iam))
        sim_check = next(check for check in report["checks"] if check["name"] == "iam_simulate_principal_policy")
        self.assertTrue(report["ok"])
        self.assertEqual("warning", report["status"])
        self.assertEqual("warn", sim_check["status"])
        self.assertEqual("simulation_denied", sim_check["details"]["classification"])

    def test_iam_access_denied_is_simulation_unavailable_not_crash(self):
        report = diagnose_bootstrap_aws(
            intent=intent(),
            session_factory=session_factory_with(iam=FakeIAM(exc=client_error("AccessDenied", f"Denied {RAW_ARN}"))),
        )
        sim_check = next(check for check in report["checks"] if check["name"] == "iam_simulate_principal_policy")
        self.assertTrue(report["ok"])
        self.assertEqual("warning", report["status"])
        self.assertEqual("simulation_unavailable", sim_check["details"]["classification"])
        self.assertEqual("iam:SimulatePrincipalPolicy", sim_check["details"]["missing_permission"])
        assert_no_raw_identifiers(self, report)

    def test_iam_simulation_skipped_when_identity_has_no_arn(self):
        report = diagnose_bootstrap_aws(
            intent=intent(),
            session_factory=session_factory_with(sts=FakeSTS(response={"Account": RAW_ACCOUNT, "UserId": "user"})),
        )
        sim_check = next(check for check in report["checks"] if check["name"] == "iam_simulate_principal_policy")
        self.assertTrue(report["ok"])
        self.assertEqual("simulation_skipped", sim_check["details"]["classification"])

    def test_unsupported_and_incomplete_auth_do_not_construct_session(self):
        def fail_if_called(**kwargs):
            raise AssertionError("session should not be constructed")

        unsupported = diagnose_bootstrap_aws(
            intent=intent(auth_method="static-keys", auth_reference=RAW_ARN),
            session_factory=fail_if_called,
        )
        self.assertEqual("blocked", unsupported["status"])
        self.assertEqual("unsupported_auth", unsupported["checks"][0]["details"]["classification"])
        assert_no_raw_identifiers(self, unsupported)

        incomplete = diagnose_bootstrap_aws(
            intent=intent(auth_method="profile", auth_reference=None),
            session_factory=fail_if_called,
        )
        self.assertEqual("blocked", incomplete["status"])
        self.assertEqual("incomplete_auth", incomplete["checks"][0]["details"]["classification"])

    def test_missing_region_and_missing_auth_are_structured(self):
        no_region = diagnose_bootstrap_aws(intent=intent(region=None), session_factory=lambda **kwargs: None)
        self.assertEqual("missing_region", no_region["checks"][0]["details"]["classification"])

        no_auth = diagnose_bootstrap_aws(intent=intent(auth_method=None), session_factory=lambda **kwargs: None)
        self.assertEqual("missing_auth_method", no_auth["checks"][0]["details"]["classification"])

    def test_setup_dict_input_is_accepted_without_live_aws(self):
        setup_dict = {
            "schema": "sweetspot.project.v1",
            "project": {"name": "example"},
            "workload": {
                "input_manifest": "s3://input/tasks.jsonl",
                "output_prefix": "s3://output/runs/example/",
                "command": ["python", "worker.py"],
                "architecture": "x86_64",
            },
            "aws": {"region": "us-east-1", "auth": {"method": "profile", "profile": RAW_PROFILE}},
        }
        sessions = []
        report = diagnose_bootstrap_aws(intent=setup_dict, session_factory=session_factory_with(captured=sessions))
        self.assertEqual("us-east-1", sessions[0].kwargs["region_name"])
        self.assertEqual("[REDACTED_AUTH_REFERENCE]", report["auth"]["reference"])
        assert_no_raw_identifiers(self, report)


if __name__ == "__main__":
    unittest.main()
