from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sweetspot.bootstrap_plan import BOOTSTRAP_PLAN_SCHEMA_V1, DEPLOYMENT_SCHEMA_V1, render_bootstrap_plan
from sweetspot.setup import load_setup, scan_for_secrets, write_project_context


ROOT = Path(__file__).resolve().parents[1]
SECRET = "AKIA1234567890ABCDEF"


class BootstrapDoctorClassifierContractTests(unittest.TestCase):
    def _doctor_module(self):
        # Import lazily so this test file pins the future S05 contract while
        # remaining discoverable before the implementation module exists.
        import sweetspot.bootstrap_doctor as bootstrap_doctor

        return bootstrap_doctor

    def _classify(self, project_dir: Path, **kwargs) -> dict:
        doctor = self._doctor_module()
        return doctor.classify_bootstrap_lifecycle(project_dir, **kwargs)

    def _write_ready_local_bundle(self, project_dir: Path) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        write_project_context(config, project_dir)

    def _write_ready_plan(self, project_dir: Path) -> dict:
        self._write_ready_local_bundle(project_dir)
        plan = render_bootstrap_plan(project_dir)
        plan["confirmation_token"] = "apply:0123456789abcdef"
        self.assertEqual(plan["schema"], BOOTSTRAP_PLAN_SCHEMA_V1)
        self.assertEqual(plan["status"], "ready")
        plan_path = project_dir / ".sweetspot" / "bootstrap-plan.json"
        plan_path.write_text(json.dumps(plan, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        return plan

    def _valid_deployment(self) -> dict:
        return {
            "schema": DEPLOYMENT_SCHEMA_V1,
            "regions": {
                "us-west-2": {
                    "sqs_queue_url": "https://sqs.us-west-2.amazonaws.com/123456789012/example",
                    "dlq_url": "https://sqs.us-west-2.amazonaws.com/123456789012/example-dlq",
                    "architectures": {
                        "x86_64": {
                            "batch_job_queue": "example-batch-project-x86_64-job-queue",
                            "job_definition": "example-batch-project-x86_64-job:1",
                            "image": "123456789012.dkr.ecr.us-west-2.amazonaws.com/example@sha256:" + "a" * 64,
                        }
                    },
                }
            },
            "bootstrap_outputs": {
                "sqs_queue_url": "https://sqs.us-west-2.amazonaws.com/123456789012/example",
                "dlq_url": "https://sqs.us-west-2.amazonaws.com/123456789012/example-dlq",
                "batch_job_queue": "example-batch-project-x86_64-job-queue",
                "batch_job_definition": "example-batch-project-x86_64-job:1",
                "worker_image_digest": "123456789012.dkr.ecr.us-west-2.amazonaws.com/example@sha256:" + "a" * 64,
                "worker_task_role_arn": "arn:aws:iam::123456789012:role/example-worker-task-role",
            },
        }

    def _write_apply_state(self, project_dir: Path, *, status: str = "output_written", category: str = "applied") -> None:
        state_path = project_dir / ".sweetspot" / "bootstrap" / "state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(
            json.dumps(
                {
                    "schema": "sweetspot.bootstrap.apply.v1",
                    "status": status,
                    "category": category,
                    "output_completeness": {"complete": status == "output_written", "deployment_output_written": status == "output_written"},
                    "command_summaries": [],
                    "recovery_hints": [],
                },
                sort_keys=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    def _write_deployment(self, project_dir: Path, deployment: dict | str | None = None) -> None:
        deployment_path = project_dir / ".sweetspot" / "deployment.json"
        deployment_path.parent.mkdir(parents=True, exist_ok=True)
        payload = self._valid_deployment() if deployment is None else deployment
        if isinstance(payload, str):
            deployment_path.write_text(payload, encoding="utf-8")
        else:
            deployment_path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")

    def _assert_report_contract(self, report: dict, classification: str) -> None:
        self.assertEqual(report["schema"], "sweetspot.bootstrap.doctor.v1")
        self.assertEqual(report["classification"], classification)
        self.assertIn(report["status"], {"ok", "action_required", "error"})
        self.assertIsInstance(report["exit_code"], int)
        self.assertIn("local_status", report)
        self.assertIsInstance(report["evidence"], list)
        self.assertIsInstance(report["next_actions"], list)
        self.assertTrue(report["next_actions"], report)
        self.assertEqual(scan_for_secrets(report), ())
        self.assertNotIn(SECRET, json.dumps(report, sort_keys=True))

    def test_no_local_sweetspot_state_maps_to_not_started_without_live_dependencies(self) -> None:
        with (
            tempfile.TemporaryDirectory() as tmpdir,
            mock.patch("subprocess.run", side_effect=AssertionError("subprocess must not run")),
            mock.patch("subprocess.Popen", side_effect=AssertionError("subprocess must not run")),
        ):
            report = self._classify(Path(tmpdir))

        self._assert_report_contract(report, "not_started")
        self.assertEqual(report["local_status"]["setup"], "missing")
        self.assertEqual(report["exit_code"], 0)
        self.assertTrue(any(item["code"] == "sweetspot_state_missing" for item in report["evidence"]))
        self.assertNotIn("aws_diagnostics", report)

    def test_ready_local_bundle_plus_ready_bootstrap_plan_maps_to_planned(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            self._write_ready_plan(project_dir)
            report = self._classify(project_dir)

        self._assert_report_contract(report, "planned")
        self.assertEqual(report["local_status"]["setup"], "ready")
        self.assertEqual(report["local_status"]["plan"], "ready")
        self.assertEqual(report["exit_code"], 0)
        self.assertTrue(any(item["code"] == "bootstrap_plan_ready" and item["path"] == ".sweetspot/bootstrap-plan.json" for item in report["evidence"]))
        self.assertTrue(any("review" in action.lower() or "apply" in action.lower() for action in report["next_actions"]))
        self.assertIn("apply:0123456789abcdef", json.dumps(report))
        self.assertNotIn("[REDACTED]", json.dumps(report.get("next_actions", [])))

    def test_failed_apply_state_maps_to_drift_error_not_planned(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            self._write_ready_plan(project_dir)
            self._write_apply_state(project_dir, status="failed", category="apply_failed")
            report = self._classify(project_dir)

        self._assert_report_contract(report, "drift_error")
        self.assertEqual(report["local_status"]["apply"], "failed")
        self.assertTrue(any(item["code"] == "bootstrap_apply_failed" for item in report["evidence"]))

    def test_output_written_state_plus_valid_deployment_maps_to_applied(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            self._write_ready_plan(project_dir)
            self._write_apply_state(project_dir)
            self._write_deployment(project_dir)
            report = self._classify(project_dir)

        self._assert_report_contract(report, "applied")
        self.assertEqual(report["local_status"]["apply"], "output_written")
        self.assertEqual(report["local_status"]["deployment"], "valid")
        self.assertTrue(any(item["code"] == "deployment_output_valid" and item["severity"] == "info" for item in report["evidence"]))
        self.assertEqual(report["exit_code"], 0)

    def test_output_written_state_with_missing_or_invalid_deployment_maps_to_drift_error(self) -> None:
        cases: dict[str, dict | str | None] = {
            "missing": None,
            "malformed_json": "{not-json " + SECRET,
            "wrong_schema": {**self._valid_deployment(), "schema": "sweetspot.deployment.v0"},
            "loader_invalid": {
                **self._valid_deployment(),
                "regions": {
                    "us-west-2": {
                        **self._valid_deployment()["regions"]["us-west-2"],
                        "architectures": {
                            "x86_64": {
                                **self._valid_deployment()["regions"]["us-west-2"]["architectures"]["x86_64"],
                                "job_definition": "example-batch-project-x86_64-job",
                            }
                        },
                    }
                },
            },
        }
        for name, deployment in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmpdir:
                project_dir = Path(tmpdir)
                self._write_ready_plan(project_dir)
                self._write_apply_state(project_dir)
                if name != "missing":
                    self._write_deployment(project_dir, deployment)

                report = self._classify(project_dir)

                self._assert_report_contract(report, "drift_error")
                self.assertEqual(report["local_status"]["apply"], "output_written")
                self.assertEqual(report["local_status"]["deployment"], "invalid")
                self.assertTrue(any(item["severity"] == "error" and item["code"].startswith("deployment_") for item in report["evidence"]), report)
                self.assertNotEqual(report["exit_code"], 0)

    def test_missing_permission_failure_diagnostics_map_to_missing_permission_with_sanitized_echoes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            self._write_ready_plan(project_dir)
            failure_path = project_dir / ".sweetspot" / "bootstrap" / "failure.json"
            failure_path.parent.mkdir(parents=True, exist_ok=True)
            failure_path.write_text(
                json.dumps(
                    {
                        "schema": "sweetspot.bootstrap.apply.v1",
                        "status": "failed",
                        "category": "missing_permission",
                        "message": f"AccessDenied for token {SECRET} and aws_secret_access_key=abcd",
                        "command_summaries": [{"command": "opentofu apply", "returncode": 1, "stderr_summary": f"denied {SECRET}"}],
                        "recovery_hints": ["Update the AWS profile permissions."],
                    },
                    sort_keys=True,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            report = self._classify(project_dir)

        self._assert_report_contract(report, "missing_permission")
        self.assertEqual(report["local_status"]["failure"], "missing_permission")
        self.assertTrue(any(item["code"] == "bootstrap_missing_permission" for item in report["evidence"]))
        self.assertIn("[redacted]", json.dumps(report, sort_keys=True).lower())

    def test_malformed_plan_state_and_failure_json_surface_sanitized_drift_error_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            self._write_ready_local_bundle(project_dir)
            (project_dir / ".sweetspot" / "bootstrap-plan.json").write_text("{bad-plan " + SECRET, encoding="utf-8")
            bootstrap_dir = project_dir / ".sweetspot" / "bootstrap"
            bootstrap_dir.mkdir(parents=True, exist_ok=True)
            (bootstrap_dir / "state.json").write_text("{bad-state " + SECRET, encoding="utf-8")
            (bootstrap_dir / "failure.json").write_text("{bad-failure " + SECRET, encoding="utf-8")

            report = self._classify(project_dir)

        self._assert_report_contract(report, "drift_error")
        codes = {item["code"] for item in report["evidence"]}
        self.assertIn("bootstrap_plan_malformed", codes)
        self.assertIn("bootstrap_state_malformed", codes)
        self.assertIn("bootstrap_failure_malformed", codes)
        self.assertTrue(all(SECRET not in json.dumps(item, sort_keys=True) for item in report["evidence"]))

    def test_injected_aws_diagnostics_can_drive_missing_permission_without_subprocess_calls(self) -> None:
        aws_diagnostics = {
            "status": "error",
            "category": "missing_permission",
            "checks": [{"name": "sts_get_caller_identity", "status": "denied", "message": f"AccessDenied for {SECRET}"}],
        }
        with (
            tempfile.TemporaryDirectory() as tmpdir,
            mock.patch.object(subprocess, "run", side_effect=AssertionError("live AWS checks must be opt-in/injected")),
            mock.patch.object(subprocess, "Popen", side_effect=AssertionError("live AWS checks must be opt-in/injected")),
        ):
            project_dir = Path(tmpdir)
            self._write_ready_plan(project_dir)
            report = self._classify(project_dir, aws_diagnostics=aws_diagnostics)

        self._assert_report_contract(report, "missing_permission")
        self.assertEqual(report["aws_diagnostics"]["category"], "missing_permission")
        self.assertTrue(any(item["code"] == "aws_missing_permission" for item in report["evidence"]))
        self.assertNotIn(SECRET, json.dumps(report["aws_diagnostics"], sort_keys=True))


if __name__ == "__main__":
    unittest.main()
