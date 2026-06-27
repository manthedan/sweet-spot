from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from sweetspot.bootstrap_plan import BOOTSTRAP_PLAN_SCHEMA_V1, DEPLOYMENT_SCHEMA_V1
from sweetspot.deployment import load_deployment


class FakeApplyRunner:
    def __init__(self) -> None:
        self.commands: list[list[str]] = []

    def __call__(self, command, *, cwd, timeout_seconds=30):
        self.commands.append(list(command))
        if any(str(part).lower() == "apply" for part in command):
            raise AssertionError("OpenTofu apply must not run when bootstrap apply is refused")
        return {"returncode": 0, "stdout": "", "stderr": "", "executable": command[0]}


class BootstrapApplyGuardContractTests(unittest.TestCase):
    def _apply_module(self):
        # Import lazily so this file documents the future S04 contract while
        # still being discoverable before the implementation exists.
        import sweetspot.bootstrap_apply as bootstrap_apply

        return bootstrap_apply

    def _ready_plan(self) -> dict:
        return {
            "schema": BOOTSTRAP_PLAN_SCHEMA_V1,
            "status": "ready",
            "project_dir": "PROJECT_DIR_REPLACED_BY_TEST",
            "intent": {
                "schema": "sweetspot.bootstrap.intent.v1",
                "status": "ready",
                "project_name": "Example Batch Project",
                "region": "us-west-2",
                "auth": {"method": "profile", "reference": "sweetspot-dev"},
            },
            "findings": [
                {
                    "code": "intent_ready",
                    "severity": "info",
                    "field_path": "$",
                    "message": "Bootstrap intent is ready for guarded apply.",
                }
            ],
            "generated_artifacts": [
                {"name": "opentofu_main", "path": ".sweetspot/infra/main.tf", "status": "rendered"},
                {"name": "opentofu_variables", "path": ".sweetspot/infra/variables.tf", "status": "rendered"},
                {"name": "opentofu_outputs", "path": ".sweetspot/infra/outputs.tf", "status": "rendered"},
                {"name": "opentofu_tfvars_stub", "path": ".sweetspot/infra/terraform.tfvars.json", "status": "rendered"},
                {"name": "bootstrap_plan", "path": ".sweetspot/bootstrap-plan.json", "status": "rendered"},
                {"name": "deployment_template", "path": ".sweetspot/deployment.template.json", "status": "rendered"},
            ],
            "resource_inventory": [],
            "expected_deployment": {
                "schema": DEPLOYMENT_SCHEMA_V1,
                "regions": {
                    "us-west-2": {
                        "sqs_queue_url": "${output.sqs_queue_url}",
                        "dlq_url": "${output.dlq_url}",
                        "architectures": {
                            "x86_64": {
                                "batch_job_queue": "example-batch-project-x86_64-job-queue",
                                "job_definition": "example-batch-project-x86_64-job:1",
                                "image": "${output.worker_image_digest}",
                            }
                        },
                    }
                },
            },
            "command_attempts": [],
            "stderr_summary": [],
            "next_actions": ["Review the bootstrap plan before applying."],
        }

    def _write_ready_artifacts(self, project_dir: Path, *, plan_overrides: dict | None = None, omit_artifact: str | None = None) -> Path:
        plan = self._ready_plan()
        plan["project_dir"] = str(project_dir)
        if plan_overrides:
            plan.update(plan_overrides)
        plan_path = project_dir / ".sweetspot" / "bootstrap-plan.json"
        plan_path.parent.mkdir(parents=True, exist_ok=True)
        rendered = {
            ".sweetspot/infra/main.tf": 'resource "aws_sqs_queue" "queue" { name = "example" }\n',
            ".sweetspot/infra/variables.tf": 'variable "aws_region" { type = string }\n',
            ".sweetspot/infra/outputs.tf": 'output "sqs_queue_url" { value = "https://sqs.us-west-2.amazonaws.com/123456789012/example" }\n',
            ".sweetspot/infra/terraform.tfvars.json": json.dumps({"aws_profile": "sweetspot-dev", "aws_region": "us-west-2", "aws_role_arn": "", "worker_image_sha256": "a" * 64}, sort_keys=True) + "\n",
            ".sweetspot/deployment.template.json": json.dumps(plan["expected_deployment"], sort_keys=True) + "\n",
        }
        for artifact in plan["generated_artifacts"]:
            relpath = artifact.get("path")
            if relpath in rendered:
                artifact["sha256"] = hashlib.sha256(rendered[relpath].encode("utf-8")).hexdigest()
        for relpath, content in rendered.items():
            if relpath == omit_artifact:
                continue
            path = project_dir / relpath
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        plan_path.write_text(json.dumps(plan, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        return plan_path

    def _assert_refusal(self, outcome: dict, *, category: str) -> None:
        apply_module = self._apply_module()
        self.assertEqual(outcome["schema"], apply_module.BOOTSTRAP_APPLY_SCHEMA_V1)
        self.assertEqual(outcome["status"], "blocked")
        self.assertEqual(outcome["category"], category)
        self.assertIsInstance(outcome["message"], str)
        self.assertNotIn("AKIA1234567890ABCDEF", json.dumps(outcome, sort_keys=True))
        self.assertIsInstance(outcome["recovery_hints"], list)
        self.assertTrue(outcome["recovery_hints"], outcome)
        self.assertIn("reviewed_plan", outcome)
        self.assertIn("confirmation", outcome)
        self.assertIn(outcome["confirmation"]["status"], {"missing", "mismatched", "not_required", "accepted"})
        self.assertIn("output_completeness", outcome)
        self.assertFalse(outcome["output_completeness"]["complete"])
        self.assertEqual(outcome["command_summaries"], [])

    def test_missing_reviewed_plan_blocks_without_invoking_runner(self) -> None:
        apply_module = self._apply_module()
        runner = FakeApplyRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            outcome = apply_module.apply_bootstrap_plan(Path(tmpdir), confirmation="anything", command_runner=runner)

        self._assert_refusal(outcome, category="missing_reviewed_plan")
        self.assertEqual(outcome["reviewed_plan"]["status"], "missing")
        self.assertEqual(runner.commands, [])

    def test_invalid_plan_schema_blocks_without_invoking_runner(self) -> None:
        apply_module = self._apply_module()
        runner = FakeApplyRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            self._write_ready_artifacts(project_dir, plan_overrides={"schema": "unexpected.schema.v1"})
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation="anything", command_runner=runner)

        self._assert_refusal(outcome, category="invalid_reviewed_plan")
        self.assertEqual(runner.commands, [])

    def test_non_ready_plan_blocks_without_invoking_runner(self) -> None:
        apply_module = self._apply_module()
        runner = FakeApplyRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            self._write_ready_artifacts(
                project_dir,
                plan_overrides={
                    "status": "incomplete",
                    "expected_deployment": None,
                    "findings": [
                        {
                            "code": "missing_input",
                            "severity": "error",
                            "field_path": "aws.auth.profile",
                            "message": "Bootstrap setup is missing aws.auth.profile.",
                        }
                    ],
                },
            )
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation="anything", command_runner=runner)

        self._assert_refusal(outcome, category="reviewed_plan_not_ready")
        self.assertEqual(outcome["reviewed_plan"]["status"], "incomplete")
        self.assertEqual(runner.commands, [])

    def test_placeholder_worker_image_digest_blocks_without_invoking_runner(self) -> None:
        apply_module = self._apply_module()
        runner = FakeApplyRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            plan_path = self._write_ready_artifacts(project_dir)
            (project_dir / ".sweetspot/infra/terraform.tfvars.json").write_text(
                json.dumps({"aws_profile": "sweetspot-dev", "aws_region": "us-west-2", "aws_role_arn": "", "worker_image_sha256": "replace-with-worker-image-digest"}) + "\n",
                encoding="utf-8",
            )
            token = apply_module.bootstrap_apply_confirmation_token(plan_path)
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation=token, command_runner=runner)

        self._assert_refusal(outcome, category="unresolved_apply_input")
        self.assertEqual(outcome["output_completeness"]["input_errors"][0]["field"], "worker_image_sha256")
        self.assertEqual(runner.commands, [])

    def test_generated_infra_artifact_drift_blocks_without_invoking_runner(self) -> None:
        apply_module = self._apply_module()
        runner = FakeApplyRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            plan_path = self._write_ready_artifacts(project_dir)
            (project_dir / ".sweetspot/infra/main.tf").write_text('resource "aws_sqs_queue" "queue" { name = "changed" }\n', encoding="utf-8")
            token = apply_module.bootstrap_apply_confirmation_token(plan_path)
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation=token, command_runner=runner)

        self._assert_refusal(outcome, category="generated_artifact_drift")
        self.assertEqual(outcome["output_completeness"]["drifted"][0]["path"], ".sweetspot/infra/main.tf")
        self.assertEqual(runner.commands, [])

    def test_missing_generated_infra_artifact_blocks_without_invoking_runner(self) -> None:
        apply_module = self._apply_module()
        runner = FakeApplyRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            self._write_ready_artifacts(project_dir, omit_artifact=".sweetspot/infra/main.tf")
            token = apply_module.bootstrap_apply_confirmation_token(project_dir / ".sweetspot" / "bootstrap-plan.json")
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation=token, command_runner=runner)

        self._assert_refusal(outcome, category="missing_generated_artifact")
        self.assertIn(".sweetspot/infra/main.tf", outcome["output_completeness"]["missing"])
        self.assertEqual(runner.commands, [])

    def test_blocking_plan_finding_blocks_without_invoking_runner(self) -> None:
        apply_module = self._apply_module()
        runner = FakeApplyRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            self._write_ready_artifacts(
                project_dir,
                plan_overrides={
                    "findings": [
                        {
                            "code": "policy_violation",
                            "severity": "error",
                            "field_path": "$.resource_inventory",
                            "message": "Refusing unsafe bootstrap finding with redacted token AKIA1234567890ABCDEF.",
                        }
                    ]
                },
            )
            token = apply_module.bootstrap_apply_confirmation_token(project_dir / ".sweetspot" / "bootstrap-plan.json")
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation=token, command_runner=runner)

        self._assert_refusal(outcome, category="blocking_plan_finding")
        self.assertEqual(runner.commands, [])

    def test_missing_confirmation_blocks_without_invoking_runner_and_reports_expected_token(self) -> None:
        apply_module = self._apply_module()
        runner = FakeApplyRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            plan_path = self._write_ready_artifacts(project_dir)
            expected = apply_module.bootstrap_apply_confirmation_token(plan_path)
            self.assertTrue(expected.startswith("apply:"), expected)
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation=None, command_runner=runner)

        self._assert_refusal(outcome, category="confirmation_missing")
        self.assertEqual(outcome["confirmation"]["status"], "missing")
        self.assertEqual(outcome["confirmation"]["expected"], expected)
        self.assertEqual(runner.commands, [])

    def test_mismatched_confirmation_blocks_without_invoking_runner(self) -> None:
        apply_module = self._apply_module()
        runner = FakeApplyRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            plan_path = self._write_ready_artifacts(project_dir)
            expected = apply_module.bootstrap_apply_confirmation_token(plan_path)
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation="apply:not-the-reviewed-plan", command_runner=runner)

        self._assert_refusal(outcome, category="confirmation_mismatched")
        self.assertEqual(outcome["confirmation"]["status"], "mismatched")
        self.assertEqual(outcome["confirmation"]["expected"], expected)
        self.assertEqual(runner.commands, [])


class ScriptedApplyRunner:
    def __init__(self, results: list[dict]) -> None:
        self.results = list(results)
        self.commands: list[list[str]] = []

    def __call__(self, command, *, cwd, timeout_seconds=30):
        self.commands.append(list(command))
        if not self.results:
            raise AssertionError(f"unexpected command: {command!r}")
        return self.results.pop(0)


class BootstrapApplyPersistenceTests(BootstrapApplyGuardContractTests):
    def _opentofu_outputs(self) -> dict:
        return {
            "batch_compute_environment": {"value": "example-batch-project-x86_64-compute"},
            "batch_job_queue": {"value": "example-batch-project-x86_64-job-queue"},
            "batch_job_definition": {"value": "example-batch-project-x86_64-job:1"},
            "dlq_url": {"value": "https://sqs.us-west-2.amazonaws.com/123456789012/example-dlq"},
            "ecr_repository_url": {"value": "123456789012.dkr.ecr.us-west-2.amazonaws.com/example"},
            "log_group": {"value": "/aws/batch/sweetspot/example"},
            "operator_role_arn": {"value": "arn:aws:iam::123456789012:role/example-bootstrap-operator-role"},
            "sqs_queue_url": {"value": "https://sqs.us-west-2.amazonaws.com/123456789012/example"},
            "worker_image_digest": {"value": "123456789012.dkr.ecr.us-west-2.amazonaws.com/example@sha256:" + "a" * 64},
            "worker_task_role_arn": {"value": "arn:aws:iam::123456789012:role/example-worker-task-role"},
        }

    def _deployment_output(self) -> dict:
        deployment = self._ready_plan()["expected_deployment"]
        deployment = json.loads(json.dumps(deployment))
        region = deployment["regions"]["us-west-2"]
        region["sqs_queue_url"] = "https://sqs.us-west-2.amazonaws.com/123456789012/example"
        region["dlq_url"] = "https://sqs.us-west-2.amazonaws.com/123456789012/example-dlq"
        region["architectures"]["x86_64"]["image"] = "123456789012.dkr.ecr.us-west-2.amazonaws.com/example@sha256:" + "a" * 64
        deployment["bootstrap_outputs"] = {key: value["value"] for key, value in self._opentofu_outputs().items()}
        return {"deployment": {"value": deployment}}

    def test_refusal_persists_state_and_failure_diagnostics(self) -> None:
        apply_module = self._apply_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation=None, command_runner=FakeApplyRunner())
            state = json.loads((project_dir / ".sweetspot/bootstrap/state.json").read_text(encoding="utf-8"))
            failure = json.loads((project_dir / ".sweetspot/bootstrap/failure.json").read_text(encoding="utf-8"))

        self.assertEqual(state, outcome)
        self.assertEqual(failure, outcome)
        self.assertEqual(state["status"], "blocked")
        self.assertEqual(state["category"], "missing_reviewed_plan")
        self.assertIn("recovery_hints", state)

    def test_successful_mocked_apply_writes_deployment_output_and_state(self) -> None:
        apply_module = self._apply_module()
        runner = ScriptedApplyRunner(
            [
                {"returncode": 0, "stdout": "init ok", "stderr": "", "executable": "tofu"},
                {"returncode": 0, "stdout": "apply ok", "stderr": "", "executable": "tofu"},
                {"returncode": 0, "stdout": json.dumps(self._opentofu_outputs()), "stderr": "", "executable": "tofu"},
            ]
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            plan_path = self._write_ready_artifacts(project_dir)
            stale_failure_path = project_dir / ".sweetspot/bootstrap/failure.json"
            stale_failure_path.parent.mkdir(parents=True, exist_ok=True)
            stale_failure_path.write_text('{"status":"failed","category":"missing_permission"}\n', encoding="utf-8")
            token = apply_module.bootstrap_apply_confirmation_token(plan_path)
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation=token, command_runner=runner)
            state = json.loads((project_dir / ".sweetspot/bootstrap/state.json").read_text(encoding="utf-8"))
            deployment_path = project_dir / ".sweetspot/deployment.json"
            deployment = json.loads(deployment_path.read_text(encoding="utf-8"))
            loaded = load_deployment(deployment_path)

        self.assertEqual(outcome["status"], "output_written")
        self.assertEqual(outcome["category"], "applied")
        self.assertEqual(outcome, state)
        self.assertFalse(stale_failure_path.exists())
        self.assertEqual(deployment["schema"], DEPLOYMENT_SCHEMA_V1)
        self.assertEqual(loaded, deployment)
        self.assertEqual(deployment["regions"]["us-west-2"]["sqs_queue_url"], "https://sqs.us-west-2.amazonaws.com/123456789012/example")
        self.assertEqual(deployment["regions"]["us-west-2"]["architectures"]["x86_64"]["batch_job_queue"], "example-batch-project-x86_64-job-queue")
        self.assertEqual(deployment["bootstrap_outputs"]["worker_task_role_arn"], "arn:aws:iam::123456789012:role/example-worker-task-role")
        self.assertEqual([cmd[0] for cmd in runner.commands], ["tofu", "tofu", "tofu"])
        self.assertEqual([cmd[1] for cmd in runner.commands], ["init", "apply", "output"])
        self.assertEqual([summary["command"] for summary in outcome["command_summaries"]], ["tofu init -backend=false", "tofu apply", "tofu output -json"])
        self.assertEqual(len(outcome["command_summaries"]), 3)
        self.assertTrue(outcome["output_completeness"]["complete"])
        self.assertTrue(outcome["output_completeness"]["deployment_output_written"])
        self.assertEqual(outcome["output_completeness"]["missing_outputs"], [])

    def test_custom_tofu_executable_is_used_for_apply_and_output(self) -> None:
        apply_module = self._apply_module()
        runner = ScriptedApplyRunner(
            [
                {"returncode": 0, "stdout": "init ok", "stderr": "", "executable": "/opt/bin/tofu"},
                {"returncode": 0, "stdout": "apply ok", "stderr": "", "executable": "/opt/bin/tofu"},
                {"returncode": 0, "stdout": json.dumps(self._opentofu_outputs()), "stderr": "", "executable": "/opt/bin/tofu"},
            ]
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            plan_path = self._write_ready_artifacts(project_dir)
            token = apply_module.bootstrap_apply_confirmation_token(plan_path)
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation=token, command_runner=runner, tofu_executable="/opt/bin/tofu")

        self.assertEqual(outcome["status"], "output_written")
        self.assertEqual([cmd[0] for cmd in runner.commands], ["/opt/bin/tofu", "/opt/bin/tofu", "/opt/bin/tofu"])
        self.assertEqual([summary["command"] for summary in outcome["command_summaries"]], ["/opt/bin/tofu init -backend=false", "/opt/bin/tofu apply", "/opt/bin/tofu output -json"])

    def test_incomplete_opentofu_outputs_fail_without_writing_deployment_output(self) -> None:
        apply_module = self._apply_module()
        outputs = self._opentofu_outputs()
        del outputs["worker_task_role_arn"]
        runner = ScriptedApplyRunner(
            [
                {"returncode": 0, "stdout": "init ok", "stderr": "", "executable": "tofu"},
                {"returncode": 0, "stdout": "apply ok", "stderr": "", "executable": "tofu"},
                {"returncode": 0, "stdout": json.dumps(outputs), "stderr": "", "executable": "tofu"},
            ]
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            plan_path = self._write_ready_artifacts(project_dir)
            token = apply_module.bootstrap_apply_confirmation_token(plan_path)
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation=token, command_runner=runner)
            failure = json.loads((project_dir / ".sweetspot/bootstrap/failure.json").read_text(encoding="utf-8"))

        self.assertEqual(outcome["status"], "failed")
        self.assertEqual(outcome["category"], "output_extraction_failed")
        self.assertEqual(outcome, failure)
        self.assertFalse((project_dir / ".sweetspot/deployment.json").exists())
        self.assertIn("worker_task_role_arn", outcome["message"])
        self.assertIn("worker_task_role_arn", outcome["output_completeness"]["missing_outputs"])

    def test_apply_started_state_does_not_claim_deployment_output_before_extraction_succeeds(self) -> None:
        apply_module = self._apply_module()

        def inspecting_runner(command, *, cwd, timeout_seconds=30):
            project_dir = Path(cwd).parents[1]
            if command[1] == "apply":
                state = json.loads((project_dir / ".sweetspot/bootstrap/state.json").read_text(encoding="utf-8"))
                self.assertEqual(state["status"], "applying")
                self.assertFalse(state["output_completeness"]["deployment_output_written"])
                self.assertFalse((project_dir / ".sweetspot/deployment.json").exists())
                return {"returncode": 0, "stdout": "apply ok", "stderr": "", "executable": "opentofu"}
            return {"returncode": 0, "stdout": json.dumps(self._opentofu_outputs()), "stderr": "", "executable": "opentofu"}

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            plan_path = self._write_ready_artifacts(project_dir)
            token = apply_module.bootstrap_apply_confirmation_token(plan_path)
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation=token, command_runner=inspecting_runner)

        self.assertEqual(outcome["status"], "output_written")

    def test_apply_permission_failure_persists_sanitized_failure_without_outputs(self) -> None:
        apply_module = self._apply_module()
        secret = "AKIA1234567890ABCDEF"
        raw_arn = "arn:aws:iam::123456789012:role/AdminSecretRole"
        raw_request = "request id req-1234567890abcdef"
        runner = ScriptedApplyRunner(
            [
                {"returncode": 0, "stdout": "init ok", "stderr": "", "executable": "tofu"},
                {
                    "returncode": 1,
                    "stdout": "",
                    "stderr": f"AccessDenied for token {secret} and aws_secret_access_key=abcd on {raw_arn} with {raw_request} profile 'prod-admin'",
                    "executable": "tofu",
                },
            ]
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            plan_path = self._write_ready_artifacts(project_dir)
            token = apply_module.bootstrap_apply_confirmation_token(plan_path)
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation=token, command_runner=runner)
            failure_text = (project_dir / ".sweetspot/bootstrap/failure.json").read_text(encoding="utf-8")

        self.assertEqual(outcome["status"], "failed")
        self.assertEqual(outcome["category"], "missing_permission")
        self.assertFalse((project_dir / ".sweetspot/deployment.json").exists())
        rendered = json.dumps(outcome, sort_keys=True)
        self.assertNotIn(secret, rendered)
        self.assertNotIn(secret, failure_text)
        for raw in (raw_arn, "123456789012", "AdminSecretRole", "req-1234567890abcdef", "prod-admin"):
            self.assertNotIn(raw, rendered)
            self.assertNotIn(raw, failure_text)
        self.assertIn("[REDACTED]", failure_text)
        self.assertIn("[REDACTED_ARN]", failure_text)
        self.assertIn("[REDACTED_AWS_REQUEST]", failure_text)

    def test_output_extraction_failure_persists_failure_after_apply(self) -> None:
        apply_module = self._apply_module()
        runner = ScriptedApplyRunner(
            [
                {"returncode": 0, "stdout": "init ok", "stderr": "", "executable": "tofu"},
                {"returncode": 0, "stdout": "apply ok", "stderr": "", "executable": "tofu"},
                {"returncode": 0, "stdout": "not-json", "stderr": "", "executable": "tofu"},
            ]
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            plan_path = self._write_ready_artifacts(project_dir)
            token = apply_module.bootstrap_apply_confirmation_token(plan_path)
            outcome = apply_module.apply_bootstrap_plan(project_dir, confirmation=token, command_runner=runner)
            failure = json.loads((project_dir / ".sweetspot/bootstrap/failure.json").read_text(encoding="utf-8"))

        self.assertEqual(outcome["status"], "failed")
        self.assertEqual(outcome["category"], "output_extraction_failed")
        self.assertEqual(outcome, failure)
        self.assertEqual([cmd[1] for cmd in runner.commands], ["init", "apply", "output"])


if __name__ == "__main__":
    unittest.main()
