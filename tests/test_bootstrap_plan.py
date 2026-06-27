from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sweetspot.bootstrap_plan import BOOTSTRAP_PLAN_SCHEMA_V1, render_bootstrap_plan, render_opentofu_bootstrap_plan
from sweetspot.setup import SWEETSPOT_CONFIG_PATH, load_setup, scan_for_secrets, setup_to_dict, write_project_context


class FakeTofuRunner:
    def __init__(self, responses):
        self.responses = list(responses)
        self.commands = []

    def __call__(self, command, *, cwd, timeout_seconds=30):
        self.commands.append(list(command))
        if any(part == "apply" for part in command):
            raise AssertionError("OpenTofu apply must never be executed")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


ROOT = Path(__file__).resolve().parents[1]


class BootstrapPlanContractTests(unittest.TestCase):
    def test_ready_plan_renders_resource_inventory_and_expected_deployment(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)

            plan = render_bootstrap_plan(project_dir)

        self.assertEqual(plan["schema"], BOOTSTRAP_PLAN_SCHEMA_V1)
        self.assertEqual(plan["status"], "ready")
        self.assertEqual(plan["command_attempts"], [])
        self.assertEqual(plan["stderr_summary"], [])
        self.assertEqual(plan["intent"]["schema"], "sweetspot.bootstrap.intent.v1")
        self.assertEqual(plan["intent"]["auth"], {"method": "profile", "reference": "sweetspot-dev"})
        groups = {group["group"]: group["resources"] for group in plan["resource_inventory"]}
        self.assertEqual(set(groups), {"iam", "batch", "sqs", "ecr", "s3", "logs", "outputs"})
        for group in ("iam", "batch", "sqs", "ecr", "s3", "logs", "outputs"):
            self.assertTrue(groups[group], group)
        all_resource_names = {resource["name"] for resources in groups.values() for resource in resources if "name" in resource}
        self.assertIn("example-batch-project-bootstrap-operator-role", all_resource_names)
        self.assertIn("example-batch-project-worker-task-role", all_resource_names)
        self.assertIn("example-batch-project-x86_64-compute", all_resource_names)
        self.assertIn("example-batch-project-worker", all_resource_names)
        self.assertIn("example-batch-project-work-queue", all_resource_names)
        self.assertIn("example-batch-project-work-dlq", all_resource_names)
        self.assertIn("/aws/batch/sweetspot/example-batch-project", all_resource_names)
        self.assertEqual(plan["expected_deployment"]["schema"], "sweetspot.deployment.v1")
        region = plan["expected_deployment"]["regions"]["us-west-2"]
        self.assertEqual(region["sqs_queue_url"], "${output.sqs_queue_url}")
        self.assertIn("x86_64", region["architectures"])
        self.assertEqual(region["architectures"]["x86_64"]["batch_job_queue"], "example-batch-project-x86_64-job-queue")
        self.assertTrue(any(artifact["status"] == "planned" for artifact in plan["generated_artifacts"]))
        self.assertEqual(scan_for_secrets(plan), ())

    def test_missing_setup_is_invalid_plan_with_reviewable_findings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            plan = render_bootstrap_plan(Path(tmpdir))

        self.assertEqual(plan["status"], "invalid")
        self.assertEqual(plan["resource_inventory"], [])
        self.assertIsNone(plan["expected_deployment"])
        self.assertTrue(any(finding["code"] == "missing_setup_config" for finding in plan["findings"]))
        self.assertTrue(all(artifact["status"] == "blocked" for artifact in plan["generated_artifacts"]))
        self.assertTrue(any("Fix the listed bootstrap intent findings" in action for action in plan["next_actions"]))

    def test_missing_auth_reference_is_incomplete_plan_not_exception(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["aws"]["auth"].pop("profile")
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            setup_path = project_dir / SWEETSPOT_CONFIG_PATH
            setup_path.parent.mkdir(parents=True)
            setup_path.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n", encoding="utf-8")

            plan = render_bootstrap_plan(project_dir)

        self.assertEqual(plan["status"], "incomplete")
        self.assertIn("aws.auth.profile", plan["intent"]["missing_inputs"])
        self.assertTrue(any(finding["field_path"] == "aws.auth.profile" for finding in plan["findings"]))
        self.assertEqual(plan["resource_inventory"], [])
        self.assertIsNone(plan["expected_deployment"])

    def test_invalid_secret_like_setup_value_is_redacted_from_plan(self) -> None:
        secret_text = "AKIA1234567890ABCDEF"
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["workload"]["command"] = ["python", "worker.py", secret_text]
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            setup_path = project_dir / SWEETSPOT_CONFIG_PATH
            setup_path.parent.mkdir(parents=True)
            setup_path.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n", encoding="utf-8")

            plan = render_bootstrap_plan(project_dir)

        serialized = json.dumps(plan, sort_keys=True)
        self.assertEqual(plan["status"], "invalid")
        self.assertIn("secret_value_aws_access_key_id", serialized)
        self.assertNotIn(secret_text, serialized)
        self.assertEqual(scan_for_secrets(plan), ())

    def test_deterministic_names_are_stable_across_renders(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)

            first = render_bootstrap_plan(project_dir)
            second = render_bootstrap_plan(project_dir)

        self.assertEqual(first, second)
        outputs = {resource["name"]: resource["value"] for group in first["resource_inventory"] if group["group"] == "outputs" for resource in group["resources"]}
        self.assertEqual(outputs["batch_job_queue"], "example-batch-project-x86_64-job-queue")
        self.assertEqual(outputs["operator_role_arn"], "${aws_iam_role.operator_role.arn}")
        self.assertIn("${var.worker_image_sha256}", outputs["worker_image_digest"])

    def test_renderer_is_pure_and_does_not_execute_aws_or_opentofu(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)
            with mock.patch("subprocess.run", side_effect=AssertionError("subprocess should not run")), mock.patch("subprocess.Popen", side_effect=AssertionError("subprocess should not run")):
                before_paths = sorted(path.relative_to(project_dir).as_posix() for path in project_dir.rglob("*"))
                plan = render_bootstrap_plan(project_dir)
                after_paths = sorted(path.relative_to(project_dir).as_posix() for path in project_dir.rglob("*"))

        self.assertEqual(plan["status"], "ready")
        self.assertEqual(before_paths, after_paths)
        self.assertEqual(plan["command_attempts"], [])

    def test_opentofu_renderer_writes_deterministic_review_files(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)

            first = render_opentofu_bootstrap_plan(project_dir)
            rendered = {path.relative_to(project_dir).as_posix(): path.read_text(encoding="utf-8") for path in sorted((project_dir / ".sweetspot" / "infra").iterdir())}
            second = render_opentofu_bootstrap_plan(project_dir)
            rendered_again = {path.relative_to(project_dir).as_posix(): path.read_text(encoding="utf-8") for path in sorted((project_dir / ".sweetspot" / "infra").iterdir())}
            plan_artifact = json.loads((project_dir / ".sweetspot" / "bootstrap-plan.json").read_text(encoding="utf-8"))

        self.assertEqual(first["status"], "ready")
        self.assertEqual(second["status"], "ready")
        self.assertEqual(rendered, rendered_again)
        self.assertEqual(plan_artifact["schema"], BOOTSTRAP_PLAN_SCHEMA_V1)
        self.assertRegex(plan_artifact["confirmation_token"], r"^apply:[0-9a-f]{16}$")
        self.assertEqual(first["confirmation_token"], plan_artifact["confirmation_token"])
        artifact_digests = [
            artifact.get("sha256") for artifact in plan_artifact["generated_artifacts"] if artifact.get("path", "").startswith(".sweetspot/infra/") and artifact.get("path") != ".sweetspot/infra/terraform.tfvars.json"
        ]
        self.assertTrue(artifact_digests)
        self.assertTrue(all(isinstance(digest, str) and len(digest) == 64 and digest != "[redacted]" for digest in artifact_digests))
        self.assertEqual(
            {p for p in rendered},
            {
                ".sweetspot/infra/main.tf",
                ".sweetspot/infra/outputs.tf",
                ".sweetspot/infra/terraform.tfvars.json",
                ".sweetspot/infra/variables.tf",
                ".sweetspot/infra/versions.tf",
            },
        )
        self.assertIn('resource "aws_batch_job_queue" "job_queue"', rendered[".sweetspot/infra/main.tf"])
        self.assertIn('resource "aws_iam_instance_profile" "ecs_instance_profile"', rendered[".sweetspot/infra/main.tf"])
        self.assertIn('type                = "EC2"', rendered[".sweetspot/infra/main.tf"])
        self.assertNotIn('type                = "SPOT"', rendered[".sweetspot/infra/main.tf"])
        self.assertIn("subnets             = data.aws_subnets.default.ids", rendered[".sweetspot/infra/main.tf"])
        self.assertNotIn("subnets            = []", rendered[".sweetspot/infra/main.tf"])
        self.assertIn("profile = var.aws_profile", rendered[".sweetspot/infra/versions.tf"])
        self.assertIn('"aws_region": "us-west-2"', rendered[".sweetspot/infra/terraform.tfvars.json"])
        self.assertIn('"aws_profile": "sweetspot-dev"', rendered[".sweetspot/infra/terraform.tfvars.json"])
        self.assertIn('"output_prefix": "runs/example"', rendered[".sweetspot/infra/terraform.tfvars.json"])
        self.assertTrue(all(artifact["status"] == "rendered" for artifact in first["generated_artifacts"] if artifact["name"].startswith("opentofu_")))

    def test_opentofu_validation_success_records_command_attempts(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        runner = FakeTofuRunner(
            [
                {"returncode": 0, "stdout": "OpenTofu v1.8.0\n", "stderr": "", "executable": "/usr/bin/tofu"},
                {"returncode": 0, "stdout": "OpenTofu has been successfully initialized!\n", "stderr": "", "executable": "/usr/bin/tofu"},
                {"returncode": 0, "stdout": "Success! The configuration is valid.\n", "stderr": "", "executable": "/usr/bin/tofu"},
            ]
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)

            plan = render_opentofu_bootstrap_plan(project_dir, validate=True, command_runner=runner)

        self.assertEqual(plan["opentofu"]["status"], "validation_passed")
        self.assertEqual(plan["opentofu"]["executable"], "/usr/bin/tofu")
        self.assertEqual(len(plan["command_attempts"]), 3)
        self.assertTrue(any("init" in attempt["command"] for attempt in plan["command_attempts"]))
        self.assertTrue(any("validate" in attempt["command"] for attempt in plan["command_attempts"]))
        self.assertFalse(any("apply" in " ".join(command) for command in runner.commands))

    def test_opentofu_missing_executable_is_structured_finding(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        runner = FakeTofuRunner([FileNotFoundError("tofu not found")])
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)

            plan = render_opentofu_bootstrap_plan(project_dir, validate=True, command_runner=runner)

        self.assertEqual(plan["opentofu"]["status"], "tofu_unavailable")
        self.assertTrue(any(finding["code"] == "tofu_unavailable" for finding in plan["findings"]))
        self.assertEqual(plan["command_attempts"][0]["status"], "tofu_unavailable")

    def test_opentofu_validation_failure_sanitizes_stderr(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        runner = FakeTofuRunner(
            [
                {"returncode": 0, "stdout": "OpenTofu v1.8.0\n", "stderr": "", "executable": "/usr/bin/tofu"},
                {"returncode": 0, "stdout": "OpenTofu has been successfully initialized!\n", "stderr": "", "executable": "/usr/bin/tofu"},
                {"returncode": 1, "stdout": "", "stderr": "bad token AKIA1234567890ABCDEF and aws_secret_access_key abc\n", "executable": "/usr/bin/tofu"},
            ]
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)

            plan = render_opentofu_bootstrap_plan(project_dir, validate=True, command_runner=runner)

        serialized = json.dumps(plan, sort_keys=True)
        self.assertEqual(plan["opentofu"]["status"], "validation_failed")
        self.assertIn("[redacted]", serialized)
        self.assertNotIn("AKIA1234567890ABCDEF", serialized)
        self.assertNotIn("aws_secret_access_key", serialized)
        self.assertTrue(any(finding["code"] == "opentofu_validation_failed" for finding in plan["findings"]))
        self.assertEqual(scan_for_secrets(plan), ())

    def test_opentofu_init_failure_blocks_validation_and_sanitizes_stderr(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        runner = FakeTofuRunner(
            [
                {"returncode": 0, "stdout": "OpenTofu v1.8.0\n", "stderr": "", "executable": "/usr/bin/tofu"},
                {"returncode": 1, "stdout": "", "stderr": "init failed with AKIA1234567890ABCDEF\n", "executable": "/usr/bin/tofu"},
            ]
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)

            plan = render_opentofu_bootstrap_plan(project_dir, validate=True, command_runner=runner)

        serialized = json.dumps(plan, sort_keys=True)
        self.assertEqual(plan["opentofu"]["status"], "validation_failed")
        self.assertTrue(any(finding["code"] == "opentofu_init_failed" for finding in plan["findings"]))
        self.assertFalse(any("validate" in command for command in runner.commands))
        self.assertNotIn("AKIA1234567890ABCDEF", serialized)
        self.assertEqual(scan_for_secrets(plan), ())

    def test_opentofu_renderer_does_not_validate_or_apply_by_default(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        runner = FakeTofuRunner([])
        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)

            plan = render_opentofu_bootstrap_plan(project_dir, command_runner=runner)

        self.assertEqual(plan["opentofu"]["status"], "not_requested")
        self.assertEqual(plan["command_attempts"], [])
        self.assertEqual(runner.commands, [])


if __name__ == "__main__":
    unittest.main()
