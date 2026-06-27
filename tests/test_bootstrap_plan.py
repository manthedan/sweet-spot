from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sweetspot.bootstrap_plan import BOOTSTRAP_PLAN_SCHEMA_V1, render_bootstrap_plan
from sweetspot.setup import SWEETSPOT_CONFIG_PATH, load_setup, scan_for_secrets, setup_to_dict, write_project_context


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
            with mock.patch("subprocess.run", side_effect=AssertionError("subprocess should not run")), mock.patch(
                "subprocess.Popen", side_effect=AssertionError("subprocess should not run")
            ):
                before_paths = sorted(path.relative_to(project_dir).as_posix() for path in project_dir.rglob("*"))
                plan = render_bootstrap_plan(project_dir)
                after_paths = sorted(path.relative_to(project_dir).as_posix() for path in project_dir.rglob("*"))

        self.assertEqual(plan["status"], "ready")
        self.assertEqual(before_paths, after_paths)
        self.assertEqual(plan["command_attempts"], [])


if __name__ == "__main__":
    unittest.main()
