from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from sweetspot.setup import (
    ARCHITECTURES,
    DEPLOYMENT_TEMPLATE_PATH,
    INFRA_VARS_STUB_PATH,
    JOB_SPEC_PATH,
    LAYOUT_FILES,
    NEXT_STEPS_PATH,
    SETUP_SCHEMA_V1,
    SWEETSPOT_CONFIG_PATH,
    SWEETSPOT_DOC_PATH,
    WORKER_NOTES_PATH,
    WORKER_SCAFFOLD_PATH,
    SetupSpecError,
    dump_setup,
    load_setup,
    render_sweetspot_doc,
    scan_for_secrets,
    setup_to_dict,
    validate_setup,
    write_project_context,
)


ROOT = Path(__file__).resolve().parents[1]


class SetupModelTests(unittest.TestCase):
    def test_example_setup_loads_as_project_model(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")

        self.assertEqual(scan_for_secrets(setup_to_dict(config)), ())
        self.assertEqual(config.schema, SETUP_SCHEMA_V1)
        self.assertEqual(config.project.name, "example-batch-project")
        self.assertEqual(config.workload.input_manifest, "s3://example-sweetspot-input/manifests/tasks.jsonl")
        self.assertEqual(config.workload.output_prefix, "s3://example-sweetspot-output/runs/example/")
        self.assertEqual(config.workload.command, ("python", "worker.py", "--task", "{task_json}"))
        self.assertEqual(config.workload.architecture, "x86_64")
        self.assertEqual(config.aws.region, "us-west-2")
        self.assertEqual(config.aws.method, "profile")
        self.assertEqual(config.aws.profile, "sweetspot-dev")
        self.assertEqual(config.bootstrap.job, JOB_SPEC_PATH)

    def test_dump_round_trips_through_safe_setup_model(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        dumped = dump_setup(config)

        self.assertNotIn("!!python", dumped)
        self.assertEqual(validate_setup(setup_to_dict(config)), validate_setup(setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))))

    def test_layout_constants_match_roadmap_names(self) -> None:
        self.assertEqual(LAYOUT_FILES["config"], SWEETSPOT_CONFIG_PATH)
        self.assertEqual(LAYOUT_FILES["doc"], SWEETSPOT_DOC_PATH)
        self.assertEqual(LAYOUT_FILES["job"], JOB_SPEC_PATH)
        self.assertEqual(LAYOUT_FILES["deployment_template"], DEPLOYMENT_TEMPLATE_PATH)
        self.assertEqual(LAYOUT_FILES["worker_notes"], WORKER_NOTES_PATH)
        self.assertEqual(LAYOUT_FILES["worker_scaffold"], WORKER_SCAFFOLD_PATH)
        self.assertEqual(LAYOUT_FILES["infra_vars_stub"], INFRA_VARS_STUB_PATH)
        self.assertEqual(LAYOUT_FILES["next_steps"], NEXT_STEPS_PATH)
        self.assertTrue(all(path.startswith(".sweetspot/") for path in LAYOUT_FILES.values()))

    def test_valid_architectures_are_explicit(self) -> None:
        self.assertEqual(ARCHITECTURES, {"x86_64", "arm64"})

    def test_missing_required_field_reports_field_path(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        del data["workload"]["input_manifest"]

        with self.assertRaisesRegex(SetupSpecError, r"workload\.input_manifest") as ctx:
            validate_setup(data)

        self.assertEqual(ctx.exception.field_path, "workload.input_manifest")

    def test_invalid_schema_reports_schema_path(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["schema"] = "sweetspot.project.v0"

        with self.assertRaisesRegex(SetupSpecError, r"schema: must be 'sweetspot.project.v1'"):
            validate_setup(data)

    def test_invalid_workload_s3_reference_reports_field_path(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["workload"]["input_manifest"] = "local/tasks.jsonl"

        with self.assertRaisesRegex(SetupSpecError, r"workload\.input_manifest: must be an s3://bucket/key URI"):
            validate_setup(data)

    def test_empty_command_token_reports_indexed_field_path(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["workload"]["command"] = ["python", ""]

        with self.assertRaisesRegex(SetupSpecError, r"workload\.command\[1\]"):
            validate_setup(data)

    def test_invalid_architecture_reports_allowed_values(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["workload"]["architecture"] = "gpu"

        with self.assertRaisesRegex(SetupSpecError, r"workload\.architecture"):
            validate_setup(data)

    def test_invalid_auth_method_reports_field_path(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["aws"]["auth"]["method"] = "access_key"

        with self.assertRaisesRegex(SetupSpecError, r"aws\.auth\.method"):
            validate_setup(data)

    def test_profile_auth_requires_reference_not_credentials(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["aws"]["auth"]["aws_secret_access_key"] = "not-allowed"

        with self.assertRaisesRegex(SetupSpecError, r"aws\.auth\.aws_secret_access_key"):
            validate_setup(data)

    def test_secret_scanner_reports_nested_key_names_with_sanitized_findings(self) -> None:
        secret_text = "do-not-echo-this-password"
        findings = scan_for_secrets({"bootstrap": {"notes": [{"session_token": secret_text}]}})

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].path, "bootstrap.notes[0].session_token")
        self.assertEqual(findings[0].code, "secret_key_name")
        self.assertEqual(findings[0].severity, "error")
        self.assertIn("auth intent", findings[0].message)
        self.assertNotIn(secret_text, str(findings[0]))
        self.assertNotIn(secret_text, findings[0].message)

    def test_secret_scanner_reports_aws_access_key_values_without_echoing_value(self) -> None:
        secret_text = "AKIA1234567890ABCDEF"
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["workload"]["command"] = ["python", "worker.py", secret_text]

        findings = scan_for_secrets(data)

        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].path, "workload.command[2]")
        self.assertEqual(findings[0].code, "secret_value_aws_access_key_id")
        self.assertNotIn(secret_text, findings[0].message)
        with self.assertRaisesRegex(SetupSpecError, r"workload\.command\[2\]: secret_value_aws_access_key_id") as ctx:
            validate_setup(data)
        self.assertNotIn(secret_text, str(ctx.exception))

    def test_secret_scanner_reports_bearer_token_and_private_key_markers(self) -> None:
        bearer_text = "Bearer abcdefghijklmnopqrstuvwxyz123456"
        private_key_text = "-----BEGIN PRIVATE KEY-----\nnot-a-real-key\n-----END PRIVATE KEY-----"
        findings = scan_for_secrets({"items": [bearer_text, {"safe_name": private_key_text}]})

        self.assertEqual([finding.path for finding in findings], ["items[0]", "items[1].safe_name"])
        self.assertEqual([finding.code for finding in findings], ["secret_value_bearer_token", "secret_value_private_key"])
        self.assertTrue(all(finding.severity == "error" for finding in findings))
        self.assertTrue(all(bearer_text not in finding.message for finding in findings))
        self.assertTrue(all(private_key_text not in finding.message for finding in findings))

    def test_validation_rejects_nested_secret_bearing_setup_dict(self) -> None:
        secret_text = "ASIA1234567890ABCDEF"
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["bootstrap"]["worker_env"] = {"AWS_ACCESS_KEY_ID": secret_text}

        with self.assertRaisesRegex(SetupSpecError, r"bootstrap\.worker_env\.AWS_ACCESS_KEY_ID: secret_key_name") as ctx:
            validate_setup(data)

        self.assertEqual(ctx.exception.field_path, "bootstrap.worker_env.AWS_ACCESS_KEY_ID")
        self.assertNotIn(secret_text, str(ctx.exception))

    def test_unresolved_bootstrap_placeholder_is_rejected(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["bootstrap"]["next_steps"] = ".sweetspot/<project>/next_steps.md"

        with self.assertRaisesRegex(SetupSpecError, r"bootstrap\.next_steps"):
            validate_setup(data)

    def test_loader_uses_safe_yaml_and_rejects_object_constructors(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "unsafe.yaml"
            path.write_text("!!python/object/apply:os.system ['echo unsafe']\n", encoding="utf-8")

            with self.assertRaises(Exception):
                load_setup(path)

    def test_write_project_context_round_trips_and_renders_sweetspot_doc_only(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            written = write_project_context(config, project_dir)

            self.assertEqual(
                written,
                [project_dir / SWEETSPOT_CONFIG_PATH, project_dir / SWEETSPOT_DOC_PATH],
            )
            self.assertEqual(load_setup(project_dir / SWEETSPOT_CONFIG_PATH), config)
            self.assertFalse((project_dir / JOB_SPEC_PATH).exists())
            self.assertFalse((project_dir / DEPLOYMENT_TEMPLATE_PATH).exists())
            self.assertFalse((project_dir / WORKER_NOTES_PATH).exists())
            self.assertFalse((project_dir / WORKER_SCAFFOLD_PATH).exists())
            self.assertFalse((project_dir / INFRA_VARS_STUB_PATH).exists())
            self.assertFalse((project_dir / NEXT_STEPS_PATH).exists())

            doc = (project_dir / SWEETSPOT_DOC_PATH).read_text(encoding="utf-8")

        self.assertEqual(doc, render_sweetspot_doc(config))
        self.assertIn("example-batch-project", doc)
        self.assertIn("s3://example-sweetspot-input/manifests/tasks.jsonl", doc)
        self.assertIn("s3://example-sweetspot-output/runs/example/", doc)
        self.assertIn("python worker.py --task {task_json}", doc)
        self.assertIn("x86_64", doc)
        self.assertIn("us-west-2", doc)
        self.assertIn("profile", doc)
        self.assertIn("sweetspot-dev", doc)
        self.assertIn(JOB_SPEC_PATH, doc)
        self.assertIn(DEPLOYMENT_TEMPLATE_PATH, doc)
        self.assertIn(WORKER_NOTES_PATH, doc)
        self.assertIn(WORKER_SCAFFOLD_PATH, doc)
        self.assertIn(INFRA_VARS_STUB_PATH, doc)
        self.assertIn(NEXT_STEPS_PATH, doc)
        self.assertIn("No AWS resources have been created", doc)
        self.assertIn("No secrets or AWS credential values are stored", doc)

    def test_write_project_context_conflicts_fail_closed_unless_overwrite_true(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)
            original_config_text = (project_dir / SWEETSPOT_CONFIG_PATH).read_text(encoding="utf-8")
            original_doc_text = (project_dir / SWEETSPOT_DOC_PATH).read_text(encoding="utf-8")

            with self.assertRaisesRegex(FileExistsError, r"\.sweetspot/sweetspot\.yaml") as ctx:
                write_project_context(config, project_dir)

            self.assertIn(".sweetspot/SWEETSPOT.md", str(ctx.exception))
            self.assertEqual((project_dir / SWEETSPOT_CONFIG_PATH).read_text(encoding="utf-8"), original_config_text)
            self.assertEqual((project_dir / SWEETSPOT_DOC_PATH).read_text(encoding="utf-8"), original_doc_text)

            written = write_project_context(config, project_dir, overwrite=True)

        self.assertEqual([path.relative_to(project_dir).as_posix() for path in written], [SWEETSPOT_CONFIG_PATH, SWEETSPOT_DOC_PATH])

    def test_write_project_context_rejects_secret_bearing_config_without_writes(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["aws"]["auth"]["session_token"] = "not-allowed"

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            with self.assertRaisesRegex(SetupSpecError, r"aws\.auth\.session_token: secret_key_name"):
                write_project_context(data, project_dir)

            self.assertFalse((project_dir / SWEETSPOT_CONFIG_PATH).exists())
            self.assertFalse((project_dir / SWEETSPOT_DOC_PATH).exists())


if __name__ == "__main__":
    unittest.main()
