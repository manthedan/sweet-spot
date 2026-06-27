from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from sweetspot.planner import FORBIDDEN_PRIMARY_JOB_SPEC_KEYS, load_job_spec
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
    bootstrap_intent_from_setup,
    bootstrap_intent_to_dict,
    load_bootstrap_intent,
    doctor_project,
    dump_setup,
    load_setup,
    render_bootstrap_intent,
    render_bootstrap_status,
    render_deployment_template,
    render_infra_vars_stub,
    render_next_steps,
    render_starter_job_spec,
    render_sweetspot_doc,
    render_worker_notes,
    render_worker_scaffold,
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

    def test_explicit_null_optional_strings_report_field_path(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["project"]["description"] = None

        with self.assertRaisesRegex(SetupSpecError, r"project\.description: must be a non-empty string"):
            validate_setup(data)

        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["bootstrap"]["job"] = None

        with self.assertRaisesRegex(SetupSpecError, r"bootstrap\.job: must be a non-empty string"):
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

    def test_secret_scanner_reports_aws_secret_access_key_values_without_echoing_value(self) -> None:
        for secret_text in (
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN",
        ):
            with self.subTest(secret_text=secret_text):
                data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
                data["aws"]["auth"]["profile"] = secret_text

                findings = scan_for_secrets(data)

                self.assertEqual(len(findings), 1)
                self.assertEqual(findings[0].path, "aws.auth.profile")
                self.assertEqual(findings[0].code, "secret_value_aws_secret_access_key")
                self.assertNotIn(secret_text, findings[0].message)
                with self.assertRaisesRegex(SetupSpecError, r"aws\.auth\.profile: secret_value_aws_secret_access_key") as ctx:
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

    def test_bootstrap_intent_derives_sanitized_local_inputs_without_external_dependencies(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")

        intent = bootstrap_intent_from_setup(config)
        report = bootstrap_intent_to_dict(intent)
        rendered = json.loads(render_bootstrap_intent(config))

        self.assertEqual(report, rendered)
        self.assertEqual(report["schema"], "sweetspot.bootstrap.intent.v1")
        self.assertEqual(report["status"], "ready")
        self.assertEqual(report["project_name"], "example-batch-project")
        self.assertEqual(report["region"], "us-west-2")
        self.assertEqual(report["auth"], {"method": "profile", "reference": "sweetspot-dev"})
        self.assertEqual(report["backend"], "opentofu-local-template")
        self.assertEqual(report["missing_inputs"], [])
        self.assertEqual(report["errors"], [])
        self.assertEqual(
            report["resource_names"],
            {
                "project_slug": "example-batch-project",
                "job_definition": "example-batch-project-x86_64-job-definition",
                "job_queue": "example-batch-project-x86_64-job-queue",
                "container_image": "example-batch-project-x86_64-worker",
                "input_bucket": "example-sweetspot-input",
                "output_bucket": "example-sweetspot-output",
                "output_prefix": "runs/example",
            },
        )
        self.assertEqual(scan_for_secrets(report), ())
        self.assertNotIn("AWS_ACCESS_KEY_ID", json.dumps(report))
        self.assertNotIn("opentofu", {error["code"] for error in report["errors"]})

    def test_load_bootstrap_intent_reports_missing_setup_without_throwing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            missing_path = Path(tmpdir) / SWEETSPOT_CONFIG_PATH
            intent = load_bootstrap_intent(missing_path)

        report = bootstrap_intent_to_dict(intent)
        self.assertEqual(report["status"], "invalid")
        self.assertIsNone(report["resource_names"])
        self.assertIn("aws.region", report["missing_inputs"])
        self.assertEqual(report["errors"], [{"field_path": SWEETSPOT_CONFIG_PATH, "code": "missing_setup_config", "message": "setup config is missing"}])

    def test_bootstrap_intent_reports_invalid_setup_without_secret_leakage(self) -> None:
        secret_text = "AKIA1234567890ABCDEF"
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["workload"]["command"] = ["python", "worker.py", secret_text]

        report = bootstrap_intent_to_dict(bootstrap_intent_from_setup(data))

        self.assertEqual(report["status"], "invalid")
        self.assertIsNone(report["resource_names"])
        self.assertIn("workload.command[2]", report["errors"][0]["field_path"])
        self.assertEqual(report["errors"][0]["code"], "secret_value_aws_access_key_id")
        self.assertNotIn(secret_text, json.dumps(report))

    def test_bootstrap_intent_reports_missing_inputs_with_field_paths_and_hints(self) -> None:
        report = bootstrap_intent_to_dict(bootstrap_intent_from_setup({"schema": SETUP_SCHEMA_V1, "project": {}, "workload": {}, "aws": {"auth": {"method": "profile"}}}))

        self.assertEqual(report["status"], "invalid")
        self.assertIsNone(report["resource_names"])
        self.assertEqual(report["auth"], {"method": None, "reference": None})
        errors = {error["field_path"]: error for error in report["errors"]}
        self.assertEqual(
            sorted(errors),
            [
                "aws.auth.profile",
                "aws.region",
                "project.name",
                "workload.architecture",
                "workload.input_manifest",
                "workload.output_prefix",
            ],
        )
        self.assertTrue(all(error["code"] == "missing_bootstrap_input" for error in errors.values()))
        self.assertTrue(all(".sweetspot/sweetspot.yaml" in error["message"] or "aws.auth.method" not in error["field_path"] for error in errors.values()))
        self.assertIn("aws.auth.profile", report["missing_inputs"])
        self.assertIn("workload.input_manifest", report["missing_inputs"])

    def test_load_bootstrap_intent_scans_secrets_before_schema_validation(self) -> None:
        secret_text = "AKIA1234567890ABCDEF"
        with tempfile.TemporaryDirectory() as tmpdir:
            setup_path = Path(tmpdir) / SWEETSPOT_CONFIG_PATH
            setup_path.parent.mkdir(parents=True)
            setup_path.write_text(f"schema: wrong\nworkload:\n  command: ['python', '{secret_text}']\n", encoding="utf-8")

            report = bootstrap_intent_to_dict(load_bootstrap_intent(setup_path))

        self.assertEqual(report["status"], "invalid")
        self.assertEqual(report["errors"][0]["field_path"], "workload.command[1]")
        self.assertEqual(report["errors"][0]["code"], "secret_value_aws_access_key_id")
        self.assertNotIn(secret_text, json.dumps(report))

    def test_load_bootstrap_intent_reports_yaml_error_without_throwing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            setup_path = Path(tmpdir) / SWEETSPOT_CONFIG_PATH
            setup_path.parent.mkdir(parents=True)
            setup_path.write_text("schema: [unterminated\n", encoding="utf-8")

            report = bootstrap_intent_to_dict(load_bootstrap_intent(setup_path))

        self.assertEqual(report["status"], "invalid")
        self.assertEqual(report["errors"][0]["field_path"], SWEETSPOT_CONFIG_PATH)
        self.assertEqual(report["errors"][0]["code"], "invalid_setup_config")

    def test_bootstrap_status_reports_ready_local_bundle_without_external_dependencies(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)
            report = json.loads(render_bootstrap_status(project_dir / ".sweetspot"))

        self.assertEqual(report["schema"], "sweetspot.bootstrap.status.v1")
        self.assertTrue(report["ok"])
        self.assertEqual(report["status"], "ready")
        self.assertEqual(report["intent"]["schema"], "sweetspot.bootstrap.intent.v1")
        self.assertEqual(report["intent"]["status"], "ready")
        self.assertEqual(report["intent"]["region"], "us-west-2")
        self.assertEqual(report["intent"]["auth"], {"method": "profile", "reference": "sweetspot-dev"})
        self.assertEqual(report["validation_findings"], [])
        artifacts = {artifact["name"]: artifact for artifact in report["generated_artifacts"]}
        self.assertEqual(set(artifacts), {"config", "deployment_template", "doc", "infra_vars_stub", "job", "next_steps", "worker_notes", "worker_scaffold"})
        self.assertTrue(all(artifact["status"] == "present" for artifact in artifacts.values()))
        self.assertIn("Review generated artifacts", report["next_actions"][0])
        self.assertEqual(scan_for_secrets(report), ())

    def test_bootstrap_status_reports_missing_artifacts_and_inputs(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        data = setup_to_dict(config)
        data["aws"]["auth"].pop("profile")

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)
            (project_dir / JOB_SPEC_PATH).unlink()
            (project_dir / SWEETSPOT_CONFIG_PATH).write_text(json.dumps(data, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            report = json.loads(render_bootstrap_status(project_dir))

        self.assertFalse(report["ok"])
        self.assertEqual(report["status"], "incomplete")
        self.assertIn("aws.auth.profile", report["intent"]["missing_inputs"])
        artifacts = {artifact["name"]: artifact for artifact in report["generated_artifacts"]}
        self.assertEqual(artifacts["job"]["status"], "missing")
        self.assertTrue(any("aws.auth.profile" in action for action in report["next_actions"]))
        self.assertTrue(any(JOB_SPEC_PATH in action for action in report["next_actions"]))

    def test_bootstrap_status_reports_invalid_setup_without_secret_leakage(self) -> None:
        secret_text = "AKIA1234567890ABCDEF"
        with tempfile.TemporaryDirectory() as tmpdir:
            sweetspot_dir = Path(tmpdir) / ".sweetspot"
            sweetspot_dir.mkdir()
            (sweetspot_dir / "sweetspot.yaml").write_text(f"schema: wrong\nworkload:\n  command: ['python', '{secret_text}']\n", encoding="utf-8")
            report = json.loads(render_bootstrap_status(sweetspot_dir))

        self.assertFalse(report["ok"])
        self.assertEqual(report["status"], "invalid")
        self.assertEqual(report["validation_findings"][0]["code"], "secret_value_aws_access_key_id")
        self.assertNotIn(secret_text, json.dumps(report))

    def test_write_project_context_round_trips_and_renders_full_starter_bundle(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        expected_paths = [
            SWEETSPOT_CONFIG_PATH,
            SWEETSPOT_DOC_PATH,
            JOB_SPEC_PATH,
            DEPLOYMENT_TEMPLATE_PATH,
            WORKER_NOTES_PATH,
            WORKER_SCAFFOLD_PATH,
            INFRA_VARS_STUB_PATH,
            NEXT_STEPS_PATH,
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            written = write_project_context(config, project_dir)

            self.assertEqual([path.relative_to(project_dir).as_posix() for path in written], expected_paths)
            self.assertEqual(load_setup(project_dir / SWEETSPOT_CONFIG_PATH), config)
            self.assertTrue((project_dir / WORKER_NOTES_PATH).parent.is_dir())
            self.assertTrue((project_dir / INFRA_VARS_STUB_PATH).parent.is_dir())
            self.assertTrue(all((project_dir / path).exists() for path in expected_paths))

            doc = (project_dir / SWEETSPOT_DOC_PATH).read_text(encoding="utf-8")
            job_path = project_dir / JOB_SPEC_PATH
            job_text = job_path.read_text(encoding="utf-8")
            loaded_job = load_job_spec(job_path)
            deployment_text = (project_dir / DEPLOYMENT_TEMPLATE_PATH).read_text(encoding="utf-8")
            worker_notes = (project_dir / WORKER_NOTES_PATH).read_text(encoding="utf-8")
            worker_scaffold = (project_dir / WORKER_SCAFFOLD_PATH).read_text(encoding="utf-8")
            infra_text = (project_dir / INFRA_VARS_STUB_PATH).read_text(encoding="utf-8")
            next_steps = (project_dir / NEXT_STEPS_PATH).read_text(encoding="utf-8")

        self.assertEqual(doc, render_sweetspot_doc(config))
        self.assertEqual(job_text, render_starter_job_spec(config))
        self.assertEqual(deployment_text, render_deployment_template(config))
        self.assertEqual(worker_notes, render_worker_notes(config))
        self.assertEqual(worker_scaffold, render_worker_scaffold(config))
        self.assertEqual(infra_text, render_infra_vars_stub(config))
        self.assertEqual(next_steps, render_next_steps(config))
        self.assertTrue(all(text.endswith("\n") for text in [doc, job_text, deployment_text, worker_notes, worker_scaffold, infra_text, next_steps]))
        self.assertEqual(json.loads(job_text), loaded_job)
        self.assertEqual(loaded_job["schema"], "sweetspot.job.v1")
        self.assertEqual(loaded_job["run_id"], "example-batch-project-starter-run")
        self.assertEqual(loaded_job["command"], ["python", "worker.py", "--task", "{task_json}"])
        self.assertEqual(loaded_job["input_manifest"], "s3://example-sweetspot-input/manifests/tasks.jsonl")
        self.assertEqual(loaded_job["output_prefix"], "s3://example-sweetspot-output/runs/example/")
        self.assertEqual(loaded_job["constraints"]["architectures"], ["x86_64"])
        self.assertEqual(loaded_job["constraints"]["regions"], ["us-west-2"])
        self.assertEqual(loaded_job["validation"], {"output_check": "done_marker"})
        self.assertFalse(FORBIDDEN_PRIMARY_JOB_SPEC_KEYS.intersection(loaded_job))
        self.assertEqual(scan_for_secrets(loaded_job), ())

        deployment = json.loads(deployment_text)
        infra = json.loads(infra_text)
        self.assertEqual(deployment["schema"], "sweetspot.deployment.template.v1")
        self.assertEqual(deployment["aws"]["region"], "us-west-2")
        self.assertEqual(deployment["aws"]["auth"], {"method": "profile", "reference": "sweetspot-dev"})
        self.assertEqual(deployment["resources"]["container"]["architecture"], "x86_64")
        self.assertFalse(deployment["ready_to_deploy"])
        self.assertEqual(deployment["status"], "template-review-only")
        self.assertIn("TODO", deployment["resources"]["batch"]["job_queue"])
        self.assertFalse(infra["ready_for_apply"])
        self.assertEqual(infra["review_status"], "template-review-only")
        self.assertEqual(infra["auth_reference"], "sweetspot-dev")
        self.assertEqual(infra["architecture"], "x86_64")

        combined_bundle = "\n".join([doc, job_text, deployment_text, worker_notes, worker_scaffold, infra_text, next_steps])
        for expected in [
            "example-batch-project",
            "s3://example-sweetspot-input/manifests/tasks.jsonl",
            "s3://example-sweetspot-output/runs/example/",
            "python worker.py --task {task_json}",
            "x86_64",
            "us-west-2",
            "profile",
            "sweetspot-dev",
            JOB_SPEC_PATH,
            DEPLOYMENT_TEMPLATE_PATH,
            WORKER_NOTES_PATH,
            WORKER_SCAFFOLD_PATH,
            INFRA_VARS_STUB_PATH,
            NEXT_STEPS_PATH,
            "starter bundle for review/customization",
            "No AWS resources have been created",
            "TODO templates, not deployable infrastructure",
        ]:
            self.assertIn(expected, combined_bundle)
        for forbidden in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "BEGIN PRIVATE KEY", "123456789012"]:
            self.assertNotIn(forbidden, combined_bundle)

    def test_doctor_project_accepts_generated_bundle_and_warns_on_review_placeholders(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)
            report = doctor_project(project_dir / ".sweetspot")

        self.assertEqual(report["schema"], "sweetspot.project.doctor.v1")
        self.assertTrue(report["ok"])
        self.assertEqual(report["status"], "warning")
        self.assertEqual(report["project_dir"], (project_dir / ".sweetspot").resolve().as_posix())
        self.assertEqual(report["root_dir"], project_dir.resolve().as_posix())
        self.assertEqual(report["summary"]["checks"], {"pass": 4, "warning": 1, "fail": 0})
        self.assertEqual(report["summary"]["findings"]["error"], 0)
        checks = {check["name"]: check for check in report["checks"]}
        self.assertEqual(set(checks), {"setup_config", "generated_artifacts", "planner_job", "secret_scan", "placeholder_review"})
        self.assertEqual(checks["setup_config"]["status"], "pass")
        self.assertEqual(checks["generated_artifacts"]["status"], "pass")
        self.assertEqual(checks["planner_job"]["status"], "pass")
        self.assertEqual(checks["secret_scan"]["status"], "pass")
        self.assertEqual(checks["placeholder_review"]["status"], "warning")
        self.assertTrue(all(finding["severity"] == "warning" for finding in checks["placeholder_review"]["findings"]))
        self.assertTrue(all(finding["code"] == "review_placeholder" for finding in checks["placeholder_review"]["findings"]))

    def test_doctor_project_accepts_project_root_containing_sweetspot_directory(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)
            from_root = doctor_project(project_dir)
            from_sweetspot = doctor_project(project_dir / ".sweetspot")

        self.assertEqual(from_root["project_dir"], from_sweetspot["project_dir"])
        self.assertEqual(from_root["root_dir"], from_sweetspot["root_dir"])
        self.assertEqual(from_root["summary"], from_sweetspot["summary"])

    def test_doctor_project_reports_missing_setup_config_as_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            sweetspot_dir = Path(tmpdir) / ".sweetspot"
            sweetspot_dir.mkdir()
            report = doctor_project(sweetspot_dir)

        checks = {check["name"]: check for check in report["checks"]}
        self.assertFalse(report["ok"])
        self.assertEqual(report["status"], "fail")
        self.assertEqual(checks["setup_config"]["status"], "fail")
        self.assertEqual(checks["setup_config"]["findings"][0]["code"], "missing_setup_config")
        self.assertEqual(checks["setup_config"]["findings"][0]["severity"], "error")

    def test_doctor_project_reports_missing_job_artifact(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)
            (project_dir / JOB_SPEC_PATH).unlink()
            report = doctor_project(project_dir / ".sweetspot")

        checks = {check["name"]: check for check in report["checks"]}
        self.assertFalse(report["ok"])
        self.assertEqual(checks["generated_artifacts"]["status"], "fail")
        self.assertIn("missing_generated_artifact", {finding["code"] for finding in checks["generated_artifacts"]["findings"]})
        self.assertEqual(checks["planner_job"]["status"], "fail")
        self.assertEqual(checks["planner_job"]["findings"][0]["code"], "missing_job_artifact")

    def test_doctor_project_reports_directory_artifact_as_invalid(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)
            worker_notes = project_dir / WORKER_NOTES_PATH
            worker_notes.unlink()
            worker_notes.mkdir()
            report = doctor_project(project_dir / ".sweetspot")

        checks = {check["name"]: check for check in report["checks"]}
        self.assertFalse(report["ok"])
        self.assertEqual(checks["generated_artifacts"]["status"], "fail")
        self.assertIn("invalid_generated_artifact", {finding["code"] for finding in checks["generated_artifacts"]["findings"]})

    def test_doctor_project_reports_directory_job_without_throwing(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)
            job_path = project_dir / JOB_SPEC_PATH
            job_path.unlink()
            job_path.mkdir()
            report = doctor_project(project_dir / ".sweetspot")

        checks = {check["name"]: check for check in report["checks"]}
        self.assertFalse(report["ok"])
        self.assertEqual(checks["generated_artifacts"]["status"], "fail")
        self.assertEqual(checks["planner_job"]["status"], "fail")
        self.assertEqual(checks["planner_job"]["findings"][0]["code"], "invalid_job_artifact")

    def test_doctor_project_reports_malformed_setup_without_throwing(self) -> None:
        secret_text = "AKIA1234567890ABCDEF"
        with tempfile.TemporaryDirectory() as tmpdir:
            sweetspot_dir = Path(tmpdir) / ".sweetspot"
            sweetspot_dir.mkdir()
            (sweetspot_dir / "sweetspot.yaml").write_text(f"schema: sweetspot.project.v1\naws_access_key_id: {secret_text}\n", encoding="utf-8")
            report = doctor_project(sweetspot_dir)

        checks = {check["name"]: check for check in report["checks"]}
        self.assertFalse(report["ok"])
        self.assertEqual(checks["setup_config"]["status"], "fail")
        setup_finding = checks["setup_config"]["findings"][0]
        self.assertEqual(setup_finding["code"], "invalid_setup_config")
        self.assertNotIn(secret_text, setup_finding["message"])
        secret_findings = checks["secret_scan"]["findings"]
        self.assertTrue(secret_findings)
        self.assertTrue(all(secret_text not in str(finding) for finding in secret_findings))

    def test_doctor_project_reports_planner_incompatible_job_json(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)
            job_path = project_dir / JOB_SPEC_PATH
            job_data = json.loads(job_path.read_text(encoding="utf-8"))
            job_data["constraints"]["architectures"] = ["gpu"]
            job_path.write_text(json.dumps(job_data, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            report = doctor_project(project_dir / ".sweetspot")

        checks = {check["name"]: check for check in report["checks"]}
        self.assertFalse(report["ok"])
        self.assertEqual(checks["planner_job"]["status"], "fail")
        self.assertEqual(checks["planner_job"]["findings"][0]["code"], "planner_incompatible_job")
        self.assertIn("unsupported architecture", checks["planner_job"]["findings"][0]["message"])

    def test_doctor_project_scans_text_artifacts_for_sanitized_secret_findings(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        secret_text = "Bearer abcdefghijklmnopqrstuvwxyz123456"

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)
            notes_path = project_dir / WORKER_NOTES_PATH
            notes_path.write_text(notes_path.read_text(encoding="utf-8") + f"\n{secret_text}\n", encoding="utf-8")
            report = doctor_project(project_dir / ".sweetspot")

        checks = {check["name"]: check for check in report["checks"]}
        self.assertFalse(report["ok"])
        self.assertEqual(checks["secret_scan"]["status"], "fail")
        findings = checks["secret_scan"]["findings"]
        self.assertTrue(any(finding["code"] == "secret_value_bearer_token" for finding in findings))
        self.assertTrue(any(finding["path"].startswith(WORKER_NOTES_PATH) for finding in findings))
        self.assertTrue(all(secret_text not in str(finding) for finding in findings))

    def test_validate_setup_rejects_bootstrap_path_collision_with_reserved_config(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["bootstrap"]["job"] = SWEETSPOT_CONFIG_PATH

        with self.assertRaisesRegex(SetupSpecError, r"bootstrap\.job: must not collide with generated setup config"):
            validate_setup(data)

    def test_validate_setup_normalizes_bootstrap_path_before_collision_check(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["bootstrap"]["job"] = ".sweetspot/./sweetspot.yaml"

        with self.assertRaisesRegex(SetupSpecError, r"bootstrap\.job: must not collide with generated setup config"):
            validate_setup(data)

    def test_validate_setup_rejects_duplicate_bootstrap_paths(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["bootstrap"]["deployment_template"] = data["bootstrap"]["job"]

        with self.assertRaisesRegex(SetupSpecError, r"bootstrap\.deployment_template: must not collide with bootstrap\.job"):
            validate_setup(data)

    def test_write_project_context_rejects_symlinked_sweetspot_dir_before_writing(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            project_dir = root / "project"
            outside_dir = root / "outside"
            project_dir.mkdir()
            outside_dir.mkdir()
            (project_dir / ".sweetspot").symlink_to(outside_dir, target_is_directory=True)

            with self.assertRaisesRegex(FileExistsError, r"paths must not contain symlinks") as ctx:
                write_project_context(config, project_dir)

            self.assertIn(".sweetspot", str(ctx.exception))
            self.assertFalse((outside_dir / "sweetspot.yaml").exists())
            self.assertFalse((outside_dir / "job.json").exists())

    def test_write_project_context_rejects_directory_destination_even_with_overwrite(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)
            config_path = project_dir / SWEETSPOT_CONFIG_PATH
            original_config = config_path.read_text(encoding="utf-8")
            job_path = project_dir / JOB_SPEC_PATH
            job_path.unlink()
            job_path.mkdir()

            with self.assertRaisesRegex(FileExistsError, r"file paths are not regular files") as ctx:
                write_project_context(config, project_dir, overwrite=True)

            self.assertIn(JOB_SPEC_PATH, str(ctx.exception))
            self.assertEqual(config_path.read_text(encoding="utf-8"), original_config)

    def test_write_project_context_rejects_parent_file_conflicts_before_writing(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            blocked_parent = project_dir / ".sweetspot" / "worker"
            blocked_parent.parent.mkdir(parents=True)
            blocked_parent.write_text("not a directory\n", encoding="utf-8")

            with self.assertRaisesRegex(FileExistsError, r"parent paths are files") as ctx:
                write_project_context(config, project_dir)

            self.assertIn(".sweetspot/worker", str(ctx.exception))
            self.assertEqual(blocked_parent.read_text(encoding="utf-8"), "not a directory\n")
            self.assertFalse((project_dir / SWEETSPOT_CONFIG_PATH).exists())
            self.assertFalse((project_dir / JOB_SPEC_PATH).exists())

    def test_write_project_context_rejects_generated_parent_path_overlap_before_writing(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["bootstrap"]["job"] = ".sweetspot/worker"

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            with self.assertRaisesRegex(FileExistsError, r"generated artifact paths overlap") as ctx:
                write_project_context(data, project_dir)

            self.assertIn(".sweetspot/worker", str(ctx.exception))
            self.assertFalse((project_dir / SWEETSPOT_CONFIG_PATH).exists())

    def test_write_project_context_conflicts_fail_closed_unless_overwrite_true(self) -> None:
        config = load_setup(ROOT / "examples" / "setup.example.yaml")
        expected_paths = [
            SWEETSPOT_CONFIG_PATH,
            SWEETSPOT_DOC_PATH,
            JOB_SPEC_PATH,
            DEPLOYMENT_TEMPLATE_PATH,
            WORKER_NOTES_PATH,
            WORKER_SCAFFOLD_PATH,
            INFRA_VARS_STUB_PATH,
            NEXT_STEPS_PATH,
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            write_project_context(config, project_dir)
            originals = {path: (project_dir / path).read_text(encoding="utf-8") for path in expected_paths}

            with self.assertRaisesRegex(FileExistsError, r"\.sweetspot/sweetspot\.yaml") as ctx:
                write_project_context(config, project_dir)

            for path in expected_paths:
                self.assertIn(path, str(ctx.exception))
                self.assertEqual((project_dir / path).read_text(encoding="utf-8"), originals[path])

            (project_dir / WORKER_SCAFFOLD_PATH).write_text("custom local worker\n", encoding="utf-8")
            with self.assertRaisesRegex(FileExistsError, r"\.sweetspot/worker/worker\.py"):
                write_project_context(config, project_dir)
            self.assertEqual((project_dir / WORKER_SCAFFOLD_PATH).read_text(encoding="utf-8"), "custom local worker\n")

            written = write_project_context(config, project_dir, overwrite=True)
            overwritten_worker = (project_dir / WORKER_SCAFFOLD_PATH).read_text(encoding="utf-8")

        self.assertEqual([path.relative_to(project_dir).as_posix() for path in written], expected_paths)
        self.assertEqual(overwritten_worker, render_worker_scaffold(config))

    def test_write_project_context_rejects_secret_bearing_config_without_writes(self) -> None:
        data = setup_to_dict(load_setup(ROOT / "examples" / "setup.example.yaml"))
        data["aws"]["auth"]["session_token"] = "not-allowed"

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            with self.assertRaisesRegex(SetupSpecError, r"aws\.auth\.session_token: secret_key_name"):
                write_project_context(data, project_dir)

            self.assertFalse((project_dir / SWEETSPOT_CONFIG_PATH).exists())
            self.assertFalse((project_dir / SWEETSPOT_DOC_PATH).exists())
            self.assertFalse((project_dir / JOB_SPEC_PATH).exists())


if __name__ == "__main__":
    unittest.main()
