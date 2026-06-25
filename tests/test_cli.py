from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    import boto3  # noqa: F401
except ModuleNotFoundError:
    boto3 = types.ModuleType("boto3")
    boto3.client = lambda *args, **kwargs: None
    boto3.Session = lambda *args, **kwargs: None
    sys.modules.setdefault("boto3", boto3)

try:
    from botocore.exceptions import ClientError as _ClientError
except ModuleNotFoundError:

    class _ClientError(Exception):
        def __init__(self, response, operation_name):
            super().__init__(f"{operation_name}: {response}")
            self.response = response

    botocore = types.ModuleType("botocore")
    exceptions = types.ModuleType("botocore.exceptions")
    exceptions.ClientError = _ClientError
    botocore.exceptions = exceptions
    sys.modules.setdefault("botocore", botocore)
    sys.modules.setdefault("botocore.exceptions", exceptions)

ClientError = _ClientError

from sweetspot.cli import (
    _auto_canary_indices,
    _job_log_group,
    _job_log_stream,
    _extract_task_id_from_log_message,
    _parse_index_selection,
    _redact_env,
    _sample_from_runtime_obj,
    _supervisor_desired_workers,
    _validate_s3_delete_prefix,
    _worker_overrides,
    cmd_cancel,
    cmd_cancel_jobs,
    cmd_cleanup_stale_messages,
    cmd_derive_canary,
    cmd_dlq,
    cmd_doctor,
    cmd_enqueue_and_submit,
    cmd_enqueue_jsonl,
    cmd_estimate_runtime,
    cmd_finalize,
    cmd_jobs,
    cmd_logs,
    cmd_repair,
    cmd_repair_plan,
    cmd_s3_delete_prefix,
    cmd_status,
    cmd_describe_job,
    cmd_supervise_workers,
    cmd_version,
    main,
)
from sweetspot.task_model import validate_task_model
from sweetspot.worker import task_hash


def _calibrated_summary_jsonl() -> str:
    return (
        json.dumps(
            {
                "returncode": 0,
                "completed_units": 1000,
                "elapsed_sec": 100,
                "telemetry": {
                    "architecture": "x86_64",
                    "region": "us-west-2",
                    "worker_vcpus": 1,
                    "worker_memory_mib": 2048,
                    "completed_units": 1000,
                    "useful_compute_seconds": 100,
                },
            }
        )
        + "\n"
    )


class VersionTests(unittest.TestCase):
    def test_version_reports_installed_package(self) -> None:
        out = io.StringIO()
        with patch("sweetspot.cli.importlib_metadata.version", return_value="1.2.3"), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_version(types.SimpleNamespace()), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["schema"], "sweetspot.version.v1")
        self.assertEqual(report["package"], "sweetspot")
        self.assertEqual(report["version"], "1.2.3")


class PlanCommandTests(unittest.TestCase):
    def test_plan_emits_blocked_json_plan_for_valid_job_spec(self) -> None:
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(main(["plan", "examples/job.x86.example.json"]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["schema"], "sweetspot.plan.v1")
        self.assertEqual(report["run_id"], "example-x86-run")
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["reasons"][0]["code"], "insufficient_telemetry")

    def test_plan_reports_invalid_job_spec_without_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "job.json"
            path.write_text(json.dumps({"schema": "sweetspot.job.v1"}))
            with self.assertRaisesRegex(SystemExit, "run_id"):
                main(["plan", str(path)])

    def test_plan_can_embed_adaptive_canary_summary_decision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summaries = Path(tmp) / "summaries.jsonl"
            summaries.write_text(_calibrated_summary_jsonl())
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(main(["plan", "examples/job.x86.example.json", "--canary-summary-jsonl", str(summaries)]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["schema"], "sweetspot.plan.v1")
        self.assertEqual(report["canaries"][0]["purpose"], "adaptive_shard_sizing")
        self.assertEqual(report["canaries"][0]["decision"]["selected_units_per_task"], 3000)

    def test_plan_writes_initial_canary_tasks_without_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.jsonl"
            canary_tasks_path = Path(tmp) / "canary_tasks.jsonl"
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(9)))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(
                    main(
                        [
                            "plan",
                            "examples/job.x86.example.json",
                            "--input-manifest-jsonl",
                            str(manifest),
                            "--out-canary-tasks-jsonl",
                            str(canary_tasks_path),
                        ]
                    ),
                    0,
                )
            report = json.loads(out.getvalue())
            self.assertEqual(report["canaries"][0]["decision"]["reasons"][0]["code"], "canary_required")
            self.assertNotIn("production_shards", report["canaries"][0])
            self.assertEqual(report["artifacts"]["canary_task_count"], 9)
            tasks = [json.loads(line) for line in canary_tasks_path.read_text().splitlines()]
        self.assertEqual({task["job_type"] for task in tasks}, {"canary"})
        self.assertEqual([task["logical_unit_start"] for task in tasks[:3]], [0, 4, 8])
        self.assertEqual({task["input"]["candidate_architecture"] for task in tasks}, {"x86_64"})
        self.assertEqual({task["input"]["candidate_vcpus"] for task in tasks}, {1, 2, 4})
        self.assertIn("/canaries/x86_64-1vcpu-2048mib/u0000000001/shards/", tasks[0]["output_s3"])
        validate_task_model(tasks[0], default_timeout_seconds=300, max_timeout_seconds=39600)

    def test_plan_writes_paired_arm_canary_tasks_when_arm_is_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.jsonl"
            canary_tasks_path = Path(tmp) / "canary_tasks.jsonl"
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(3)))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(
                    main(
                        [
                            "plan",
                            "examples/job.arm-eligible.example.json",
                            "--input-manifest-jsonl",
                            str(manifest),
                            "--out-canary-tasks-jsonl",
                            str(canary_tasks_path),
                        ]
                    ),
                    0,
                )
            report = json.loads(out.getvalue())
            self.assertEqual(report["artifacts"]["canary_task_count"], 18)
            tasks = [json.loads(line) for line in canary_tasks_path.read_text().splitlines()]
        self.assertEqual({task["input"]["candidate_architecture"] for task in tasks}, {"x86_64", "arm64"})
        self.assertEqual({task["input"]["candidate_memory_mib"] for task in tasks}, {2048, 4096, 8192})

    def test_plan_counts_manifest_units_for_adaptive_shard_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summaries = Path(tmp) / "summaries.jsonl"
            manifest = Path(tmp) / "manifest.jsonl"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(6500)))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(
                    main(
                        [
                            "plan",
                            "examples/job.x86.example.json",
                            "--canary-summary-jsonl",
                            str(summaries),
                            "--input-manifest-jsonl",
                            str(manifest),
                        ]
                    ),
                    0,
                )
        shard_plan = json.loads(out.getvalue())["canaries"][0]["production_shards"]
        self.assertEqual(shard_plan["logical_unit_count"], 6500)
        self.assertEqual(shard_plan["task_count"], 3)

    def test_plan_allows_empty_manifest_for_adaptive_shard_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summaries = Path(tmp) / "summaries.jsonl"
            manifest = Path(tmp) / "empty.jsonl"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text("")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(
                    main(["plan", "examples/job.x86.example.json", "--canary-summary-jsonl", str(summaries), "--input-manifest-jsonl", str(manifest)]),
                    0,
                )
        shard_plan = json.loads(out.getvalue())["canaries"][0]["production_shards"]
        self.assertEqual(shard_plan["logical_unit_count"], 0)
        self.assertEqual(shard_plan["task_count"], 0)

    def test_plan_can_write_calibrated_production_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summaries = Path(tmp) / "summaries.jsonl"
            manifest = Path(tmp) / "manifest.jsonl"
            tasks_path = Path(tmp) / "artifacts" / "tasks.jsonl"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(6500)))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(
                    main(
                        [
                            "plan",
                            "examples/job.x86.example.json",
                            "--canary-summary-jsonl",
                            str(summaries),
                            "--input-manifest-jsonl",
                            str(manifest),
                            "--out-production-tasks-jsonl",
                            str(tasks_path),
                        ]
                    ),
                    0,
                )
            report = json.loads(out.getvalue())
            self.assertEqual(report["artifacts"]["production_tasks_jsonl"], str(tasks_path))
            self.assertEqual(report["artifacts"]["production_task_count"], 3)
            tasks = [json.loads(line) for line in tasks_path.read_text().splitlines()]
        self.assertEqual([task["task_id"] for task in tasks], ["shard-000000", "shard-000001", "shard-000002"])
        self.assertEqual(tasks[-1]["logical_unit_start"], 6000)
        self.assertEqual(tasks[-1]["logical_unit_count"], 500)
        validate_task_model(tasks[0], default_timeout_seconds=300, max_timeout_seconds=39600)

    def test_plan_requires_resource_calibration_for_production_task_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summaries = Path(tmp) / "summaries.jsonl"
            manifest = Path(tmp) / "manifest.jsonl"
            tasks_path = Path(tmp) / "tasks.jsonl"
            summaries.write_text(json.dumps({"returncode": 0, "completed_units": 1000, "elapsed_sec": 100}) + "\n")
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(6500)))
            with self.assertRaisesRegex(SystemExit, "resource decisions need calibrated"):
                main(
                    [
                        "plan",
                        "examples/job.x86.example.json",
                        "--canary-summary-jsonl",
                        str(summaries),
                        "--input-manifest-jsonl",
                        str(manifest),
                        "--out-production-tasks-jsonl",
                        str(tasks_path),
                    ]
                )

    def test_plan_requires_manifest_for_production_task_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summaries = Path(tmp) / "summaries.jsonl"
            tasks_path = Path(tmp) / "tasks.jsonl"
            summaries.write_text(_calibrated_summary_jsonl())
            with self.assertRaisesRegex(SystemExit, "requires --canary-summary-jsonl and --input-manifest-jsonl"):
                main(["plan", "examples/job.x86.example.json", "--canary-summary-jsonl", str(summaries), "--out-production-tasks-jsonl", str(tasks_path)])


class RunCommandTests(unittest.TestCase):
    def test_run_emits_dry_run_controller_report_without_mutation(self) -> None:
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(main(["run", "examples/job.x86.example.json"]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["schema"], "sweetspot.run.v1")
        self.assertEqual(report["run_id"], "example-x86-run")
        self.assertEqual(report["mode"], "dry_run")
        self.assertFalse(report["applied"])
        self.assertFalse(report["controller"]["mutations_allowed"])
        self.assertEqual(report["plan"]["schema"], "sweetspot.plan.v1")
        self.assertEqual(report["phases"][0]["name"], "plan")
        self.assertEqual(report["phases"][1]["status"], "skipped")

    def test_run_writes_initial_canary_tasks_when_summaries_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.jsonl"
            artifact_dir = root / "artifacts"
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(9)))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(
                    main(
                        [
                            "run",
                            "examples/job.x86.example.json",
                            "--input-manifest-jsonl",
                            str(manifest),
                            "--artifact-dir",
                            str(artifact_dir),
                        ]
                    ),
                    0,
                )
            report = json.loads(out.getvalue())
            canary_tasks_path = artifact_dir / "canary_tasks.jsonl"
            phases = {phase["name"]: phase for phase in report["phases"]}
            self.assertEqual(report["artifacts"]["canary_tasks_jsonl"], str(canary_tasks_path))
            self.assertEqual(report["artifacts"]["canary_task_count"], 9)
            self.assertEqual(phases["materialize_production_tasks"]["status"], "skipped")
            self.assertEqual(phases["materialize_canary_tasks"]["status"], "completed")
            tasks = [json.loads(line) for line in canary_tasks_path.read_text().splitlines()]
        self.assertEqual({task["job_type"] for task in tasks}, {"canary"})
        self.assertEqual([task["logical_unit_start"] for task in tasks[:3]], [0, 4, 8])
        self.assertEqual({task["input"]["candidate_vcpus"] for task in tasks}, {1, 2, 4})

    def test_run_writes_state_and_default_production_tasks_in_artifact_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summaries = root / "summaries.jsonl"
            manifest = root / "manifest.jsonl"
            artifact_dir = root / "artifacts"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(6500)))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(
                    main(
                        [
                            "run",
                            "examples/job.x86.example.json",
                            "--canary-summary-jsonl",
                            str(summaries),
                            "--input-manifest-jsonl",
                            str(manifest),
                            "--artifact-dir",
                            str(artifact_dir),
                        ]
                    ),
                    0,
                )
            report = json.loads(out.getvalue())
            tasks_path = artifact_dir / "production_tasks.jsonl"
            state_path = artifact_dir / "run_state.json"
            self.assertEqual(report["artifacts"]["production_tasks_jsonl"], str(tasks_path))
            self.assertEqual(report["artifacts"]["production_task_count"], 3)
            self.assertEqual(report["artifacts"]["run_state_json"], str(state_path))
            self.assertTrue(state_path.exists())
            tasks = [json.loads(line) for line in tasks_path.read_text().splitlines()]
        self.assertEqual([task["task_id"] for task in tasks], ["shard-000000", "shard-000001", "shard-000002"])

    def test_run_does_not_emit_canaries_for_calibrated_terminal_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summaries = root / "summaries.jsonl"
            manifest = root / "manifest.jsonl"
            artifact_dir = root / "artifacts"
            job_spec = root / "job.json"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(6500)))
            spec = json.loads(Path("examples/job.x86.example.json").read_text())
            spec["constraints"]["max_cost_usd"] = 0.000001
            job_spec.write_text(json.dumps(spec))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(
                    main(
                        [
                            "run",
                            str(job_spec),
                            "--canary-summary-jsonl",
                            str(summaries),
                            "--input-manifest-jsonl",
                            str(manifest),
                            "--artifact-dir",
                            str(artifact_dir),
                        ]
                    ),
                    0,
                )
            report = json.loads(out.getvalue())
            phases = {phase["name"]: phase for phase in report["phases"]}
            self.assertEqual(report["plan"]["status"], "blocked")
            self.assertEqual(report["plan"]["canaries"][0]["decision"]["next_action"], "produce_production")
            self.assertNotIn("canary_tasks_jsonl", report.get("artifacts", {}))
            self.assertFalse((artifact_dir / "canary_tasks.jsonl").exists())
            self.assertEqual(phases["materialize_canary_tasks"]["status"], "skipped")

    def test_run_apply_requires_artifact_dir_for_resume_state(self) -> None:
        with self.assertRaisesRegex(SystemExit, "requires --artifact-dir"):
            main(["run", "examples/job.x86.example.json", "--apply", "--queue-url", "https://sqs.example/q", "--batch-job-queue", "jq", "--job-definition", "jd"])

    def test_run_apply_rejects_legacy_sizing_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summaries = root / "summaries.jsonl"
            manifest = root / "manifest.jsonl"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text(json.dumps({"unit": 0}) + "\n")
            with self.assertRaisesRegex(SystemExit, "Plan-authoritative"):
                main(
                    [
                        "run",
                        "examples/job.x86.example.json",
                        "--canary-summary-jsonl",
                        str(summaries),
                        "--input-manifest-jsonl",
                        str(manifest),
                        "--artifact-dir",
                        str(root / "artifacts"),
                        "--queue-url",
                        "https://sqs.example/q",
                        "--batch-job-queue",
                        "jq",
                        "--job-definition",
                        "jd",
                        "--max-workers",
                        "1",
                        "--apply",
                    ]
                )

    def test_run_apply_enqueues_and_submits_workers_once(self) -> None:
        sqs = FakeQueueDepthSQS()
        batch = FakeSubmitBatch()

        def fake_client(service):
            if service == "sqs":
                return sqs
            if service == "batch":
                return batch
            raise AssertionError(service)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summaries = root / "summaries.jsonl"
            manifest = root / "manifest.jsonl"
            artifact_dir = root / "artifacts"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(6500)))
            argv = [
                "run",
                "examples/job.x86.example.json",
                "--canary-summary-jsonl",
                str(summaries),
                "--input-manifest-jsonl",
                str(manifest),
                "--artifact-dir",
                str(artifact_dir),
                "--queue-url",
                "https://sqs.example/q",
                "--batch-job-queue",
                "jq",
                "--job-definition",
                "jd",
                "--apply",
            ]
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(out):
                self.assertEqual(main(argv), 0)
            report = json.loads(out.getvalue())
            self.assertEqual(report["mode"], "apply")
            self.assertTrue(report["applied"])
            self.assertEqual(report["status"], "workers_submitted")
            phases = {phase["name"]: phase for phase in report["phases"]}
            self.assertEqual(phases["enqueue_tasks"]["sent"], 3)
            self.assertEqual(phases["submit_workers"]["submitted_count"], 1)
            self.assertEqual(phases["submit_workers"]["messages_per_worker"], 3)
            self.assertEqual(sqs.sent, 3)
            self.assertEqual(len(batch.submitted), 1)
            self.assertTrue(str(batch.submitted[0]["jobName"]).startswith("example-x86-run-worker-"))
            self.assertEqual(batch.submitted[0]["jobQueue"], "jq")
            self.assertEqual(batch.submitted[0]["jobDefinition"], "jd")
            state = json.loads((artifact_dir / "run_state.json").read_text())
            self.assertEqual(state["status"], "workers_submitted")
            tasks_path = artifact_dir / "production_tasks.jsonl"
            original_tasks_text = tasks_path.read_text()

            out = io.StringIO()
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(out):
                self.assertEqual(main(argv), 0)
            resumed = json.loads(out.getvalue())
            resumed_phases = {phase["name"]: phase for phase in resumed["phases"]}
            self.assertTrue(resumed_phases["enqueue_tasks"]["resumed"])
            self.assertTrue(resumed_phases["submit_workers"]["resumed"])
            self.assertEqual(sqs.sent, 3)
            self.assertEqual(len(batch.submitted), 1)

            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(9000)))
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), self.assertRaisesRegex(SystemExit, "plan/deployment/manifest binding differs"):
                main(argv)
            self.assertEqual(tasks_path.read_text(), original_tasks_text)
            self.assertEqual(sqs.sent, 3)
            self.assertEqual(len(batch.submitted), 1)

    def test_run_apply_rejects_hashless_legacy_run_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summaries = root / "summaries.jsonl"
            manifest = root / "manifest.jsonl"
            artifact_dir = root / "artifacts"
            artifact_dir.mkdir()
            tasks_path = artifact_dir / "production_tasks.jsonl"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text(json.dumps({"unit": 0}) + "\n")
            tasks_path.write_text(json.dumps({"run_id": "example-x86-run", "task_id": "old"}) + "\n")
            (artifact_dir / "run_state.json").write_text(
                json.dumps(
                    {
                        "schema": "sweetspot.run.v1",
                        "run_id": "example-x86-run",
                        "mode": "dry_run",
                        "applied": False,
                        "artifacts": {"production_tasks_jsonl": str(tasks_path)},
                    }
                )
            )
            with self.assertRaisesRegex(SystemExit, "does not record job_spec_sha256"):
                main(
                    [
                        "run",
                        "examples/job.x86.example.json",
                        "--canary-summary-jsonl",
                        str(summaries),
                        "--input-manifest-jsonl",
                        str(manifest),
                        "--artifact-dir",
                        str(artifact_dir),
                        "--queue-url",
                        "https://sqs.example/q",
                        "--batch-job-queue",
                        "jq",
                        "--job-definition",
                        "jd",
                        "--apply",
                    ]
                )

    def test_run_dry_run_refuses_to_clobber_apply_state(self) -> None:
        sqs = FakeQueueDepthSQS()
        batch = FakeSubmitBatch()

        def fake_client(service):
            if service == "sqs":
                return sqs
            if service == "batch":
                return batch
            raise AssertionError(service)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summaries = root / "summaries.jsonl"
            manifest = root / "manifest.jsonl"
            artifact_dir = root / "artifacts"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(6500)))
            base_argv = [
                "run",
                "examples/job.x86.example.json",
                "--canary-summary-jsonl",
                str(summaries),
                "--input-manifest-jsonl",
                str(manifest),
                "--artifact-dir",
                str(artifact_dir),
            ]
            apply_argv = base_argv + ["--queue-url", "https://sqs.example/q", "--batch-job-queue", "jq", "--job-definition", "jd", "--apply"]
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(apply_argv), 0)
            state_path = artifact_dir / "run_state.json"
            tasks_path = artifact_dir / "production_tasks.jsonl"
            state_text = state_path.read_text()
            tasks_text = tasks_path.read_text()

            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(9000)))
            with self.assertRaisesRegex(SystemExit, "refuses to overwrite"):
                main(base_argv)
            self.assertEqual(state_path.read_text(), state_text)
            self.assertEqual(tasks_path.read_text(), tasks_text)

    def test_run_apply_reuses_reviewed_dry_run_task_artifact(self) -> None:
        sqs = FakeQueueDepthSQS()
        batch = FakeSubmitBatch()

        def fake_client(service):
            if service == "sqs":
                return sqs
            if service == "batch":
                return batch
            raise AssertionError(service)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summaries = root / "summaries.jsonl"
            manifest = root / "manifest.jsonl"
            artifact_dir = root / "artifacts"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(6500)))
            base_argv = [
                "run",
                "examples/job.x86.example.json",
                "--canary-summary-jsonl",
                str(summaries),
                "--input-manifest-jsonl",
                str(manifest),
                "--artifact-dir",
                str(artifact_dir),
            ]
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(main(base_argv), 0)
            tasks_path = artifact_dir / "production_tasks.jsonl"
            reviewed_tasks_text = tasks_path.read_text()
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(9500)))

            apply_argv = base_argv + ["--queue-url", "https://sqs.example/q", "--batch-job-queue", "jq", "--job-definition", "jd", "--apply"]
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(out):
                self.assertEqual(main(apply_argv), 0)
            report = json.loads(out.getvalue())
            phases = {phase["name"]: phase for phase in report["phases"]}
            self.assertEqual(report["plan"]["canaries"][0]["production_shards"]["logical_unit_count"], 6500)
            self.assertEqual(report["plan"]["canaries"][0]["production_shards"]["task_count"], 3)
            self.assertEqual(phases["materialize_production_tasks"]["task_count"], 3)
            self.assertEqual(phases["enqueue_tasks"]["sent"], 3)
            self.assertEqual(tasks_path.read_text(), reviewed_tasks_text)

    def test_run_apply_sizes_workers_from_run_tasks_not_global_queue_depth(self) -> None:
        class DirtyQueueSQS(FakeQueueDepthSQS):
            def get_queue_attributes(self, **kwargs):
                self.depth_calls += 1
                return {"Attributes": {"ApproximateNumberOfMessages": "100", "ApproximateNumberOfMessagesNotVisible": "50", "ApproximateNumberOfMessagesDelayed": "0"}}

        sqs = DirtyQueueSQS()
        batch = FakeSubmitBatch()

        def fake_client(service):
            if service == "sqs":
                return sqs
            if service == "batch":
                return batch
            raise AssertionError(service)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summaries = root / "summaries.jsonl"
            manifest = root / "manifest.jsonl"
            artifact_dir = root / "artifacts"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text(json.dumps({"unit": 0}) + "\n")
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(out):
                self.assertEqual(
                    main(
                        [
                            "run",
                            "examples/job.x86.example.json",
                            "--canary-summary-jsonl",
                            str(summaries),
                            "--input-manifest-jsonl",
                            str(manifest),
                            "--artifact-dir",
                            str(artifact_dir),
                            "--queue-url",
                            "https://sqs.example/q",
                            "--batch-job-queue",
                            "jq",
                            "--job-definition",
                            "jd",
                            "--include-not-visible",
                            "--apply",
                        ]
                    ),
                    0,
                )
            report = json.loads(out.getvalue())
            submit_phase = {phase["name"]: phase for phase in report["phases"]}["submit_workers"]
            self.assertEqual(sqs.sent, 1)
            self.assertEqual(submit_phase["backlog_used_for_sizing"], 1)
            self.assertEqual(submit_phase["submitted_count"], 1)
            self.assertEqual(len(batch.submitted), 1)

    def test_run_apply_persists_enqueue_before_later_aws_calls(self) -> None:
        class CrashingAfterSendSQS:
            def __init__(self) -> None:
                self.sent = 0

            def send_message_batch(self, *, QueueUrl, Entries):
                self.sent += len(Entries)
                return {"Successful": [{"Id": e["Id"]} for e in Entries]}

            def get_queue_attributes(self, **kwargs):
                raise RuntimeError("queue depth unavailable after send")

        first_sqs = CrashingAfterSendSQS()
        batch = FakeSubmitBatch()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summaries = root / "summaries.jsonl"
            manifest = root / "manifest.jsonl"
            artifact_dir = root / "artifacts"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(6500)))
            argv = [
                "run",
                "examples/job.x86.example.json",
                "--canary-summary-jsonl",
                str(summaries),
                "--input-manifest-jsonl",
                str(manifest),
                "--artifact-dir",
                str(artifact_dir),
                "--queue-url",
                "https://sqs.example/q",
                "--batch-job-queue",
                "jq",
                "--job-definition",
                "jd",
                "--apply",
            ]

            def crashing_client(service):
                if service == "sqs":
                    return first_sqs
                if service == "batch":
                    return batch
                raise AssertionError(service)

            with patch("sweetspot.cli.boto3.client", side_effect=crashing_client), self.assertRaisesRegex(RuntimeError, "queue depth unavailable"):
                main(argv)
            self.assertEqual(first_sqs.sent, 3)
            state = json.loads((artifact_dir / "run_state.json").read_text())
            enqueue_phase = {phase["name"]: phase for phase in state["phases"]}["enqueue_tasks"]
            self.assertEqual(enqueue_phase["status"], "completed")
            self.assertEqual(enqueue_phase["sent"], 3)

            second_sqs = FakeQueueDepthSQS()

            def resume_client(service):
                if service == "sqs":
                    return second_sqs
                if service == "batch":
                    return batch
                raise AssertionError(service)

            changed_queue_argv = ["https://sqs.example/other" if item == "https://sqs.example/q" else item for item in argv]
            with patch("sweetspot.cli.boto3.client", side_effect=resume_client), self.assertRaisesRegex(SystemExit, "plan/deployment/manifest binding differs"):
                main(changed_queue_argv)

            out = io.StringIO()
            with patch("sweetspot.cli.boto3.client", side_effect=resume_client), contextlib.redirect_stdout(out):
                self.assertEqual(main(argv), 0)
            resumed = json.loads(out.getvalue())
            resumed_phases = {phase["name"]: phase for phase in resumed["phases"]}
            self.assertTrue(resumed_phases["enqueue_tasks"]["resumed"])
            self.assertEqual(second_sqs.sent, 0)
            self.assertEqual(resumed_phases["submit_workers"]["submitted_count"], 1)

    def test_run_apply_rejects_worker_resume_config_drift(self) -> None:
        sqs = FakeQueueDepthSQS()
        batch = FakeSubmitBatch()

        def fake_client(service):
            if service == "sqs":
                return sqs
            if service == "batch":
                return batch
            raise AssertionError(service)

        class FakeRunSession:
            def client(self, service, region_name=None):
                return fake_client(service)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summaries = root / "summaries.jsonl"
            manifest = root / "manifest.jsonl"
            artifact_dir = root / "artifacts"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(6500)))
            argv = [
                "run",
                "examples/job.x86.example.json",
                "--canary-summary-jsonl",
                str(summaries),
                "--input-manifest-jsonl",
                str(manifest),
                "--artifact-dir",
                str(artifact_dir),
                "--queue-url",
                "https://sqs.example/q",
                "--batch-job-queue",
                "jq",
                "--job-definition",
                "jd",
                "--apply",
            ]
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(argv), 0)
            state_path = artifact_dir / "run_state.json"
            state = json.loads(state_path.read_text())
            phases = {phase["name"]: phase for phase in state["phases"]}
            submit_phase = phases["submit_workers"]
            submit_phase["status"] = "in_progress"
            submit_phase["to_submit"] = 2
            submit_phase["submitted"] = submit_phase["submitted"][:1]
            submit_phase["submitted_count"] = 1
            state_path.write_text(json.dumps(state, indent=2, sort_keys=True))

            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(argv + ["--region", "us-east-1"]), 0)
            self.assertEqual(len(batch.submitted), 2)
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), self.assertRaisesRegex(SystemExit, "different worker settings"):
                main(argv + ["--allow-legacy-done-markers"])

    def test_run_apply_refuses_ambiguous_worker_submit_resume(self) -> None:
        class FailingSecondSubmitBatch(FakeSubmitBatch):
            def submit_job(self, **kwargs):
                if len(self.submitted) >= 1:
                    raise RuntimeError("batch submit failed after one worker")
                return super().submit_job(**kwargs)

        sqs = FakeQueueDepthSQS()
        batch = FailingSecondSubmitBatch()

        def fake_client(service):
            if service == "sqs":
                return sqs
            if service == "batch":
                return batch
            raise AssertionError(service)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summaries = root / "summaries.jsonl"
            manifest = root / "manifest.jsonl"
            artifact_dir = root / "artifacts"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text("".join(json.dumps({"unit": i}) + "\n" for i in range(6500)))
            job_spec = root / "job.json"
            spec = json.loads(Path("examples/job.x86.example.json").read_text())
            spec["constraints"]["deadline_hours"] = 0.07
            job_spec.write_text(json.dumps(spec))
            argv = [
                "run",
                str(job_spec),
                "--canary-summary-jsonl",
                str(summaries),
                "--input-manifest-jsonl",
                str(manifest),
                "--artifact-dir",
                str(artifact_dir),
                "--queue-url",
                "https://sqs.example/q",
                "--batch-job-queue",
                "jq",
                "--job-definition",
                "jd",
                "--apply",
            ]
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), self.assertRaisesRegex(RuntimeError, "batch submit failed"):
                main(argv)
            self.assertEqual(len(batch.submitted), 1)
            state = json.loads((artifact_dir / "run_state.json").read_text())
            submit_phase = {phase["name"]: phase for phase in state["phases"]}["submit_workers"]
            self.assertEqual(submit_phase["status"], "needs_review")
            self.assertEqual(submit_phase["submitted_count"], 1)

            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), self.assertRaisesRegex(SystemExit, "ambiguous worker submission"):
                main(argv)
            self.assertEqual(len(batch.submitted), 1)

    def test_run_apply_rejects_unscoped_worker_job_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            summaries = root / "summaries.jsonl"
            manifest = root / "manifest.jsonl"
            summaries.write_text(_calibrated_summary_jsonl())
            manifest.write_text(json.dumps({"unit": 0}) + "\n")
            with self.assertRaisesRegex(SystemExit, "must start with RUN_ID-"):
                main(
                    [
                        "run",
                        "examples/job.x86.example.json",
                        "--canary-summary-jsonl",
                        str(summaries),
                        "--input-manifest-jsonl",
                        str(manifest),
                        "--artifact-dir",
                        str(root / "artifacts"),
                        "--queue-url",
                        "https://sqs.example/q",
                        "--batch-job-queue",
                        "jq",
                        "--job-definition",
                        "jd",
                        "--job-name-prefix",
                        "other-run-worker",
                        "--apply",
                    ]
                )


class ConfigTests(unittest.TestCase):
    def test_config_prepopulates_required_worker_submit_flags(self) -> None:
        class FakeSQS:
            def get_queue_attributes(self, **kwargs):
                self.queue_url = kwargs["QueueUrl"]
                return {
                    "Attributes": {
                        "ApproximateNumberOfMessages": "4",
                        "ApproximateNumberOfMessagesNotVisible": "0",
                        "ApproximateNumberOfMessagesDelayed": "0",
                    }
                }

        sqs = FakeSQS()

        def fake_client(service):
            if service == "sqs":
                return sqs
            if service == "batch":
                return object()
            raise AssertionError(service)

        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "sweetspot.json"
            config_path.write_text(
                json.dumps(
                    {
                        "defaults": {"queue_url": "configured-queue"},
                        "submit-workers": {"batch_job_queue": "jq", "job_definition": "jd", "messages_per_worker": 2},
                    }
                )
            )
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(out):
                self.assertEqual(main(["--config", str(config_path), "submit-workers"]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(sqs.queue_url, "configured-queue")
        self.assertEqual(report["messages_per_worker"], 2)
        self.assertEqual(report["raw_desired_workers"], 2)

    def test_config_after_regular_subcommand_still_applies(self) -> None:
        class FakeSQS:
            def get_queue_attributes(self, **kwargs):
                return {
                    "Attributes": {
                        "ApproximateNumberOfMessages": "4",
                        "ApproximateNumberOfMessagesNotVisible": "0",
                        "ApproximateNumberOfMessagesDelayed": "0",
                    }
                }

        def fake_client(service):
            if service == "sqs":
                return FakeSQS()
            if service == "batch":
                return object()
            raise AssertionError(service)

        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "sweetspot.json"
            config_path.write_text(
                json.dumps(
                    {
                        "defaults": {"queue_url": "configured-queue"},
                        "submit-workers": {"batch_job_queue": "jq", "job_definition": "jd", "messages_per_worker": 2},
                    }
                )
            )
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(out):
                self.assertEqual(main(["submit-workers", "--config", str(config_path)]), 0)
        self.assertEqual(json.loads(out.getvalue())["raw_desired_workers"], 2)

    def test_config_defaults_do_not_break_non_configurable_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "sweetspot.json"
            config_path.write_text(json.dumps({"defaults": {"profile": "prod", "queue_url": "q"}}))
            out = io.StringIO()
            with patch("sweetspot.cli.importlib_metadata.version", return_value="1.0"), contextlib.redirect_stdout(out):
                self.assertEqual(main(["--config", str(config_path), "version"]), 0)
        self.assertEqual(json.loads(out.getvalue())["version"], "1.0")


class NestedToolCommandTests(unittest.TestCase):
    def test_scout_subcommand_forwards_arguments(self) -> None:
        with patch("sweetspot.cli.scout.main", return_value=0) as fake:
            self.assertEqual(main(["scout", "--preset", "arm", "--regions", "us-west-2"]), 0)
        fake.assert_called_once_with(["--preset", "arm", "--regions", "us-west-2"], prog="sweetspot scout")

    def test_lane_manager_keeps_its_config_argument(self) -> None:
        with patch("sweetspot.cli.lane_manager.main", return_value=0) as fake:
            self.assertEqual(main(["lane-manager", "--config", "lanes.json", "--submit"]), 0)
        fake.assert_called_once_with(["--config", "lanes.json", "--submit"], prog="sweetspot lane-manager")


class StatusTests(unittest.TestCase):
    def test_status_reports_identity_queue_and_active_workers(self) -> None:
        class FakeSTS:
            def get_caller_identity(self):
                return {"Account": "123", "Arn": "arn:aws:iam::123:user/test", "UserId": "u"}

        class FakeSQS:
            def get_queue_attributes(self, **kwargs):
                visible = "5" if kwargs["QueueUrl"] == "source" else "1"
                return {
                    "Attributes": {
                        "ApproximateNumberOfMessages": visible,
                        "ApproximateNumberOfMessagesNotVisible": "2",
                        "ApproximateNumberOfMessagesDelayed": "0",
                    }
                }

        class FakePaginator:
            def paginate(self, **kwargs):
                if kwargs["jobStatus"] == "RUNNING":
                    return [{"jobSummaryList": [{"jobId": "j1", "jobName": "run-worker", "createdAt": 1}]}]
                return [{"jobSummaryList": []}]

        class FakeBatch:
            def get_paginator(self, name):
                self.name = name
                return FakePaginator()

        class FakeSession:
            region_name = "us-west-2"

            def __init__(self, profile_name=None, region_name=None):
                self.profile_name = profile_name
                self.region_name = region_name

            def client(self, service, region_name=None):
                if service == "sts":
                    return FakeSTS()
                if service == "sqs":
                    return FakeSQS()
                if service == "batch":
                    return FakeBatch()
                raise AssertionError(service)

        args = types.SimpleNamespace(profile="prof", region="us-west-2", queue_url="source", dlq_url="dlq", job_queue="jq", job_name_prefix="run", format="json")
        out = io.StringIO()
        with patch("sweetspot.cli.boto3.Session", FakeSession), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_status(args), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["schema"], "sweetspot.status.v1")
        self.assertEqual(report["identity"]["account"], "123")
        self.assertEqual(report["queues"]["source"]["depth"]["visible"], 5)
        self.assertEqual(report["queues"]["dlq"]["depth"]["visible"], 1)
        self.assertEqual(report["batch"]["active_count"], 1)
        self.assertEqual(report["batch"]["active_by_status"], {"RUNNING": 1})

        args.format = "table"
        out = io.StringIO()
        with patch("sweetspot.cli.boto3.Session", FakeSession), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_status(args), 0)
        table = out.getvalue()
        self.assertIn("SweetSpot status", table)
        self.assertIn("source\t5\t2\t0\tsource", table)
        self.assertIn("active_count\t1\nstatus\tcount\nRUNNING\t1", table)
        self.assertNotIn("\nstatus\nstatus\tcount", table)

    def test_status_run_id_summarizes_local_artifacts_without_aws(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "artifacts" / "run-1"
            artifact_dir.mkdir(parents=True)
            (artifact_dir / "run_state.json").write_text(json.dumps({"schema": "sweetspot.run.v1", "run_id": "run-1", "status": "submitted"}) + "\n")
            (artifact_dir / "production_tasks.jsonl").write_text(json.dumps({"task_id": "t0"}) + "\n" + json.dumps({"task_id": "t1"}) + "\n")
            finalizer_dir = artifact_dir / "finalizer"
            finalizer_dir.mkdir()
            (finalizer_dir / "task_status.jsonl").write_text(json.dumps({"task_id": "t0", "state": "done"}) + "\n" + json.dumps({"task_id": "t1", "state": "incomplete"}) + "\n")
            (finalizer_dir / "repair_tasks.jsonl").write_text(json.dumps({"task_id": "t1"}) + "\n")
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.Session", side_effect=AssertionError("AWS should not be contacted")), contextlib.redirect_stdout(out):
                self.assertEqual(main(["status", "run-1", "--artifact-dir", str(artifact_dir)]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["run"]["run_id"], "run-1")
        self.assertEqual(report["run"]["status"], "repair_needed")
        self.assertEqual(report["run"]["production_task_count"], 2)
        self.assertEqual(report["run"]["task_status"]["total"], 2)
        self.assertEqual(report["run"]["task_status"]["by_status"], {"done": 1, "incomplete": 1})
        self.assertEqual(report["run"]["repair_task_count"], 1)
        self.assertIsNone(report["identity"])

    def test_status_run_id_ignores_queue_url_env_for_local_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "artifacts" / "run-1"
            artifact_dir.mkdir(parents=True)
            (artifact_dir / "run_state.json").write_text(json.dumps({"schema": "sweetspot.run.v1", "run_id": "run-1"}) + "\n")
            out = io.StringIO()
            with (
                patch.dict(os.environ, {"SWEETSPOT_SQS_QUEUE_URL": "env-queue"}),
                patch("sweetspot.cli.boto3.Session", side_effect=AssertionError("AWS should not be contacted")),
                contextlib.redirect_stdout(out),
            ):
                self.assertEqual(main(["status", "run-1", "--artifact-dir", str(artifact_dir)]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["queues"], {})
        self.assertIsNone(report["identity"])

    def test_status_run_id_defaults_batch_prefix_to_run_id(self) -> None:
        class FakeSTS:
            def get_caller_identity(self):
                return {"Account": "123", "Arn": "arn", "UserId": "u"}

        class FakePaginator:
            def paginate(self, **kwargs):
                if kwargs["jobStatus"] == "RUNNING":
                    return [{"jobSummaryList": [{"jobId": "j1", "jobName": "run-1-worker", "status": "RUNNING"}]}]
                return [{"jobSummaryList": []}]

        class FakeBatch:
            def get_paginator(self, name):
                return FakePaginator()

        class FakeSession:
            region_name = "us-west-2"

            def __init__(self, profile_name=None, region_name=None):
                self.region_name = region_name

            def client(self, service, region_name=None):
                if service == "sts":
                    return FakeSTS()
                if service == "batch":
                    return FakeBatch()
                raise AssertionError(service)

        with tempfile.TemporaryDirectory() as tmp:
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.Session", FakeSession), contextlib.redirect_stdout(out):
                self.assertEqual(main(["status", "run-1", "--artifact-dir", str(Path(tmp) / "missing"), "--job-queue", "jq"]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["batch"]["job_name_prefix"], "run-1-")
        self.assertEqual(report["batch"]["active_count"], 1)

    def test_status_artifact_only_uses_inferred_run_id_for_batch_prefix(self) -> None:
        class FakeSTS:
            def get_caller_identity(self):
                return {"Account": "123", "Arn": "arn", "UserId": "u"}

        class FakePaginator:
            def paginate(self, **kwargs):
                if kwargs["jobStatus"] == "RUNNING":
                    return [{"jobSummaryList": [{"jobId": "j1", "jobName": "run-1-worker", "status": "RUNNING"}]}]
                return [{"jobSummaryList": []}]

        class FakeBatch:
            def get_paginator(self, name):
                return FakePaginator()

        class FakeSession:
            region_name = "us-west-2"

            def __init__(self, profile_name=None, region_name=None):
                self.region_name = region_name

            def client(self, service, region_name=None):
                if service == "sts":
                    return FakeSTS()
                if service == "batch":
                    return FakeBatch()
                raise AssertionError(service)

        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "artifacts" / "run-1"
            artifact_dir.mkdir(parents=True)
            (artifact_dir / "run_state.json").write_text(json.dumps({"schema": "sweetspot.run.v1", "run_id": "run-1"}) + "\n")
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.Session", FakeSession), contextlib.redirect_stdout(out):
                self.assertEqual(main(["status", "--artifact-dir", str(artifact_dir), "--job-queue", "jq"]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["batch"]["job_name_prefix"], "run-1-")
        self.assertEqual(report["batch"]["active_count"], 1)

    def test_status_treats_mixed_success_names_as_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "artifacts" / "run-1"
            artifact_dir.mkdir(parents=True)
            (artifact_dir / "task_status.jsonl").write_text(json.dumps({"task_id": "t0", "state": "done"}) + "\n" + json.dumps({"task_id": "t1", "status": "completed"}) + "\n")
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.Session", side_effect=AssertionError("AWS should not be contacted")), contextlib.redirect_stdout(out):
                self.assertEqual(main(["status", "run-1", "--artifact-dir", str(artifact_dir)]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["run"]["status"], "complete")

    def test_status_marks_empty_status_for_expected_tasks_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "artifacts" / "run-1"
            artifact_dir.mkdir(parents=True)
            (artifact_dir / "production_tasks.jsonl").write_text(json.dumps({"task_id": "t0"}) + "\n")
            (artifact_dir / "task_status.jsonl").write_text("")
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.Session", side_effect=AssertionError("AWS should not be contacted")), contextlib.redirect_stdout(out):
                self.assertEqual(main(["status", "run-1", "--artifact-dir", str(artifact_dir)]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["run"]["status"], "incomplete")
        self.assertEqual(report["run"]["missing_task_status_count"], 1)

    def test_status_does_not_mark_partial_status_coverage_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "artifacts" / "run-1"
            artifact_dir.mkdir(parents=True)
            (artifact_dir / "production_tasks.jsonl").write_text(json.dumps({"task_id": "t0"}) + "\n" + json.dumps({"task_id": "t1"}) + "\n")
            (artifact_dir / "task_status.jsonl").write_text(json.dumps({"task_id": "t0", "state": "done"}) + "\n")
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.Session", side_effect=AssertionError("AWS should not be contacted")), contextlib.redirect_stdout(out):
                self.assertEqual(main(["status", "run-1", "--artifact-dir", str(artifact_dir)]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["run"]["status"], "incomplete")
        self.assertEqual(report["run"]["missing_task_status_count"], 1)

    def test_status_does_not_mark_duplicate_status_rows_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "artifacts" / "run-1"
            artifact_dir.mkdir(parents=True)
            (artifact_dir / "production_tasks.jsonl").write_text(json.dumps({"task_id": "t0"}) + "\n" + json.dumps({"task_id": "t1"}) + "\n")
            (artifact_dir / "task_status.jsonl").write_text(json.dumps({"task_id": "t0", "state": "done"}) + "\n" + json.dumps({"task_id": "t0", "state": "done"}) + "\n")
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.Session", side_effect=AssertionError("AWS should not be contacted")), contextlib.redirect_stdout(out):
                self.assertEqual(main(["status", "run-1", "--artifact-dir", str(artifact_dir)]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["run"]["status"], "invalid_artifacts")
        self.assertEqual(report["run"]["missing_task_status_count"], 1)
        self.assertEqual(report["run"]["task_status"]["duplicate_task_status_count"], 1)

    def test_status_does_not_mark_duplicate_production_tasks_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "artifacts" / "run-1"
            artifact_dir.mkdir(parents=True)
            (artifact_dir / "production_tasks.jsonl").write_text(json.dumps({"task_id": "t0"}) + "\n" + json.dumps({"task_id": "t0"}) + "\n")
            (artifact_dir / "task_status.jsonl").write_text(json.dumps({"task_id": "t0", "state": "done"}) + "\n")
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.Session", side_effect=AssertionError("AWS should not be contacted")), contextlib.redirect_stdout(out):
                self.assertEqual(main(["status", "run-1", "--artifact-dir", str(artifact_dir)]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["run"]["status"], "invalid_artifacts")
        self.assertEqual(report["run"]["duplicate_production_task_count"], 1)

    def test_status_does_not_mark_unknown_status_tasks_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "artifacts" / "run-1"
            artifact_dir.mkdir(parents=True)
            (artifact_dir / "production_tasks.jsonl").write_text(json.dumps({"task_id": "t0"}) + "\n")
            (artifact_dir / "task_status.jsonl").write_text(json.dumps({"task_id": "t0", "state": "done"}) + "\n" + json.dumps({"task_id": "extra", "state": "done"}) + "\n")
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.Session", side_effect=AssertionError("AWS should not be contacted")), contextlib.redirect_stdout(out):
                self.assertEqual(main(["status", "run-1", "--artifact-dir", str(artifact_dir)]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["run"]["status"], "invalid_artifacts")
        self.assertEqual(report["run"]["task_status"]["unknown_task_status_count"], 1)

    def test_status_run_id_rejects_non_scoped_job_prefix(self) -> None:
        with self.assertRaisesRegex(SystemExit, "must start with RUN_ID"):
            main(["status", "run-1", "--job-name-prefix", "other"])

    def test_status_run_id_rejects_prefix_collision(self) -> None:
        with self.assertRaisesRegex(SystemExit, "must start with RUN_ID"):
            main(["status", "run-1", "--job-name-prefix", "run-10-"])

    def test_status_run_id_rejects_bare_run_id_prefix(self) -> None:
        with self.assertRaisesRegex(SystemExit, "must start with RUN_ID"):
            main(["status", "run-1", "--job-name-prefix", "run-1"])


class HelpExamplesTests(unittest.TestCase):
    def test_high_traffic_help_includes_examples(self) -> None:
        out = io.StringIO()
        with patch.object(sys, "argv", ["sweetspot", "enqueue-and-submit", "--help"]), contextlib.redirect_stdout(out):
            with self.assertRaises(SystemExit) as cm:
                main()
        self.assertEqual(cm.exception.code, 0)
        help_text = out.getvalue()
        self.assertIn("examples:", help_text)
        self.assertIn("sweetspot enqueue-and-submit", help_text)


class QueueAliasTests(unittest.TestCase):
    def test_enqueue_jsonl_accepts_sqs_queue_url_alias(self) -> None:
        task = {"schema": "sweetspot.task.v1", "run_id": "r1", "task_id": "t0", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://bucket/runs/r1/done/t0.done.json"}
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            tasks_path.write_text(json.dumps(task) + "\n")
            argv = [
                "sweetspot",
                "enqueue-jsonl",
                "--sqs-queue-url",
                "alias-queue",
                "--tasks-jsonl",
                str(tasks_path),
                "--allowed-s3-prefix",
                "s3://bucket/runs/r1",
            ]
            out = io.StringIO()
            with patch.object(sys, "argv", argv), contextlib.redirect_stdout(out):
                self.assertEqual(main(), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["queue_url"], "alias-queue")

    def test_enqueue_jsonl_config_accepts_sqs_queue_url_key(self) -> None:
        task = {"schema": "sweetspot.task.v1", "run_id": "r1", "task_id": "t0", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://bucket/runs/r1/done/t0.done.json"}
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            tasks_path.write_text(json.dumps(task) + "\n")
            config_path = Path(tmp) / "sweetspot.json"
            config_path.write_text(json.dumps({"enqueue-jsonl": {"sqs_queue_url": "configured-queue", "allowed_s3_prefix": ["s3://bucket/runs/r1"]}}))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(main(["--config", str(config_path), "enqueue-jsonl", "--tasks-jsonl", str(tasks_path)]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["queue_url"], "configured-queue")

    def test_enqueue_and_submit_accepts_sqs_queue_url_alias(self) -> None:
        task = {"schema": "sweetspot.task.v1", "run_id": "r1", "task_id": "t0", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://bucket/runs/r1/done/t0.done.json"}
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            tasks_path.write_text(json.dumps(task) + "\n")
            sqs = FakePartialVisibleSQS(visible=0)
            batch = FakeSubmitBatch()

            def fake_client(service):
                if service == "sqs":
                    return sqs
                if service == "batch":
                    return batch
                raise AssertionError(service)

            argv = [
                "sweetspot",
                "enqueue-and-submit",
                "--sqs-queue-url",
                "alias-queue",
                "--tasks-jsonl",
                str(tasks_path),
                "--batch-job-queue",
                "jq",
                "--job-definition",
                "jd",
                "--allowed-s3-prefix",
                "s3://bucket/runs/r1",
            ]
            out = io.StringIO()
            with patch.object(sys, "argv", argv), patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(out):
                self.assertEqual(main(), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["queue_url"], "alias-queue")

    def test_enqueue_and_submit_config_accepts_sqs_queue_url_key(self) -> None:
        task = {"schema": "sweetspot.task.v1", "run_id": "r1", "task_id": "t0", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://bucket/runs/r1/done/t0.done.json"}
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            tasks_path.write_text(json.dumps(task) + "\n")
            config_path = Path(tmp) / "sweetspot.json"
            config_path.write_text(
                json.dumps(
                    {
                        "enqueue-and-submit": {
                            "sqs_queue_url": "configured-queue",
                            "allowed_s3_prefix": ["s3://bucket/runs/r1"],
                            "batch_job_queue": "jq",
                            "job_definition": "jd",
                        }
                    }
                )
            )
            sqs = FakePartialVisibleSQS(visible=0)
            batch = FakeSubmitBatch()

            def fake_client(service):
                if service == "sqs":
                    return sqs
                if service == "batch":
                    return batch
                raise AssertionError(service)

            out = io.StringIO()
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(out):
                self.assertEqual(main(["--config", str(config_path), "enqueue-and-submit", "--tasks-jsonl", str(tasks_path)]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["queue_url"], "configured-queue")

    def test_submit_workers_accepts_queue_url_alias(self) -> None:
        class FakeSQS:
            def get_queue_attributes(self, **kwargs):
                self.queue_url = kwargs["QueueUrl"]
                return {
                    "Attributes": {
                        "ApproximateNumberOfMessages": "3",
                        "ApproximateNumberOfMessagesNotVisible": "0",
                        "ApproximateNumberOfMessagesDelayed": "0",
                    }
                }

        sqs = FakeSQS()

        def fake_client(service):
            if service == "sqs":
                return sqs
            if service == "batch":
                return object()
            raise AssertionError(service)

        argv = [
            "sweetspot",
            "submit-workers",
            "--queue-url",
            "alias-queue",
            "--batch-job-queue",
            "jq",
            "--job-definition",
            "jd",
        ]
        out = io.StringIO()
        with patch.object(sys, "argv", argv), patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(out):
            self.assertEqual(main(), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(sqs.queue_url, "alias-queue")
        self.assertEqual(report["backlog_used_for_sizing"], 3)


class CanaryTests(unittest.TestCase):
    def test_explicit_descending_canary_range_is_rejected(self) -> None:
        with self.assertRaisesRegex(SystemExit, "descending"):
            _parse_index_selection("5-3", 10)

    def test_auto_canary_indices_are_deterministic_and_include_tail(self) -> None:
        tasks = [{"task_id": f"t{i}", "schema": "sweetspot.task.v1", "run_id": "r"} for i in range(8)]
        self.assertEqual(_auto_canary_indices(tasks, 3), [0, 7, 4])

    def test_derive_canary_rejects_overwriting_any_generated_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            tasks_path = out_dir / "canary_manifest.json"
            tasks_path.write_text(json.dumps({"task_id": "t0"}) + "\n")
            args = types.SimpleNamespace(
                tasks_jsonl=tasks_path,
                out_dir=out_dir,
                run_id="r",
                selected_indices="0",
                task_count=1,
                rewrite_run_id=False,
                include_dlq_probe=False,
            )
            with self.assertRaisesRegex(SystemExit, "overwrite"):
                cmd_derive_canary(args)
            self.assertEqual(tasks_path.read_text(), json.dumps({"task_id": "t0"}) + "\n")

    def test_derive_canary_rejects_overwriting_source_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            tasks_path = out_dir / "canary_tasks.jsonl"
            tasks_path.write_text(json.dumps({"task_id": "t0"}) + "\n")
            args = types.SimpleNamespace(
                tasks_jsonl=tasks_path,
                out_dir=out_dir,
                run_id="r",
                selected_indices="0",
                task_count=1,
                rewrite_run_id=False,
                include_dlq_probe=False,
            )
            with self.assertRaisesRegex(SystemExit, "overwrite"):
                cmd_derive_canary(args)
            self.assertEqual(tasks_path.read_text(), json.dumps({"task_id": "t0"}) + "\n")

    def test_derive_canary_dlq_probe_uses_preserved_single_source_run_id(self) -> None:
        tasks = [{"schema": "sweetspot.task.v1", "run_id": "source-r", "task_id": "t0"}]
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            out_dir = Path(tmp) / "canary"
            tasks_path.write_text("".join(json.dumps(t) + "\n" for t in tasks))
            args = types.SimpleNamespace(
                tasks_jsonl=tasks_path,
                out_dir=out_dir,
                run_id="requested-r",
                selected_indices="0",
                task_count=1,
                rewrite_run_id=False,
                include_dlq_probe=True,
                dlq_probe_prefix="s3://b/r/dlq-probes",
            )
            with contextlib.redirect_stdout(io.StringIO()):
                cmd_derive_canary(args)
            manifest = json.loads((out_dir / "canary_manifest.json").read_text())
            probe = json.loads((out_dir / "dlq_probe_task.jsonl").read_text())
            self.assertEqual(manifest["run_id"], "source-r")
            self.assertEqual(probe["run_id"], "source-r")
            self.assertEqual(probe["done_s3"], "s3://b/r/dlq-probes/source-r-intentional-dlq-probe.done.json")
            validate_task_model(probe, default_timeout_seconds=1800, max_timeout_seconds=43200)

    def test_derive_canary_rewrite_run_id_rejects_existing_s3_markers(self) -> None:
        tasks = [{"schema": "sweetspot.task.v1", "run_id": "r", "task_id": "t0", "output_s3": "s3://b/r/shards/t0"}]
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            out_dir = Path(tmp) / "canary"
            tasks_path.write_text("".join(json.dumps(t) + "\n" for t in tasks))
            args = types.SimpleNamespace(
                tasks_jsonl=tasks_path,
                out_dir=out_dir,
                run_id="canary-r",
                selected_indices="0",
                task_count=1,
                rewrite_run_id=True,
                include_dlq_probe=False,
            )
            with self.assertRaisesRegex(SystemExit, "explicit output_s3"):
                cmd_derive_canary(args)

    def test_derive_canary_writes_manifest_and_dlq_probe(self) -> None:
        tasks = [
            {"schema": "sweetspot.task.v1", "run_id": "r", "task_id": "t0", "output_s3": "s3://b/r/shards/t0"},
            {"schema": "sweetspot.task.v1", "run_id": "r", "task_id": "t1", "done_s3": "s3://b/r/done/t1"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            out_dir = Path(tmp) / "canary"
            tasks_path.write_text("".join(json.dumps(t) + "\n" for t in tasks))
            args = types.SimpleNamespace(
                tasks_jsonl=tasks_path,
                out_dir=out_dir,
                run_id="canary-r",
                selected_indices="1",
                task_count=1,
                rewrite_run_id=False,
                include_dlq_probe=True,
            )
            with contextlib.redirect_stdout(io.StringIO()):
                rc = cmd_derive_canary(args)
            self.assertEqual(rc, 0)
            canary_task = json.loads((out_dir / "canary_tasks.jsonl").read_text())
            self.assertEqual(canary_task["run_id"], "r")
            manifest = json.loads((out_dir / "canary_manifest.json").read_text())
            self.assertEqual(manifest["selected_indices"], [1])
            self.assertEqual(manifest["expected_done_s3"], ["s3://b/r/done/t1"])
            probe = json.loads((out_dir / "dlq_probe_task.jsonl").read_text())
            self.assertEqual(probe["done_s3"], "s3://b/r/done/r-intentional-dlq-probe.done.json")
            validate_task_model(probe, default_timeout_seconds=1800, max_timeout_seconds=43200)


class EnqueueValidationTests(unittest.TestCase):
    def test_enqueue_rejects_task_outside_allowed_s3_prefix(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "r1",
            "task_id": "t0",
            "command": [sys.executable, "-c", "pass"],
            "done_s3": "s3://other/runs/r1/done/t0.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            tasks_path.write_text(json.dumps(task) + "\n")
            args = types.SimpleNamespace(queue_url="", tasks_jsonl=tasks_path, run_id=None, artifact_dir=Path(tmp) / "artifacts", allowed_s3_prefix=["s3://bucket/runs/r1"], submit=False)
            with self.assertRaisesRegex(SystemExit, "outside allowed prefixes"):
                cmd_enqueue_jsonl(args)

    def test_enqueue_rejects_exact_object_equal_to_allowed_prefix(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "r1",
            "task_id": "t0",
            "command": [sys.executable, "-c", "pass"],
            "done_s3": "s3://bucket/runs/r1",
        }
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            tasks_path.write_text(json.dumps(task) + "\n")
            args = types.SimpleNamespace(queue_url="", tasks_jsonl=tasks_path, run_id=None, artifact_dir=Path(tmp) / "artifacts", allowed_s3_prefix=["s3://bucket/runs/r1"], submit=False)
            with self.assertRaisesRegex(SystemExit, "outside allowed prefixes"):
                cmd_enqueue_jsonl(args)

    def test_enqueue_rejects_duplicate_task_ids(self) -> None:
        tasks = [
            {"schema": "sweetspot.task.v1", "run_id": "r1", "task_id": "dup", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://bucket/runs/r1/done/dup-a.done.json"},
            {"schema": "sweetspot.task.v1", "run_id": "r1", "task_id": "dup", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://bucket/runs/r1/done/dup-b.done.json"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            tasks_path.write_text("".join(json.dumps(t) + "\n" for t in tasks))
            args = types.SimpleNamespace(queue_url="", tasks_jsonl=tasks_path, run_id=None, artifact_dir=Path(tmp) / "artifacts", allowed_s3_prefix=["s3://bucket/runs/r1"], submit=False)
            with self.assertRaisesRegex(SystemExit, "duplicate task_id"):
                cmd_enqueue_jsonl(args)

    def test_enqueue_uses_profile_region_session_when_supplied(self) -> None:
        class FakeSQS:
            def send_message_batch(self, **kwargs):
                return {"Successful": kwargs["Entries"]}

        class FakeSession:
            def __init__(self, profile_name=None, region_name=None):
                self.profile_name = profile_name
                self.region_name = region_name
                sessions.append(self)

            def client(self, service, region_name=None):
                client_calls.append((service, region_name))
                return FakeSQS()

        sessions = []
        client_calls = []
        task = {"schema": "sweetspot.task.v1", "run_id": "r1", "task_id": "t0", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://bucket/runs/r1/done/t0.done.json"}
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            tasks_path.write_text(json.dumps(task) + "\n")
            args = types.SimpleNamespace(
                profile="prof",
                region="us-west-2",
                queue_url="q",
                tasks_jsonl=tasks_path,
                run_id=None,
                artifact_dir=Path(tmp) / "artifacts",
                allowed_s3_prefix=["s3://bucket/runs/r1"],
                submit=True,
            )
            with patch("sweetspot.cli.boto3.Session", FakeSession):
                self.assertEqual(cmd_enqueue_jsonl(args), 0)
        self.assertEqual([(s.profile_name, s.region_name) for s in sessions], [("prof", "us-west-2")])
        self.assertEqual(client_calls, [("sqs", "us-west-2")])

    def test_enqueue_reports_sqs_batch_failure_without_traceback(self) -> None:
        class FailingSQS:
            def send_message_batch(self, **kwargs):
                return {"Failed": [{"Id": "0", "Code": "AccessDenied"}]}

        task = {"schema": "sweetspot.task.v1", "run_id": "r1", "task_id": "t0", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://bucket/runs/r1/done/t0.done.json"}
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            tasks_path.write_text(json.dumps(task) + "\n")
            args = types.SimpleNamespace(
                profile=None,
                region=None,
                queue_url="q",
                tasks_jsonl=tasks_path,
                run_id=None,
                artifact_dir=Path(tmp) / "artifacts",
                allowed_s3_prefix=["s3://bucket/runs/r1"],
                submit=True,
            )
            with patch("sweetspot.cli.boto3.client", return_value=FailingSQS()), self.assertRaisesRegex(SystemExit, "send_message_batch failed"):
                cmd_enqueue_jsonl(args)

    def test_worker_overrides_pass_allowed_s3_prefixes(self) -> None:
        overrides = _worker_overrides(
            sqs_queue_url="https://sqs.example/q",
            messages_per_worker=1,
            visibility_timeout=1800,
            heartbeat_seconds=300,
            task_timeout_seconds=3600,
            env=[],
            allowed_s3_prefixes=["s3://bucket/runs/r1/", "s3://bucket/runs/r2"],
            vcpus=None,
            memory=None,
        )
        env = {row["name"]: row["value"] for row in overrides["environment"]}
        self.assertEqual(env["SWEETSPOT_ALLOWED_S3_PREFIXES"], "s3://bucket/runs/r1,s3://bucket/runs/r2")

    def test_worker_overrides_pass_resource_shape_telemetry_env(self) -> None:
        overrides = _worker_overrides(
            sqs_queue_url="https://sqs.example/q",
            messages_per_worker=1,
            visibility_timeout=1800,
            heartbeat_seconds=300,
            task_timeout_seconds=3600,
            env=[],
            allowed_s3_prefixes=[],
            vcpus=4,
            memory=8192,
        )
        env = {row["name"]: row["value"] for row in overrides["environment"]}
        self.assertEqual(env["SWEETSPOT_WORKER_VCPUS"], "4")
        self.assertEqual(env["SWEETSPOT_WORKER_MEMORY_MIB"], "8192")
        self.assertEqual(overrides["vcpus"], 4)
        self.assertEqual(overrides["memory"], 8192)

    def test_worker_overrides_pass_observability_controls(self) -> None:
        overrides = _worker_overrides(
            sqs_queue_url="https://sqs.example/q",
            messages_per_worker=1,
            visibility_timeout=1800,
            heartbeat_seconds=300,
            task_timeout_seconds=3600,
            env=[],
            allowed_s3_prefixes=[],
            log_tail_bytes=123,
            max_log_bytes=456,
            redact_regexes=["token=[^ ]+"],
        )
        env = {row["name"]: row["value"] for row in overrides["environment"]}
        self.assertEqual(env["SWEETSPOT_LOG_TAIL_BYTES"], "123")
        self.assertEqual(env["SWEETSPOT_MAX_LOG_BYTES"], "456")
        self.assertEqual(env["SWEETSPOT_REDACT_REGEXES"], "token=[^ ]+")


class FakeQueueDepthSQS:
    def __init__(self) -> None:
        self.sent = 0
        self.depth_calls = 0

    def send_message_batch(self, *, QueueUrl, Entries):
        self.sent += len(Entries)
        return {"Successful": [{"Id": e["Id"]} for e in Entries]}

    def get_queue_attributes(self, **kwargs):
        self.depth_calls += 1
        visible = 0 if self.depth_calls <= 2 else self.sent
        return {"Attributes": {"ApproximateNumberOfMessages": str(visible), "ApproximateNumberOfMessagesNotVisible": "0", "ApproximateNumberOfMessagesDelayed": "0"}}


class FakePartialVisibleSQS:
    def __init__(self, visible: int) -> None:
        self.visible = visible
        self.sent = 0
        self.depth_calls = 0

    def send_message_batch(self, *, QueueUrl, Entries):
        self.sent += len(Entries)
        return {"Successful": [{"Id": e["Id"]} for e in Entries]}

    def get_queue_attributes(self, **kwargs):
        self.depth_calls += 1
        visible = 0 if self.depth_calls == 1 else self.visible
        return {"Attributes": {"ApproximateNumberOfMessages": str(visible), "ApproximateNumberOfMessagesNotVisible": "0", "ApproximateNumberOfMessagesDelayed": "0"}}


class FakeSubmitBatch:
    def __init__(self) -> None:
        self.submitted: list[dict[str, object]] = []

    def submit_job(self, **kwargs):
        job_id = f"job-{len(self.submitted)}"
        self.submitted.append(kwargs)
        return {"jobId": job_id, "jobArn": f"arn:{job_id}"}


class FakeCleanupSQS:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    def receive_message(self, **kwargs):
        if self.deleted:
            return {"Messages": []}
        task = {"schema": "sweetspot.task.v1", "run_id": "r", "task_id": "t0", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://b/r/t0"}
        return {"Messages": [{"Body": json.dumps(task), "ReceiptHandle": "rh"}]}

    def delete_message(self, **kwargs):
        self.deleted.append(kwargs["ReceiptHandle"])


class RuntimeAndRepairTests(unittest.TestCase):
    def test_extract_task_id_from_json_log_message(self) -> None:
        self.assertEqual(_extract_task_id_from_log_message(json.dumps({"event": "message_received", "task_id": "t1"})), "t1")
        self.assertIsNone(_extract_task_id_from_log_message("not json"))

    def test_sample_from_runtime_obj_reads_telemetry(self) -> None:
        self.assertEqual(_sample_from_runtime_obj({"telemetry": {"completed_units": 100, "useful_compute_seconds": 25}}), (100.0, 25.0))

    def test_estimate_runtime_warns_when_task_exceeds_timeout_budget(self) -> None:
        args = types.SimpleNamespace(
            sample_jsonl=[],
            completed_units=1000,
            elapsed_seconds=100,
            target_units=None,
            task_count=10,
            units_per_task=1000,
            active_workers=2,
            vcpus_per_worker=2,
            price_per_vcpu_hour=0.05,
            task_timeout_seconds=60,
            timeout_safety_fraction=0.8,
            spot=True,
            max_spot_task_seconds=30,
        )
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(cmd_estimate_runtime(args), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["sample_count"], 1)
        self.assertGreaterEqual(len(report["warnings"]), 2)

    def test_cleanup_stale_messages_deletes_done_messages_when_applied(self) -> None:
        sqs = FakeCleanupSQS()

        def fake_client(service):
            if service == "sqs":
                return sqs
            if service == "s3":
                return object()
            raise AssertionError(service)

        args = types.SimpleNamespace(queue_url="q", run_id="r", max_messages=1, wait_time=0, visibility_timeout=5, allow_legacy_done_markers=False, apply=True)
        out = io.StringIO()
        with (
            patch("sweetspot.cli.boto3.client", side_effect=fake_client),
            patch("sweetspot.cli._check_task", return_value={"state": "done"}),
            contextlib.redirect_stdout(out),
        ):
            self.assertEqual(cmd_cleanup_stale_messages(args), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["deleted"], 1)
        self.assertEqual(sqs.deleted, ["rh"])

    def test_enqueue_and_submit_dry_run_sizes_from_tasks_that_would_be_sent(self) -> None:
        tasks = [{"schema": "sweetspot.task.v1", "run_id": "r", "task_id": f"t{i}", "command": [sys.executable, "-c", "pass"], "done_s3": f"s3://bucket/r/done/t{i}.done.json"} for i in range(100)]
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            tasks_path.write_text("".join(json.dumps(task) + "\n" for task in tasks))
            sqs = FakePartialVisibleSQS(visible=0)
            batch = FakeSubmitBatch()

            def fake_client(service):
                if service == "sqs":
                    return sqs
                if service == "batch":
                    return batch
                raise AssertionError(service)

            args = types.SimpleNamespace(
                queue_url="q",
                tasks_jsonl=tasks_path,
                run_id=None,
                artifact_dir=Path(tmp) / "artifacts",
                allowed_s3_prefix=["s3://bucket/r"],
                batch_job_queue="jq",
                job_definition="jd",
                job_name_prefix="worker",
                messages_per_worker=10,
                min_workers=0,
                max_workers=20,
                subtract_active=False,
                include_not_visible=False,
                vcpus=None,
                memory=None,
                visibility_timeout=1800,
                heartbeat_seconds=300,
                task_timeout_seconds=3600,
                retry_attempts=None,
                env=[],
                log_tail_bytes=100,
                max_log_bytes=1000,
                redact_regex=[],
                allow_legacy_done_markers=False,
                wait_for_visible_seconds=0,
                wait_for_visible_min=None,
                wait_interval_seconds=0.01,
                submit=False,
            )
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(out):
                self.assertEqual(cmd_enqueue_and_submit(args), 0)
            report = json.loads(out.getvalue())
            self.assertEqual(report["sent"], 0)
            self.assertEqual(report["simulated_sent_for_sizing"], 100)
            self.assertEqual(report["backlog_used_for_sizing"], 100)
            self.assertEqual(report["to_submit"], 10)
            self.assertEqual(report["submitted_count"], 0)

    def test_enqueue_and_submit_sizes_from_sent_count_when_sqs_depth_lags(self) -> None:
        tasks = [{"schema": "sweetspot.task.v1", "run_id": "r", "task_id": f"t{i}", "command": [sys.executable, "-c", "pass"], "done_s3": f"s3://bucket/r/done/t{i}.done.json"} for i in range(100)]
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            tasks_path.write_text("".join(json.dumps(task) + "\n" for task in tasks))
            sqs = FakePartialVisibleSQS(visible=10)
            batch = FakeSubmitBatch()

            def fake_client(service):
                if service == "sqs":
                    return sqs
                if service == "batch":
                    return batch
                raise AssertionError(service)

            args = types.SimpleNamespace(
                queue_url="q",
                tasks_jsonl=tasks_path,
                run_id=None,
                artifact_dir=Path(tmp) / "artifacts",
                allowed_s3_prefix=["s3://bucket/r"],
                batch_job_queue="jq",
                job_definition="jd",
                job_name_prefix="worker",
                messages_per_worker=10,
                min_workers=0,
                max_workers=20,
                subtract_active=False,
                include_not_visible=False,
                vcpus=None,
                memory=None,
                visibility_timeout=1800,
                heartbeat_seconds=300,
                task_timeout_seconds=3600,
                retry_attempts=None,
                env=[],
                log_tail_bytes=100,
                max_log_bytes=1000,
                redact_regex=[],
                allow_legacy_done_markers=False,
                wait_for_visible_seconds=0,
                wait_for_visible_min=None,
                wait_interval_seconds=0.01,
                submit=True,
            )
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(out):
                self.assertEqual(cmd_enqueue_and_submit(args), 0)
            report = json.loads(out.getvalue())
            self.assertEqual(report["sent"], 100)
            self.assertEqual(report["queue_depth"]["visible"], 10)
            self.assertEqual(report["backlog_floor_used_for_sizing"], 100)
            self.assertEqual(report["submitted_count"], 10)

    def test_enqueue_and_submit_waits_for_visible_depth(self) -> None:
        task = {"schema": "sweetspot.task.v1", "run_id": "r", "task_id": "t0", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://bucket/r/done/t0.done.json"}
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            tasks_path.write_text(json.dumps(task) + "\n")
            sqs = FakeQueueDepthSQS()
            batch = FakeSubmitBatch()

            def fake_client(service):
                if service == "sqs":
                    return sqs
                if service == "batch":
                    return batch
                raise AssertionError(service)

            args = types.SimpleNamespace(
                queue_url="q",
                tasks_jsonl=tasks_path,
                run_id=None,
                artifact_dir=Path(tmp) / "artifacts",
                allowed_s3_prefix=["s3://bucket/r"],
                batch_job_queue="jq",
                job_definition="jd",
                job_name_prefix="worker",
                messages_per_worker=1,
                min_workers=0,
                max_workers=10,
                subtract_active=False,
                include_not_visible=False,
                vcpus=None,
                memory=None,
                visibility_timeout=1800,
                heartbeat_seconds=300,
                task_timeout_seconds=3600,
                retry_attempts=None,
                env=[],
                log_tail_bytes=100,
                max_log_bytes=1000,
                redact_regex=[],
                allow_legacy_done_markers=False,
                wait_for_visible_seconds=1,
                wait_for_visible_min=None,
                wait_interval_seconds=0.01,
                submit=True,
            )
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.client", side_effect=fake_client), contextlib.redirect_stdout(out):
                self.assertEqual(cmd_enqueue_and_submit(args), 0)
            report = json.loads(out.getvalue())
            self.assertEqual(report["sent"], 1)
            self.assertEqual(report["submitted_count"], 1)
            self.assertGreaterEqual(len(report["wait_history"]), 2)


class FakeRepairPaginator:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, **kwargs):
        return self.pages.get(kwargs["jobStatus"], [])


class FakeRepairBatch:
    def get_paginator(self, name):
        self.assert_name = name
        return FakeRepairPaginator(
            {
                "RUNNING": [{"jobSummaryList": [{"jobId": "active", "jobName": "run-active-worker", "createdAt": 1}]}],
                "FAILED": [{"jobSummaryList": [{"jobId": "failed", "jobName": "run-failed-worker", "createdAt": 2}]}],
            }
        )

    def describe_jobs(self, *, jobs):
        out = []
        for job_id in jobs:
            out.append(
                {
                    "jobId": job_id,
                    "jobName": f"run-{job_id}-worker",
                    "status": "RUNNING" if job_id == "active" else "FAILED",
                    "container": {"logStreamName": job_id, "logConfiguration": {"options": {"awslogs-group": "lg"}}},
                }
            )
        return {"jobs": out}


class FakeRepairLogs:
    def filter_log_events(self, **kwargs):
        stream = kwargs["logStreamNames"][0]
        task_id = "t0" if stream == "active" else "t1"
        return {"events": [{"message": json.dumps({"event": "message_received", "task_id": task_id})}]}

    def get_log_events(self, **kwargs):
        stream = kwargs["logStreamName"]
        task_id = "t0" if stream == "active" else "t1"
        return {"events": [{"message": json.dumps({"event": "message_received", "task_id": task_id})}]}


class FakePaginatedRepairLogs:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def filter_log_events(self, **kwargs):
        self.calls.append(kwargs)
        if "nextToken" not in kwargs:
            return {"events": [], "nextToken": "page-2"}
        return {"events": [{"message": json.dumps({"event": "message_received", "task_id": "t0"})}]}

    def get_log_events(self, **kwargs):
        raise AssertionError("repair-plan should use FilterLogEvents before fallback")


class FakeRepairSession:
    def client(self, service: str, region_name=None):
        if service == "batch":
            return FakeRepairBatch()
        if service == "logs":
            return FakeRepairLogs()
        raise AssertionError(service)


class FakeRepairApplySQS:
    def __init__(self) -> None:
        self.sent_messages: list[str] = []

    def send_message_batch(self, *, QueueUrl: str, Entries: list[dict[str, str]]):
        self.sent_messages.extend(entry["MessageBody"] for entry in Entries)
        return {"Successful": [{"Id": entry["Id"]} for entry in Entries]}

    def get_queue_attributes(self, *, QueueUrl: str, AttributeNames: list[str]):
        return {
            "Attributes": {
                "ApproximateNumberOfMessages": str(len(self.sent_messages)),
                "ApproximateNumberOfMessagesNotVisible": "0",
                "ApproximateNumberOfMessagesDelayed": "0",
            }
        }


class FakeRepairApplyBatch(FakeRepairBatch):
    def __init__(self) -> None:
        self.submitted: list[dict[str, object]] = []

    def submit_job(self, **kwargs):
        self.submitted.append(kwargs)
        return {"jobId": f"submitted-{len(self.submitted)}", "jobArn": f"arn:aws:batch:::job/submitted-{len(self.submitted)}"}


class FakeRepairApplySession:
    def __init__(self, sqs: FakeRepairApplySQS, batch: FakeRepairApplyBatch) -> None:
        self.sqs = sqs
        self.batch = batch
        self.logs = FakeRepairLogs()

    def client(self, service: str, region_name=None):
        if service == "batch":
            return self.batch
        if service == "logs":
            return self.logs
        if service == "sqs":
            return self.sqs
        raise AssertionError(service)


class FakeCancelPaginator:
    def __init__(self, jobs_by_status):
        self.jobs_by_status = jobs_by_status

    def paginate(self, **kwargs):
        yield {"jobSummaryList": self.jobs_by_status.get(kwargs["jobStatus"], [])}


class FakeCancelBatch:
    def __init__(self) -> None:
        self.jobs_by_status = {
            "PENDING": [{"jobId": "pending-1", "jobName": "run-pending-worker"}],
            "RUNNING": [{"jobId": "running-1", "jobName": "run-running-worker"}],
        }
        self.cancelled: list[dict[str, str]] = []
        self.terminated: list[dict[str, str]] = []

    def get_paginator(self, name):
        self.assert_name = name
        return FakeCancelPaginator(self.jobs_by_status)

    def describe_jobs(self, *, jobs):
        status_by_id = {"pending-1": "PENDING", "running-1": "RUNNING"}
        return {"jobs": [{"jobId": job_id, "jobName": f"run-{job_id.split('-')[0]}-worker", "jobQueue": "jq", "status": status_by_id[job_id]} for job_id in jobs]}

    def cancel_job(self, *, jobId: str, reason: str) -> None:
        self.cancelled.append({"jobId": jobId, "reason": reason})

    def terminate_job(self, *, jobId: str, reason: str) -> None:
        self.terminated.append({"jobId": jobId, "reason": reason})


class FakeCancelSession:
    def __init__(self, batch: FakeCancelBatch) -> None:
        self.batch = batch

    def client(self, service: str, region_name=None):
        if service == "batch":
            return self.batch
        raise AssertionError(service)


class CancelJobsTests(unittest.TestCase):
    def test_cancel_jobs_dry_run_lists_without_mutating(self) -> None:
        batch = FakeCancelBatch()
        args = types.SimpleNamespace(
            profile=None,
            region=None,
            job_queue=["jq"],
            status=["PENDING", "RUNNING"],
            job_name_regex="run-",
            max_jobs=10,
            apply=False,
            terminate_running=False,
            reason="test reason",
        )
        out = io.StringIO()
        with patch("sweetspot.cli.boto3.Session", return_value=FakeCancelSession(batch)), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_cancel_jobs(args), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["matched_count"], 2)
        self.assertEqual(report["actionable_count"], 1)
        self.assertEqual(report["cancelled_count"], 0)
        self.assertEqual(report["terminated_count"], 0)
        self.assertEqual(report["skipped_count"], 1)
        self.assertEqual(batch.cancelled, [])
        self.assertEqual(batch.terminated, [])

    def test_cancel_jobs_table_output_lists_matches(self) -> None:
        batch = FakeCancelBatch()
        args = types.SimpleNamespace(
            profile=None,
            region=None,
            job_queue=["jq"],
            status=["PENDING", "RUNNING"],
            job_name_regex="run-",
            max_jobs=10,
            apply=False,
            terminate_running=False,
            reason="test reason",
            format="table",
        )
        out = io.StringIO()
        with patch("sweetspot.cli.boto3.Session", return_value=FakeCancelSession(batch)), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_cancel_jobs(args), 0)
        table = out.getvalue()
        self.assertIn("SweetSpot cancel-jobs", table)
        self.assertIn("matched_count\t2", table)
        self.assertIn("jobs", table)
        self.assertIn("pending-1\trun-pending-worker\tjq\tPENDING\tcancel", table)
        self.assertIn("running-1\trun-running-worker\tjq\tRUNNING\tskip\trequires --terminate-running", table)

    def test_cancel_jobs_apply_cancels_and_terminates_only_when_requested(self) -> None:
        batch = FakeCancelBatch()
        args = types.SimpleNamespace(
            profile=None,
            region=None,
            job_queue=["jq"],
            status=None,
            job_name_regex="run-",
            max_jobs=10,
            apply=True,
            terminate_running=True,
            reason="test reason",
        )
        out = io.StringIO()
        with patch("sweetspot.cli.boto3.Session", return_value=FakeCancelSession(batch)), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_cancel_jobs(args), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["statuses"], ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"])
        self.assertEqual(report["cancelled_count"], 1)
        self.assertEqual(report["terminated_count"], 1)
        self.assertEqual(batch.cancelled, [{"jobId": "pending-1", "reason": "test reason"}])
        self.assertEqual(batch.terminated, [{"jobId": "running-1", "reason": "test reason"}])

    def test_cancel_dry_run_matches_run_scoped_job_prefix(self) -> None:
        batch = FakeCancelBatch()
        args = types.SimpleNamespace(
            run_id="run",
            profile=None,
            region=None,
            job_queue=["jq"],
            status=["PENDING", "RUNNING"],
            job_name_prefix=None,
            max_jobs=10,
            apply=False,
            terminate_running=False,
            reason=None,
        )
        out = io.StringIO()
        with patch("sweetspot.cli.boto3.Session", return_value=FakeCancelSession(batch)), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_cancel(args), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["schema"], "sweetspot.cancel.v1")
        self.assertEqual(report["run_id"], "run")
        self.assertEqual(report["job_name_regex"], "^run(?:-|$)")
        self.assertEqual(report["matched_count"], 2)
        self.assertEqual(report["actionable_count"], 1)
        self.assertFalse(report["apply"])
        self.assertEqual(batch.cancelled, [])
        self.assertEqual(batch.terminated, [])

    def test_cancel_apply_can_terminate_run_scoped_workers(self) -> None:
        batch = FakeCancelBatch()
        batch.jobs_by_status = {
            "PENDING": [{"jobId": "pending-1", "jobName": "run-workers-20260624-0000"}],
            "RUNNING": [{"jobId": "running-1", "jobName": "run-workers-20260624-0001"}],
        }
        args = types.SimpleNamespace(
            run_id="run",
            profile=None,
            region=None,
            job_queue=["jq"],
            status=None,
            job_name_prefix="run-workers",
            max_jobs=10,
            apply=True,
            terminate_running=True,
            reason="stop run",
        )
        out = io.StringIO()
        with patch("sweetspot.cli.boto3.Session", return_value=FakeCancelSession(batch)), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_cancel(args), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["job_name_regex"], "^run\\-workers(?:-|$)")
        self.assertEqual(report["cancelled_count"], 1)
        self.assertEqual(report["terminated_count"], 1)
        self.assertEqual(batch.cancelled, [{"jobId": "pending-1", "reason": "stop run"}])
        self.assertEqual(batch.terminated, [{"jobId": "running-1", "reason": "stop run"}])

    def test_cancel_rejects_prefix_that_does_not_include_run_id(self) -> None:
        args = types.SimpleNamespace(
            run_id="run-1",
            profile=None,
            region=None,
            job_queue=["jq"],
            status=None,
            job_name_prefix="sweetspot-worker",
            max_jobs=10,
            apply=False,
            terminate_running=False,
            reason=None,
        )
        with self.assertRaisesRegex(SystemExit, "must include RUN_ID"):
            cmd_cancel(args)

    def test_cancel_jobs_requires_name_regex_guardrail(self) -> None:
        args = types.SimpleNamespace(job_name_regex="", max_jobs=10)
        with self.assertRaisesRegex(SystemExit, "job-name-regex"):
            cmd_cancel_jobs(args)


class FakePaginatedRepairSession:
    def __init__(self) -> None:
        self.logs = FakePaginatedRepairLogs()

    def client(self, service: str, region_name=None):
        if service == "batch":
            return FakeRepairBatch()
        if service == "logs":
            return self.logs
        raise AssertionError(service)


class RepairPlanTests(unittest.TestCase):
    def _write_repair_inputs(self, tmp: str) -> tuple[Path, Path, Path]:
        tasks = [
            {"schema": "sweetspot.task.v1", "run_id": "run", "task_id": "t0", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://b/r/t0"},
            {"schema": "sweetspot.task.v1", "run_id": "run", "task_id": "t1", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://b/r/t1"},
        ]
        statuses = [{"task_id": "t0", "state": "incomplete"}, {"task_id": "t1", "state": "incomplete"}]
        tasks_path = Path(tmp) / "tasks.jsonl"
        status_path = Path(tmp) / "status.jsonl"
        out_path = Path(tmp) / "repair.jsonl"
        tasks_path.write_text("".join(json.dumps(t) + "\n" for t in tasks))
        status_path.write_text("".join(json.dumps(s) + "\n" for s in statuses))
        return tasks_path, status_path, out_path

    def test_repair_plan_excludes_active_task_ids(self) -> None:
        tasks = [
            {"schema": "sweetspot.task.v1", "run_id": "r", "task_id": "t0", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://b/r/t0"},
            {"schema": "sweetspot.task.v1", "run_id": "r", "task_id": "t1", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://b/r/t1"},
        ]
        statuses = [{"task_id": "t0", "state": "incomplete"}, {"task_id": "t1", "state": "incomplete"}]
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            status_path = Path(tmp) / "status.jsonl"
            out_path = Path(tmp) / "repair.jsonl"
            tasks_path.write_text("".join(json.dumps(t) + "\n" for t in tasks))
            status_path.write_text("".join(json.dumps(s) + "\n" for s in statuses))
            args = types.SimpleNamespace(
                profile=None,
                region=None,
                tasks_jsonl=tasks_path,
                task_status_jsonl=status_path,
                out_jsonl=out_path,
                job_queue=["jq"],
                job_name_regex="run",
                active_status=["RUNNING"],
                failed_status=["FAILED"],
                include_active=False,
                only_known_failed=False,
                log_group="lg",
                log_tail=10,
                max_jobs=10,
            )
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.Session", return_value=FakeRepairSession()), contextlib.redirect_stdout(out):
                self.assertEqual(cmd_repair_plan(args), 0)
            report = json.loads(out.getvalue())
            self.assertEqual(report["blocked_active_task_ids"], ["t0"])
            self.assertEqual(report["repair_task_ids"], ["t1"])
            self.assertEqual(json.loads(out_path.read_text())["task_id"], "t1")

    def test_repair_plan_scans_past_first_log_page_for_active_task_ids(self) -> None:
        tasks = [
            {"schema": "sweetspot.task.v1", "run_id": "r", "task_id": "t0", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://b/r/t0"},
            {"schema": "sweetspot.task.v1", "run_id": "r", "task_id": "t1", "command": [sys.executable, "-c", "pass"], "done_s3": "s3://b/r/t1"},
        ]
        statuses = [{"task_id": "t0", "state": "incomplete"}, {"task_id": "t1", "state": "incomplete"}]
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path = Path(tmp) / "tasks.jsonl"
            status_path = Path(tmp) / "status.jsonl"
            out_path = Path(tmp) / "repair.jsonl"
            tasks_path.write_text("".join(json.dumps(t) + "\n" for t in tasks))
            status_path.write_text("".join(json.dumps(s) + "\n" for s in statuses))
            args = types.SimpleNamespace(
                profile=None,
                region=None,
                tasks_jsonl=tasks_path,
                task_status_jsonl=status_path,
                out_jsonl=out_path,
                job_queue=["jq"],
                job_name_regex="active",
                active_status=["RUNNING"],
                failed_status=["FAILED"],
                include_active=False,
                only_known_failed=False,
                log_group="lg",
                log_tail=1000,
                max_jobs=10,
            )
            session = FakePaginatedRepairSession()
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.Session", return_value=session), contextlib.redirect_stdout(out):
                self.assertEqual(cmd_repair_plan(args), 0)
            report = json.loads(out.getvalue())
            self.assertEqual(len(session.logs.calls), 2)
            self.assertEqual(session.logs.calls[0]["filterPattern"], '"task_id"')
            self.assertEqual(session.logs.calls[0]["logStreamNames"], ["active"])
            self.assertEqual(report["blocked_active_task_ids"], ["t0"])
            self.assertEqual(report["repair_task_ids"], ["t1"])

    def test_repair_dry_run_builds_run_scoped_repair_plan(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path, status_path, out_path = self._write_repair_inputs(tmp)
            args = types.SimpleNamespace(
                run_id="run",
                profile=None,
                region=None,
                tasks_jsonl=tasks_path,
                task_status_jsonl=status_path,
                out_jsonl=out_path,
                artifact_dir=None,
                job_queue=["jq"],
                job_name_prefix=None,
                active_status=["RUNNING"],
                failed_status=["FAILED"],
                include_active=False,
                only_known_failed=False,
                log_group="lg",
                log_tail=10,
                max_jobs=10,
                sqs_queue_url="",
                apply=False,
                submit_workers=False,
                batch_job_queue=None,
                job_definition=None,
                worker_job_name_prefix=None,
                messages_per_worker=1,
                max_workers=64,
                min_workers=0,
                subtract_active=False,
                include_not_visible=False,
                vcpus=None,
                memory=None,
                visibility_timeout=1800,
                heartbeat_seconds=300,
                task_timeout_seconds=3600,
                retry_attempts=None,
                env=[],
                allowed_s3_prefix=[],
                log_tail_bytes=100,
                max_log_bytes=1000,
                redact_regex=[],
                allow_legacy_done_markers=False,
            )
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.Session", return_value=FakeRepairSession()), contextlib.redirect_stdout(out):
                self.assertEqual(cmd_repair(args), 0)
            report = json.loads(out.getvalue())
            self.assertEqual(report["schema"], "sweetspot.repair.v1")
            self.assertEqual(report["run_id"], "run")
            self.assertFalse(report["apply"])
            self.assertEqual(report["repair_task_count"], 1)
            self.assertEqual(report["sent"], 0)
            self.assertEqual(json.loads(out_path.read_text())["task_id"], "t1")

    def test_repair_apply_enqueues_repair_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path, status_path, out_path = self._write_repair_inputs(tmp)
            sqs = FakeRepairApplySQS()
            batch = FakeRepairApplyBatch()
            args = types.SimpleNamespace(
                run_id="run",
                profile=None,
                region=None,
                tasks_jsonl=tasks_path,
                task_status_jsonl=status_path,
                out_jsonl=out_path,
                artifact_dir=None,
                job_queue=["jq"],
                job_name_prefix=None,
                active_status=["RUNNING"],
                failed_status=["FAILED"],
                include_active=False,
                only_known_failed=False,
                log_group="lg",
                log_tail=10,
                max_jobs=10,
                sqs_queue_url="https://sqs.example/queue",
                apply=True,
                submit_workers=False,
                batch_job_queue=None,
                job_definition=None,
                worker_job_name_prefix=None,
                messages_per_worker=1,
                max_workers=64,
                min_workers=0,
                subtract_active=False,
                include_not_visible=False,
                vcpus=None,
                memory=None,
                visibility_timeout=1800,
                heartbeat_seconds=300,
                task_timeout_seconds=3600,
                retry_attempts=None,
                env=[],
                allowed_s3_prefix=[],
                log_tail_bytes=100,
                max_log_bytes=1000,
                redact_regex=[],
                allow_legacy_done_markers=False,
            )
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.Session", return_value=FakeRepairApplySession(sqs, batch)), contextlib.redirect_stdout(out):
                self.assertEqual(cmd_repair(args), 0)
            report = json.loads(out.getvalue())
            self.assertTrue(report["apply"])
            self.assertEqual(report["sent"], 1)
            self.assertEqual(json.loads(sqs.sent_messages[0])["task_id"], "t1")
            self.assertEqual(batch.submitted, [])

    def test_repair_apply_can_submit_workers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path, status_path, out_path = self._write_repair_inputs(tmp)
            sqs = FakeRepairApplySQS()
            batch = FakeRepairApplyBatch()
            args = types.SimpleNamespace(
                run_id="run",
                profile=None,
                region=None,
                tasks_jsonl=tasks_path,
                task_status_jsonl=status_path,
                out_jsonl=out_path,
                artifact_dir=None,
                job_queue=["jq"],
                job_name_prefix=None,
                active_status=["RUNNING"],
                failed_status=["FAILED"],
                include_active=False,
                only_known_failed=False,
                log_group="lg",
                log_tail=10,
                max_jobs=10,
                sqs_queue_url="https://sqs.example/queue",
                apply=True,
                submit_workers=True,
                batch_job_queue="worker-jq",
                job_definition="worker-jd",
                worker_job_name_prefix=None,
                messages_per_worker=1,
                max_workers=64,
                min_workers=0,
                subtract_active=False,
                include_not_visible=False,
                vcpus=None,
                memory=None,
                visibility_timeout=1800,
                heartbeat_seconds=300,
                task_timeout_seconds=3600,
                retry_attempts=None,
                env=[],
                allowed_s3_prefix=[],
                log_tail_bytes=100,
                max_log_bytes=1000,
                redact_regex=[],
                allow_legacy_done_markers=False,
            )
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.Session", return_value=FakeRepairApplySession(sqs, batch)), contextlib.redirect_stdout(out):
                self.assertEqual(cmd_repair(args), 0)
            report = json.loads(out.getvalue())
            self.assertEqual(report["submitted_count"], 1)
            self.assertEqual(batch.submitted[0]["jobQueue"], "worker-jq")
            self.assertEqual(batch.submitted[0]["jobDefinition"], "worker-jd")
            self.assertTrue(str(batch.submitted[0]["jobName"]).startswith("run-repair-worker-"))

    def test_repair_rejects_tasks_from_another_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path, status_path, out_path = self._write_repair_inputs(tmp)
            records = [json.loads(line) for line in tasks_path.read_text().splitlines()]
            records[1]["run_id"] = "other-run"
            tasks_path.write_text("".join(json.dumps(record) + "\n" for record in records))
            args = types.SimpleNamespace(
                run_id="run",
                profile=None,
                region=None,
                tasks_jsonl=tasks_path,
                task_status_jsonl=status_path,
                out_jsonl=out_path,
                artifact_dir=None,
                job_queue=["jq"],
                job_name_prefix=None,
                active_status=["RUNNING"],
                failed_status=["FAILED"],
                include_active=False,
                only_known_failed=False,
                log_group="lg",
                log_tail=10,
                max_jobs=10,
                sqs_queue_url="https://sqs.example/queue",
                apply=True,
                submit_workers=False,
                batch_job_queue=None,
                job_definition=None,
                worker_job_name_prefix=None,
                messages_per_worker=1,
                max_workers=64,
                min_workers=0,
                subtract_active=False,
                include_not_visible=False,
                vcpus=None,
                memory=None,
                visibility_timeout=1800,
                heartbeat_seconds=300,
                task_timeout_seconds=3600,
                retry_attempts=None,
                env=[],
                allowed_s3_prefix=[],
                log_tail_bytes=100,
                max_log_bytes=1000,
                redact_regex=[],
                allow_legacy_done_markers=False,
            )
            sqs = FakeRepairApplySQS()
            batch = FakeRepairApplyBatch()
            with patch("sweetspot.cli.boto3.Session", return_value=FakeRepairApplySession(sqs, batch)), self.assertRaisesRegex(SystemExit, "run_id='run'"):
                cmd_repair(args)
            self.assertEqual(sqs.sent_messages, [])

    def test_repair_submit_worker_validation_happens_before_enqueue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path, status_path, out_path = self._write_repair_inputs(tmp)
            sqs = FakeRepairApplySQS()
            batch = FakeRepairApplyBatch()
            args = types.SimpleNamespace(
                run_id="run",
                profile=None,
                region=None,
                tasks_jsonl=tasks_path,
                task_status_jsonl=status_path,
                out_jsonl=out_path,
                artifact_dir=None,
                job_queue=["jq"],
                job_name_prefix=None,
                active_status=["RUNNING"],
                failed_status=["FAILED"],
                include_active=False,
                only_known_failed=False,
                log_group="lg",
                log_tail=10,
                max_jobs=10,
                sqs_queue_url="https://sqs.example/queue",
                apply=True,
                submit_workers=True,
                batch_job_queue=None,
                job_definition="worker-jd",
                worker_job_name_prefix=None,
                messages_per_worker=1,
                max_workers=64,
                min_workers=0,
                subtract_active=False,
                include_not_visible=False,
                vcpus=None,
                memory=None,
                visibility_timeout=1800,
                heartbeat_seconds=300,
                task_timeout_seconds=3600,
                retry_attempts=None,
                env=[],
                allowed_s3_prefix=[],
                log_tail_bytes=100,
                max_log_bytes=1000,
                redact_regex=[],
                allow_legacy_done_markers=False,
            )
            with patch("sweetspot.cli.boto3.Session", return_value=FakeRepairApplySession(sqs, batch)), self.assertRaisesRegex(SystemExit, "requires --batch-job-queue"):
                cmd_repair(args)
            self.assertEqual(sqs.sent_messages, [])
            self.assertEqual(batch.submitted, [])

    def test_repair_rejects_prefix_that_does_not_include_run_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tasks_path, status_path, out_path = self._write_repair_inputs(tmp)
            args = types.SimpleNamespace(
                run_id="run-1",
                profile=None,
                region=None,
                tasks_jsonl=tasks_path,
                task_status_jsonl=status_path,
                out_jsonl=out_path,
                artifact_dir=None,
                job_queue=["jq"],
                job_name_prefix="workers",
                active_status=None,
                failed_status=None,
                include_active=False,
                only_known_failed=False,
                log_group="lg",
                log_tail=10,
                max_jobs=10,
                sqs_queue_url="",
                apply=False,
                submit_workers=False,
                batch_job_queue=None,
                job_definition=None,
                worker_job_name_prefix=None,
                messages_per_worker=1,
                max_workers=64,
                min_workers=0,
                subtract_active=False,
                include_not_visible=False,
                vcpus=None,
                memory=None,
                visibility_timeout=1800,
                heartbeat_seconds=300,
                task_timeout_seconds=3600,
                retry_attempts=None,
                env=[],
                allowed_s3_prefix=[],
                log_tail_bytes=100,
                max_log_bytes=1000,
                redact_regex=[],
                allow_legacy_done_markers=False,
            )
            with self.assertRaisesRegex(SystemExit, "must include RUN_ID"):
                cmd_repair(args)


class FakeLogsClient:
    def __init__(self, events: list[dict[str, object]] | None = None) -> None:
        self.kwargs: dict[str, object] | None = None
        self.events = events or []

    def get_log_events(self, **kwargs):
        self.kwargs = kwargs
        return {"events": self.events, "nextForwardToken": "next"}


class FakeBatchClient:
    def __init__(self, job: dict[str, object]) -> None:
        self.job = job

    def describe_jobs(self, *, jobs):
        return {"jobs": [self.job]}


class FakeLogSession:
    def __init__(self, logs_client: FakeLogsClient, batch_client: FakeBatchClient | None = None) -> None:
        self.logs_client = logs_client
        self.batch_client = batch_client

    def client(self, service: str, region_name=None):
        if service == "logs":
            return self.logs_client
        if service == "batch" and self.batch_client is not None:
            return self.batch_client
        raise AssertionError(service)


class FakeDoctorClient:
    def get_queue_attributes(self, **kwargs):
        return {"Attributes": {"ApproximateNumberOfMessages": "0", "VisibilityTimeout": "1800", "RedrivePolicy": "{}"}}

    def describe_job_queues(self, **kwargs):
        return {"jobQueues": [{"jobQueueName": "jq", "state": "ENABLED", "status": "VALID", "computeEnvironmentOrder": []}]}

    def describe_job_definitions(self, **kwargs):
        return {
            "jobDefinitions": [
                {
                    "jobDefinitionName": "jd",
                    "revision": 1,
                    "containerProperties": {"image": "repo/worker:tag", "jobRoleArn": "arn", "command": ["sweetspot", "worker"], "logConfiguration": {"options": {"awslogs-group": "/aws/batch/miser"}}},
                }
            ]
        }

    def describe_log_groups(self, **kwargs):
        return {"logGroups": [{"logGroupName": "/aws/batch/miser", "retentionInDays": 14, "storedBytes": 0}]}

    def list_objects_v2(self, **kwargs):
        return {"Contents": []}

    def list_metrics(self, **kwargs):
        return {"Metrics": [{"Namespace": kwargs.get("Namespace"), "MetricName": kwargs.get("MetricName"), "Dimensions": kwargs.get("Dimensions", [])}]}


class FakeDoctorSession:
    def client(self, service: str, region_name=None):
        return FakeDoctorClient()


class DoctorTests(unittest.TestCase):
    def test_doctor_reports_ok_for_basic_resources(self) -> None:
        args = types.SimpleNamespace(
            profile=None,
            region="us-west-2",
            queue_url="https://sqs.us-west-2.amazonaws.com/123/q",
            dlq_url="https://sqs.us-west-2.amazonaws.com/123/dlq",
            job_queue="jq",
            job_definition="jd",
            log_group=None,
            validate_batch_metrics=True,
            s3_prefix=["s3://bucket/runs/r1"],
            write_probe=False,
            visibility_timeout=1800,
            heartbeat_seconds=300,
            task_timeout_seconds=3600,
            redact_regex=[],
        )
        out = io.StringIO()
        with patch("sweetspot.cli.boto3.Session", return_value=FakeDoctorSession()), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_doctor(args), 0)
        report = json.loads(out.getvalue())
        self.assertTrue(report["ok"])
        names = [c["name"] for c in report["checks"]]
        self.assertIn("cloudwatch_log_group", names)
        self.assertIn("batch_metrics", names)


class ReadCommandTableTests(unittest.TestCase):
    def test_jobs_table_output(self) -> None:
        class Paginator:
            def paginate(self, **kwargs):
                return [{"jobSummaryList": [{"jobId": "job-1", "jobName": "run-worker", "createdAt": 1, "startedAt": 2, "stoppedAt": None}]}]

        class Batch:
            def get_paginator(self, name):
                return Paginator()

        class Session:
            def client(self, service: str, region_name=None):
                if service == "batch":
                    return Batch()
                raise AssertionError(service)

        args = types.SimpleNamespace(profile=None, region=None, job_queue="jq", status=["RUNNING"], name_regex=None, max_jobs=10, format="table")
        out = io.StringIO()
        with patch("sweetspot.cli.boto3.Session", return_value=Session()), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_jobs(args), 0)
        table = out.getvalue()
        self.assertIn("SweetSpot jobs", table)
        self.assertIn("jobId\tjobName\tstatus", table)
        self.assertIn("job-1\trun-worker\tRUNNING", table)

    def test_describe_job_table_output(self) -> None:
        job = {"jobId": "job-1", "jobName": "run-worker", "jobQueue": "jq", "status": "SUCCEEDED", "container": {"logStreamName": "stream-1", "exitCode": 0}}
        args = types.SimpleNamespace(profile=None, region=None, job_id="job-1", format="table")
        out = io.StringIO()
        with patch("sweetspot.cli.boto3.Session", return_value=FakeLogSession(FakeLogsClient(), FakeBatchClient(job))), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_describe_job(args), 0)
        table = out.getvalue()
        self.assertIn("SweetSpot job", table)
        self.assertIn("status\tSUCCEEDED", table)
        self.assertIn("logStreamName\tstream-1", table)

    def test_logs_table_output_escapes_control_characters(self) -> None:
        logs = FakeLogsClient(events=[{"timestamp": 10, "message": "hello\x1b[2J\rnext"}])
        args = types.SimpleNamespace(profile=None, region=None, log_stream="stream", job_id=None, log_group="/aws/batch/job", limit=10, start_from_head=False, next_token=None, filter_regex=None, tail=0, format="table")
        out = io.StringIO()
        with patch("sweetspot.cli.boto3.Session", return_value=FakeLogSession(logs)), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_logs(args), 0)
        table = out.getvalue()
        self.assertIn("SweetSpot logs", table)
        self.assertIn("timestamp\tmessage", table)
        self.assertIn("10\thello\\x1b[2J\\rnext", table)
        self.assertNotIn("\x1b", table)

    def test_doctor_table_output(self) -> None:
        args = types.SimpleNamespace(
            profile=None,
            region="us-west-2",
            queue_url="https://sqs.us-west-2.amazonaws.com/123/q",
            dlq_url=None,
            job_queue=None,
            job_definition=None,
            log_group=None,
            validate_batch_metrics=False,
            s3_prefix=[],
            write_probe=False,
            visibility_timeout=1800,
            heartbeat_seconds=300,
            task_timeout_seconds=3600,
            redact_regex=[],
            format="table",
        )
        out = io.StringIO()
        with patch("sweetspot.cli.boto3.Session", return_value=FakeDoctorSession()), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_doctor(args), 0)
        table = out.getvalue()
        self.assertIn("SweetSpot doctor", table)
        self.assertIn("sqs_work_queue", table)

    def test_dlq_table_output(self) -> None:
        class SQS:
            def receive_message(self, **kwargs):
                return {"Messages": []}

        args = types.SimpleNamespace(
            profile=None,
            region=None,
            apply=False,
            dlq_url="dlq",
            queue_url=None,
            native_redrive=False,
            run_id=None,
            task_id_regex=None,
            max_messages=10,
            wait_time=0,
            visibility_timeout=1,
            format="table",
        )
        out = io.StringIO()
        with patch("sweetspot.cli.boto3.client", return_value=SQS()), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_dlq(args), 0)
        table = out.getvalue()
        self.assertIn("SweetSpot DLQ", table)
        self.assertIn("scanned\t0", table)


class BatchOperatorTests(unittest.TestCase):
    def test_logs_next_token_forces_start_from_head(self) -> None:
        logs = FakeLogsClient()
        args = types.SimpleNamespace(
            profile=None,
            region=None,
            log_stream="stream",
            job_id=None,
            log_group="/aws/batch/job",
            limit=10,
            start_from_head=False,
            next_token="token",
            filter_regex=None,
            tail=0,
        )
        with patch("sweetspot.cli.boto3.Session", return_value=FakeLogSession(logs)), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cmd_logs(args), 0)
        self.assertEqual(logs.kwargs["startFromHead"], True)

    def test_logs_accepts_clearer_limit_aliases(self) -> None:
        logs = FakeLogsClient()
        out = io.StringIO()
        with patch("sweetspot.cli.boto3.Session", return_value=FakeLogSession(logs)), contextlib.redirect_stdout(out):
            self.assertEqual(main(["logs", "--log-stream", "stream", "--log-group", "/aws/batch/job", "--max-events", "3", "--last", "2"]), 0)
        self.assertEqual(logs.kwargs["limit"], 3)
        self.assertEqual(json.loads(out.getvalue())["events"], [])

    def test_job_log_stream_reads_ecs_task_properties(self) -> None:
        job = {"attempts": [{"taskProperties": [{"containers": [{"logStreamName": "ecs-stream"}]}]}]}
        self.assertEqual(_job_log_stream(job), "ecs-stream")

    def test_job_log_stream_prefers_latest_attempt(self) -> None:
        job = {
            "container": {"logStreamName": "container-stream"},
            "attempts": [
                {"container": {"logStreamName": "attempt-1"}},
                {"container": {"logStreamName": "attempt-2"}},
            ],
        }
        self.assertEqual(_job_log_stream(job), "attempt-2")

    def test_job_log_group_reads_batch_log_configuration(self) -> None:
        job = {"container": {"logConfiguration": {"options": {"awslogs-group": "/aws/batch/miser"}}}}
        self.assertEqual(_job_log_group(job), "/aws/batch/miser")

    def test_logs_discovers_job_log_group_when_omitted(self) -> None:
        logs = FakeLogsClient()
        job = {
            "jobId": "job-1",
            "container": {
                "logStreamName": "stream-1",
                "logConfiguration": {"options": {"awslogs-group": "/aws/batch/miser"}},
            },
        }
        args = types.SimpleNamespace(
            profile=None,
            region=None,
            log_stream=None,
            job_id="job-1",
            log_group=None,
            limit=10,
            start_from_head=False,
            next_token=None,
            filter_regex=None,
            tail=0,
        )
        session = FakeLogSession(logs, FakeBatchClient(job))
        with patch("sweetspot.cli.boto3.Session", return_value=session), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cmd_logs(args), 0)
        self.assertEqual(logs.kwargs["logGroupName"], "/aws/batch/miser")


class S3DeletePrefixTests(unittest.TestCase):
    def test_delete_prefix_rejects_bucket_root(self) -> None:
        with self.assertRaisesRegex(SystemExit, "dangerous"):
            _validate_s3_delete_prefix("s3://bucket/", min_prefix_chars=8)

    def test_delete_prefix_normalizes_safe_prefix(self) -> None:
        self.assertEqual(_validate_s3_delete_prefix("s3://bucket/runs/old-run", min_prefix_chars=8), ("bucket", "runs/old-run/"))

    def test_delete_prefix_fails_on_delete_object_errors_before_marker(self) -> None:
        class Paginator:
            def paginate(self, **kwargs):
                yield {"Contents": [{"Key": "runs/old-run/a"}]}

        class S3:
            marker_written = False

            def get_paginator(self, name):
                return Paginator()

            def delete_objects(self, **kwargs):
                return {"Errors": [{"Key": "runs/old-run/a", "Code": "AccessDenied"}]}

            def put_object(self, **kwargs):
                self.marker_written = True
                return {}

        class Session:
            def __init__(self, s3):
                self.s3 = s3

            def client(self, service, region_name=None):
                return self.s3

        s3 = S3()
        with tempfile.TemporaryDirectory() as tmp:
            args = types.SimpleNamespace(
                prefix="s3://bucket/runs/old-run/",
                min_prefix_chars=8,
                delete=True,
                confirm_prefix="s3://bucket/runs/old-run/",
                batch_size=1000,
                profile=None,
                region=None,
                artifact_dir=Path(tmp),
                completion_marker_s3="s3://bucket/markers/done.json",
            )
            with patch("sweetspot.cli.boto3.Session", return_value=Session(s3)), self.assertRaisesRegex(SystemExit, "DeleteObjects"):
                cmd_s3_delete_prefix(args)
        self.assertFalse(s3.marker_written)

    def test_delete_prefix_can_include_versions_and_delete_markers(self) -> None:
        class Paginator:
            def paginate(self, **kwargs):
                yield {
                    "Versions": [{"Key": "runs/old-run/a", "VersionId": "v1"}],
                    "DeleteMarkers": [{"Key": "runs/old-run/a", "VersionId": "d1"}],
                }

        class S3:
            def __init__(self) -> None:
                self.deleted = None

            def get_paginator(self, name):
                self.name = name
                return Paginator()

            def delete_objects(self, **kwargs):
                self.deleted = kwargs["Delete"]["Objects"]
                return {}

        class Session:
            def __init__(self, s3):
                self.s3 = s3

            def client(self, service, region_name=None):
                return self.s3

        s3 = S3()
        with tempfile.TemporaryDirectory() as tmp:
            args = types.SimpleNamespace(
                prefix="s3://bucket/runs/old-run/",
                min_prefix_chars=8,
                delete=True,
                confirm_prefix="s3://bucket/runs/old-run/",
                batch_size=1000,
                include_versions=True,
                profile=None,
                region=None,
                artifact_dir=Path(tmp),
                completion_marker_s3=None,
            )
            with patch("sweetspot.cli.boto3.Session", return_value=Session(s3)), contextlib.redirect_stdout(io.StringIO()):
                cmd_s3_delete_prefix(args)
        self.assertEqual(s3.name, "list_object_versions")
        self.assertEqual(s3.deleted, [{"Key": "runs/old-run/a", "VersionId": "v1"}, {"Key": "runs/old-run/a", "VersionId": "d1"}])


class DLQTests(unittest.TestCase):
    def test_native_redrive_starts_message_move_task(self) -> None:
        class SQS:
            def __init__(self) -> None:
                self.kwargs = None

            def get_queue_attributes(self, *, QueueUrl, AttributeNames):
                name = QueueUrl.rsplit("/", 1)[-1]
                return {"Attributes": {"QueueArn": f"arn:aws:sqs:us-west-2:123:{name}"}}

            def start_message_move_task(self, **kwargs):
                self.kwargs = kwargs
                return {"TaskHandle": "move-1"}

        sqs = SQS()
        args = types.SimpleNamespace(
            dlq_url="https://sqs.us-west-2.amazonaws.com/123/dlq",
            queue_url="https://sqs.us-west-2.amazonaws.com/123/q",
            run_id=None,
            task_id_regex=None,
            native_redrive=True,
            max_messages_per_second=50,
            apply=True,
        )
        with patch("sweetspot.cli.boto3.client", return_value=sqs), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cmd_dlq(args), 0)
        self.assertEqual(sqs.kwargs, {"SourceArn": "arn:aws:sqs:us-west-2:123:dlq", "DestinationArn": "arn:aws:sqs:us-west-2:123:q", "MaxNumberOfMessagesPerSecond": 50})

    def test_native_redrive_rejects_filters(self) -> None:
        args = types.SimpleNamespace(dlq_url="dlq", queue_url="q", run_id="r1", task_id_regex=None, native_redrive=True, apply=True)
        with patch("sweetspot.cli.boto3.client"), self.assertRaisesRegex(SystemExit, "whole DLQ"):
            cmd_dlq(args)


class SupervisorPlanningTests(unittest.TestCase):
    def test_supervisor_sizes_to_backlog_when_not_keep_full(self) -> None:
        self.assertEqual(
            _supervisor_desired_workers(
                backlog=17,
                messages_per_worker=4,
                target_active_workers=10,
                max_active_workers=20,
                keep_full_pool=False,
            ),
            5,
        )

    def test_supervisor_keep_full_pool_ignores_empty_backlog_but_honors_cap(self) -> None:
        self.assertEqual(
            _supervisor_desired_workers(
                backlog=0,
                messages_per_worker=4,
                target_active_workers=10,
                max_active_workers=6,
                keep_full_pool=True,
            ),
            6,
        )

    def test_supervisor_redacts_user_env_values_in_config(self) -> None:
        self.assertEqual(_redact_env([{"name": "SECRET", "value": "token"}]), [{"name": "SECRET", "value": "<redacted>"}])

    def test_stop_on_dlq_requires_dlq_url(self) -> None:
        args = types.SimpleNamespace(sqs_queue_url="https://sqs.us-west-2.amazonaws.com/123/q", stop_on_dlq=True, dlq_url=None)
        with self.assertRaisesRegex(SystemExit, "--dlq-url"):
            cmd_supervise_workers(args)


class FakeFinalizeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, object]] = {}
        self.deleted: list[tuple[str, str]] = []

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        obj = self.objects[(Bucket, Key)]
        body = obj.get("Body", b"")
        return {"ContentLength": len(body), "Metadata": obj.get("Metadata", {})}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)].get("Body", b""))}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str, **kwargs) -> dict[str, object]:
        self.objects[(Bucket, Key)] = {"Body": Body, "ContentType": ContentType, "Metadata": kwargs.get("Metadata", {})}
        return {}

    def delete_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        self.deleted.append((Bucket, Key))
        self.objects.pop((Bucket, Key), None)
        return {}

    def upload_file(self, Filename: str, Bucket: str, Key: str, ExtraArgs=None) -> None:
        with open(Filename, "rb") as f:
            body = f.read()
        self.objects[(Bucket, Key)] = {"Body": body, "ContentType": (ExtraArgs or {}).get("ContentType"), "Metadata": (ExtraArgs or {}).get("Metadata", {})}

    def get_paginator(self, name):
        s3 = self

        class Paginator:
            def paginate(self, *, Bucket, Prefix):
                contents = [{"Key": key} for (bucket, key), obj in s3.objects.items() if bucket == Bucket and key.startswith(Prefix)]
                yield {"Contents": contents}

        return Paginator()


class FinalizeTests(unittest.TestCase):
    def test_publish_ready_requires_upload(self) -> None:
        args = types.SimpleNamespace(publish_ready=True, upload=False)
        with self.assertRaisesRegex(SystemExit, "--upload"):
            cmd_finalize(args)

    def test_finalize_dry_run_skips_uploads_and_ready_mutations(self) -> None:
        s3 = FakeFinalizeS3()
        s3.objects[("bucket", "runs/r1/done/task-1.done.json")] = {
            "Body": json.dumps({"schema": "sweetspot.done_marker.v1", "run_id": "r1", "task_id": "task-1", "output_s3": "s3://bucket/runs/r1/shards/task-1.txt"}).encode()
        }
        s3.objects[("bucket", "runs/r1/shards/task-1.txt")] = {"Body": b"ok"}
        task = {"run_id": "r1", "task_id": "task-1", "output_s3": "s3://bucket/runs/r1/shards/task-1.txt", "done_s3": "s3://bucket/runs/r1/done/task-1.done.json"}
        with tempfile.TemporaryDirectory() as tmp:
            task_path = Path(tmp) / "tasks.jsonl"
            task_path.write_text(json.dumps(task) + "\n")
            args = types.SimpleNamespace(
                run_id="r1",
                output_prefix="s3://bucket/runs/r1",
                tasks_jsonl=task_path,
                tasks_s3=None,
                artifact_dir=Path(tmp) / "finalizer",
                workers=1,
                progress_interval=0,
                write_repair_jsonl=None,
                upload=True,
                dry_run=True,
                publish_ready=True,
                ready_key="READY",
                allow_incomplete_ready=False,
                allow_legacy_done_markers=True,
                require_complete=True,
            )
            out = io.StringIO()
            with patch("sweetspot.cli.boto3.client", return_value=s3), contextlib.redirect_stdout(out):
                rc = cmd_finalize(args)
            self.assertEqual(rc, 0)
            self.assertEqual(s3.deleted, [])
            self.assertNotIn(("bucket", "runs/r1/manifests/final_manifest.json"), s3.objects)
            self.assertNotIn(("bucket", "runs/r1/READY"), s3.objects)
            manifest = json.loads((Path(tmp) / "finalizer" / "final_manifest.json").read_text())
            self.assertTrue(manifest["dry_run"])
            self.assertIsNone(manifest["final_manifest_s3"])
            self.assertEqual(manifest["would_final_manifest_s3"], "s3://bucket/runs/r1/manifests/final_manifest.json")
            summary = json.loads(out.getvalue())
            self.assertTrue(summary["dry_run"])
            self.assertIsNone(summary["ready_s3"])
            self.assertEqual(summary["would_ready_s3"], "s3://bucket/runs/r1/READY")

    def test_ready_key_cannot_collide_with_manifest(self) -> None:
        args = types.SimpleNamespace(publish_ready=True, upload=True, ready_key="manifests/final_manifest.json")
        with self.assertRaisesRegex(SystemExit, "collide"):
            cmd_finalize(args)

    def test_finalize_rejects_duplicate_task_ids(self) -> None:
        tasks = [
            {"run_id": "r1", "task_id": "dup", "done_s3": "s3://bucket/runs/r1/done/dup-a.done.json"},
            {"run_id": "r1", "task_id": "dup", "done_s3": "s3://bucket/runs/r1/done/dup-b.done.json"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            task_path = Path(tmp) / "tasks.jsonl"
            task_path.write_text("".join(json.dumps(t) + "\n" for t in tasks))
            args = types.SimpleNamespace(
                run_id="r1",
                output_prefix="s3://bucket/runs/r1",
                tasks_jsonl=task_path,
                tasks_s3=None,
                artifact_dir=Path(tmp) / "finalizer",
                workers=1,
                progress_interval=0,
                write_repair_jsonl=None,
                upload=False,
                publish_ready=False,
                ready_key="READY",
                allow_incomplete_ready=False,
                allow_legacy_done_markers=True,
                require_complete=False,
            )
            with patch("sweetspot.cli.boto3.client", return_value=FakeFinalizeS3()):
                with self.assertRaisesRegex(SystemExit, "duplicate task_id"):
                    cmd_finalize(args)

    def test_finalize_writes_repair_tasks_and_refuses_ready_when_incomplete(self) -> None:
        s3 = FakeFinalizeS3()
        s3.objects[("bucket", "runs/r1/done/task-1.done.json")] = {"Body": json.dumps({"schema": "sweetspot.done_marker.v1", "run_id": "r1", "task_id": "task-1", "output_s3": ""}).encode()}
        tasks = [
            {"run_id": "r1", "task_id": "task-1", "done_s3": "s3://bucket/runs/r1/done/task-1.done.json"},
            {"run_id": "r1", "task_id": "task-2", "done_s3": "s3://bucket/runs/r1/done/task-2.done.json"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            task_path = Path(tmp) / "tasks.jsonl"
            repair_path = Path(tmp) / "repair.jsonl"
            task_path.write_text("".join(json.dumps(t) + "\n" for t in tasks))
            args = types.SimpleNamespace(
                run_id="r1",
                output_prefix="s3://bucket/runs/r1",
                tasks_jsonl=task_path,
                tasks_s3=None,
                artifact_dir=Path(tmp) / "finalizer",
                workers=2,
                progress_interval=0,
                write_repair_jsonl=repair_path,
                upload=True,
                publish_ready=True,
                ready_key="READY",
                allow_incomplete_ready=False,
                allow_legacy_done_markers=True,
                require_complete=False,
            )
            with patch("sweetspot.cli.boto3.client", return_value=s3), contextlib.redirect_stdout(io.StringIO()):
                rc = cmd_finalize(args)
            self.assertEqual(rc, 2)
            repair_rows = [json.loads(line) for line in repair_path.read_text().splitlines()]
            self.assertEqual([r["task_id"] for r in repair_rows], ["task-2"])
            self.assertEqual(s3.deleted, [("bucket", "runs/r1/READY")])
            self.assertIn(("bucket", "runs/r1/manifests/final_manifest.json"), s3.objects)
            manifest = json.loads(s3.objects[("bucket", "runs/r1/manifests/final_manifest.json")]["Body"])
            self.assertIsNone(manifest["ready_s3"])
            self.assertNotIn(("bucket", "runs/r1/READY"), s3.objects)

    def test_finalize_refuses_ready_when_done_exists_but_output_missing(self) -> None:
        s3 = FakeFinalizeS3()
        s3.objects[("bucket", "runs/r1/done/task-1.done.json")] = {
            "Body": json.dumps({"schema": "sweetspot.done_marker.v1", "run_id": "r1", "task_id": "task-1", "output_s3": "s3://bucket/runs/r1/shards/task-1.txt"}).encode()
        }
        tasks = [
            {
                "run_id": "r1",
                "task_id": "task-1",
                "output_s3": "s3://bucket/runs/r1/shards/task-1.txt",
                "done_s3": "s3://bucket/runs/r1/done/task-1.done.json",
            }
        ]
        with tempfile.TemporaryDirectory() as tmp:
            task_path = Path(tmp) / "tasks.jsonl"
            task_path.write_text("".join(json.dumps(t) + "\n" for t in tasks))
            args = types.SimpleNamespace(
                run_id="r1",
                output_prefix="s3://bucket/runs/r1",
                tasks_jsonl=task_path,
                tasks_s3=None,
                artifact_dir=Path(tmp) / "finalizer",
                workers=1,
                progress_interval=0,
                write_repair_jsonl=None,
                upload=True,
                publish_ready=True,
                ready_key="READY",
                allow_incomplete_ready=False,
                allow_legacy_done_markers=True,
                require_complete=False,
            )
            with patch("sweetspot.cli.boto3.client", return_value=s3), contextlib.redirect_stdout(io.StringIO()):
                rc = cmd_finalize(args)
            self.assertEqual(rc, 2)
            manifest = json.loads(s3.objects[("bucket", "runs/r1/manifests/final_manifest.json")]["Body"])
            repair_rows = [json.loads(line) for line in (Path(tmp) / "finalizer" / "repair_tasks.jsonl").read_text().splitlines()]
            self.assertEqual(repair_rows[0]["output_s3"], "s3://bucket/runs/r1/shards/task-1.txt")
            self.assertTrue(repair_rows[0]["done_s3"].startswith("s3://bucket/runs/r1/done/task-1.done.json.repair-"))
            self.assertEqual(manifest["missing_done_count"], 1)
            self.assertEqual(manifest["missing_output_count"], 1)
            self.assertFalse(manifest["complete"])
            self.assertNotIn(("bucket", "runs/r1/READY"), s3.objects)

    def test_finalize_treats_missing_v2_attempt_output_as_repairable(self) -> None:
        s3 = FakeFinalizeS3()
        task = {
            "run_id": "r1",
            "task_id": "task-v2",
            "command": ["echo", "ok"],
            "output_s3": "s3://bucket/runs/r1/shards/task-v2.txt",
            "done_s3": "s3://bucket/runs/r1/done/task-v2.done.json",
        }
        marker = {
            "schema": "sweetspot.done_marker.v2",
            "run_id": "r1",
            "task_id": "task-v2",
            "task_hash": task_hash(task),
            "attempt_id": "attempt-a",
            "done_s3": task["done_s3"],
            "output_s3": task["output_s3"],
            "output": {"logical_uri": task["output_s3"], "uri": "s3://bucket/runs/r1/shards/task-v2.txt.attempts/attempt-a/output", "size_bytes": 2, "sha256": "0" * 64},
        }
        s3.objects[("bucket", "runs/r1/done/task-v2.done.json")] = {"Body": json.dumps(marker).encode()}
        with tempfile.TemporaryDirectory() as tmp:
            task_path = Path(tmp) / "tasks.jsonl"
            task_path.write_text(json.dumps(task) + "\n")
            args = types.SimpleNamespace(
                run_id="r1",
                output_prefix="s3://bucket/runs/r1",
                tasks_jsonl=task_path,
                tasks_s3=None,
                artifact_dir=Path(tmp) / "finalizer",
                workers=1,
                progress_interval=0,
                write_repair_jsonl=None,
                upload=False,
                publish_ready=False,
                ready_key="READY",
                allow_incomplete_ready=False,
                allow_legacy_done_markers=True,
                require_complete=True,
            )
            with patch("sweetspot.cli.boto3.client", return_value=s3), contextlib.redirect_stdout(io.StringIO()):
                rc = cmd_finalize(args)
            self.assertEqual(rc, 2)
            manifest = json.loads((Path(tmp) / "finalizer" / "final_manifest.json").read_text())
            self.assertEqual(manifest["missing_output_count"], 1)
            repair_rows = [json.loads(line) for line in (Path(tmp) / "finalizer" / "repair_tasks.jsonl").read_text().splitlines()]
            self.assertEqual(repair_rows[0]["output_s3"], task["output_s3"])

    def test_finalize_publishes_ready_after_complete_manifest_upload(self) -> None:
        s3 = FakeFinalizeS3()
        s3.objects[("bucket", "runs/r1/done/task-10.done.json")] = {
            "Body": json.dumps({"schema": "sweetspot.done_marker.v1", "run_id": "r1", "task_id": "task-10", "output_s3": "s3://bucket/runs/r1/shards/task-10.txt"}).encode()
        }
        s3.objects[("bucket", "runs/r1/done/task-2.done.json")] = {
            "Body": json.dumps({"schema": "sweetspot.done_marker.v1", "run_id": "r1", "task_id": "task-2", "output_s3": "s3://bucket/runs/r1/shards/task-2.txt"}).encode()
        }
        s3.objects[("bucket", "runs/r1/shards/task-10.txt")] = {"Body": b"ok"}
        s3.objects[("bucket", "runs/r1/shards/task-2.txt")] = {"Body": b"ok"}
        tasks = [
            {
                "run_id": "r1",
                "task_id": "task-10",
                "output_s3": "s3://bucket/runs/r1/shards/task-10.txt",
                "done_s3": "s3://bucket/runs/r1/done/task-10.done.json",
            },
            {
                "run_id": "r1",
                "task_id": "task-2",
                "output_s3": "s3://bucket/runs/r1/shards/task-2.txt",
                "done_s3": "s3://bucket/runs/r1/done/task-2.done.json",
            },
        ]
        with tempfile.TemporaryDirectory() as tmp:
            task_path = Path(tmp) / "tasks.jsonl"
            task_path.write_text("".join(json.dumps(t) + "\n" for t in tasks))
            args = types.SimpleNamespace(
                run_id="r1",
                output_prefix="s3://bucket/runs/r1",
                tasks_jsonl=task_path,
                tasks_s3=None,
                artifact_dir=Path(tmp) / "finalizer",
                workers=2,
                progress_interval=0,
                write_repair_jsonl=None,
                upload=True,
                publish_ready=True,
                ready_key="READY",
                allow_incomplete_ready=False,
                allow_legacy_done_markers=True,
                require_complete=True,
            )
            with patch("sweetspot.cli.boto3.client", return_value=s3), contextlib.redirect_stdout(io.StringIO()):
                rc = cmd_finalize(args)
            self.assertEqual(rc, 0)
            self.assertEqual(s3.deleted, [("bucket", "runs/r1/READY")])
            self.assertIn(("bucket", "runs/r1/manifests/final_manifest.json"), s3.objects)
            manifest = json.loads(s3.objects[("bucket", "runs/r1/manifests/final_manifest.json")]["Body"])
            self.assertEqual(
                manifest["outputs"],
                ["s3://bucket/runs/r1/shards/task-10.txt", "s3://bucket/runs/r1/shards/task-2.txt"],
            )
            self.assertIn(("bucket", "runs/r1/READY"), s3.objects)

    def test_finalize_treats_corrupt_done_marker_as_repairable(self) -> None:
        s3 = FakeFinalizeS3()
        s3.objects[("bucket", "runs/r1/done/task-1.done.json")] = {"Body": b"not-json"}
        task = {"run_id": "r1", "task_id": "task-1", "done_s3": "s3://bucket/runs/r1/done/task-1.done.json"}
        with tempfile.TemporaryDirectory() as tmp:
            task_path = Path(tmp) / "tasks.jsonl"
            task_path.write_text(json.dumps(task) + "\n")
            args = types.SimpleNamespace(
                run_id="r1",
                output_prefix="s3://bucket/runs/r1",
                tasks_jsonl=task_path,
                tasks_s3=None,
                artifact_dir=Path(tmp) / "finalizer",
                workers=1,
                progress_interval=0,
                max_inline_outputs=1000,
                use_listing_index=False,
                preload_s3_prefix=[],
                write_repair_jsonl=None,
                upload=False,
                publish_ready=False,
                ready_key="READY",
                allow_incomplete_ready=False,
                allow_legacy_done_markers=True,
                require_complete=True,
            )
            with patch("sweetspot.cli.boto3.client", return_value=s3), contextlib.redirect_stdout(io.StringIO()):
                rc = cmd_finalize(args)
            self.assertEqual(rc, 2)
            manifest = json.loads((Path(tmp) / "finalizer" / "final_manifest.json").read_text())
            self.assertEqual(manifest["done_count"], 0)
            self.assertEqual(manifest["done_marker_count"], 1)
            self.assertEqual(manifest["invalid_marker_count"], 1)
            repair_rows = [json.loads(line) for line in (Path(tmp) / "finalizer" / "repair_tasks.jsonl").read_text().splitlines()]
            self.assertTrue(repair_rows[0]["done_s3"].startswith(task["done_s3"] + ".repair-"))

    def test_finalize_accepts_valid_repair_done_marker_for_invalid_canonical_marker(self) -> None:
        s3 = FakeFinalizeS3()
        task = {"run_id": "r1", "task_id": "task-1", "done_s3": "s3://bucket/runs/r1/done/task-1.done.json"}
        repair_done = task["done_s3"] + ".repair-abc"
        repair_task = dict(task)
        repair_task["done_s3"] = repair_done
        repair_task["sweetspot_repair_reason"] = "invalid_done_marker"
        s3.objects[("bucket", "runs/r1/done/task-1.done.json")] = {"Body": b"not-json"}
        s3.objects[("bucket", "runs/r1/done/task-1.done.json.repair-abc")] = {
            "Body": json.dumps(
                {
                    "schema": "sweetspot.done_marker.v2",
                    "run_id": "r1",
                    "task_id": "task-1",
                    "task_hash": task_hash(repair_task),
                    "attempt_id": "attempt-r",
                    "done_s3": repair_done,
                    "output_s3": "",
                }
            ).encode()
        }
        with tempfile.TemporaryDirectory() as tmp:
            task_path = Path(tmp) / "tasks.jsonl"
            task_path.write_text(json.dumps(task) + "\n")
            args = types.SimpleNamespace(
                run_id="r1",
                output_prefix="s3://bucket/runs/r1",
                tasks_jsonl=task_path,
                tasks_s3=None,
                artifact_dir=Path(tmp) / "finalizer",
                workers=1,
                progress_interval=0,
                max_inline_outputs=1000,
                use_listing_index=False,
                preload_s3_prefix=[],
                write_repair_jsonl=None,
                upload=False,
                publish_ready=False,
                ready_key="READY",
                allow_incomplete_ready=False,
                allow_legacy_done_markers=True,
                require_complete=True,
            )
            with patch("sweetspot.cli.boto3.client", return_value=s3), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(cmd_finalize(args), 0)
            manifest = json.loads((Path(tmp) / "finalizer" / "final_manifest.json").read_text())
            self.assertEqual(manifest["done_count"], 1)
            self.assertEqual(manifest["missing_count"], 0)
            statuses = [json.loads(line) for line in (Path(tmp) / "finalizer" / "task_status.jsonl").read_text().splitlines()]
            self.assertEqual(statuses[0]["done_s3"], repair_done)

    def test_finalize_streams_outputs_manifest_and_caps_inline_outputs(self) -> None:
        s3 = FakeFinalizeS3()
        tasks = []
        for i in range(3):
            task = {
                "run_id": "r1",
                "task_id": f"task-{i}",
                "output_s3": f"s3://bucket/runs/r1/shards/task-{i}.txt",
                "done_s3": f"s3://bucket/runs/r1/done/task-{i}.done.json",
            }
            tasks.append(task)
            s3.objects[("bucket", f"runs/r1/done/task-{i}.done.json")] = {"Body": json.dumps({"schema": "sweetspot.done_marker.v1", "run_id": "r1", "task_id": f"task-{i}", "output_s3": task["output_s3"]}).encode()}
            s3.objects[("bucket", f"runs/r1/shards/task-{i}.txt")] = {"Body": b"ok"}
        with tempfile.TemporaryDirectory() as tmp:
            task_path = Path(tmp) / "tasks.jsonl"
            task_path.write_text("".join(json.dumps(t) + "\n" for t in tasks))
            args = types.SimpleNamespace(
                run_id="r1",
                output_prefix="s3://bucket/runs/r1",
                tasks_jsonl=task_path,
                tasks_s3=None,
                artifact_dir=Path(tmp) / "finalizer",
                workers=2,
                progress_interval=0,
                max_inline_outputs=2,
                use_listing_index=True,
                preload_s3_prefix=[],
                write_repair_jsonl=None,
                upload=False,
                publish_ready=False,
                ready_key="READY",
                allow_incomplete_ready=False,
                allow_legacy_done_markers=True,
                require_complete=True,
            )
            with patch("sweetspot.cli.boto3.client", return_value=s3), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(cmd_finalize(args), 0)
            manifest = json.loads((Path(tmp) / "finalizer" / "final_manifest.json").read_text())
            outputs_rows = [json.loads(line) for line in (Path(tmp) / "finalizer" / "outputs.jsonl").read_text().splitlines()]
            self.assertEqual(manifest["task_count"], 3)
            self.assertEqual(len(manifest["outputs"]), 2)
            self.assertTrue(manifest["outputs_truncated"])
            self.assertEqual(len(outputs_rows), 3)
            self.assertIn("s3://bucket/runs/r1/done/", manifest["existence_index_prefixes"])


if __name__ == "__main__":
    unittest.main()
