from __future__ import annotations

import contextlib
import io
import json
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


class VersionTests(unittest.TestCase):
    def test_version_reports_installed_package(self) -> None:
        out = io.StringIO()
        with patch("sweetspot.cli.importlib_metadata.version", return_value="1.2.3"), contextlib.redirect_stdout(out):
            self.assertEqual(cmd_version(types.SimpleNamespace()), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["schema"], "sweetspot.version.v1")
        self.assertEqual(report["package"], "sweetspot")
        self.assertEqual(report["version"], "1.2.3")


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
        self.assertIn("RUNNING\t1", table)


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
