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

from spotbatch.cli import _auto_canary_indices, _job_log_group, _job_log_stream, _parse_index_selection, _redact_env, _supervisor_desired_workers, _validate_s3_delete_prefix, cmd_derive_canary, cmd_finalize, cmd_logs, cmd_s3_delete_prefix, cmd_supervise_workers
from spotbatch.worker import task_hash


class CanaryTests(unittest.TestCase):
    def test_explicit_descending_canary_range_is_rejected(self) -> None:
        with self.assertRaisesRegex(SystemExit, "descending"):
            _parse_index_selection("5-3", 10)

    def test_auto_canary_indices_are_deterministic_and_include_tail(self) -> None:
        tasks = [{"task_id": f"t{i}", "schema": "spotbatch.task.v1", "run_id": "r"} for i in range(8)]
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
        tasks = [{"schema": "spotbatch.task.v1", "run_id": "source-r", "task_id": "t0"}]
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
            )
            with contextlib.redirect_stdout(io.StringIO()):
                cmd_derive_canary(args)
            manifest = json.loads((out_dir / "canary_manifest.json").read_text())
            probe = json.loads((out_dir / "dlq_probe_task.jsonl").read_text())
            self.assertEqual(manifest["run_id"], "source-r")
            self.assertEqual(probe["run_id"], "source-r")

    def test_derive_canary_rewrite_run_id_rejects_existing_s3_markers(self) -> None:
        tasks = [{"schema": "spotbatch.task.v1", "run_id": "r", "task_id": "t0", "output_s3": "s3://b/r/shards/t0"}]
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
            {"schema": "spotbatch.task.v1", "run_id": "r", "task_id": "t0", "output_s3": "s3://b/r/shards/t0"},
            {"schema": "spotbatch.task.v1", "run_id": "r", "task_id": "t1", "done_s3": "s3://b/r/done/t1"},
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
            self.assertTrue((out_dir / "dlq_probe_task.jsonl").exists())


class FakeLogsClient:
    def __init__(self) -> None:
        self.kwargs: dict[str, object] | None = None

    def get_log_events(self, **kwargs):
        self.kwargs = kwargs
        return {"events": [], "nextForwardToken": "next"}


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
        with patch("spotbatch.cli.boto3.Session", return_value=FakeLogSession(logs)), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cmd_logs(args), 0)
        self.assertEqual(logs.kwargs["startFromHead"], True)

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
        with patch("spotbatch.cli.boto3.Session", return_value=session), contextlib.redirect_stdout(io.StringIO()):
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
            with patch("spotbatch.cli.boto3.Session", return_value=Session(s3)), self.assertRaisesRegex(RuntimeError, "DeleteObjects"):
                cmd_s3_delete_prefix(args)
        self.assertFalse(s3.marker_written)


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


class FinalizeTests(unittest.TestCase):
    def test_publish_ready_requires_upload(self) -> None:
        args = types.SimpleNamespace(publish_ready=True, upload=False)
        with self.assertRaisesRegex(SystemExit, "--upload"):
            cmd_finalize(args)

    def test_ready_key_cannot_collide_with_manifest(self) -> None:
        args = types.SimpleNamespace(publish_ready=True, upload=True, ready_key="manifests/final_manifest.json")
        with self.assertRaisesRegex(SystemExit, "collide"):
            cmd_finalize(args)

    def test_finalize_writes_repair_tasks_and_refuses_ready_when_incomplete(self) -> None:
        s3 = FakeFinalizeS3()
        s3.objects[("bucket", "runs/r1/done/task-1.done.json")] = {"Body": json.dumps({"schema": "spotbatch.done_marker.v1", "run_id": "r1", "task_id": "task-1", "output_s3": ""}).encode()}
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
                require_complete=False,
            )
            with patch("spotbatch.cli.boto3.client", return_value=s3), contextlib.redirect_stdout(io.StringIO()):
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
        s3.objects[("bucket", "runs/r1/done/task-1.done.json")] = {"Body": json.dumps({"schema": "spotbatch.done_marker.v1", "run_id": "r1", "task_id": "task-1", "output_s3": "s3://bucket/runs/r1/shards/task-1.txt"}).encode()}
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
                require_complete=False,
            )
            with patch("spotbatch.cli.boto3.client", return_value=s3), contextlib.redirect_stdout(io.StringIO()):
                rc = cmd_finalize(args)
            self.assertEqual(rc, 2)
            manifest = json.loads(s3.objects[("bucket", "runs/r1/manifests/final_manifest.json")]["Body"])
            repair_rows = [json.loads(line) for line in (Path(tmp) / "finalizer" / "repair_tasks.jsonl").read_text().splitlines()]
            self.assertEqual(repair_rows[0]["output_s3"], "s3://bucket/runs/r1/shards/task-1.txt")
            self.assertTrue(repair_rows[0]["done_s3"].startswith("s3://bucket/runs/r1/done/task-1.done.json.repair-"))
            self.assertEqual(manifest["missing_done_count"], 0)
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
            "schema": "spotbatch.done_marker.v2",
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
                require_complete=True,
            )
            with patch("spotbatch.cli.boto3.client", return_value=s3), contextlib.redirect_stdout(io.StringIO()):
                rc = cmd_finalize(args)
            self.assertEqual(rc, 2)
            manifest = json.loads((Path(tmp) / "finalizer" / "final_manifest.json").read_text())
            self.assertEqual(manifest["missing_output_count"], 1)
            repair_rows = [json.loads(line) for line in (Path(tmp) / "finalizer" / "repair_tasks.jsonl").read_text().splitlines()]
            self.assertEqual(repair_rows[0]["output_s3"], task["output_s3"])

    def test_finalize_publishes_ready_after_complete_manifest_upload(self) -> None:
        s3 = FakeFinalizeS3()
        s3.objects[("bucket", "runs/r1/done/task-10.done.json")] = {"Body": json.dumps({"schema": "spotbatch.done_marker.v1", "run_id": "r1", "task_id": "task-10", "output_s3": "s3://bucket/runs/r1/shards/task-10.txt"}).encode()}
        s3.objects[("bucket", "runs/r1/done/task-2.done.json")] = {"Body": json.dumps({"schema": "spotbatch.done_marker.v1", "run_id": "r1", "task_id": "task-2", "output_s3": "s3://bucket/runs/r1/shards/task-2.txt"}).encode()}
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
                require_complete=True,
            )
            with patch("spotbatch.cli.boto3.client", return_value=s3), contextlib.redirect_stdout(io.StringIO()):
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


if __name__ == "__main__":
    unittest.main()
