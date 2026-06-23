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

from spotbatch.cli import _auto_canary_indices, _parse_index_selection, _redact_env, _supervisor_desired_workers, cmd_derive_canary, cmd_finalize, cmd_supervise_workers


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
                rewrite_run_id=True,
                include_dlq_probe=True,
            )
            with contextlib.redirect_stdout(io.StringIO()):
                rc = cmd_derive_canary(args)
            self.assertEqual(rc, 0)
            canary_task = json.loads((out_dir / "canary_tasks.jsonl").read_text())
            self.assertEqual(canary_task["run_id"], "canary-r")
            manifest = json.loads((out_dir / "canary_manifest.json").read_text())
            self.assertEqual(manifest["selected_indices"], [1])
            self.assertEqual(manifest["expected_done_s3"], ["s3://b/r/done/t1"])
            self.assertTrue((out_dir / "dlq_probe_task.jsonl").exists())


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
        return {}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str) -> dict[str, object]:
        self.objects[(Bucket, Key)] = {"Body": Body, "ContentType": ContentType}
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
        s3.objects[("bucket", "runs/r1/done/task-1.done.json")] = {"Body": b"{}"}
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

    def test_finalize_publishes_ready_after_complete_manifest_upload(self) -> None:
        s3 = FakeFinalizeS3()
        s3.objects[("bucket", "runs/r1/done/task-10.done.json")] = {"Body": b"{}"}
        s3.objects[("bucket", "runs/r1/done/task-2.done.json")] = {"Body": b"{}"}
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
