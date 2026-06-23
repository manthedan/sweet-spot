from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest import mock

if "boto3" not in sys.modules:
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda *_args, **_kwargs: object())
if "botocore" not in sys.modules:
    sys.modules["botocore"] = types.ModuleType("botocore")
if "botocore.exceptions" not in sys.modules:
    exceptions = types.ModuleType("botocore.exceptions")

    class ClientError(Exception):
        def __init__(self, response: dict[str, object] | None = None) -> None:
            super().__init__(response)
            self.response = response or {}

    exceptions.ClientError = ClientError
    sys.modules["botocore.exceptions"] = exceptions

from spotbatch.worker import run_task


class RunTaskTests(unittest.TestCase):
    def test_existing_done_marker_wins_before_timeout_or_command_validation(self) -> None:
        task = {
            "run_id": "run-1",
            "task_id": "already-done",
            "timeout_seconds": "inf",
            "command": "not-a-list",
            "done_s3": "s3://bucket/run/done/already-done.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("spotbatch.worker.s3_exists", return_value=True):
            result = run_task(task, s3=object(), work_root=Path(tmp))
        self.assertEqual(result["event"], "skip_existing_done")

    def test_missing_expected_output_does_not_publish_done_marker(self) -> None:
        text_uploads: list[tuple[str, dict[str, object]]] = []

        def capture_text(_s3, text: str, uri: str, _content_type: str = "application/json") -> None:
            text_uploads.append((uri, json.loads(text)))

        task = {
            "run_id": "run-1",
            "task_id": "task-1",
            "command": [sys.executable, "-c", "pass"],
            "output_s3": "s3://bucket/run/shards/task-1.txt",
            "summary_s3": "s3://bucket/run/summaries/task-1.summary.json",
            "done_s3": "s3://bucket/run/done/task-1.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("spotbatch.worker.s3_exists", return_value=False), \
             mock.patch("spotbatch.worker.s3_upload_file") as upload_file, \
             mock.patch("spotbatch.worker.s3_upload_text", side_effect=capture_text):
            with self.assertRaisesRegex(RuntimeError, "expected output file was not produced"):
                run_task(task, s3=object(), work_root=Path(tmp))

        upload_file.assert_not_called()
        uploaded_uris = [uri for uri, _payload in text_uploads]
        self.assertIn("s3://bucket/run/summaries/task-1.summary.json", uploaded_uris)
        self.assertNotIn("s3://bucket/run/done/task-1.done.json", uploaded_uris)
        summary = text_uploads[0][1]
        self.assertIn("expected output file was not produced", str(summary["framework_error"]))

    def test_task_id_collisions_do_not_reuse_stale_output(self) -> None:
        file_uploads: list[tuple[str, str]] = []

        def capture_file(_s3, path: Path, uri: str, _content_type: str | None = None) -> None:
            file_uploads.append((uri, path.read_text()))

        base = {
            "run_id": "run-1",
            "summary_s3": "s3://bucket/run/summaries/task.summary.json",
        }
        first = {
            **base,
            "task_id": "a/b",
            "command": [sys.executable, "-c", "from pathlib import Path; import os; Path(os.environ['SPOTBATCH_OUTPUT_PATH']).write_text('fresh')"],
            "output_s3": "s3://bucket/run/shards/a-b.txt",
            "done_s3": "s3://bucket/run/done/a-b.done.json",
        }
        second = {
            **base,
            "task_id": "a_b",
            "command": [sys.executable, "-c", "pass"],
            "output_s3": "s3://bucket/run/shards/a_b.txt",
            "done_s3": "s3://bucket/run/done/a_b.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("spotbatch.worker.s3_exists", return_value=False), \
             mock.patch("spotbatch.worker.s3_upload_file", side_effect=capture_file), \
             mock.patch("spotbatch.worker.s3_upload_text"):
            # This is the stale path the previous implementation would have reused for both ids.
            stale_dir = Path(tmp) / "a_b"
            stale_dir.mkdir()
            (stale_dir / "output").write_text("stale")

            run_task(first, s3=object(), work_root=Path(tmp))
            with self.assertRaisesRegex(RuntimeError, "expected output file was not produced"):
                run_task(second, s3=object(), work_root=Path(tmp))

        self.assertEqual(file_uploads, [("s3://bucket/run/shards/a-b.txt", "fresh")])

    def test_default_timeout_bounds_hung_commands_and_writes_summary(self) -> None:
        text_uploads: list[tuple[str, dict[str, object]]] = []

        def capture_text(_s3, text: str, uri: str, _content_type: str = "application/json") -> None:
            text_uploads.append((uri, json.loads(text)))

    def test_run_task_creates_missing_work_root(self) -> None:
        task = {
            "run_id": "run-1",
            "task_id": "task-1",
            "command": [sys.executable, "-c", "pass"],
            "done_s3": "s3://bucket/run/done/task-1.done.json",
        }
        text_uploads: list[tuple[str, dict[str, object]]] = []

        def capture_text(_s3, text: str, uri: str, _content_type: str = "application/json") -> None:
            text_uploads.append((uri, json.loads(text)))

        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("spotbatch.worker.s3_exists", return_value=False), \
             mock.patch("spotbatch.worker.s3_upload_text", side_effect=capture_text):
            work_root = Path(tmp) / "missing-work-root"
            result = run_task(task, s3=object(), work_root=work_root)

        self.assertEqual(result["event"], "processed")
        self.assertTrue(work_root.is_dir())
        self.assertIn("s3://bucket/run/done/task-1.done.json", [uri for uri, _payload in text_uploads])

        task = {
            "run_id": "run-1",
            "task_id": "slow",
            "command": [sys.executable, "-c", "import time; time.sleep(2)"],
            "summary_s3": "s3://bucket/run/summaries/slow.summary.json",
            "done_s3": "s3://bucket/run/done/slow.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("spotbatch.worker.s3_exists", return_value=False), \
             mock.patch("spotbatch.worker.s3_upload_text", side_effect=capture_text):
            with self.assertRaises(subprocess.TimeoutExpired):
                run_task(task, s3=object(), work_root=Path(tmp), default_timeout_seconds=0.05)

        self.assertEqual(len(text_uploads), 1)
        self.assertEqual(text_uploads[0][0], "s3://bucket/run/summaries/slow.summary.json")
        self.assertTrue(text_uploads[0][1]["timed_out"])
        self.assertIn("timed out", str(text_uploads[0][1]["framework_error"]))

    def test_rejects_non_finite_timeouts(self) -> None:
        task = {
            "run_id": "run-1",
            "task_id": "bad-timeout",
            "timeout_seconds": "inf",
            "command": [sys.executable, "-c", "pass"],
            "done_s3": "s3://bucket/run/done/bad-timeout.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch("spotbatch.worker.s3_exists", return_value=False):
            with self.assertRaisesRegex(ValueError, "positive finite"):
                run_task(task, s3=object(), work_root=Path(tmp))

    def test_successful_task_cleans_up_background_descendants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "background-ran"
            child = (
                "import pathlib, sys, time; "
                "time.sleep(0.5); pathlib.Path(sys.argv[1]).write_text('alive')"
            )
            parent = (
                "import subprocess, sys; "
                "subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2]])"
            )
            task = {
                "run_id": "run-1",
                "task_id": "background",
                "command": [sys.executable, "-c", parent, child, str(marker)],
                "done_s3": "s3://bucket/run/done/background.done.json",
            }
            with mock.patch("spotbatch.worker.s3_exists", return_value=False), \
                 mock.patch("spotbatch.worker.s3_upload_text"):
                run_task(task, s3=object(), work_root=Path(tmp), default_timeout_seconds=2)
            time.sleep(0.7)
            self.assertFalse(marker.exists())

    def test_timeout_kills_subprocess_group_descendants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "grandchild-ran"
            grandchild = (
                "import pathlib, signal, sys, time; "
                "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                "time.sleep(0.5); pathlib.Path(sys.argv[1]).write_text('alive')"
            )
            parent = (
                "import subprocess, sys, time; "
                "subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2]]); "
                "time.sleep(5)"
            )
            task = {
                "run_id": "run-1",
                "task_id": "slow-tree",
                "command": [sys.executable, "-c", parent, grandchild, str(marker)],
                "done_s3": "s3://bucket/run/done/slow-tree.done.json",
            }
            with mock.patch("spotbatch.worker.s3_exists", return_value=False):
                with self.assertRaises(subprocess.TimeoutExpired):
                    run_task(task, s3=object(), work_root=Path(tmp), default_timeout_seconds=0.05)
            time.sleep(0.7)
            self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
