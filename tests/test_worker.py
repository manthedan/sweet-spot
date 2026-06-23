from __future__ import annotations

import hashlib
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

from sweetspot.worker import SAFE_TASK_TIMEOUT_SECONDS, _heartbeat, run_task, task_hash, validate_worker_timing


class RunTaskTests(unittest.TestCase):
    def test_existing_done_marker_wins_before_timeout_or_command_validation(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "already-done",
            "timeout_seconds": "inf",
            "command": "not-a-list",
            "done_s3": "s3://bucket/run/done/already-done.done.json",
        }
        marker = {"schema": "sweetspot.done_marker.v1", "run_id": "run-1", "task_id": "already-done", "output_s3": ""}
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists", return_value=True), mock.patch("sweetspot.worker.s3_download_text", return_value=json.dumps(marker)):
            result = run_task(task, s3=object(), work_root=Path(tmp))
        self.assertEqual(result["event"], "skip_existing_done")

    def test_missing_expected_output_does_not_publish_done_marker(self) -> None:
        text_uploads: list[tuple[str, dict[str, object]]] = []

        def capture_text(_s3, text: str, uri: str, _content_type: str = "application/json") -> None:
            text_uploads.append((uri, json.loads(text)))

        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "task-1",
            "command": [sys.executable, "-c", "pass"],
            "output_s3": "s3://bucket/run/shards/task-1.txt",
            "summary_s3": "s3://bucket/run/summaries/task-1.summary.json",
            "done_s3": "s3://bucket/run/done/task-1.done.json",
        }
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch("sweetspot.worker.s3_exists", return_value=False),
            mock.patch("sweetspot.worker.s3_upload_file") as upload_file,
            mock.patch("sweetspot.worker.s3_upload_text", side_effect=capture_text),
        ):
            with self.assertRaisesRegex(RuntimeError, "expected output file was not produced"):
                run_task(task, s3=object(), work_root=Path(tmp))

        upload_file.assert_not_called()
        uploaded_uris = [uri for uri, _payload in text_uploads]
        self.assertTrue(any(uri.startswith("s3://bucket/run/summaries/task-1.summary.json.attempts/") for uri in uploaded_uris))
        self.assertNotIn("s3://bucket/run/done/task-1.done.json", uploaded_uris)
        summary = text_uploads[0][1]
        self.assertIn("expected output file was not produced", str(summary["framework_error"]))

    def test_task_id_collisions_do_not_reuse_stale_output(self) -> None:
        file_uploads: list[tuple[str, str]] = []

        def capture_file(_s3, path: Path, uri: str, _content_type: str | None = None, **_kwargs) -> None:
            file_uploads.append((uri, path.read_text()))

        base = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "summary_s3": "s3://bucket/run/summaries/task.summary.json",
        }
        first = {
            "schema": "sweetspot.task.v1",
            **base,
            "task_id": "a/b",
            "command": [sys.executable, "-c", "from pathlib import Path; import os; Path(os.environ['SWEETSPOT_OUTPUT_PATH']).write_text('fresh')"],
            "output_s3": "s3://bucket/run/shards/a-b.txt",
            "done_s3": "s3://bucket/run/done/a-b.done.json",
        }
        second = {
            "schema": "sweetspot.task.v1",
            **base,
            "task_id": "a_b",
            "command": [sys.executable, "-c", "pass"],
            "output_s3": "s3://bucket/run/shards/a_b.txt",
            "done_s3": "s3://bucket/run/done/a_b.done.json",
        }
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch("sweetspot.worker.s3_exists", return_value=False),
            mock.patch("sweetspot.worker.s3_upload_file", side_effect=capture_file),
            mock.patch("sweetspot.worker.s3_upload_text"),
            mock.patch("sweetspot.worker.s3_upload_text_if_absent", return_value=True),
        ):
            # This is the stale path the previous implementation would have reused for both ids.
            stale_dir = Path(tmp) / "a_b"
            stale_dir.mkdir()
            (stale_dir / "output").write_text("stale")

            run_task(first, s3=object(), work_root=Path(tmp))
            with self.assertRaisesRegex(RuntimeError, "expected output file was not produced"):
                run_task(second, s3=object(), work_root=Path(tmp))

        self.assertEqual(len(file_uploads), 1)
        self.assertTrue(file_uploads[0][0].startswith("s3://bucket/run/shards/a-b.txt.attempts/"))
        self.assertEqual(file_uploads[0][1], "fresh")

    def test_default_timeout_bounds_hung_commands_and_writes_summary(self) -> None:
        text_uploads: list[tuple[str, dict[str, object]]] = []

        def capture_text(_s3, text: str, uri: str, _content_type: str = "application/json") -> None:
            text_uploads.append((uri, json.loads(text)))

        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "slow",
            "command": [sys.executable, "-c", "import time; time.sleep(2)"],
            "summary_s3": "s3://bucket/run/summaries/slow.summary.json",
            "done_s3": "s3://bucket/run/done/slow.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists", return_value=False), mock.patch("sweetspot.worker.s3_upload_text", side_effect=capture_text):
            with self.assertRaises(subprocess.TimeoutExpired):
                run_task(task, s3=object(), work_root=Path(tmp), default_timeout_seconds=0.05)

        self.assertEqual(len(text_uploads), 1)
        self.assertTrue(text_uploads[0][0].startswith("s3://bucket/run/summaries/slow.summary.json.attempts/"))
        self.assertTrue(text_uploads[0][1]["timed_out"])
        self.assertIn("timed out", str(text_uploads[0][1]["framework_error"]))

    def test_run_task_creates_missing_work_root(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "task-1",
            "command": [sys.executable, "-c", "pass"],
            "done_s3": "s3://bucket/run/done/task-1.done.json",
        }
        text_uploads: list[tuple[str, dict[str, object]]] = []

        def capture_text(_s3, text: str, uri: str, _content_type: str = "application/json") -> None:
            text_uploads.append((uri, json.loads(text)))

        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch("sweetspot.worker.s3_exists", return_value=False),
            mock.patch("sweetspot.worker.s3_upload_text", side_effect=capture_text),
            mock.patch("sweetspot.worker.s3_upload_text_if_absent", return_value=True),
        ):
            work_root = Path(tmp) / "missing-work-root"
            result = run_task(task, s3=object(), work_root=work_root)
            self.assertTrue(work_root.is_dir())

        self.assertEqual(result["event"], "processed")
        self.assertEqual(text_uploads, [])

    def test_task_hash_covers_custom_payload_fields(self) -> None:
        base = {"run_id": "run-1", "task_id": "hash", "command": ["echo", "ok"], "done_s3": "s3://bucket/run/done/hash.done.json"}
        changed = {**base, "input_s3": "s3://bucket/other/input.json"}
        changed_attempt = {**base, "attempt_id": "user-data"}
        self.assertNotEqual(task_hash(base), task_hash(changed))
        self.assertNotEqual(task_hash(base), task_hash(changed_attempt))

    def test_rejects_missing_task_schema_when_not_already_done(self) -> None:
        task = {
            "run_id": "run-1",
            "task_id": "missing-schema",
            "command": [sys.executable, "-c", "pass"],
            "done_s3": "s3://bucket/run/done/missing-schema.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists", return_value=False):
            with self.assertRaisesRegex(ValueError, "task schema"):
                run_task(task, s3=object(), work_root=Path(tmp))

    def test_rejects_done_marker_outside_allowed_prefix_before_s3_access(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "bad-done-prefix",
            "command": [sys.executable, "-c", "pass"],
            "done_s3": "s3://evil-bucket/runs/r1/done/bad.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists") as exists:
            with self.assertRaisesRegex(ValueError, "outside allowed prefixes"):
                run_task(task, s3=object(), work_root=Path(tmp), allowed_s3_prefixes=["s3://bucket/runs/r1"])
            exists.assert_not_called()

    def test_rejects_s3_uri_outside_allowed_prefixes(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "bad-s3-prefix",
            "command": [sys.executable, "-c", "print('s3://evil-bucket/runs/r1/input')"],
            "done_s3": "s3://bucket/runs/r1/done/bad-s3-prefix.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists", return_value=False):
            with self.assertRaisesRegex(ValueError, "outside allowed prefixes"):
                run_task(task, s3=object(), work_root=Path(tmp), allowed_s3_prefixes=["s3://bucket/runs/r1"])

    def test_rejects_non_finite_timeouts(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "bad-timeout",
            "timeout_seconds": "inf",
            "command": [sys.executable, "-c", "pass"],
            "done_s3": "s3://bucket/run/done/bad-timeout.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists", return_value=False):
            with self.assertRaisesRegex(ValueError, "positive finite"):
                run_task(task, s3=object(), work_root=Path(tmp))

    def test_rejects_disallowed_second_s3_uri_in_same_argument(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "mixed-s3-prefix",
            "command": [sys.executable, "-c", "print('s3://bucket/runs/r1/input; aws s3 cp s3://evil-bucket/secret -')"],
            "done_s3": "s3://bucket/runs/r1/done/mixed-s3-prefix.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists", return_value=False):
            with self.assertRaisesRegex(ValueError, "outside allowed prefixes"):
                run_task(task, s3=object(), work_root=Path(tmp), allowed_s3_prefixes=["s3://bucket/runs/r1"])

    def test_rejects_adjacent_disallowed_s3_uri_in_same_argument(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "adjacent-s3-prefix",
            "command": [sys.executable, "-c", "print('--inputs=s3://bucket/runs/r1/input,s3://evil-bucket/secret')"],
            "done_s3": "s3://bucket/runs/r1/done/adjacent-s3-prefix.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists", return_value=False):
            with self.assertRaisesRegex(ValueError, "outside allowed prefixes"):
                run_task(task, s3=object(), work_root=Path(tmp), allowed_s3_prefixes=["s3://bucket/runs/r1"])

    def test_rejects_embedded_bucket_root_s3_uri_outside_allowed_prefix(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "bucket-root-s3-prefix",
            "command": [sys.executable, "-c", "print('aws s3 sync s3://evil-bucket/ /tmp/in')"],
            "done_s3": "s3://bucket/runs/r1/done/bucket-root-s3-prefix.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists", return_value=False):
            with self.assertRaisesRegex(ValueError, "outside allowed prefixes"):
                run_task(task, s3=object(), work_root=Path(tmp), allowed_s3_prefixes=["s3://bucket/runs/r1"])

    def test_rejects_sibling_key_with_trailing_dot_as_outside_allowed_prefix(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "sibling-dot-s3-prefix",
            "command": [sys.executable, "-c", "print('s3://bucket/runs/r1.')"],
            "done_s3": "s3://bucket/runs/r1/done/sibling-dot-s3-prefix.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists", return_value=False):
            with self.assertRaisesRegex(ValueError, "outside allowed prefixes"):
                run_task(task, s3=object(), work_root=Path(tmp), allowed_s3_prefixes=["s3://bucket/runs/r1"])

    def test_rejects_sibling_key_with_comma_as_outside_allowed_prefix(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "sibling-comma-s3-prefix",
            "command": [sys.executable, "-c", "print('s3://bucket/runs/r1,secret')"],
            "done_s3": "s3://bucket/runs/r1/done/sibling-comma-s3-prefix.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists", return_value=False):
            with self.assertRaisesRegex(ValueError, "outside allowed prefixes"):
                run_task(task, s3=object(), work_root=Path(tmp), allowed_s3_prefixes=["s3://bucket/runs/r1"])

    def test_task_metrics_file_populates_cost_telemetry(self) -> None:
        uploads: dict[str, str] = {}

        def capture_text(_s3, text: str, uri: str, _content_type: str = "application/json") -> None:
            uploads[uri] = text

        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "telemetry",
            "command": [
                sys.executable,
                "-c",
                "import json, os; open(os.environ['SWEETSPOT_METRICS_PATH'], 'w').write(json.dumps({'completed_units': 250, 'useful_compute_seconds': 5, 'input_bytes': 1000}))",
            ],
            "summary_s3": "s3://bucket/run/summaries/telemetry.summary.json",
            "done_s3": "s3://bucket/run/done/telemetry.done.json",
        }
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch.dict("os.environ", {"SWEETSPOT_INSTANCE_TYPE": "c7i.large", "AWS_REGION": "us-west-2", "AWS_BATCH_JOB_ATTEMPT": "2"}, clear=False),
            mock.patch("sweetspot.worker.s3_exists", return_value=False),
            mock.patch("sweetspot.worker.s3_upload_text", side_effect=capture_text),
            mock.patch("sweetspot.worker.s3_upload_text_if_absent", return_value=True),
        ):
            run_task(task, s3=object(), work_root=Path(tmp), worker_context={"receive_count": "2", "sent_timestamp_ms": str(int((time.time() - 3) * 1000))})

        summary_uri = next(uri for uri in uploads if uri.endswith("/summary.json"))
        telemetry = json.loads(uploads[summary_uri])["telemetry"]
        self.assertEqual(telemetry["instance_type"], "c7i.large")
        self.assertEqual(telemetry["region"], "us-west-2")
        self.assertEqual(telemetry["completed_units"], 250.0)
        self.assertEqual(telemetry["useful_compute_seconds"], 5.0)
        self.assertEqual(telemetry["units_per_second"], 50.0)
        self.assertEqual(telemetry["input_bytes"], 1000.0)
        self.assertTrue(telemetry["retry"])
        self.assertEqual(telemetry["receive_count"], 2)
        self.assertGreaterEqual(telemetry["startup_delay_seconds"], 0)

    def test_streams_redacts_caps_and_summarizes_task_logs(self) -> None:
        uploads: dict[str, str] = {}

        def capture_text(_s3, text: str, uri: str, _content_type: str = "application/json") -> None:
            uploads[uri] = text

        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "logs",
            "command": [sys.executable, "-c", "print('token=SECRET ' + 'x' * 80)"],
            "summary_s3": "s3://bucket/run/summaries/logs.summary.json",
            "done_s3": "s3://bucket/run/done/logs.done.json",
        }
        import contextlib
        import io

        out = io.StringIO()
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch("sweetspot.worker.s3_exists", return_value=False),
            mock.patch("sweetspot.worker.s3_upload_text", side_effect=capture_text),
            mock.patch("sweetspot.worker.s3_upload_text_if_absent", return_value=True),
            contextlib.redirect_stdout(out),
        ):
            result = run_task(task, s3=object(), work_root=Path(tmp), log_tail_bytes=16, max_log_bytes=20, redact_regexes=[r"token=\w+"])

        self.assertEqual(result["event"], "processed")
        self.assertIn("<redacted>", out.getvalue())
        self.assertNotIn("SECRET", out.getvalue())
        summary_uri = next(uri for uri in uploads if uri.endswith("/summary.json"))
        summary = json.loads(uploads[summary_uri])
        self.assertTrue(summary["stdout_log"]["truncated"])
        self.assertLessEqual(len(summary["stdout_tail"].encode()), 16)
        self.assertNotIn("SECRET", summary["stdout_tail"])
        stdout_uri = next(uri for uri in uploads if uri.endswith("/stdout.txt"))
        self.assertLessEqual(len(uploads[stdout_uri].encode()), 20)
        self.assertIn("<redacted>", uploads[stdout_uri])

    def test_redaction_handles_tokens_split_across_pipe_chunks(self) -> None:
        uploads: dict[str, str] = {}

        def capture_text(_s3, text: str, uri: str, _content_type: str = "application/json") -> None:
            uploads[uri] = text

        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "split-redact",
            "command": [sys.executable, "-c", "import sys; sys.stdout.write('x' * 8190 + 'ABCDEF\\n')"],
            "summary_s3": "s3://bucket/run/summaries/split-redact.summary.json",
            "done_s3": "s3://bucket/run/done/split-redact.done.json",
        }
        import contextlib
        import io

        out = io.StringIO()
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch("sweetspot.worker.s3_exists", return_value=False),
            mock.patch("sweetspot.worker.s3_upload_text", side_effect=capture_text),
            mock.patch("sweetspot.worker.s3_upload_text_if_absent", return_value=True),
            contextlib.redirect_stdout(out),
        ):
            run_task(task, s3=object(), work_root=Path(tmp), log_tail_bytes=9000, max_log_bytes=9000, redact_regexes=[r"ABCDEF"])

        self.assertNotIn("ABCDEF", out.getvalue())
        self.assertIn("<redacted>", out.getvalue())
        stdout_uri = next(uri for uri in uploads if uri.endswith("/stdout.txt"))
        self.assertNotIn("ABCDEF", uploads[stdout_uri])
        self.assertIn("<redacted>", uploads[stdout_uri])

    def test_redaction_suppresses_overlong_unterminated_records(self) -> None:
        uploads: dict[str, str] = {}

        def capture_text(_s3, text: str, uri: str, _content_type: str = "application/json") -> None:
            uploads[uri] = text

        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "long-redact",
            "command": [sys.executable, "-c", "import sys; sys.stdout.write('token=' + 'S' * 70000)"],
            "summary_s3": "s3://bucket/run/summaries/long-redact.summary.json",
            "done_s3": "s3://bucket/run/done/long-redact.done.json",
        }
        import contextlib
        import io

        out = io.StringIO()
        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch("sweetspot.worker.s3_exists", return_value=False),
            mock.patch("sweetspot.worker.s3_upload_text", side_effect=capture_text),
            mock.patch("sweetspot.worker.s3_upload_text_if_absent", return_value=True),
            contextlib.redirect_stdout(out),
        ):
            run_task(task, s3=object(), work_root=Path(tmp), log_tail_bytes=2000, max_log_bytes=2000, redact_regexes=[r"token=\S+"])

        self.assertIn("stream-redaction-window-exceeded", out.getvalue())
        self.assertNotIn("SSSSSS", out.getvalue())
        stdout_uri = next(uri for uri in uploads if uri.endswith("/stdout.txt"))
        self.assertIn("stream-redaction-window-exceeded", uploads[stdout_uri])
        self.assertNotIn("SSSSSS", uploads[stdout_uri])

    def test_rejects_timeouts_above_sqs_safe_cap(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "too-long",
            "timeout_seconds": SAFE_TASK_TIMEOUT_SECONDS + 1,
            "command": [sys.executable, "-c", "pass"],
            "done_s3": "s3://bucket/run/done/too-long.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists", return_value=False):
            with self.assertRaisesRegex(ValueError, "SQS 12h visibility"):
                run_task(task, s3=object(), work_root=Path(tmp))

    def test_rejects_reserved_task_env_overrides(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "bad-env",
            "command": [sys.executable, "-c", "pass"],
            "env": {"SWEETSPOT_DONE_S3": "s3://evil/done"},
            "done_s3": "s3://bucket/run/done/bad-env.done.json",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists", return_value=False):
            with self.assertRaisesRegex(ValueError, "reserved prefix"):
                run_task(task, s3=object(), work_root=Path(tmp))

    def test_worker_timing_validation_rejects_bad_heartbeat_relationships(self) -> None:
        with self.assertRaisesRegex(ValueError, "less than visibility_timeout"):
            validate_worker_timing(visibility_timeout=300, heartbeat_seconds=300, task_timeout_seconds=60)
        with self.assertRaisesRegex(ValueError, "visibility_timeout"):
            validate_worker_timing(visibility_timeout=43201, heartbeat_seconds=300, task_timeout_seconds=60)

    def test_heartbeat_failure_emits_structured_stderr(self) -> None:
        class SQS:
            def change_message_visibility(self, **kwargs):
                raise RuntimeError("lease lost")

        class Stop:
            def __init__(self) -> None:
                self.calls = 0

            def wait(self, seconds: int) -> bool:
                self.calls += 1
                return self.calls > 1

        import contextlib
        import io

        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            _heartbeat(SQS(), "queue-url", "receipt", 30, 1, Stop())
        event = json.loads(err.getvalue())
        self.assertEqual(event["schema"], "sweetspot.heartbeat_error.v1")
        self.assertEqual(event["error"], "lease lost")

    def test_conditional_done_marker_conflict_validates_winner(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "race",
            "command": [sys.executable, "-c", "from pathlib import Path; import os; Path(os.environ['SWEETSPOT_OUTPUT_PATH']).write_text('ok')"],
            "output_s3": "s3://bucket/run/shards/race.txt",
            "summary_s3": "s3://bucket/run/summaries/race.summary.json",
            "done_s3": "s3://bucket/run/done/race.done.json",
        }
        winner_output = "s3://bucket/run/shards/race.txt.attempts/winner/output"
        winner = {
            "schema": "sweetspot.done_marker.v2",
            "run_id": "run-1",
            "task_id": "race",
            "task_hash": task_hash(task),
            "attempt_id": "winner",
            "done_at": "now",
            "done_s3": "s3://bucket/run/done/race.done.json",
            "output_s3": "s3://bucket/run/shards/race.txt",
            "summary_s3": "",
            "output": {"logical_uri": "s3://bucket/run/shards/race.txt", "uri": winner_output, "size_bytes": 2, "sha256": hashlib.sha256(b"ok").hexdigest()},
        }
        uploads: dict[str, str] = {}

        def capture_text(_s3, text: str, uri: str, _content_type: str = "application/json") -> None:
            uploads[uri] = text

        with (
            tempfile.TemporaryDirectory() as tmp,
            mock.patch("sweetspot.worker.s3_exists", side_effect=[False, True]),
            mock.patch("sweetspot.worker.s3_upload_file"),
            mock.patch("sweetspot.worker.s3_upload_text", side_effect=capture_text),
            mock.patch("sweetspot.worker.s3_upload_text_if_absent", return_value=False),
            mock.patch("sweetspot.worker.s3_download_text", return_value=json.dumps(winner)),
            mock.patch(
                "sweetspot.worker.s3_head_object", return_value={"ContentLength": 2, "Metadata": {"sha256": hashlib.sha256(b"ok").hexdigest(), "sweetspot-task-hash": task_hash(task), "sweetspot-attempt-id": "winner"}}
            ),
        ):
            result = run_task(task, s3=object(), work_root=Path(tmp))
        self.assertEqual(result["event"], "commit_lost_existing_done")
        self.assertEqual(result["winning_attempt_id"], "winner")
        summary_uri = next(uri for uri in uploads if uri.endswith("/summary.json"))
        summary = json.loads(uploads[summary_uri])
        self.assertEqual(summary["commit_status"], "lost")
        self.assertGreater(summary["telemetry"]["discarded_compute_seconds"], 0)

    def test_v2_done_marker_rejects_wrong_attempt_output_uri(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "wrong-output",
            "command": [sys.executable, "-c", "pass"],
            "output_s3": "s3://bucket/run/shards/wrong-output.txt",
            "done_s3": "s3://bucket/run/done/wrong-output.done.json",
        }
        marker = {
            "schema": "sweetspot.done_marker.v2",
            "run_id": "run-1",
            "task_id": "wrong-output",
            "task_hash": task_hash(task),
            "attempt_id": "attempt-a",
            "done_s3": task["done_s3"],
            "output_s3": task["output_s3"],
            "output": {"logical_uri": task["output_s3"], "uri": "s3://bucket/other/object", "size_bytes": 0, "sha256": hashlib.sha256(b"").hexdigest()},
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists", return_value=True), mock.patch("sweetspot.worker.s3_download_text", return_value=json.dumps(marker)):
            with self.assertRaisesRegex(ValueError, "output uri"):
                run_task(task, s3=object(), work_root=Path(tmp))

    def test_corrupt_existing_done_marker_is_not_skipped(self) -> None:
        task = {
            "schema": "sweetspot.task.v1",
            "run_id": "run-1",
            "task_id": "stale",
            "command": [sys.executable, "-c", "pass"],
            "done_s3": "s3://bucket/run/done/stale.done.json",
        }
        marker = {"schema": "sweetspot.done_marker.v2", "run_id": "run-1", "task_id": "stale", "task_hash": "0" * 64, "done_s3": task["done_s3"], "output_s3": ""}
        with tempfile.TemporaryDirectory() as tmp, mock.patch("sweetspot.worker.s3_exists", return_value=True), mock.patch("sweetspot.worker.s3_download_text", return_value=json.dumps(marker)):
            with self.assertRaisesRegex(ValueError, "task_hash mismatch"):
                run_task(task, s3=object(), work_root=Path(tmp))

    def test_successful_task_cleans_up_background_descendants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "background-ran"
            child = "import pathlib, sys, time; " "time.sleep(0.5); pathlib.Path(sys.argv[1]).write_text('alive')"
            parent = "import subprocess, sys; " "subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2]])"
            task = {
                "schema": "sweetspot.task.v1",
                "run_id": "run-1",
                "task_id": "background",
                "command": [sys.executable, "-c", parent, child, str(marker)],
                "done_s3": "s3://bucket/run/done/background.done.json",
            }
            with mock.patch("sweetspot.worker.s3_exists", return_value=False), mock.patch("sweetspot.worker.s3_upload_text"), mock.patch("sweetspot.worker.s3_upload_text_if_absent", return_value=True):
                run_task(task, s3=object(), work_root=Path(tmp), default_timeout_seconds=2)
            time.sleep(0.7)
            self.assertFalse(marker.exists())

    def test_timeout_kills_subprocess_group_descendants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "grandchild-ran"
            grandchild = "import pathlib, signal, sys, time; " "signal.signal(signal.SIGTERM, signal.SIG_IGN); " "time.sleep(0.5); pathlib.Path(sys.argv[1]).write_text('alive')"
            parent = "import subprocess, sys, time; " "subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2]]); " "time.sleep(5)"
            task = {
                "schema": "sweetspot.task.v1",
                "run_id": "run-1",
                "task_id": "slow-tree",
                "command": [sys.executable, "-c", parent, grandchild, str(marker)],
                "done_s3": "s3://bucket/run/done/slow-tree.done.json",
            }
            with mock.patch("sweetspot.worker.s3_exists", return_value=False):
                with self.assertRaises(subprocess.TimeoutExpired):
                    run_task(task, s3=object(), work_root=Path(tmp), default_timeout_seconds=0.05)
            time.sleep(0.7)
            self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
