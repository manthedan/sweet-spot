import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sweetspot import protocol
from sweetspot.runner import run_task
from sweetspot.store import Store
from sweetspot.tasks import task_hash, validate
from tests.fakeaws import BUCKET, AwsTestCase


def _attempt(store, prefix, task, content, workdir):
    """Run one attempt of `task` (same task every time) whose output is `content`: a nondeterministic duplicate."""
    t = validate({**task, "command": ["sh", "-c", "echo $SS_TEST_CONTENT > {out}/r.txt"]})
    with mock.patch.dict("os.environ", {"SS_TEST_CONTENT": content}):
        result = run_task(t, store=store, task_dir=Path(tempfile.mkdtemp(dir=workdir)) / "t", echo=False)
    attempt = protocol.new_attempt_id()
    return attempt, protocol.record_attempt(store, prefix, t, task_hash(t), attempt, result, {"host": "test"})


class LocalProtocolTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.prefix = str(self.root / "run")
        self.store = Store()

    def test_first_attempt_wins_and_loser_never_reaches_out(self):
        task = {"id": "t1"}
        a1, o1 = _attempt(self.store, self.prefix, task, "first", self.root)
        a2, o2 = _attempt(self.store, self.prefix, task, "second", self.root)
        self.assertEqual((o1, o2), ("committed", "duplicate"))
        self.assertEqual((self.root / "run/out/t1/r.txt").read_text(), "first\n")
        self.assertFalse((self.root / f"run/attempts/t1/{a2}/out/r.txt").exists())
        marker = json.loads((self.root / "run/done/t1.json").read_text())
        self.assertEqual(marker["attempt"], a1)
        summary = json.loads((self.root / f"run/attempts/t1/{a2}/summary.json").read_text())
        self.assertEqual(summary["outcome"], "duplicate")

    def test_failed_attempt_writes_failure_record_only(self):
        t = validate({"id": "f", "command": ["sh", "-c", "echo oops >&2; exit 2"]})
        result = run_task(t, store=self.store, task_dir=self.root / "x", echo=False)
        outcome = protocol.record_attempt(self.store, self.prefix, t, task_hash(t), "A1", result, {})
        self.assertEqual(outcome, "failed")
        self.assertFalse((self.root / "run/done/f.json").exists())
        rec = json.loads((self.root / "run/failures/f/A1.json").read_text())
        self.assertEqual((rec["status"], rec["exit_code"]), ("failed", 2))
        self.assertIn("oops", rec["stderr_tail"])
        self.assertEqual((self.root / "run/attempts/f/A1/stderr.txt").read_text(), "oops\n")

    def test_diagnostics_failure_does_not_fail_a_successful_attempt(self):
        t = validate({"id": "d", "command": ["sh", "-c", "echo log; echo 1 > {out}/x"]})
        result = run_task(t, store=self.store, task_dir=self.root / "x", echo=False)
        real_upload = self.store.upload

        def flaky_upload(path, uri):
            if uri.endswith("stdout.txt"):
                raise OSError("log upload failed")
            return real_upload(path, uri)

        with mock.patch.object(self.store, "upload", side_effect=flaky_upload):
            self.assertEqual(protocol.record_attempt(self.store, self.prefix, t, task_hash(t), "A1", result, {}), "committed")

    def test_lost_put_response_still_counts_as_our_win(self):
        t = validate({"id": "l", "command": ["sh", "-c", "echo 1 > {out}/x"]})
        result = run_task(t, store=self.store, task_dir=self.root / "x", echo=False)
        real = self.store.put_if_absent

        def put_then_pretend_412(uri, data):
            real(uri, data)  # the write landed...
            return False  # ...but the client saw a retry hit 412

        with mock.patch.object(self.store, "put_if_absent", side_effect=put_then_pretend_412):
            outcome = protocol.record_attempt(self.store, self.prefix, t, task_hash(t), "A1", result, {})
        self.assertEqual(outcome, "committed")
        self.assertTrue((self.root / "run/out/l/x").exists())
        self.assertTrue((self.root / "run/attempts/l/A1/out/x").exists())

    def test_marker_for_a_different_task_is_an_error(self):
        _attempt(self.store, self.prefix, {"id": "m"}, "v1", self.root)
        marker = protocol.read_json(self.store, protocol.done_uri(self.prefix, "m"))
        with self.assertRaisesRegex(ValueError, "different contents"):
            protocol.check_marker(marker, "m", "0" * 64)


class S3ProtocolTest(AwsTestCase):
    def test_commit_and_promote_on_s3(self):
        prefix = f"s3://{BUCKET}/ss/r"
        a1, o1 = _attempt(self.store, prefix, {"id": "s"}, "one", self.tmp)
        _, o2 = _attempt(self.store, prefix, {"id": "s"}, "two", self.tmp)
        self.assertEqual((o1, o2), ("committed", "duplicate"))
        body = self.s3.get_object(Bucket=BUCKET, Key="ss/r/out/s/r.txt")["Body"].read()
        self.assertEqual(body, b"one\n")
        self.assertEqual(self.get_json("ss/r/done/s.json")["attempt"], a1)
