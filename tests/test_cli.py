import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path

from sweetspot.cli import main


def run_cli(*argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        rc = main(list(argv))
    return rc, out.getvalue(), err.getvalue()


class CliTest(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        self.cwd = os.getcwd()
        os.chdir(self.d)
        self.addCleanup(os.chdir, self.cwd)
        for i in range(2):
            (self.d / f"shard_{i}.txt").write_text(f"line{i}\n" * 3)
        (self.d / "inputs.txt").write_text("".join(f"{self.d}/shard_{i}.txt\n" for i in range(2)))

    def test_tasks_then_local(self):
        rc, out, _ = run_cli("tasks", "--inputs", "inputs.txt", "--chunks", "2", "--units", "3", "--", "sh", "-c", "echo {chunk} > {out}/c.txt; wc -l < {input} >> {out}/c.txt")
        self.assertEqual(rc, 0)
        tasks = [json.loads(line) for line in out.splitlines()]
        self.assertEqual([t["id"] for t in tasks], ["shard_0-c0", "shard_0-c1", "shard_1-c0", "shard_1-c1"])
        self.assertEqual(tasks[0]["units"], 3)
        Path("tasks.jsonl").write_text(out)

        rc, out, _ = run_cli("local", "tasks.jsonl", "-j", "2", "--limit", "3")
        self.assertEqual((rc, json.loads(out.splitlines()[-1])["committed"]), (0, 3))
        self.assertEqual(Path("sweetspot-local/tasks/out/shard_0-c1/c.txt").read_text(), "1\n3\n")
        rc, out, _ = run_cli("local", "tasks.jsonl")
        self.assertEqual(json.loads(out.splitlines()[-1]), {"committed": 1, "duplicate": 0, "failed": 0, "skipped": 3, "interrupted": 0})

    def test_local_reports_failure_exit_code(self):
        Path("t.jsonl").write_text(json.dumps({"id": "x", "command": ["false"]}) + "\n")
        rc, _, err = run_cli("local", "t.jsonl")
        self.assertEqual(rc, 1)
        self.assertIn("[failed] x failed", err)

    def test_errors_are_one_line(self):
        Path("bad.jsonl").write_text('{"id": "x"}\n')
        rc, _, err = run_cli("local", "bad.jsonl")
        self.assertEqual(rc, 2)
        self.assertIn("command must be", err)
        rc, _, err = run_cli("status", "r1", "--config", "missing.json")
        self.assertEqual(rc, 2)
        self.assertIn("config file not found", err)
