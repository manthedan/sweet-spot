import io
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from sweetspot.runner import run_task
from sweetspot.store import Store
from sweetspot.tasks import validate


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.n = 0

    def run_(self, task, **kw):
        self.n += 1
        return run_task(validate({"id": "t", **task}), store=Store(), task_dir=self.root / f"t{self.n}", echo=False, **kw)

    def test_success_collects_outputs_metrics_and_inputs(self):
        src = self.root / "input.txt"
        src.write_text("abc\n")
        r = self.run_(
            {
                "command": ["sh", "-c", "cat {in}/i.txt > {out}/a.txt; mkdir {out}/d; echo x > {out}/d/b.txt; echo '{\"units\": 7}' > {out}/_metrics.json; echo hi; echo err >&2"],
                "inputs": {"i.txt": str(src)},
            }
        )
        self.assertEqual(r.status, "ok", r.error)
        self.assertEqual([o.rel for o in r.outputs], ["a.txt", "d/b.txt"])
        self.assertEqual(r.metrics, {"units": 7})
        self.assertEqual(r.inputs["i.txt"]["size"], 4)
        self.assertEqual((r.stdout_tail, r.stderr_tail), ("hi\n", "err\n"))
        self.assertIsNotNone(r.peak_rss_mib)

    def test_allowlist_cwd_and_static_units(self):
        work = self.root / "work"
        work.mkdir()
        r = self.run_({"command": ["sh", "-c", "pwd > {out}/where.txt; echo junk > {out}/part.tmp"], "cwd": str(work), "outputs": ["*.txt"], "units": 3})
        self.assertEqual([o.rel for o in r.outputs], ["where.txt"])
        self.assertEqual(Path(r.outputs[0].path).read_text().strip(), str(work))
        self.assertEqual(r.metrics, {"units": 3})

    def test_failures_are_classified(self):
        # "failed" is the task's fault; "error" might be the host's (counts toward a worker giving up).
        self.assertEqual(self.run_({"command": ["sh", "-c", "exit 4"]}).exit_code, 4)
        self.assertEqual(self.run_({"command": ["true"]}).status, "failed")  # no output files
        self.assertEqual(self.run_({"command": ["true"]}, require_output=False).status, "ok")
        self.assertEqual(self.run_({"command": ["/no/such/binary"]}).status, "failed")
        r = self.run_({"command": ["true"], "inputs": {"x": str(self.root / "missing")}})
        self.assertEqual(r.status, "failed")
        self.assertIn("staging input", r.error)
        r = self.run_({"command": ["sh", "-c", "ln -s /etc/passwd {out}/p"]})
        self.assertEqual(r.status, "failed")
        self.assertIn("not a regular file", r.error)

    def test_staging_network_error_is_an_infrastructure_error(self):
        store = Store()
        with mock.patch.object(store, "download", side_effect=ConnectionResetError("reset")):
            r = run_task(validate({"id": "t", "command": ["true"], "inputs": {"x": "s3://b/k"}}), store=store, task_dir=self.root / "n", echo=False)
        self.assertEqual(r.status, "error")

    def test_bad_metrics_units_are_dropped(self):
        r = self.run_({"command": ["sh", "-c", 'echo 1 > {out}/x; echo \'{"units": "rows"}\' > {out}/_metrics.json'], "units": 9})
        self.assertEqual(r.status, "ok")
        self.assertEqual(r.metrics["units"], 9)
        self.assertIn("units_error", r.metrics)

    def test_broken_log_sink_does_not_block_the_command(self):
        class Broken(io.StringIO):
            def write(self, _s):
                raise OSError("sink gone")

        with mock.patch("sys.stdout", Broken()):
            r = run_task(validate({"id": "t", "command": ["sh", "-c", "head -c 3000000 /dev/zero | tr '\\0' x; echo 1 > {out}/x"], "timeout": 20}), store=Store(), task_dir=self.root / "b")
        self.assertEqual(r.status, "ok", r.error)

    def test_timeout_kills_the_process_group(self):
        marker = self.root / "survivor"
        t0 = time.monotonic()
        r = self.run_({"command": ["sh", "-c", f"(sleep 3; touch {marker}) & sleep 30"], "timeout": 0.5})
        self.assertEqual(r.status, "timeout")
        self.assertLess(time.monotonic() - t0, 5)
        time.sleep(3.5)
        self.assertFalse(marker.exists(), "background child survived the timeout")

    def test_stop_event_interrupts(self):
        stop = threading.Event()
        threading.Timer(0.3, stop.set).start()
        r = self.run_({"command": ["sleep", "30"]}, stop=stop)
        self.assertEqual(r.status, "interrupted")
        self.assertLess(r.elapsed, 5)

    def test_summary_is_json_serializable(self):
        json.dumps(self.run_({"command": ["sh", "-c", "echo 1 > {out}/x"]}).summary())

    def test_relative_task_dir_with_cwd(self):
        work = self.root / "w"
        work.mkdir()
        old = os.getcwd()
        os.chdir(self.root)
        try:
            r = run_task(validate({"id": "t", "command": ["sh", "-c", "echo 1 > {out}/x"], "cwd": str(work)}), store=Store(), task_dir=Path("rel/t"), echo=False)
        finally:
            os.chdir(old)
        self.assertEqual(r.status, "ok", r.error)
