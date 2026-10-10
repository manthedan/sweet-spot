import tempfile
import threading
import time
import unittest
from pathlib import Path

from sweetspot.local import run_local
from sweetspot.store import Store
from sweetspot.tasks import validate


class LocalTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.prefix = str(self.root / "run")

    def test_resume_repairs_an_interrupted_promotion(self):
        tasks = [validate({"id": "a", "command": ["sh", "-c", "echo 1 > {out}/x"]})]
        self.assertEqual(run_local(tasks, prefix=self.prefix, store=Store(), echo=False)["committed"], 1)
        (self.root / "run/out/a/x").unlink()  # as if the process died between the marker and the copy
        self.assertEqual(run_local(tasks, prefix=self.prefix, store=Store(), echo=False)["skipped"], 1)
        self.assertEqual((self.root / "run/out/a/x").read_text(), "1\n")

    def test_stop_interrupts_running_tasks_and_skips_the_rest(self):
        tasks = [validate({"id": f"t{i}", "command": ["sleep", "30"]}) for i in range(3)]
        stop = threading.Event()
        threading.Timer(0.5, stop.set).start()
        t0 = time.monotonic()
        counts = run_local(tasks, prefix=self.prefix, store=Store(), jobs=1, echo=False, stop=stop)
        self.assertLess(time.monotonic() - t0, 15)
        self.assertEqual(counts["interrupted"], 3)
        self.assertEqual(counts["committed"], 0)

    def test_never_rewrites_a_cloud_runs_ledger(self):
        run = self.root / "run"
        run.mkdir()
        (run / "run.json").write_text('{"task_count": 1}')
        (run / "tasks.jsonl").write_text('{"command":["sh","-c","echo 1 > {out}/x"],"id":"a"}\n')
        stranger = [validate({"id": "z", "command": ["true"]})]
        with self.assertRaisesRegex(ValueError, "not part of the run"):
            run_local(stranger, prefix=self.prefix, store=Store(), echo=False)
        self.assertIn('"id":"a"', (run / "tasks.jsonl").read_text())
        mine = [validate({"id": "a", "command": ["sh", "-c", "echo 1 > {out}/x"]})]
        self.assertEqual(run_local(mine, prefix=self.prefix, store=Store(), echo=False)["committed"], 1)

    def test_changed_done_task_is_refused_before_anything_runs(self):
        run_local([validate({"id": "a", "command": ["sh", "-c", "echo 1 > {out}/x"]})], prefix=self.prefix, store=Store(), echo=False)
        changed = [validate({"id": "a", "command": ["sh", "-c", "echo 2 > {out}/x"]}), validate({"id": "b", "command": ["sh", "-c", "echo 3 > {out}/x"]})]
        with self.assertRaisesRegex(ValueError, "different contents"):
            run_local(changed[1:], ledger=changed, prefix=self.prefix, store=Store(), echo=False)
        self.assertFalse((self.root / "run/done/b.json").exists())
        self.assertFalse((self.root / "run/tasks.jsonl").exists())  # local mode never writes a ledger

    def test_same_size_damage_is_repaired_on_resume(self):
        tasks = [validate({"id": "a", "command": ["sh", "-c", "echo 1 > {out}/x"]})]
        run_local(tasks, prefix=self.prefix, store=Store(), echo=False)
        (self.root / "run/out/a/x").write_text("2\n")
        run_local(tasks, prefix=self.prefix, store=Store(), echo=False)
        self.assertEqual((self.root / "run/out/a/x").read_text(), "1\n")
