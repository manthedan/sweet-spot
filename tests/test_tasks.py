import json
import tempfile
import unittest
from pathlib import Path

from sweetspot.tasks import expand, generate, read_tasks, task_hash, validate


class ValidateTest(unittest.TestCase):
    def test_normalizes_and_keeps_known_fields(self):
        t = validate({"id": "a", "command": ["x"], "env": {"N": 3}, "inputs": {"b": "s3://x/b", "a": "s3://x/a"}, "timeout": 5, "cwd": "/work", "outputs": ["*.zst"], "units": 10})
        self.assertEqual(t["env"], {"N": "3"})
        self.assertEqual(list(t["inputs"]), ["a", "b"])
        self.assertEqual((t["cwd"], t["outputs"], t["units"]), ("/work", ["*.zst"], 10))

    def test_rejects(self):
        bad = [
            ({"id": "a b", "command": ["x"]}, "id"),
            ({"id": "a", "command": []}, "command"),
            ({"id": "a", "command": ["x"], "extra": 1}, "unknown keys"),
            ({"id": "a", "command": ["x"], "inputs": {"../x": "s3://b/k"}}, "relative path"),
            ({"id": "a", "command": ["x"], "inputs": {"/etc/x": "s3://b/k"}}, "relative path"),
            ({"id": "a", "command": ["x"], "env": {"AWS_REGION": "x"}}, "reserved"),
            ({"id": "a", "command": ["x"], "env": {"SWEETSPOT_X": "x"}}, "reserved"),
            ({"id": "a", "command": ["x"], "timeout": 12 * 3600}, "timeout"),
            ({"id": "a", "command": ["x"], "timeout": True}, "timeout"),
            ({"id": "a", "command": ["x"], "outputs": []}, "outputs"),
            ({"id": "a", "command": ["x"], "units": -1}, "units"),
        ]
        for raw, needle in bad:
            with self.subTest(raw=raw), self.assertRaisesRegex(ValueError, needle):
                validate(raw)

    def test_hash_is_order_independent(self):
        a = validate({"id": "a", "command": ["x"], "env": {"A": "1", "B": "2"}})
        b = validate({"env": {"B": "2", "A": "1"}, "command": ["x"], "id": "a"})
        self.assertEqual(task_hash(a), task_hash(b))
        self.assertNotEqual(task_hash(a), task_hash(validate({"id": "a", "command": ["y"]})))


class ExpandTest(unittest.TestCase):
    def test_only_runtime_placeholders(self):
        self.assertEqual(expand("{in}/x {out} {id} {other} {{in}}", in_dir="/i", out_dir="/o", task_id="t"), "/i/x /o t {other} {/i}")


class GenerateTest(unittest.TestCase):
    def test_inputs_and_chunks(self):
        tasks = list(generate(["run", "{input}", "--part", "{chunk}/{chunks}", "--out", "{out}/{stem}.out"], inputs=["s3://b/d/shard_00.jsonl.zst", "s3://b/d/shard_01.jsonl.zst"], chunks=2, extra={"units": 5}))
        self.assertEqual([t["id"] for t in tasks], ["shard_00-c0", "shard_00-c1", "shard_01-c0", "shard_01-c1"])
        self.assertEqual(tasks[1]["command"], ["run", "{in}/shard_00.jsonl.zst", "--part", "1/2", "--out", "{out}/shard_00.out"])
        self.assertEqual(tasks[1]["inputs"], {"shard_00.jsonl.zst": "s3://b/d/shard_00.jsonl.zst"})
        self.assertEqual(tasks[0]["units"], 5)

    def test_count(self):
        tasks = list(generate(["sim", "--seed", "{i}"], count=3))
        self.assertEqual([t["id"] for t in tasks], ["task-0000", "task-0001", "task-0002"])
        self.assertEqual(tasks[2]["command"], ["sim", "--seed", "2"])

    def test_errors(self):
        with self.assertRaisesRegex(ValueError, "unique stems"):
            list(generate(["x"], inputs=["s3://b/a/x.gz", "s3://b/b/x.gz"]))
        with self.assertRaisesRegex(ValueError, "same task id"):
            list(generate(["x"], inputs=["/tmp/a+b.txt", "/tmp/a b.txt"]))
        with self.assertRaisesRegex(ValueError, "need --inputs"):
            list(generate(["x", "{input}"], count=1))
        with self.assertRaisesRegex(ValueError, "NUL"):
            validate({"id": "a", "command": ["x"], "env": {"X": "a\x00b"}})

    def test_literal_question_marks_in_s3_and_local_names(self):
        t = list(generate(["cat", "{input}"], inputs=["s3://b/data?part.jsonl", "https://h/x.jsonl?sig=1"]))
        self.assertEqual([list(x["inputs"]) for x in t], [["data?part.jsonl"], ["x.jsonl"]])
        with self.assertRaisesRegex(ValueError, "need --inputs"):
            list(generate(["x", "{input}"], count=1))


class ReadTasksTest(unittest.TestCase):
    def test_duplicate_ids_and_bad_json_name_the_line(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "t.jsonl"
            p.write_text(json.dumps({"id": "a", "command": ["x"]}) + "\n\n" + json.dumps({"id": "a", "command": ["y"]}) + "\n")
            with self.assertRaisesRegex(ValueError, "t.jsonl:3: duplicate task id"):
                read_tasks(p)
            p.write_text("{nope\n")
            with self.assertRaisesRegex(ValueError, "t.jsonl:1: invalid JSON"):
                read_tasks(p)
