import json
from unittest import mock

from sweetspot import runs
from sweetspot.worker import run_worker
from tests.fakeaws import BUCKET, AwsTestCase


class RunsBase(AwsTestCase):
    def setUp(self):
        super().setUp()
        self.inp = self.put("data/a.txt", b"hello\n")
        self.tasks = [
            {"id": "a", "command": ["sh", "-c", "tr a-z A-Z < {in}/a.txt > {out}/A.txt"], "inputs": {"a.txt": self.inp}, "units": 6},
            {"id": "b", "command": ["sh", "-c", 'test -n "$FIXED" && echo b > {out}/b.txt']},
        ]
        self.tf = self.tasks_file(self.tasks)

    def qurl(self, run="r1", dlq=False):
        r = runs.Run(self.cfg, run)
        return runs.queue_url(self.c, r, r.dlq_name if dlq else r.queue_name)

    def drain(self, run="r1", **kw):
        q = self.qurl(run)
        return run_worker(sqs=self.sqs, queue_url=q, prefix=self.cfg.run_prefix(run), store=self.store, poll_seconds=1, idle_polls=1, **kw)


class RunsTest(RunsBase):
    def test_submit_creates_run_queue_job_definition_and_array_job(self):
        rep = runs.submit(self.c, self.cfg, "r1", self.tf, image="img:1", workers=3, vcpus=2, memory=3000)
        self.assertEqual((rep["enqueued"], rep["launch"]["workers"]), (2, 3))
        ident = self.get_json("ss/r1/run.json")
        self.assertEqual(ident["task_count"], 2)
        q = self.qurl()
        attrs = self.sqs.get_queue_attributes(QueueUrl=q, AttributeNames=["All"])["Attributes"]
        self.assertEqual(attrs["ApproximateNumberOfMessages"], "2")
        self.assertEqual(json.loads(attrs["RedrivePolicy"])["maxReceiveCount"], 5)
        jd = self.batch.describe_job_definitions(jobDefinitionName="ss-r1")["jobDefinitions"][0]
        env = {e["name"]: e["value"] for e in jd["containerProperties"]["environment"]}
        self.assertEqual((env["SWEETSPOT_PREFIX"], env["SWEETSPOT_IMAGE"], env["SWEETSPOT_VCPUS"]), (f"s3://{BUCKET}/ss/r1", "img:1", "2"))
        self.assertEqual(jd["containerProperties"]["command"], ["sweetspot", "worker"])
        self.assertEqual(len(self.keys("ss/r1/launches/")), 1)
        self.assertEqual(jd["tags"], {"sweetspot-project": "ss", "sweetspot-run": "r1"})

    def test_resubmit_is_idempotent_and_ledger_is_frozen(self):
        runs.submit(self.c, self.cfg, "r1", self.tf, image="img:1", workers=1)
        rep = runs.submit(self.c, self.cfg, "r1", self.tf, workers=0)
        self.assertEqual(rep["enqueued"], 0)
        self.assertIn("still holds", rep["notes"][0])
        other = self.tasks_file(self.tasks[:1], "other.jsonl")
        with self.assertRaisesRegex(runs.RunError, "differs"):
            runs.submit(self.c, self.cfg, "r1", other)
        with self.assertRaisesRegex(runs.RunError, "does not exist"):
            runs.submit(self.c, self.cfg, "nope", None)
        with self.assertRaisesRegex(runs.RunError, "--image"):
            runs.submit(self.c, self.cfg, "r2", self.tf)
        for bad in ("R1", "a_b", "a--b", "a-", "x" * 49):
            with self.assertRaisesRegex(runs.RunError, "run names"):
                runs.submit(self.c, self.cfg, bad, self.tf)

    def test_stalled_when_tasks_are_queued_but_nobody_works(self):
        runs.submit(self.c, self.cfg, "r1", self.tf, image="img:1", workers=0)
        st = runs.status(self.c, self.cfg, "r1")
        self.assertEqual((st["state"], st["queue"]["visible"]), ("stalled", 2))
        self.assertIn("--workers", st["next"])

    def test_failure_retry_finish_lifecycle(self):
        runs.submit(self.c, self.cfg, "r1", self.tf, image="img:1", workers=0)
        with mock.patch("sweetspot.worker._backoff", return_value=0):
            self.drain()  # a commits; b fails on every delivery until SQS moves it to the DLQ
        st = runs.status(self.c, self.cfg, "r1")
        self.assertEqual((st["done"], st["with_failed_attempts"], st["state"], st["dlq"]), (1, 1, "failed", 1))
        self.assertIn("--retry", st["next"])
        self.assertIn("exited with 1", st["recent_failures"][0]["error"])
        self.assertEqual(runs.finish(self.c, self.cfg, "r1")["missing"], ["b"])
        rep = runs.submit(self.c, self.cfg, "r1", None, workers=0)
        self.assertEqual(rep["enqueued"], 0)
        self.assertIn("--retry", rep["notes"][0])

        # fix the cause and retry: the DLQ's message goes back to the queue
        rep = runs.submit(self.c, self.cfg, "r1", None, workers=0, retry=True)
        self.assertEqual((rep["redriven"], rep["enqueued"]), (1, 0))
        with mock.patch.dict("os.environ", {"FIXED": "1"}):
            self.drain()
        self.assertEqual(runs.status(self.c, self.cfg, "r1")["state"], "complete")
        self.assertEqual(len(runs.attempts(self.c, self.cfg, "r1", "b")), 6)  # 5 deliveries before the DLQ + the fixed one

        # an interrupted promotion is repaired by finish
        self.s3.delete_object(Bucket=BUCKET, Key="ss/r1/out/a/A.txt")
        rep = runs.finish(self.c, self.cfg, "r1")
        self.assertTrue(rep["complete"])
        self.assertEqual((rep["repaired_promotions"], rep["tasks"], rep["units"]), (1, 2, 6.0))
        self.assertEqual(self.s3.get_object(Bucket=BUCKET, Key="ss/r1/out/a/A.txt")["Body"].read(), b"HELLO\n")
        manifest = [json.loads(line) for line in self.s3.get_object(Bucket=BUCKET, Key="ss/r1/manifest.jsonl")["Body"].read().splitlines()]
        self.assertEqual([m["id"] for m in manifest], ["a", "b"])
        self.assertEqual(manifest[0]["files"][0]["uri"], f"s3://{BUCKET}/ss/r1/out/a/A.txt")
        self.assertIsNone(self.qurl())
        self.assertEqual([k for k in self.keys("ss/r1/attempts/") if "/out/" in k], [])
        self.assertTrue([k for k in self.keys("ss/r1/attempts/") if k.endswith("summary.json")])

        # publishing is idempotent, and a finished run refuses new work
        ready = self.get_json("ss/r1/READY")
        self.assertEqual(runs.finish(self.c, self.cfg, "r1")["published_at"], ready["published_at"])
        self.assertEqual(runs.status(self.c, self.cfg, "r1")["state"], "finished")
        with self.assertRaisesRegex(runs.RunError, "finished"):
            runs.submit(self.c, self.cfg, "r1", None)

    def test_conflicting_ready_is_not_overwritten(self):
        runs.submit(self.c, self.cfg, "r1", self.tasks_file(self.tasks[:1], "one.jsonl"), image="img:1", workers=0)
        self.drain()
        self.put("ss/r1/READY", json.dumps({"ledger_sha256": "x", "manifest_sha256": "y"}).encode())
        with self.assertRaisesRegex(runs.RunError, "different manifest"):
            runs.finish(self.c, self.cfg, "r1")

    def test_cancel_releases_and_submit_resumes(self):
        runs.submit(self.c, self.cfg, "r1", self.tf, image="img:1", workers=2)
        rep = runs.cancel(self.c, self.cfg, "r1")
        self.assertEqual(sorted(rep["deleted_queues"]), ["ss-r1", "ss-r1_dlq"])
        self.assertIsNone(self.qurl())
        rep = runs.submit(self.c, self.cfg, "r1", None, workers=0)
        self.assertEqual(rep["enqueued"], 2)  # the deleted queue's messages are gone; pending tasks go back in

    def test_dry_run_mutates_nothing(self):
        rep = runs.submit(self.c, self.cfg, "r1", self.tf, image="img:1", dry_run=True)
        self.assertEqual((rep["dry_run"], rep["workers"]), (True, 2))
        self.assertEqual(self.keys("ss/"), [])
        self.assertIsNone(self.qurl())


class RunsRegressionTest(RunsBase):
    """Failures found in review; each test reproduces one."""

    def test_output_names_with_url_characters_stay_distinct(self):
        tf = self.tasks_file([{"id": "h", "command": ["sh", "-c", "echo 1 > '{out}/r'; echo 2 > '{out}/r#part'; echo 3 > '{out}/r?x'"]}], "h.jsonl")
        runs.submit(self.c, self.cfg, "h", tf, image="img:1", workers=0)
        self.drain("h")
        rep = runs.finish(self.c, self.cfg, "h")
        self.assertEqual(rep["files"], 3)
        got = {k: self.s3.get_object(Bucket=BUCKET, Key=k)["Body"].read() for k in self.keys("ss/h/out/")}
        self.assertEqual(got, {"ss/h/out/h/r": b"1\n", "ss/h/out/h/r#part": b"2\n", "ss/h/out/h/r?x": b"3\n"})

    def test_conflicting_ready_leaves_the_published_manifest_alone(self):
        runs.submit(self.c, self.cfg, "r1", self.tasks_file(self.tasks[:1], "one.jsonl"), image="img:1", workers=0)
        self.drain()
        runs.finish(self.c, self.cfg, "r1")
        published = self.s3.get_object(Bucket=BUCKET, Key="ss/r1/manifest.jsonl")["Body"].read()
        self.put("ss/r1/READY", json.dumps({"ledger_sha256": "x", "manifest_sha256": "y"}).encode())
        with self.assertRaisesRegex(runs.RunError, "different manifest"):
            runs.finish(self.c, self.cfg, "r1")
        self.assertEqual(self.s3.get_object(Bucket=BUCKET, Key="ss/r1/manifest.jsonl")["Body"].read(), published)

    def test_a_different_ledger_cannot_replace_a_claimed_one(self):
        self.put("ss/r1/tasks.jsonl", b'{"command":["x"],"id":"other"}\n')  # someone else's concurrent create
        with self.assertRaisesRegex(runs.RunError, "different task list"):
            runs.submit(self.c, self.cfg, "r1", self.tf, image="img:1", workers=0)
        self.assertEqual(self.s3.get_object(Bucket=BUCKET, Key="ss/r1/tasks.jsonl")["Body"].read(), b'{"command":["x"],"id":"other"}\n')

    def test_queues_and_jobs_of_other_runs_are_left_alone(self):
        self.sqs.create_queue(QueueName="ss-r1")  # same name, not ours (no tags)
        with self.assertRaisesRegex(runs.RunError, "not tagged"):
            runs.submit(self.c, self.cfg, "r1", self.tf, image="img:1", workers=0)
        self.sqs.delete_queue(QueueUrl=self.sqs.get_queue_url(QueueName="ss-r1")["QueueUrl"])
        runs.submit(self.c, self.cfg, "foo", self.tf, image="img:1", workers=2)
        runs.submit(self.c, self.cfg, "foo-bar", self.tf, image="img:1", workers=3)
        jobs = runs.worker_jobs(self.c, runs.Run(self.cfg, "foo"))
        self.assertEqual([j["name"].split("--")[0] for j in jobs], ["ss-foo"])
        rep = runs.cancel(self.c, self.cfg, "foo")
        self.assertEqual(sorted(rep["deleted_queues"]), ["ss-foo", "ss-foo_dlq"])
        self.assertIsNotNone(self.qurl("foo-bar"))

    def test_retry_works_while_other_tasks_are_still_queued(self):
        tasks = [{"id": "bad", "command": ["sh", "-c", 'test -n "$FIXED" && echo 1 > {out}/x']}, {"id": "slow", "command": ["sh", "-c", "echo 1 > {out}/x"]}]
        runs.submit(self.c, self.cfg, "r1", self.tasks_file(tasks, "t2.jsonl"), image="img:1", workers=0)
        with mock.patch("sweetspot.worker._backoff", return_value=0):
            self.drain(max_tasks=6)  # bad fails into the DLQ; slow may or may not have run
        q = self.qurl()
        self.sqs.send_message(QueueUrl=q, MessageBody=json.dumps({"id": "slow", "command": ["sh", "-c", "echo 1 > {out}/x"]}))
        rep = runs.submit(self.c, self.cfg, "r1", None, workers=0, retry=True)
        self.assertEqual(rep["redriven"], 1)

    def test_interrupted_attempts_do_not_block_resume_after_cancel(self):
        runs.submit(self.c, self.cfg, "r1", self.tf, image="img:1", workers=0)
        from sweetspot import protocol

        protocol.record_failure(self.store, self.cfg.run_prefix("r1"), "b", "A1", "interrupted", "worker stopped", {})
        runs.cancel(self.c, self.cfg, "r1")
        rep = runs.submit(self.c, self.cfg, "r1", None, workers=0)
        self.assertEqual(rep["enqueued"], 2)
        self.assertNotEqual(runs.status(self.c, self.cfg, "r1")["state"], "failed")

    def test_an_interrupted_enqueue_is_finished_once_and_then_retry_rules_apply(self):
        runs.submit(self.c, self.cfg, "r1", self.tf, image="img:1", workers=0)
        self.put("ss/r1/enqueues/00000000000000000000-crashed.json", json.dumps({"complete": False}).encode())  # oldest, incomplete
        rep = runs.submit(self.c, self.cfg, "r1", None, workers=0)
        self.assertEqual(rep["enqueued"], 0)  # the latest wave is complete; the old one is history

    def test_cleanup_keeps_diagnostics_for_a_task_named_out(self):
        tf = self.tasks_file([{"id": "out", "command": ["sh", "-c", "echo hi; echo 1 > {out}/x"]}], "o.jsonl")
        runs.submit(self.c, self.cfg, "o", tf, image="img:1", workers=0)
        self.drain("o")
        runs.finish(self.c, self.cfg, "o")
        left = self.keys("ss/o/attempts/")
        self.assertTrue(any(k.endswith("summary.json") for k in left) and any(k.endswith("stdout.txt") for k in left), left)
        self.assertFalse(any("/out/x" in k for k in left), left)

    def test_large_messages_are_packed_under_the_batch_limit(self):
        big = "x" * 100_000
        sent = []
        real = self.sqs.send_message_batch

        def spy(**kw):
            sent.append(sum(len(e["MessageBody"]) for e in kw["Entries"]))
            return real(**kw)

        tasks = [{"id": f"t{i}", "command": ["echo", big]} for i in range(7)]
        with mock.patch.object(self.sqs, "send_message_batch", side_effect=spy):
            runs.submit(self.c, self.cfg, "big", self.tasks_file(tasks, "big.jsonl"), image="img:1", workers=0)
        self.assertTrue(all(n <= runs.MAX_BATCH_BYTES for n in sent), sent)
        self.assertEqual(len(sent), 4)


class FinalReviewRegressionTest(RunsBase):
    def test_finish_replaces_same_length_damage_in_out(self):
        runs.submit(self.c, self.cfg, "r1", self.tasks_file(self.tasks[:1], "one.jsonl"), image="img:1", workers=0)
        self.drain()
        self.put("ss/r1/out/a/A.txt", b"EVIL!\n")  # same length as HELLO\n
        runs.finish(self.c, self.cfg, "r1")
        self.assertEqual(self.s3.get_object(Bucket=BUCKET, Key="ss/r1/out/a/A.txt")["Body"].read(), b"HELLO\n")

    def test_repeated_finish_restores_a_damaged_manifest(self):
        runs.submit(self.c, self.cfg, "r1", self.tasks_file(self.tasks[:1], "one.jsonl"), image="img:1", workers=0)
        self.drain()
        runs.finish(self.c, self.cfg, "r1")
        good = self.s3.get_object(Bucket=BUCKET, Key="ss/r1/manifest.jsonl")["Body"].read()
        self.s3.delete_object(Bucket=BUCKET, Key="ss/r1/manifest.jsonl")
        rep = runs.finish(self.c, self.cfg, "r1")
        self.assertIn("restored", rep["warnings"][-1])
        self.assertEqual(self.s3.get_object(Bucket=BUCKET, Key="ss/r1/manifest.jsonl")["Body"].read(), good)

    def test_cancel_wins_over_a_submit_already_in_flight(self):
        real = runs.ensure_queues

        def cancel_meanwhile(c, r, max_attempts):
            out = real(c, r, max_attempts)
            runs.cancel(self.c, self.cfg, "r1")
            return out

        runs.submit(self.c, self.cfg, "r1", self.tf, image="img:1", workers=0)
        with mock.patch("sweetspot.runs.ensure_queues", side_effect=cancel_meanwhile):
            with self.assertRaisesRegex(runs.RunError, "cancelled while this submit"):
                runs.submit(self.c, self.cfg, "r1", None, image="img:1", workers=2)
        self.assertEqual(len(self.keys("ss/r1/launches/")), 0)

    def test_gpu_shapes_need_a_gpu_lane(self):
        with self.assertRaisesRegex(runs.RunError, "not a GPU lane"):
            runs.submit(self.c, self.cfg, "r1", self.tf, image="img:1", gpus=1)

    def test_retry_reports_dlq_messages_it_could_not_delete(self):
        runs.submit(self.c, self.cfg, "r1", self.tasks_file(self.tasks[1:], "b.jsonl"), image="img:1", workers=0)
        with mock.patch("sweetspot.worker._backoff", return_value=0):
            self.drain()
        real = self.sqs.delete_message_batch

        def half_fail(**kw):
            if kw["Entries"][1:]:
                real(**{**kw, "Entries": kw["Entries"][1:]})
            return {"Failed": [{"Id": kw["Entries"][0]["Id"], "Code": "x", "SenderFault": False}]}

        with mock.patch.object(self.sqs, "delete_message_batch", side_effect=half_fail), mock.patch("sweetspot.runs.time.sleep"):
            with self.assertRaisesRegex(runs.RunError, "could not delete"):
                runs.submit(self.c, self.cfg, "r1", None, workers=0, retry=True)

    def test_published_outputs_are_never_overwritten(self):
        runs.submit(self.c, self.cfg, "r1", self.tasks_file(self.tasks[:1], "one.jsonl"), image="img:1", workers=0)
        self.drain()
        runs.finish(self.c, self.cfg, "r1", keep_attempts=True)
        staged = [k for k in self.keys("ss/r1/attempts/a/") if k.endswith("/out/A.txt")][0]
        self.put(staged, b"EVIL!\n")  # damage the retained staged copy
        runs.finish(self.c, self.cfg, "r1")
        self.assertEqual(self.s3.get_object(Bucket=BUCKET, Key="ss/r1/out/a/A.txt")["Body"].read(), b"HELLO\n")

    def test_verify_hashes_catches_same_length_damage_after_cleanup(self):
        runs.submit(self.c, self.cfg, "r1", self.tasks_file(self.tasks[:1], "one.jsonl"), image="img:1", workers=0)
        self.drain()
        runs.finish(self.c, self.cfg, "r1")
        self.put("ss/r1/out/a/A.txt", b"EVIL!\n")
        runs.finish(self.c, self.cfg, "r1")  # sizes still match
        with self.assertRaisesRegex(runs.RunError, "sha256"):
            runs.finish(self.c, self.cfg, "r1", verify_hashes=True)

    def test_a_cancel_landing_early_in_a_submit_still_stops_it(self):
        runs.submit(self.c, self.cfg, "r1", self.tf, image="img:1", workers=0)
        real = runs.read_tasks

        def read_then_cancel(path):
            out = real(path)
            runs.cancel(self.c, self.cfg, "r1")
            return out

        with mock.patch("sweetspot.runs.read_tasks", side_effect=read_then_cancel):
            with self.assertRaisesRegex(runs.RunError, "cancelled while this submit"):
                runs.submit(self.c, self.cfg, "r1", self.tf, image="img:1", workers=2)
        self.assertEqual(len(self.keys("ss/r1/launches/")), 0)
