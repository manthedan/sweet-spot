import json
import threading
import time
from unittest import mock

from sweetspot.worker import EXIT_GAVE_UP, EXIT_INTERRUPTED, MAX_BACKOFF, run_worker
from tests.fakeaws import BUCKET, AwsTestCase

PREFIX = f"s3://{BUCKET}/ss/w"


class WorkerTest(AwsTestCase):
    def setUp(self):
        super().setUp()
        self.q = self.sqs.create_queue(QueueName="ss-w")["QueueUrl"]

    def send(self, *tasks):
        for t in tasks:
            self.sqs.send_message(QueueUrl=self.q, MessageBody=json.dumps(t))

    def counts(self):
        a = self.sqs.get_queue_attributes(QueueUrl=self.q, AttributeNames=["All"])["Attributes"]
        return int(a["ApproximateNumberOfMessages"]), int(a["ApproximateNumberOfMessagesNotVisible"])

    def work(self, **kw):
        kw = {"poll_seconds": 1, "idle_polls": 1, "heartbeat": 1, **kw}
        return run_worker(sqs=self.sqs, queue_url=self.q, prefix=PREFIX, store=self.store, **kw)

    def test_commits_skips_duplicates_and_backs_off_failures(self):
        ok = {"id": "ok", "command": ["sh", "-c", "echo hi > {out}/x"]}
        self.send(ok, {"id": "bad", "command": ["sh", "-c", "exit 1"]}, ok)
        self.assertEqual(self.work(), 0)
        self.assertIn("ss/w/out/ok/x", self.keys("ss/w/out/"))
        self.assertEqual(len(self.keys("ss/w/failures/bad/")), 1)
        self.assertEqual(self.counts(), (0, 1))  # the failed task waits out its backoff, invisible

    def test_interrupt_releases_task_and_exits_75(self):
        self.send({"id": "slow", "command": ["sleep", "30"]})
        stop = threading.Event()
        threading.Timer(1.5, stop.set).start()
        t0 = time.monotonic()
        self.assertEqual(self.work(stop=stop), EXIT_INTERRUPTED)
        self.assertLess(time.monotonic() - t0, 15)
        self.assertEqual(self.counts(), (1, 0))  # immediately visible for another worker
        rec = self.get_json(self.keys("ss/w/failures/slow/")[0])
        self.assertEqual(rec["status"], "interrupted")
        summary = self.get_json([k for k in self.keys("ss/w/attempts/slow/") if k.endswith("summary.json")][0])
        self.assertEqual(summary["outcome"], "interrupted")

    def test_gives_up_after_consecutive_infrastructure_errors(self):
        self.send(*({"id": f"m{i}", "command": ["true"], "inputs": {"x": f"s3://{BUCKET}/in/x"}} for i in range(3)))
        with mock.patch.object(self.store, "download", side_effect=ConnectionResetError("network down")):
            self.assertEqual(self.work(max_infra_errors=2), EXIT_GAVE_UP)

    def test_task_problems_do_not_stop_the_worker(self):
        missing = f"s3://{BUCKET}/nope/missing.txt"
        self.send({"id": "noinput", "command": ["true"], "inputs": {"x": missing}}, {"id": "nooutput", "command": ["true"]}, {"id": "nocmd", "command": ["/no/such"]})
        self.send({"id": "fine", "command": ["sh", "-c", "echo 1 > {out}/x"]})
        self.assertEqual(self.work(max_infra_errors=2), 0)
        self.assertEqual(self.keys("ss/w/done/"), ["ss/w/done/fine.json"])

    def test_command_failures_do_not_stop_the_worker(self):
        self.send(*({"id": f"f{i}", "command": ["false"]} for i in range(4)))
        self.assertEqual(self.work(max_infra_errors=2), 0)
        self.assertEqual(self.counts(), (0, 4))

    def test_sweeper_waits_for_in_flight_work(self):
        self.send({"id": "late", "command": ["sh", "-c", "echo 1 > {out}/x"]})
        self.sqs.receive_message(QueueUrl=self.q, VisibilityTimeout=2)  # held by a worker that then dies
        self.assertEqual(self.work(), 0)  # an ordinary worker sees an empty queue and leaves
        self.assertEqual(self.keys("ss/w/done/"), [])
        self.assertEqual(self.work(sweeper=True, sweeper_grace=2), 0)
        self.assertEqual(self.keys("ss/w/done/"), ["ss/w/done/late.json"])

    def test_sweeper_leaves_at_once_when_the_ledger_is_done(self):
        self.put("ss/w/run.json", json.dumps({"task_count": 1}).encode())
        self.send({"id": "only", "command": ["sh", "-c", "echo 1 > {out}/x"]})
        t0 = time.monotonic()
        self.assertEqual(self.work(sweeper=True, sweeper_grace=600), 0)
        self.assertLess(time.monotonic() - t0, 30)

    def test_sweeper_does_not_trust_a_quiet_queue_before_the_grace_period(self):
        # SQS counts can read zero while a dead worker still holds a lease
        self.send({"id": "held", "command": ["sh", "-c", "echo 1 > {out}/x"]})
        self.sqs.receive_message(QueueUrl=self.q, VisibilityTimeout=3)
        with mock.patch("sweetspot.worker._outstanding", return_value=0):
            self.assertEqual(self.work(sweeper=True, sweeper_grace=6), 0)
        self.assertEqual(self.keys("ss/w/done/"), ["ss/w/done/held.json"])

    def test_bad_message_is_parked_for_the_dlq(self):
        self.sqs.send_message(QueueUrl=self.q, MessageBody="not json")
        self.send({"id": "fine", "command": ["sh", "-c", "echo 1 > {out}/x"]})
        self.assertEqual(self.work(), 0)
        self.assertEqual(self.keys("ss/w/done/"), ["ss/w/done/fine.json"])
        self.assertEqual(self.counts(), (0, 1))
        self.assertGreaterEqual(MAX_BACKOFF, 300)

    def test_exits_cleanly_when_the_queue_is_deleted(self):
        self.sqs.delete_queue(QueueUrl=self.q)
        self.assertEqual(self.work(), 0)
