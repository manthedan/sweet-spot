"""A moto-backed AWS account with one bucket, one Batch lane and a SweetSpot config."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from typing import Any

import boto3
from moto import mock_aws

from sweetspot import config, runs
from sweetspot.store import Store

REGION = "us-east-1"
BUCKET = "sweetspot-test"


class AwsTestCase(unittest.TestCase):
    def setUp(self) -> None:
        for k, v in {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "AWS_DEFAULT_REGION": REGION}.items():
            patcher = mock.patch.dict(os.environ, {k: v})
            patcher.start()
            self.addCleanup(patcher.stop)
        self.mock = mock_aws(config={"batch": {"use_docker": False}})
        self.mock.start()
        self.addCleanup(self.mock.stop)
        self.s3 = boto3.client("s3", region_name=REGION)
        self.s3.create_bucket(Bucket=BUCKET)
        self.sqs = boto3.client("sqs", region_name=REGION)
        self.batch = boto3.client("batch", region_name=REGION)
        role = boto3.client("iam", region_name=REGION).create_role(RoleName="worker", AssumeRolePolicyDocument="{}")["Role"]["Arn"]
        ce = self.batch.create_compute_environment(computeEnvironmentName="ce-test", type="UNMANAGED", state="ENABLED", serviceRole=role)["computeEnvironmentArn"]
        self.batch.create_job_queue(jobQueueName="q-x86", state="ENABLED", priority=1, computeEnvironmentOrder=[{"order": 1, "computeEnvironment": ce}])
        self.cfg = config.parse({"region": REGION, "prefix": f"s3://{BUCKET}/ss", "name": "ss", "job_role_arn": role, "lanes": {"x86": {"job_queue": "q-x86", "instance_types": ["c7a.medium"]}}})
        self.store = Store(s3=self.s3)
        self.c = runs.Clients(self.store, self.sqs, self.batch)
        self.tmp = Path(tempfile.mkdtemp())

    def put(self, key: str, body: bytes) -> str:
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=body)
        return f"s3://{BUCKET}/{key}"

    def tasks_file(self, tasks: list[dict[str, Any]], name: str = "tasks.jsonl") -> Path:
        path = self.tmp / name
        path.write_text("".join(json.dumps(t) + "\n" for t in tasks))
        return path

    def keys(self, prefix: str) -> list[str]:
        resp = self.s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        return sorted(o["Key"] for o in resp.get("Contents", []))

    def get_json(self, key: str) -> Any:
        return json.loads(self.s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
