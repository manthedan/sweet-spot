from __future__ import annotations

import base64
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from sweetspot.deployment import canary_deployment_target_for, load_deployment, select_canary_deployment_region, select_deployment_target, validate_local_manifest_matches_head, validate_target_matches_job
from sweetspot.planner import load_plan


ROOT = Path(__file__).resolve().parents[1]


class DeploymentRegistryTests(unittest.TestCase):
    def test_example_deployment_selects_plan_target(self) -> None:
        plan = load_plan(ROOT / "examples" / "plan.example.json")
        registry = load_deployment(ROOT / "examples" / "deployment.example.json")
        target = select_deployment_target(plan, registry)
        self.assertEqual(target.region, "us-west-2")
        self.assertEqual(target.architecture, "x86_64")
        self.assertEqual(target.batch_job_queue, "sweetspot-x86-spot-queue")
        self.assertEqual(target.job_definition, "sweetspot-worker-x86:1")

    def test_deployment_rejects_queue_region_drift(self) -> None:
        registry = json.loads((ROOT / "examples" / "deployment.example.json").read_text())
        registry["regions"]["us-west-2"]["sqs_queue_url"] = "https://sqs.us-east-1.amazonaws.com/123456789012/sweetspot-work"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "deployment.json"
            path.write_text(json.dumps(registry))
            with self.assertRaisesRegex(ValueError, "sqs_queue_url in region"):
                load_deployment(path)

    def test_deployment_job_definition_must_pin_revision(self) -> None:
        registry = json.loads((ROOT / "examples" / "deployment.example.json").read_text())
        registry["regions"]["us-west-2"]["architectures"]["x86_64"]["job_definition"] = "sweetspot-worker-x86"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "deployment.json"
            path.write_text(json.dumps(registry))
            with self.assertRaisesRegex(ValueError, "job_definition must pin"):
                load_deployment(path)

    def test_deployment_image_digest_must_be_valid_sha256(self) -> None:
        registry = json.loads((ROOT / "examples" / "deployment.example.json").read_text())
        registry["regions"]["us-west-2"]["architectures"]["x86_64"]["image"] = "123456789012.dkr.ecr.us-west-2.amazonaws.com/sweetspot-worker@sha256:not-a-digest"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "deployment.json"
            path.write_text(json.dumps(registry))
            with self.assertRaisesRegex(ValueError, "image must be digest-pinned"):
                load_deployment(path)

    def test_deployment_image_digest_must_match_job_spec(self) -> None:
        plan = load_plan(ROOT / "examples" / "plan.example.json")
        registry = load_deployment(ROOT / "examples" / "deployment.example.json")
        target = select_deployment_target(plan, registry)
        job_spec = json.loads((ROOT / "examples" / "job.x86.example.json").read_text())
        validate_target_matches_job(job_spec=job_spec, target=target)
        job_spec["image"] = job_spec["image"].replace("a" * 64, "b" * 64)
        with self.assertRaisesRegex(ValueError, "image digest"):
            validate_target_matches_job(job_spec=job_spec, target=target)

    def test_canary_routes_are_isolated_per_candidate(self) -> None:
        registry = json.loads((ROOT / "examples" / "deployment.example.json").read_text())
        registry["regions"]["us-west-2"]["canary_routes"] = {
            "x86_64-1vcpu-2048mib": {
                "sqs_queue_url": "https://sqs.us-west-2.amazonaws.com/123456789012/sweetspot-canary-x86-1vcpu",
                "batch_job_queue": "sweetspot-x86-spot-queue",
                "job_definition": "sweetspot-worker-x86:1",
                "image": registry["regions"]["us-west-2"]["architectures"]["x86_64"]["image"],
            }
        }
        job_spec = json.loads((ROOT / "examples" / "job.x86.example.json").read_text())
        region = select_canary_deployment_region(job_spec, registry)
        task = {"task_id": "c", "input": {"candidate_architecture": "x86_64", "candidate_vcpus": 1, "candidate_memory_mib": 2048}}
        target = canary_deployment_target_for(registry, region=region, task=task)
        self.assertEqual(target.sqs_queue_url, "https://sqs.us-west-2.amazonaws.com/123456789012/sweetspot-canary-x86-1vcpu")
        registry["regions"]["us-west-2"]["canary_routes"]["x86_64-1vcpu-2048mib"]["sqs_queue_url"] = registry["regions"]["us-west-2"]["sqs_queue_url"]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "deployment.json"
            path.write_text(json.dumps(registry))
            with self.assertRaisesRegex(ValueError, "isolated from the production queue"):
                load_deployment(path)

    def test_manifest_binding_requires_local_remote_checksum_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "manifest.jsonl"
            body = b'{"unit": 1}\n'
            path.write_bytes(body)
            sha = hashlib.sha256(body).hexdigest()
            validate_local_manifest_matches_head(local_path=path, local_sha256=sha, head={"ContentLength": len(body), "Metadata": {"sha256": sha}})
            checksum = base64.b64encode(hashlib.sha256(body).digest()).decode("ascii")
            validate_local_manifest_matches_head(local_path=path, local_sha256=sha, head={"ContentLength": len(body), "ChecksumSHA256": checksum})
            with self.assertRaisesRegex(ValueError, "size does not match"):
                validate_local_manifest_matches_head(local_path=path, local_sha256=sha, head={"ContentLength": len(body) + 1, "Metadata": {"sha256": sha}})
            with self.assertRaisesRegex(ValueError, "must expose SHA256 metadata"):
                validate_local_manifest_matches_head(local_path=path, local_sha256=sha, head={"ContentLength": len(body), "ETag": "multipart-etag-2"})


if __name__ == "__main__":
    unittest.main()
