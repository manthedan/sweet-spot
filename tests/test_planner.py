from __future__ import annotations

import unittest
from pathlib import Path

from sweetspot.planner import PlannerSpecError, load_job_spec, load_plan, plan_with_adaptive_canaries, validate_job_spec, validate_plan


ROOT = Path(__file__).resolve().parents[1]


class PlannerContractTests(unittest.TestCase):
    def test_job_examples_validate(self) -> None:
        for name in ["job.x86.example.json", "job.arm-eligible.example.json", "job.low-urgency.example.json"]:
            with self.subTest(name=name):
                spec = load_job_spec(ROOT / "examples" / name)
                self.assertEqual(spec["schema"], "sweetspot.job.v1")
                self.assertIn("max_cost_usd", spec["constraints"])

    def test_plan_example_validates(self) -> None:
        plan = load_plan(ROOT / "examples" / "plan.example.json")
        self.assertEqual(plan["schema"], "sweetspot.plan.v1")
        self.assertEqual(plan["reasons"][0]["code"], "using_conservative_defaults")

    def test_job_spec_rejects_primary_sizing_controls(self) -> None:
        spec = self._valid_job_spec()
        spec["messages_per_worker"] = 4
        with self.assertRaisesRegex(PlannerSpecError, "must not set sizing controls"):
            validate_job_spec(spec)

    def test_job_spec_requires_deadline_or_low_urgency(self) -> None:
        spec = self._valid_job_spec()
        del spec["constraints"]["deadline_hours"]
        with self.assertRaisesRegex(PlannerSpecError, "deadline_hours or low_urgency"):
            validate_job_spec(spec)

    def test_job_spec_rejects_numeric_strings(self) -> None:
        spec = self._valid_job_spec()
        spec["constraints"]["max_cost_usd"] = "10"
        with self.assertRaisesRegex(PlannerSpecError, "finite JSON number"):
            validate_job_spec(spec)

    def test_job_spec_rejects_unknown_architecture(self) -> None:
        spec = self._valid_job_spec()
        spec["constraints"]["architectures"] = ["x86_64", "sparc"]
        with self.assertRaisesRegex(PlannerSpecError, "unsupported architecture"):
            validate_job_spec(spec)

    def test_job_spec_rejects_non_string_architecture(self) -> None:
        spec = self._valid_job_spec()
        spec["constraints"]["architectures"] = [123]
        with self.assertRaisesRegex(PlannerSpecError, "unsupported architecture"):
            validate_job_spec(spec)

    def test_plan_rejects_unknown_reason_code(self) -> None:
        plan = {
            "schema": "sweetspot.plan.v1",
            "run_id": "run-1",
            "status": "blocked",
            "reasons": [{"code": "mystery", "severity": "error"}],
        }
        with self.assertRaisesRegex(PlannerSpecError, "unknown Plan reason code"):
            validate_plan(plan)

    def test_plan_with_adaptive_canaries_embeds_shard_decision(self) -> None:
        plan = plan_with_adaptive_canaries(
            self._valid_job_spec(),
            [{"returncode": 0, "completed_units": 1000, "elapsed_sec": 100}],
        )
        self.assertEqual(plan["status"], "blocked")
        self.assertEqual(plan["reasons"][0]["code"], "insufficient_telemetry")
        decision = plan["canaries"][0]["decision"]
        self.assertEqual(decision["schema"], "sweetspot.adaptive_shard_decision.v1")
        self.assertEqual(decision["selected_units_per_task"], 3000)
        self.assertEqual(decision["target_task_seconds"], 300.0)

    def test_plan_with_adaptive_canaries_surfaces_oom_as_blocker(self) -> None:
        plan = plan_with_adaptive_canaries(
            self._valid_job_spec(),
            [{"returncode": 137, "framework_error": "out of memory", "completed_units": 10, "elapsed_sec": 1}],
        )
        self.assertEqual(plan["status"], "blocked")
        self.assertEqual(plan["reasons"][0]["code"], "memory_shape_rejected_oom")
        self.assertEqual(plan["canaries"][0]["decision"]["status"], "blocked")

    def test_plan_with_adaptive_canaries_counts_production_shards(self) -> None:
        plan = plan_with_adaptive_canaries(
            self._valid_job_spec(),
            [{"returncode": 0, "completed_units": 1000, "elapsed_sec": 100}],
            logical_unit_count=6500,
        )
        shard_plan = plan["canaries"][0]["production_shards"]
        self.assertEqual(shard_plan["schema"], "sweetspot.logical_shard_plan.v1")
        self.assertEqual(shard_plan["units_per_task"], 3000)
        self.assertEqual(shard_plan["logical_unit_count"], 6500)
        self.assertEqual(shard_plan["task_count"], 3)
        self.assertEqual(shard_plan["ranges_omitted"], 3)

    def test_ready_plan_requires_selected_execution_settings(self) -> None:
        plan = {
            "schema": "sweetspot.plan.v1",
            "run_id": "run-1",
            "status": "ready",
            "reasons": [{"code": "using_conservative_defaults"}],
        }
        with self.assertRaisesRegex(PlannerSpecError, "ready Plan requires selected"):
            validate_plan(plan)

    def test_ready_plan_requires_selected_region(self) -> None:
        plan = load_plan(ROOT / "examples" / "plan.example.json")
        del plan["selected"]["region"]
        with self.assertRaisesRegex(PlannerSpecError, "selected.region"):
            validate_plan(plan)

    def test_blocked_plan_can_omit_execution_settings(self) -> None:
        plan = {
            "schema": "sweetspot.plan.v1",
            "run_id": "run-1",
            "status": "blocked",
            "reasons": [{"code": "deadline_unachievable", "severity": "error"}],
        }
        self.assertIs(validate_plan(plan), plan)

    def _valid_job_spec(self) -> dict:
        return {
            "schema": "sweetspot.job.v1",
            "run_id": "run-1",
            "image": "123456789012.dkr.ecr.us-west-2.amazonaws.com/worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "command": ["python", "/app/process.py"],
            "input_manifest": "s3://bucket/inputs/items.jsonl",
            "output_prefix": "s3://bucket/runs/run-1",
            "constraints": {
                "max_cost_usd": 10,
                "deadline_hours": 2,
                "completion_fraction": 1.0,
                "architectures": ["x86_64"],
            },
            "validation": {"output_check": "done_marker"},
        }


if __name__ == "__main__":
    unittest.main()
