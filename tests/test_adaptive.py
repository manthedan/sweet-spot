from __future__ import annotations

import unittest

from sweetspot.adaptive import canary_observation_from_summary, choose_next_shard_units, choose_resource_candidate, logical_shard_plan


class AdaptiveShardTests(unittest.TestCase):
    def test_choose_next_shard_units_starts_with_minimum_without_canary(self) -> None:
        decision = choose_next_shard_units([], target_task_seconds=300, min_units=5)
        self.assertEqual(decision["schema"], "sweetspot.adaptive_shard_decision.v1")
        self.assertEqual(decision["status"], "ready")
        self.assertEqual(decision["selected_units_per_task"], 5)
        self.assertFalse(decision["calibrated"])
        self.assertEqual(decision["next_action"], "run_canary")
        self.assertIn("canary_required", {reason["code"] for reason in decision["reasons"]})

    def test_choose_next_shard_units_targets_duration_from_median_rate(self) -> None:
        decision = choose_next_shard_units(
            [
                {"success": True, "completed_units": 100, "useful_compute_seconds": 10},
                {"success": True, "completed_units": 120, "useful_compute_seconds": 10},
                {"success": True, "completed_units": 50, "useful_compute_seconds": 10},
            ],
            target_task_seconds=30,
            min_units=1,
            growth_factor=100,
        )
        self.assertEqual(decision["status"], "ready")
        self.assertEqual(decision["observations_used"], 3)
        self.assertEqual(decision["median_units_per_second"], 10.0)
        self.assertEqual(decision["selected_units_per_task"], 300)
        self.assertTrue(decision["calibrated"])
        self.assertEqual(decision["next_action"], "produce_production")
        self.assertIn("target_duration_selected", {reason["code"] for reason in decision["reasons"]})

    def test_choose_next_shard_units_caps_geometric_growth(self) -> None:
        decision = choose_next_shard_units(
            [{"success": True, "completed_units": 10, "useful_compute_seconds": 1}],
            target_task_seconds=300,
            min_units=1,
            growth_factor=4,
        )
        self.assertEqual(decision["selected_units_per_task"], 40)
        self.assertFalse(decision["calibrated"])
        self.assertEqual(decision["next_action"], "run_canary")
        self.assertIn("geometric_growth_cap", {reason["code"] for reason in decision["reasons"]})

    def test_choose_next_shard_units_normalizes_summary_without_telemetry(self) -> None:
        decision = choose_next_shard_units(
            [{"returncode": 0, "completed_units": 100, "elapsed_sec": 10}],
            target_task_seconds=30,
            min_units=1,
            growth_factor=100,
        )
        self.assertEqual(decision["observations_used"], 1)
        self.assertEqual(decision["selected_units_per_task"], 300)
        self.assertNotIn("canary_required", {reason["code"] for reason in decision["reasons"]})

    def test_choose_next_shard_units_blocks_on_oom(self) -> None:
        decision = choose_next_shard_units(
            [{"success": False, "completed_units": 10, "useful_compute_seconds": 1, "oom": True}],
            target_task_seconds=300,
        )
        self.assertEqual(decision["status"], "blocked")
        self.assertIsNone(decision["selected_units_per_task"])
        self.assertIn("memory_shape_rejected_oom", {reason["code"] for reason in decision["reasons"]})

    def test_choose_next_shard_units_blocks_on_validation_failure(self) -> None:
        decision = choose_next_shard_units(
            [
                {
                    "returncode": 0,
                    "framework_error": "expected output file was not produced: /tmp/task/output",
                    "completed_units": 10,
                    "elapsed_sec": 1,
                }
            ],
            target_task_seconds=300,
        )
        self.assertEqual(decision["status"], "blocked")
        self.assertIsNone(decision["selected_units_per_task"])
        self.assertIn("canary_validation_failed", {reason["code"] for reason in decision["reasons"]})

    def test_choose_next_shard_units_blocks_on_commit_validation_failure(self) -> None:
        decision = choose_next_shard_units(
            [{"returncode": 0, "commit_status": "validation_failed", "completed_units": 10, "elapsed_sec": 1}],
            target_task_seconds=300,
        )
        self.assertEqual(decision["status"], "blocked")
        self.assertIn("canary_validation_failed", {reason["code"] for reason in decision["reasons"]})

    def test_choose_next_shard_units_skips_duplicate_loser_commit_status(self) -> None:
        decision = choose_next_shard_units(
            [{"returncode": 0, "commit_status": "lost", "completed_units": 10, "elapsed_sec": 1}],
            target_task_seconds=300,
            min_units=5,
        )
        self.assertEqual(decision["status"], "ready")
        self.assertEqual(decision["selected_units_per_task"], 5)
        self.assertIn("canary_required", {reason["code"] for reason in decision["reasons"]})

    def test_logical_shard_plan_emits_deterministic_ranges(self) -> None:
        plan = logical_shard_plan(25, 10)
        self.assertEqual(plan["schema"], "sweetspot.logical_shard_plan.v1")
        self.assertEqual(plan["task_count"], 3)
        self.assertEqual(
            plan["ranges"],
            [
                {"shard_index": 0, "unit_start": 0, "unit_count": 10},
                {"shard_index": 1, "unit_start": 10, "unit_count": 10},
                {"shard_index": 2, "unit_start": 20, "unit_count": 5},
            ],
        )

    def test_logical_shard_plan_can_omit_large_range_lists(self) -> None:
        plan = logical_shard_plan(25, 10, max_inline_ranges=2)
        self.assertEqual(plan["task_count"], 3)
        self.assertEqual(plan["ranges_omitted"], 3)
        self.assertNotIn("ranges", plan)

    def test_logical_shard_plan_allows_empty_manifest(self) -> None:
        plan = logical_shard_plan(0, 10)
        self.assertEqual(plan["logical_unit_count"], 0)
        self.assertEqual(plan["task_count"], 0)
        self.assertEqual(plan["ranges"], [])

    def test_canary_observation_from_worker_summary(self) -> None:
        observation = canary_observation_from_summary(
            {
                "task_id": "canary-1",
                "returncode": 0,
                "telemetry": {"completed_units": 50, "useful_compute_seconds": 5},
            }
        )
        self.assertEqual(observation["task_id"], "canary-1")
        self.assertTrue(observation["success"])
        self.assertEqual(observation["units_per_second"], 10.0)

    def test_canary_observation_marks_oom_text(self) -> None:
        observation = canary_observation_from_summary(
            {
                "task_id": "canary-oom",
                "returncode": 137,
                "framework_error": "container killed: out of memory",
                "telemetry": {"completed_units": 10, "useful_compute_seconds": 2},
            }
        )
        self.assertFalse(observation["success"])
        self.assertTrue(observation["oom"])

    def test_canary_observation_marks_validation_failure(self) -> None:
        observation = canary_observation_from_summary(
            {
                "task_id": "canary-output-missing",
                "returncode": 0,
                "framework_error": "expected output file was not produced: /tmp/task/output",
                "telemetry": {"completed_units": 10, "useful_compute_seconds": 2},
            }
        )
        self.assertFalse(observation["success"])
        self.assertTrue(observation["validation_failed"])

    def test_canary_observation_marks_commit_validation_failure(self) -> None:
        observation = canary_observation_from_summary(
            {
                "task_id": "canary-commit-failed",
                "returncode": 0,
                "commit_status": "validation_failed",
                "telemetry": {"completed_units": 10, "useful_compute_seconds": 2},
            }
        )
        self.assertFalse(observation["success"])
        self.assertTrue(observation["validation_failed"])

    def test_canary_observation_skips_duplicate_loser_without_validation_failure(self) -> None:
        observation = canary_observation_from_summary(
            {
                "task_id": "canary-lost",
                "returncode": 0,
                "commit_status": "lost",
                "telemetry": {"completed_units": 10, "useful_compute_seconds": 2},
            }
        )
        self.assertFalse(observation["success"])
        self.assertFalse(observation["validation_failed"])

    def test_canary_observation_includes_resource_telemetry(self) -> None:
        observation = canary_observation_from_summary(
            {
                "task_id": "canary-x86",
                "returncode": 0,
                "telemetry": {"completed_units": 50, "useful_compute_seconds": 5, "architecture": "amd64", "worker_vcpus": 2, "worker_memory_mib": 4096, "peak_memory_mib": 1024},
            }
        )
        self.assertEqual(observation["architecture"], "x86_64")
        self.assertEqual(observation["worker_vcpus"], 2.0)
        self.assertEqual(observation["worker_memory_mib"], 4096.0)
        self.assertEqual(observation["peak_memory_mib"], 1024.0)

    def test_choose_resource_candidate_selects_lowest_vcpu_seconds(self) -> None:
        selection = choose_resource_candidate(
            [
                {"success": True, "architecture": "x86_64", "worker_vcpus": 2, "worker_memory_mib": 4096, "completed_units": 100, "useful_compute_seconds": 10},
                {"success": True, "architecture": "arm64", "worker_vcpus": 1, "worker_memory_mib": 2048, "completed_units": 100, "useful_compute_seconds": 10},
            ],
            allowed_architectures=["x86_64", "arm64"],
        )
        self.assertEqual(selection["status"], "ready")
        self.assertEqual(selection["selected"]["architecture"], "arm64")
        self.assertIn("resource_shape_selected", {reason["code"] for reason in selection["reasons"]})

    def test_choose_resource_candidate_requires_x86_baseline_before_selecting_arm(self) -> None:
        selection = choose_resource_candidate(
            [{"success": True, "architecture": "arm64", "worker_vcpus": 1, "worker_memory_mib": 2048, "completed_units": 100, "useful_compute_seconds": 10}],
            allowed_architectures=["x86_64", "arm64"],
        )
        self.assertEqual(selection["status"], "needs_canary")
        self.assertIsNone(selection["selected"])
        self.assertIn("resource_canary_required", {reason["code"] for reason in selection["reasons"]})

    def test_choose_resource_candidate_rejects_arm_when_materially_worse_than_x86(self) -> None:
        selection = choose_resource_candidate(
            [
                {"success": True, "architecture": "x86_64", "worker_vcpus": 1, "worker_memory_mib": 2048, "completed_units": 100, "useful_compute_seconds": 10},
                {"success": True, "architecture": "arm64", "worker_vcpus": 2, "worker_memory_mib": 4096, "completed_units": 100, "useful_compute_seconds": 10},
            ],
            allowed_architectures=["x86_64", "arm64"],
        )
        self.assertEqual(selection["status"], "ready")
        self.assertEqual(selection["selected"]["architecture"], "x86_64")
        self.assertIn("arm_cost_rejected", {reason["code"] for reason in selection["reasons"]})

    def test_choose_resource_candidate_keeps_region_measurements_separate(self) -> None:
        selection = choose_resource_candidate(
            [
                {"success": True, "architecture": "x86_64", "region": "us-west-2", "worker_vcpus": 1, "worker_memory_mib": 2048, "completed_units": 100, "useful_compute_seconds": 10},
                {"success": True, "architecture": "x86_64", "region": "us-east-1", "worker_vcpus": 1, "worker_memory_mib": 2048, "completed_units": 100, "useful_compute_seconds": 1},
            ],
            allowed_architectures=["x86_64"],
        )
        self.assertEqual(selection["status"], "ready")
        self.assertEqual(selection["selected"]["region"], "us-east-1")
        self.assertEqual(selection["selected"]["median_units_per_second"], 100.0)
        self.assertEqual(len(selection["candidates"]), 2)

    def test_choose_resource_candidate_rejects_shape_when_any_sample_fails(self) -> None:
        selection = choose_resource_candidate(
            [
                {"success": True, "architecture": "x86_64", "region": "us-west-2", "worker_vcpus": 1, "worker_memory_mib": 2048, "completed_units": 100, "useful_compute_seconds": 10},
                {"success": False, "architecture": "x86_64", "region": "us-west-2", "worker_vcpus": 1, "worker_memory_mib": 2048, "completed_units": 0, "useful_compute_seconds": 1},
            ],
            allowed_architectures=["x86_64"],
        )
        self.assertEqual(selection["status"], "blocked")
        self.assertEqual(selection["candidates"][0]["status"], "rejected")

    def test_choose_resource_candidate_blocks_failed_arm_without_x86_fallback(self) -> None:
        selection = choose_resource_candidate(
            [{"success": False, "architecture": "arm64", "worker_vcpus": 1, "worker_memory_mib": 2048, "validation_failed": True}],
            allowed_architectures=["arm64"],
        )
        self.assertEqual(selection["status"], "blocked")
        self.assertIsNone(selection["selected"])
        self.assertIn("arm_canary_failed", {reason["code"] for reason in selection["reasons"]})

    def test_choose_resource_candidate_needs_canary_without_resource_telemetry(self) -> None:
        selection = choose_resource_candidate([{"success": True, "completed_units": 100, "useful_compute_seconds": 10}], allowed_architectures=["x86_64"])
        self.assertEqual(selection["status"], "needs_canary")
        self.assertIsNone(selection["selected"])

    def test_canary_observation_does_not_match_oom_inside_words(self) -> None:
        observation = canary_observation_from_summary(
            {
                "task_id": "canary-ok",
                "returncode": 0,
                "stderr_tail": "bloom filter warmed room cache",
                "telemetry": {"completed_units": 10, "useful_compute_seconds": 2},
            }
        )
        self.assertTrue(observation["success"])
        self.assertFalse(observation["oom"])


if __name__ == "__main__":
    unittest.main()
