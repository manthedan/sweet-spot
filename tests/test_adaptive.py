from __future__ import annotations

import unittest

from sweetspot.adaptive import canary_observation_from_summary, choose_next_shard_units, logical_shard_plan


class AdaptiveShardTests(unittest.TestCase):
    def test_choose_next_shard_units_starts_with_minimum_without_canary(self) -> None:
        decision = choose_next_shard_units([], target_task_seconds=300, min_units=5)
        self.assertEqual(decision["schema"], "sweetspot.adaptive_shard_decision.v1")
        self.assertEqual(decision["status"], "ready")
        self.assertEqual(decision["selected_units_per_task"], 5)
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
        self.assertIn("target_duration_selected", {reason["code"] for reason in decision["reasons"]})

    def test_choose_next_shard_units_caps_geometric_growth(self) -> None:
        decision = choose_next_shard_units(
            [{"success": True, "completed_units": 10, "useful_compute_seconds": 1}],
            target_task_seconds=300,
            min_units=1,
            growth_factor=4,
        )
        self.assertEqual(decision["selected_units_per_task"], 40)
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
