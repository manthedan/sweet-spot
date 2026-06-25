from __future__ import annotations

import unittest

from sweetspot.controller import choose_worker_top_up, group_canary_tasks_by_candidate


class ControllerTests(unittest.TestCase):
    def test_choose_worker_top_up_is_bounded_by_plan_target(self) -> None:
        self.assertEqual(choose_worker_top_up(backlog=100, active_workers=0, target_workers=8), 8)
        self.assertEqual(choose_worker_top_up(backlog=100, active_workers=5, target_workers=8), 3)
        self.assertEqual(choose_worker_top_up(backlog=100, active_workers=9, target_workers=8), 0)
        self.assertEqual(choose_worker_top_up(backlog=0, active_workers=0, target_workers=8), 0)

    def test_group_canary_tasks_by_candidate_requires_routing_metadata(self) -> None:
        groups = group_canary_tasks_by_candidate(
            [
                {"task_id": "a", "input": {"candidate_architecture": "x86_64", "candidate_vcpus": 4, "candidate_memory_mib": 8192, "canary_units_per_task": 100}},
                {"task_id": "b", "input": {"candidate_architecture": "x86_64", "candidate_vcpus": 4, "candidate_memory_mib": 8192, "canary_units_per_task": 100}},
                {"task_id": "c", "input": {"candidate_architecture": "arm64", "candidate_vcpus": 4, "candidate_memory_mib": 8192, "canary_units_per_task": 100}},
            ]
        )
        self.assertEqual(sorted(groups), ["arm64-4vcpu-8192mib-u100", "x86_64-4vcpu-8192mib-u100"])
        self.assertEqual(len(groups["x86_64-4vcpu-8192mib-u100"]), 2)
        with self.assertRaises(ValueError):
            group_canary_tasks_by_candidate([{"task_id": "bad", "input": {"candidate_architecture": "x86_64"}}])


if __name__ == "__main__":
    unittest.main()
