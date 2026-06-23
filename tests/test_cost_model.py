from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sweetspot import lane_manager
from sweetspot.scout import expected_cost_per_1m_units, noncompute_cost_per_1m_units, observed_perf


class CostModelTests(unittest.TestCase):
    def test_observed_perf_reads_worker_telemetry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "a.summary.json"
            p.write_text(
                json.dumps(
                    {
                        "schema": "sweetspot.task_summary.v2",
                        "telemetry": {
                            "instance_type": "c7i.large",
                            "completed_units": 1000,
                            "useful_compute_seconds": 10,
                            "discarded_compute_seconds": 2,
                            "bytes_transferred": 4096,
                            "retry": True,
                        },
                    }
                )
            )
            obs = observed_perf(mock.Mock(), [tmp], max_files=10)
        self.assertEqual(obs["count"], 1)
        self.assertEqual(obs["global_median_units_per_s"], 100.0)
        self.assertEqual(obs["by_instance_type"]["c7i.large"]["median_units_per_s"], 100.0)
        self.assertEqual(obs["retry_fraction"], 1.0)
        self.assertAlmostEqual(obs["observed_replay_fraction"], 0.2)
        self.assertEqual(obs["bytes_transferred"], 4096.0)

    def test_observed_perf_does_not_count_lost_attempts_as_useful(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "winner.summary.json").write_text(json.dumps({"returncode": 0, "telemetry": {"completed_units": 1000, "useful_compute_seconds": 10}}))
            Path(tmp, "loser.summary.json").write_text(json.dumps({"returncode": 0, "commit_status": "lost", "telemetry": {"completed_units": 1000, "useful_compute_seconds": 10, "discarded_compute_seconds": 10}}))
            obs = observed_perf(mock.Mock(), [tmp], max_files=10)
        self.assertEqual(obs["count"], 1)
        self.assertEqual(obs["useful_compute_seconds"], 10.0)
        self.assertEqual(obs["discarded_compute_seconds"], 10.0)
        self.assertEqual(obs["observed_replay_fraction"], 1.0)

    def test_expected_cost_includes_replay_startup_and_noncompute(self) -> None:
        cost = expected_cost_per_1m_units(
            hourly_price=1.0,
            units_per_hour=1_000_000,
            replay_fraction=0.25,
            startup_overhead_seconds=60,
            useful_task_seconds=600,
            noncompute_per_1m=0.40,
        )
        self.assertAlmostEqual(cost, 1.75)
        args = argparse.Namespace(
            extra_cost_per_1m_units=0.1,
            cross_region_gb_per_1m_units=2,
            cross_region_cost_per_gb=0.02,
            nat_gb_per_1m_units=3,
            nat_cost_per_gb=0.045,
            cloudwatch_log_gb_per_1m_units=1,
            cloudwatch_log_cost_per_gb=0.5,
            s3_storage_gb_month_per_1m_units=10,
            s3_storage_cost_per_gb_month=0.023,
        )
        self.assertAlmostEqual(noncompute_cost_per_1m_units(args, bucket_local=False), 1.005)
        self.assertAlmostEqual(noncompute_cost_per_1m_units(args, bucket_local=True), 0.965)

    def test_lane_manager_counts_active_workers_before_cost_allocation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = {
                "sqs_queue_url": "https://sqs.us-west-2.amazonaws.com/123/q",
                "instance_types": ["c7i.large"],
                "lanes": [
                    {
                        "name": "expensive-active",
                        "region": "us-west-2",
                        "batch_job_queue": "q1",
                        "job_definition": "jd1",
                        "job_name_prefix": "exp",
                        "max_workers": 10,
                        "messages_per_worker": 1,
                        "expected_total_cost_per_1m_units": 9.0,
                    },
                    {
                        "name": "cheap-empty",
                        "region": "us-east-1",
                        "batch_job_queue": "q2",
                        "job_definition": "jd2",
                        "job_name_prefix": "cheap",
                        "max_workers": 10,
                        "messages_per_worker": 1,
                        "expected_total_cost_per_1m_units": 1.0,
                    },
                ],
            }
            cfg_path = Path(tmp) / "lanes.json"
            cfg_path.write_text(json.dumps(cfg))
            out = io.StringIO()
            with (
                mock.patch.object(sys, "argv", ["sweetspot-lane-manager", "--config", str(cfg_path), "--target-workers", "10"]),
                mock.patch("sweetspot.lane_manager.boto3.Session", return_value=mock.Mock(client=mock.Mock(return_value=mock.Mock()))),
                mock.patch("sweetspot.lane_manager.queue_depth", return_value={"visible": 100, "not_visible": 0, "delayed": 0}),
                mock.patch("sweetspot.lane_manager.placement_score", return_value=7),
                mock.patch("sweetspot.lane_manager.active_jobs", side_effect=[10, 0]),
                contextlib.redirect_stdout(out),
            ):
                lane_manager.main()
        report = json.loads(out.getvalue())
        self.assertEqual(report["active_workers_before_submit"], 10)
        self.assertEqual([lane["to_submit"] for lane in report["lanes"]], [0, 0])

    def test_lane_manager_fails_closed_on_unknown_placement_score_when_min_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = {
                "sqs_queue_url": "https://sqs.us-west-2.amazonaws.com/123/q",
                "instance_types": ["c7i.large"],
                "lanes": [
                    {
                        "name": "unknown-score",
                        "region": "us-west-2",
                        "batch_job_queue": "q1",
                        "job_definition": "jd1",
                        "job_name_prefix": "lane",
                        "max_workers": 2,
                        "messages_per_worker": 1,
                        "min_placement_score": 7,
                    }
                ],
            }
            cfg_path = Path(tmp) / "lanes.json"
            cfg_path.write_text(json.dumps(cfg))
            out = io.StringIO()
            with (
                mock.patch.object(sys, "argv", ["sweetspot-lane-manager", "--config", str(cfg_path), "--target-workers", "2"]),
                mock.patch("sweetspot.lane_manager.boto3.Session", return_value=mock.Mock(client=mock.Mock(return_value=mock.Mock()))),
                mock.patch("sweetspot.lane_manager.queue_depth", return_value={"visible": 2, "not_visible": 0, "delayed": 0}),
                mock.patch("sweetspot.lane_manager.placement_score", return_value=None),
                mock.patch("sweetspot.lane_manager.active_jobs", return_value=0),
                contextlib.redirect_stdout(out),
            ):
                lane_manager.main()
        report = json.loads(out.getvalue())
        self.assertFalse(report["lanes"][0]["eligible"])
        self.assertEqual(report["lanes"][0]["to_submit"], 0)

    def test_lane_manager_allocates_cheapest_eligible_lanes_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = {
                "sqs_queue_url": "https://sqs.us-west-2.amazonaws.com/123/q",
                "instance_types": ["c7i.large"],
                "lanes": [
                    {
                        "name": "expensive",
                        "region": "us-west-2",
                        "batch_job_queue": "q1",
                        "job_definition": "jd1",
                        "job_name_prefix": "exp",
                        "max_workers": 2,
                        "messages_per_worker": 1,
                        "expected_total_cost_per_1m_units": 9.0,
                    },
                    {
                        "name": "cheap",
                        "region": "us-east-1",
                        "batch_job_queue": "q2",
                        "job_definition": "jd2",
                        "job_name_prefix": "cheap",
                        "max_workers": 2,
                        "messages_per_worker": 1,
                        "expected_total_cost_per_1m_units": 1.0,
                    },
                ],
            }
            cfg_path = Path(tmp) / "lanes.json"
            cfg_path.write_text(json.dumps(cfg))
            out = io.StringIO()
            with (
                mock.patch.object(sys, "argv", ["sweetspot-lane-manager", "--config", str(cfg_path), "--target-workers", "3"]),
                mock.patch("sweetspot.lane_manager.boto3.Session", return_value=mock.Mock(client=mock.Mock(return_value=mock.Mock()))),
                mock.patch("sweetspot.lane_manager.queue_depth", return_value={"visible": 3, "not_visible": 0, "delayed": 0}),
                mock.patch("sweetspot.lane_manager.placement_score", return_value=7),
                mock.patch("sweetspot.lane_manager.active_jobs", return_value=0),
                contextlib.redirect_stdout(out),
            ):
                lane_manager.main()
        report = json.loads(out.getvalue())
        self.assertEqual([lane["name"] for lane in report["lanes"]], ["cheap", "expensive"])
        self.assertEqual(report["lanes"][0]["desired_for_lane"], 2)
        self.assertEqual(report["lanes"][1]["desired_for_lane"], 1)


if __name__ == "__main__":
    unittest.main()
