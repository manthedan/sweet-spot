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

from sweetspot import lane_manager, scout
from sweetspot.scout import expected_cost_per_1m_units, noncompute_cost_per_1m_units, observed_perf


class CostModelTests(unittest.TestCase):
    def test_scout_presets_include_smallest_medium_lanes(self) -> None:
        self.assertEqual(scout.PRESETS["smallest"], ["c7a.medium", "c7g.medium", "c6g.medium"])
        self.assertIn("c7a.medium", scout.PRESETS["mixed"])
        self.assertIn("c7g.medium", scout.PRESETS["mixed"])

    def test_scout_reads_attempt_scoped_summary_json_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "summaries" / "task.summary.json.attempts" / "attempt" / "summary.json"
            p.parent.mkdir(parents=True)
            p.write_text(
                json.dumps(
                    {
                        "schema": "sweetspot.task_summary.v2",
                        "telemetry": {
                            "instance_type": "c7g.medium",
                            "completed_units": 1,
                            "useful_compute_seconds": 2,
                        },
                    }
                )
            )
            obs = observed_perf(mock.Mock(), [tmp], max_files=10)
        self.assertEqual(obs["count"], 1)
        self.assertEqual(obs["by_instance_type"]["c7g.medium"]["median_units_per_s"], 0.5)

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

    def test_observed_perf_uses_receive_count_as_replay_lower_bound(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "retry.summary.json").write_text(
                json.dumps(
                    {
                        "returncode": 0,
                        "telemetry": {
                            "completed_units": 1000,
                            "useful_compute_seconds": 10,
                            "receive_count": "3",
                        },
                    }
                )
            )
            obs = observed_perf(mock.Mock(), [tmp], max_files=10)
        self.assertEqual(obs["receive_count_observed_attempts"], 1)
        self.assertEqual(obs["receive_count_replay_lower_bound"], 2)
        self.assertEqual(obs["receive_count_replay_lower_bound_fraction"], 2.0)
        self.assertEqual(obs["retry_fraction"], 1.0)
        self.assertEqual(obs["replay_observation_status"], "receive_count_lower_bound_only")

    def test_observed_perf_receive_count_lower_bound_uses_max_per_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            for receive_count in (1, 2, 3):
                Path(tmp, f"attempt-{receive_count}.summary.json").write_text(
                    json.dumps(
                        {
                            "task_id": "same-task",
                            "returncode": 0,
                            "telemetry": {
                                "completed_units": 1000,
                                "useful_compute_seconds": 10,
                                "receive_count": receive_count,
                            },
                        }
                    )
                )
            obs = observed_perf(mock.Mock(), [tmp], max_files=10)
        self.assertEqual(obs["receive_count_observed_attempts"], 3)
        self.assertEqual(obs["receive_count_replay_lower_bound"], 2)
        self.assertEqual(obs["retry_fraction"], 2 / 3)

    def test_observed_perf_marks_mixed_replay_telemetry_as_partial(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "lost.summary.json").write_text(
                json.dumps(
                    {
                        "task_id": "same-task",
                        "returncode": 0,
                        "commit_status": "lost",
                        "telemetry": {
                            "completed_units": 1000,
                            "useful_compute_seconds": 10,
                            "discarded_compute_seconds": 10,
                        },
                    }
                )
            )
            Path(tmp, "winner.summary.json").write_text(
                json.dumps(
                    {
                        "task_id": "same-task",
                        "returncode": 0,
                        "telemetry": {
                            "completed_units": 1000,
                            "useful_compute_seconds": 10,
                            "receive_count": 3,
                        },
                    }
                )
            )
            obs = observed_perf(mock.Mock(), [tmp], max_files=10)
        self.assertEqual(obs["discarded_compute_observed_attempts"], 1)
        self.assertEqual(obs["receive_count_replay_lower_bound"], 2)
        self.assertEqual(obs["replay_observation_status"], "discarded_compute_partial_receive_count_lower_bound")

    def test_observed_perf_partial_replay_check_is_per_task(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "task-a-lost.summary.json").write_text(
                json.dumps(
                    {
                        "task_id": "task-a",
                        "returncode": 0,
                        "commit_status": "lost",
                        "telemetry": {"completed_units": 1000, "useful_compute_seconds": 10, "discarded_compute_seconds": 10},
                    }
                )
            )
            Path(tmp, "task-b-winner.summary.json").write_text(
                json.dumps(
                    {
                        "task_id": "task-b",
                        "returncode": 0,
                        "telemetry": {"completed_units": 1000, "useful_compute_seconds": 10, "receive_count": 2},
                    }
                )
            )
            obs = observed_perf(mock.Mock(), [tmp], max_files=10)
        self.assertEqual(obs["discarded_compute_observed_attempts"], 1)
        self.assertEqual(obs["receive_count_replay_lower_bound"], 1)
        self.assertEqual(obs["receive_count_missing_discarded_attempts"], 1)
        self.assertEqual(obs["replay_observation_status"], "discarded_compute_partial_receive_count_lower_bound")

    def test_observed_perf_does_not_count_lost_attempts_as_useful(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "winner.summary.json").write_text(json.dumps({"returncode": 0, "telemetry": {"completed_units": 1000, "useful_compute_seconds": 10}}))
            Path(tmp, "loser.summary.json").write_text(json.dumps({"returncode": 0, "commit_status": "lost", "telemetry": {"completed_units": 1000, "useful_compute_seconds": 10, "discarded_compute_seconds": 10}}))
            obs = observed_perf(mock.Mock(), [tmp], max_files=10)
        self.assertEqual(obs["count"], 1)
        self.assertEqual(obs["useful_compute_seconds"], 10.0)
        self.assertEqual(obs["discarded_compute_seconds"], 10.0)
        self.assertEqual(obs["observed_replay_fraction"], 1.0)

    def test_scout_warns_when_replay_is_receive_count_lower_bound_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "retry.summary.json").write_text(
                json.dumps(
                    {
                        "returncode": 0,
                        "telemetry": {
                            "completed_units": 1000,
                            "useful_compute_seconds": 10,
                            "receive_count": 2,
                        },
                    }
                )
            )
            out = io.StringIO()
            with (
                mock.patch("sweetspot.scout.boto3.Session", return_value=mock.Mock(client=mock.Mock(return_value=mock.Mock()))),
                mock.patch("sweetspot.scout.placement_scores", return_value={512: {"us-west-2": 8}}),
                mock.patch("sweetspot.scout.latest_spot_prices", return_value={("c7i.large", "usw2-az1"): 0.02}),
                mock.patch("sweetspot.scout.instance_vcpus", return_value={"c7i.large": 2}),
                contextlib.redirect_stdout(out),
            ):
                self.assertEqual(scout.main(["--regions", "us-west-2", "--instance-types", "c7i.large", "--observed-summaries", tmp]), 0)
            report = json.loads(out.getvalue())
        reason_codes = {reason["code"] for reason in report["reasons"]}
        self.assertIn("replay_fraction_lower_bound_only", reason_codes)
        self.assertEqual(report["observed_perf"]["receive_count_replay_lower_bound"], 1)
        self.assertEqual(report["observed_perf"]["replay_observation_status"], "receive_count_lower_bound_only")

    def test_scout_warns_when_replay_is_partially_observed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "lost.summary.json").write_text(
                json.dumps(
                    {
                        "task_id": "same-task",
                        "returncode": 0,
                        "commit_status": "lost",
                        "telemetry": {"completed_units": 1000, "useful_compute_seconds": 10, "discarded_compute_seconds": 10},
                    }
                )
            )
            Path(tmp, "winner.summary.json").write_text(
                json.dumps(
                    {
                        "task_id": "same-task",
                        "returncode": 0,
                        "telemetry": {"completed_units": 1000, "useful_compute_seconds": 10, "receive_count": 3},
                    }
                )
            )
            out = io.StringIO()
            with (
                mock.patch("sweetspot.scout.boto3.Session", return_value=mock.Mock(client=mock.Mock(return_value=mock.Mock()))),
                mock.patch("sweetspot.scout.placement_scores", return_value={512: {"us-west-2": 8}}),
                mock.patch("sweetspot.scout.latest_spot_prices", return_value={("c7i.large", "usw2-az1"): 0.02}),
                mock.patch("sweetspot.scout.instance_vcpus", return_value={"c7i.large": 2}),
                contextlib.redirect_stdout(out),
            ):
                self.assertEqual(scout.main(["--regions", "us-west-2", "--instance-types", "c7i.large", "--observed-summaries", tmp]), 0)
            report = json.loads(out.getvalue())
        self.assertIn("replay_fraction_partially_observed", {reason["code"] for reason in report["reasons"]})
        self.assertEqual(report["observed_perf"]["replay_observation_status"], "discarded_compute_partial_receive_count_lower_bound")

    def test_scout_reports_placement_score_as_configuration_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            json_out = Path(tmp) / "scout.json"
            out = io.StringIO()
            with (
                mock.patch("sweetspot.scout.boto3.Session", return_value=mock.Mock(client=mock.Mock(return_value=mock.Mock()))),
                mock.patch("sweetspot.scout.placement_scores", return_value={512: {"us-west-2": 8}}),
                mock.patch("sweetspot.scout.latest_spot_prices", return_value={("c7i.large", "usw2-az1"): 0.02}),
                mock.patch("sweetspot.scout.instance_vcpus", return_value={"c7i.large": 2}),
                contextlib.redirect_stdout(out),
            ):
                self.assertEqual(scout.main(["--regions", "us-west-2", "--instance-types", "c7i.large", "--json-out", str(json_out), "--format", "table"]), 0)
            report = json.loads(json_out.read_text())
        pool = report["top_instance_pools"][0]
        self.assertEqual(pool["configuration_placement_score"], 8)
        self.assertEqual(pool["placement_score_scope"], "region_instance_type_configuration")
        self.assertNotIn("placement_score", pool)
        self.assertIn("cfg_score", out.getvalue())

    def test_scout_emits_json_stdout_by_default(self) -> None:
        out = io.StringIO()
        with (
            mock.patch("sweetspot.scout.boto3.Session", return_value=mock.Mock(client=mock.Mock(return_value=mock.Mock()))),
            mock.patch("sweetspot.scout.placement_scores", return_value={512: {"us-west-2": 8}}),
            mock.patch("sweetspot.scout.latest_spot_prices", return_value={("c7i.large", "usw2-az1"): 0.02}),
            mock.patch("sweetspot.scout.instance_vcpus", return_value={"c7i.large": 2}),
            contextlib.redirect_stdout(out),
        ):
            self.assertEqual(scout.main(["--regions", "us-west-2", "--instance-types", "c7i.large"]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual(report["schema"], "sweetspot.scout.v1")
        self.assertEqual(report["top_instance_pools"][0]["instance_type"], "c7i.large")
        self.assertIn("throughput_default_used", {reason["code"] for reason in report["reasons"]})
        self.assertIn("cost_components_not_provided", {reason["code"] for reason in report["reasons"]})
        self.assertIn("cloudwatch_log_ingest", report["cost_model"]["omitted_cost_components"])
        self.assertNotIn("TOP INSTANCE POOLS", out.getvalue())

    def test_scout_caps_packed_workers_by_memory_request(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            json_out = Path(tmp) / "scout.json"
            out = io.StringIO()
            with (
                mock.patch("sweetspot.scout.boto3.Session", return_value=mock.Mock(client=mock.Mock(return_value=mock.Mock()))),
                mock.patch("sweetspot.scout.placement_scores", return_value={512: {"us-west-2": 8}}),
                mock.patch("sweetspot.scout.latest_spot_prices", return_value={("c7i.2xlarge", "usw2-az1"): 0.08}),
                mock.patch("sweetspot.scout.instance_vcpus", return_value={"c7i.2xlarge": 8}),
                mock.patch("sweetspot.scout.instance_memory_mib", return_value={"c7i.2xlarge": 8192}),
                contextlib.redirect_stdout(out),
            ):
                self.assertEqual(
                    scout.main(
                        [
                            "--regions",
                            "us-west-2",
                            "--instance-types",
                            "c7i.2xlarge",
                            "--worker-vcpus",
                            "2",
                            "--worker-memory",
                            "4096",
                            "--json-out",
                            str(json_out),
                            "--format",
                            "table",
                        ]
                    ),
                    0,
                )
            report = json.loads(json_out.read_text())
        pool = report["top_instance_pools"][0]
        self.assertEqual(report["worker_memory_mib"], 4096)
        self.assertEqual(report["instance_memory_reserve_mib"], 512)
        self.assertEqual(pool["vcpu_fit_workers"], 4)
        self.assertEqual(pool["memory_fit_workers"], 1)
        self.assertEqual(pool["packed_workers"], 1)
        self.assertEqual(pool["packing_limiter"], "memory")
        self.assertEqual(pool["memory_mib"], 8192)
        self.assertEqual(pool["memory_available_mib"], 7680)
        self.assertIn("memAvail", out.getvalue())

    def test_scout_skips_instances_without_memory_metadata_when_memory_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            json_out = Path(tmp) / "scout.json"
            with (
                mock.patch("sweetspot.scout.boto3.Session", return_value=mock.Mock(client=mock.Mock(return_value=mock.Mock()))),
                mock.patch("sweetspot.scout.placement_scores", return_value={512: {"us-west-2": 8}}),
                mock.patch("sweetspot.scout.latest_spot_prices", return_value={("c7i.large", "usw2-az1"): 0.02}),
                mock.patch("sweetspot.scout.instance_vcpus", return_value={"c7i.large": 2}),
                mock.patch("sweetspot.scout.instance_memory_mib", return_value={}),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(
                    scout.main(
                        [
                            "--regions",
                            "us-west-2",
                            "--instance-types",
                            "c7i.large",
                            "--worker-memory",
                            "4096",
                            "--json-out",
                            str(json_out),
                        ]
                    ),
                    0,
                )
            report = json.loads(json_out.read_text())
        self.assertEqual(report["top_instance_pools"], [])

    def test_scout_skips_instances_too_small_for_worker_vcpus(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            json_out = Path(tmp) / "scout.json"
            with (
                mock.patch("sweetspot.scout.boto3.Session", return_value=mock.Mock(client=mock.Mock(return_value=mock.Mock()))),
                mock.patch("sweetspot.scout.placement_scores", return_value={512: {"us-west-2": 8}}),
                mock.patch(
                    "sweetspot.scout.latest_spot_prices",
                    return_value={
                        ("c7i.large", "usw2-az1"): 0.01,
                        ("c7i.2xlarge", "usw2-az1"): 0.08,
                    },
                ),
                mock.patch("sweetspot.scout.instance_vcpus", return_value={"c7i.large": 2, "c7i.2xlarge": 8}),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(
                    scout.main(
                        [
                            "--regions",
                            "us-west-2",
                            "--instance-types",
                            "c7i.large",
                            "c7i.2xlarge",
                            "--worker-vcpus",
                            "4",
                            "--json-out",
                            str(json_out),
                        ]
                    ),
                    0,
                )
            report = json.loads(json_out.read_text())
        self.assertEqual([pool["instance_type"] for pool in report["top_instance_pools"]], ["c7i.2xlarge"])

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

    def test_lane_manager_uses_per_lane_instance_types_for_mixed_arch_configs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = {
                "sqs_queue_url": "https://sqs.us-west-2.amazonaws.com/123/q",
                "instance_types": ["c7i.large"],
                "lanes": [
                    {
                        "name": "x86",
                        "region": "us-west-2",
                        "batch_job_queue": "qx86",
                        "job_definition": "jdx86",
                        "job_name_prefix": "x86",
                        "max_workers": 1,
                        "messages_per_worker": 1,
                    },
                    {
                        "name": "arm",
                        "region": "us-west-2",
                        "instance_types": ["c7g.large", "m7g.large"],
                        "batch_job_queue": "qarm",
                        "job_definition": "jdarm",
                        "job_name_prefix": "arm",
                        "max_workers": 1,
                        "messages_per_worker": 1,
                    },
                ],
            }
            cfg_path = Path(tmp) / "lanes.json"
            cfg_path.write_text(json.dumps(cfg))
            out = io.StringIO()
            seen_instance_types = []

            def fake_placement_score(_ec2_home: object, _lane: dict[str, object], instance_types: list[str], _target_vcpus: int) -> int:
                seen_instance_types.append(instance_types)
                return 7

            with (
                mock.patch.object(sys, "argv", ["sweetspot-lane-manager", "--config", str(cfg_path), "--target-workers", "2"]),
                mock.patch("sweetspot.lane_manager.boto3.Session", return_value=mock.Mock(client=mock.Mock(return_value=mock.Mock()))),
                mock.patch("sweetspot.lane_manager.queue_depth", return_value={"visible": 2, "not_visible": 0, "delayed": 0}),
                mock.patch("sweetspot.lane_manager.placement_score", side_effect=fake_placement_score),
                mock.patch("sweetspot.lane_manager.active_jobs", return_value=0),
                contextlib.redirect_stdout(out),
            ):
                lane_manager.main()
        report = json.loads(out.getvalue())
        self.assertEqual(seen_instance_types, [["c7i.large"], ["c7g.large", "m7g.large"]])
        self.assertEqual([lane["instance_types"] for lane in report["lanes"]], [["c7i.large"], ["c7g.large", "m7g.large"]])


if __name__ == "__main__":
    unittest.main()
