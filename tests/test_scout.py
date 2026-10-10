import argparse
import contextlib
import io
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

from sweetspot.config import Config, Lane
from sweetspot.scout import add_arguments, observations, run
from sweetspot.store import Store, join


class ScoutTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store()
        self.cfg = Config(
            "us-west-2",
            Path(self.tmp.name).as_uri(),
            lanes={
                "x86": Lane("x86", "queue-x86", "x86_64", ("c7a.medium", "c6a.large")),
                "arm": Lane("arm", "queue-arm", "arm64", ("c7g.medium",)),
            },
            default_lane="x86",
        )
        self.prefix = self.cfg.run_prefix("labels")
        self.now = datetime.now(timezone.utc)
        self.session = Mock()
        self.ec2 = self.session.client.return_value
        self.calls = []

        def paginator(name):
            def pages(**kwargs):
                self.calls.append((name, kwargs))
                types = kwargs["InstanceTypes"]
                if name == "describe_instance_types":
                    return [{"InstanceTypes": [self.spec(t) for t in types]}]
                # Deliberately unordered across pages; latest timestamp must win, with no page cap.
                return [
                    {"SpotPriceHistory": [self.price(t, "a", 0.02, self.now) for t in types]},
                    {"SpotPriceHistory": [self.price(t, "a", 0.99, self.now - timedelta(hours=2)) for t in types]},
                    {"SpotPriceHistory": [self.price(t, "b", 0.03, self.now) for t in types]},
                ]

            p = Mock()
            p.paginate.side_effect = pages
            return p

        self.ec2.get_paginator.side_effect = paginator

    def spec(self, kind):
        spec = {
            "InstanceType": kind,
            "VCpuInfo": {"DefaultVCpus": 2 if kind == "c6a.large" else 1},
            "MemoryInfo": {"SizeInMiB": 4096 if kind == "c6a.large" else 2048},
            "ProcessorInfo": {"SupportedArchitectures": ["arm64" if kind == "c7g.medium" else "x86_64"]},
        }
        if kind.startswith("g5."):
            spec["VCpuInfo"]["DefaultVCpus"] = 4
            spec["MemoryInfo"]["SizeInMiB"] = 16384
            spec["GpuInfo"] = {"Gpus": [{"Count": 1}]}
        return spec

    def price(self, kind, az, hourly, stamp):
        return {"InstanceType": kind, "AvailabilityZone": "us-west-2" + az, "SpotPrice": str(hourly), "Timestamp": stamp}

    def record(self, task_id, *, units=100, elapsed=10, image="image:v1", kind="c7a.medium", arch="x86_64", vcpus=1, memory=1536, gpus=0, lane=None, attempt=None, outcome=None):
        obj = {"task_id": task_id, "units": units, "elapsed": elapsed, "worker": {"instance_type": kind, "arch": arch, "vcpus": vcpus, "memory_mib": memory, "image": image, "cpus": 64}}
        if gpus is not None:
            obj["worker"]["gpus"] = gpus
        if lane is not None:
            obj["worker"]["lane"] = lane
        leaf = f"attempts/{task_id}/{attempt}/summary.json" if attempt else f"done/{task_id}.json"
        if outcome:
            obj["outcome"] = outcome
        self.store.put(join(self.prefix, leaf), json.dumps(obj).encode())

    def execute(self, flags=(), cfg=True):
        p = argparse.ArgumentParser()
        add_arguments(p)
        output = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(stderr):
            rc = run(p.parse_args(list(flags)), self.cfg if cfg else None, self.store, self.session)
        self.last_stderr = stderr.getvalue()
        return rc, output.getvalue()

    def report(self, flags=(), cfg=True):
        rc, output = self.execute([*flags, "--json"], cfg=cfg)
        self.assertEqual(rc, 0, output)
        return json.loads(output)

    def test_no_run_has_prices_and_never_invents_throughput(self):
        report = self.report()
        self.assertTrue(report["estimate"])
        self.assertEqual(len(report["rows"]), 6)
        for row in report["rows"]:
            self.assertIsNone(row["units_per_second_per_worker"])
            self.assertIsNone(row["replay_fraction"])
            self.assertIsNone(row["usd_per_1m_units"])
        row = next(r for r in report["rows"] if r["instance_type"] == "c6a.large" and r["az"].endswith("a"))
        self.assertEqual(row["packed_workers"], 2)
        self.assertEqual(row["worker_hour_usd"], 0.01)
        self.assertEqual(row["spot_hour_usd"], 0.02)
        self.assertNotIn("PaginationConfig", self.calls[1][1])

    def test_memory_limits_packing_and_no_fit_has_no_worker_cost(self):
        report = self.report(["--memory", "2000"])
        row = next(r for r in report["rows"] if r["instance_type"] == "c6a.large")
        self.assertEqual(row["packed_workers"], 1)
        small = next(r for r in report["rows"] if r["instance_type"] == "c7a.medium")
        self.assertEqual(small["packed_workers"], 0)
        self.assertIsNone(small["worker_hour_usd"])
        self.assertIsNotNone(small["vcpu_hour_usd"])

    def test_exact_group_median_and_replay(self):
        for task_id, units in [("a", 100), ("b", 300), ("c", 2000)]:
            self.record(task_id, units=units)
        for attempt, outcome, elapsed in [("a1", "failed", 5), ("a2", "interrupted", 4), ("a3", "duplicate", 6), ("winner", "committed", 10)]:
            self.record("a", attempt=attempt, outcome=outcome, elapsed=elapsed)
        report = self.report(["--run", "labels"])
        row = report["rows"][0]
        self.assertEqual(row["units_per_second_per_worker"], 30)
        self.assertEqual(row["observations"], 3)
        self.assertEqual(row["committed_seconds"], 30)
        self.assertEqual(row["discarded_seconds"], 15)
        self.assertEqual(row["replay_fraction"], 0.5)
        self.assertAlmostEqual(row["usd_per_1m_units"], 0.02 * 1e6 * 1.5 / (30 * 3600))
        self.assertIsNone(next(r for r in report["rows"] if r["instance_type"] == "c6a.large")["usd_per_1m_units"])

    def test_images_shapes_arches_and_types_stay_separate(self):
        self.record("a")
        self.record("b", image="image:v2", units=1000)
        self.record("c", vcpus=2, units=10000)
        self.record("d", memory=1024, units=10000)
        self.record("e", arch="arm64", units=10000)
        self.record("f", kind="c6a.large", units=200)
        self.record("g", image="image:v2", attempt="f", outcome="failed", elapsed=30)
        # This discarded attempt has no sampled completion; it must not inflate the sample's denominator.
        rows = self.report(["--run", "labels", "--vcpus", "1", "--memory", "1536"])["rows"]
        by_image = {r["image"]: r for r in rows if r["instance_type"] == "c7a.medium" and r["az"].endswith("a")}
        self.assertEqual(set(by_image), {"image:v1", "image:v2"})
        self.assertEqual(by_image["image:v1"]["units_per_second_per_worker"], 10)
        self.assertEqual(by_image["image:v2"]["units_per_second_per_worker"], 100)
        self.assertEqual(by_image["image:v2"]["replay_fraction"], 0)
        self.assertEqual(rows[0]["image"], "image:v2")

    def test_arm_alias_and_lane_filter(self):
        self.record("a", kind="c7g.medium", arch="aarch64")
        report = self.report(["--run", "labels", "--lane", "arm"])
        self.assertEqual({r["arch"] for r in report["rows"]}, {"arm64"})
        self.assertEqual(report["rows"][0]["units_per_second_per_worker"], 10)
        self.assertEqual(self.calls[0][1]["InstanceTypes"], ["c7g.medium"])
        self.assertEqual(self.execute(["--lane", "arm", "--arch", "x86_64"])[0], 2)

    def test_explicit_types_and_region_without_config(self):
        report = self.report(["--region", "us-east-1", "--instance-types", "c7a.medium,c6a.large"], cfg=False)
        self.assertEqual(report["region"], "us-east-1")
        self.session.client.assert_called_once_with("ec2", region_name="us-east-1")
        self.assertEqual(len(report["rows"]), 4)

    def test_explicit_types_intersect_selected_lane(self):
        report = self.report(["--lane", "x86", "--instance-types", "c7a.medium", "c7g.medium"])
        self.assertEqual({r["instance_type"] for r in report["rows"]}, {"c7a.medium"})

    def test_arch_filters_specs_of_explicit_types(self):
        rows = self.report(["--arch", "arm64", "--instance-types", "c7a.medium,c7g.medium"])["rows"]
        self.assertEqual({r["instance_type"] for r in rows}, {"c7g.medium"})

    def test_sample_bounds_replay_and_never_counts_committed_summary_twice(self):
        self.record("a")
        self.record("b", units=100000)
        self.record("a", attempt="f", outcome="failed", elapsed=5)
        self.record("a", attempt="c", outcome="committed", elapsed=10)
        self.record("b", attempt="f", outcome="failed", elapsed=500)
        report = self.report(["--run", "labels", "--max-markers", "1"])
        self.assertEqual(report["sampling"]["markers_sampled"], 1)
        self.assertTrue(report["sampling"]["marker_limit_reached"])
        self.assertEqual(report["rows"][0]["replay_fraction"], 0.5)

    def test_malformed_and_incomplete_telemetry_does_not_make_up_a_shape(self):
        self.record("a", image=None)
        self.record("b", elapsed=0)
        self.record("c", units=float("nan"))
        self.record("d", vcpus=None)
        self.store.put(join(self.prefix, "done/e.json"), b"not-json")
        rows = self.report(["--run", "labels"])["rows"]
        self.assertTrue(all(r["usd_per_1m_units"] is None for r in rows))

    def test_bad_summary_is_skipped_and_zero_discard_seconds_is_valid(self):
        self.record("a")
        self.record("a", attempt="f", outcome="interrupted", elapsed=0)
        self.store.put(join(self.prefix, "attempts/a/bad/summary.json"), b"[]")
        groups, sample = observations(self.store, self.prefix, 2000)
        self.assertEqual(next(iter(groups.values()))["discarded_seconds"], 0)
        self.assertEqual(sample["skipped_records"], 1)

    def test_zero_unit_tasks_are_in_the_measured_median(self):
        self.record("a", units=100)
        self.record("b", units=0)
        self.record("c", units=0)
        row = next(r for r in self.report(["--run", "labels"])["rows"] if r["instance_type"] == "c7a.medium")
        self.assertEqual(row["observations"], 3)
        self.assertEqual(row["units_per_second_per_worker"], 0)
        self.assertIsNone(row["usd_per_1m_units"])

    def test_invalid_arguments_fail_before_ec2(self):
        for flags in (["--vcpus", "0"], ["--memory", "-1"], ["--gpus", "-1"], ["--hours", "nan"], ["--hours", "2161"], ["--instance-memory-reserve-mib", "-1"], ["--max-markers", "0"]):
            with self.subTest(flags=flags):
                self.assertEqual(self.execute(flags)[0], 2)
        self.assertEqual(self.execute(["--run", "labels", "--region", "us-east-1"], cfg=False)[0], 2)
        self.ec2.get_paginator.assert_not_called()

    def test_empty_prices_are_reported(self):
        self.ec2.get_paginator.return_value = Mock()
        self.ec2.get_paginator.side_effect = lambda name: Mock(paginate=Mock(return_value=[]))
        rc, out = self.execute(["--json"])
        self.assertEqual(rc, 1)
        self.assertEqual(json.loads(out)["unpriced_instance_types"], ["c6a.large", "c7a.medium", "c7g.medium"])

    def test_human_table_labels_estimates_and_missing_measurements(self):
        rc, out = self.execute()
        self.assertEqual(rc, 0)
        self.assertIn("Spot compute estimates", out)
        self.assertIn("$/worker-h", out)
        self.assertIn("lower bound", out)

    def test_parser_leaves_shape_unspecified(self):
        parser = argparse.ArgumentParser()
        add_arguments(parser)
        args = parser.parse_args([])
        self.assertIsNone(args.vcpus)
        self.assertIsNone(args.memory)
        self.assertIsNone(args.gpus)
        self.assertFalse(args.by_az)

    def test_run_infers_single_shape_and_prices_measured_units(self):
        self.record("a", kind="c6a.large", vcpus=2, memory=3000)
        report = self.report(["--run", "labels"])
        row = report["rows"][0]
        self.assertEqual((row["vcpus"], row["memory_mib"]), (2, 3000))
        self.assertEqual(row["packed_workers"], 1)
        self.assertEqual(row["units_per_second_per_worker"], 10)
        self.assertAlmostEqual(row["usd_per_1m_units"], 0.02 * 1e6 / (10 * 3600))
        self.assertEqual(report["sampling"]["shapes"], [{"vcpus": 2, "memory_mib": 3000, "gpus": 0}])
        rc, out = self.execute(["--run", "labels"])
        self.assertEqual(rc, 0)
        self.assertIn("worker 2 vCPU / 3000 MiB", out)

    def test_shape_inference_does_not_require_throughput_or_image(self):
        self.record("a", image=None, units=None, elapsed=0, vcpus=2, memory=3000)
        report = self.report(["--run", "labels"])
        self.assertTrue(all((r["vcpus"], r["memory_mib"]) == (2, 3000) for r in report["rows"]))
        self.assertTrue(all(r["usd_per_1m_units"] is None for r in report["rows"]))

    def test_explicit_dimensions_override_the_single_inferred_shape(self):
        self.record("a", kind="c6a.large", vcpus=2, memory=3000)
        for flags, expected in [(["--vcpus", "1"], (1, 3000)), (["--memory", "2000"], (2, 2000)), (["--vcpus", "1", "--memory", "1024"], (1, 1024))]:
            with self.subTest(flags=flags):
                rows = self.report(["--run", "labels", *flags])["rows"]
                self.assertTrue(all((r["vcpus"], r["memory_mib"]) == expected for r in rows))
                self.assertTrue(all(r["usd_per_1m_units"] is None for r in rows))

    def test_mixed_marker_shapes_are_listed_without_querying_ec2(self):
        self.record("a")
        self.record("b", kind="c6a.large", vcpus=2, memory=3000)
        rc, _ = self.execute(["--run", "labels"])
        self.assertEqual(rc, 2)
        self.assertIn("multiple worker shapes", self.last_stderr)
        self.assertIn("1 vCPU / 1536 MiB", self.last_stderr)
        self.assertIn("2 vCPU / 3000 MiB", self.last_stderr)
        self.assertIn("--vcpus and --memory", self.last_stderr)
        self.session.client.assert_not_called()
        row = self.report(["--run", "labels", "--vcpus", "2", "--memory", "3000"])["rows"][0]
        self.assertEqual(row["units_per_second_per_worker"], 10)

    def test_partial_explicit_shape_can_resolve_a_mixed_sample(self):
        self.record("a")
        self.record("b", kind="c6a.large", vcpus=2, memory=3000)
        for flags in (["--vcpus", "2"], ["--memory", "3000"]):
            with self.subTest(flags=flags):
                row = self.report(["--run", "labels", *flags])["rows"][0]
                self.assertEqual((row["vcpus"], row["memory_mib"]), (2, 3000))
                self.assertIsNotNone(row["usd_per_1m_units"])

    def test_partial_explicit_shape_still_ambiguous_or_unmatched(self):
        self.record("a", kind="c6a.large", vcpus=2, memory=3000)
        self.record("b", kind="c6a.large", vcpus=2, memory=3500)
        for flags in (["--vcpus", "2"], ["--vcpus", "4"]):
            with self.subTest(flags=flags):
                self.assertEqual(self.execute(["--run", "labels", *flags])[0], 2)
                self.assertIn("2 vCPU / 3000 MiB", self.last_stderr)
                self.assertIn("2 vCPU / 3500 MiB", self.last_stderr)
        self.session.client.assert_not_called()

    def test_inference_uses_only_sampled_markers_not_discarded_attempt_shapes(self):
        self.record("a", kind="c6a.large", vcpus=2, memory=3000)
        self.record("b")
        self.record("a", attempt="f", outcome="failed", vcpus=4, memory=8192)
        report = self.report(["--run", "labels", "--max-markers", "1"])
        self.assertEqual(report["sampling"]["shapes"], [{"vcpus": 2, "memory_mib": 3000, "gpus": 0}])
        self.assertEqual(report["rows"][0]["units_per_second_per_worker"], 10)

    def test_no_markers_keeps_the_price_only_default_shape(self):
        rows = self.report(["--run", "labels"])["rows"]
        self.assertTrue(all((r["vcpus"], r["memory_mib"]) == (1, 1536) for r in rows))
        self.assertTrue(all(r["usd_per_1m_units"] is None for r in rows))
        self.assertTrue(all(r["gpus"] == 0 for r in rows))

    def test_gpu_allocation_is_inferred_and_limits_packing(self):
        self.record("a", kind="g5.xlarge", gpus=1)
        row = self.report(["--run", "labels", "--instance-types", "g5.xlarge"])["rows"][0]
        self.assertEqual((row["vcpus"], row["memory_mib"], row["gpus"]), (1, 1536, 1))
        self.assertEqual(row["host_gpus"], 1)
        self.assertEqual(row["packed_workers"], 1)
        self.assertEqual(row["worker_hour_usd"], 0.02)
        self.assertAlmostEqual(row["usd_per_1m_units"], 0.02 * 1e6 / (10 * 3600))

    def test_gpu_count_sums_devices_and_cpu_memory_still_constrain_packing(self):
        original = self.spec

        def multi_gpu(kind):
            spec = original(kind)
            spec["GpuInfo"] = {"Gpus": [{"Count": 2}, {"Count": 2}]}
            return spec

        self.spec = multi_gpu
        for flags, packed in [(["--gpus", "2"], 2), (["--gpus", "1", "--vcpus", "3"], 1), (["--gpus", "1", "--memory", "10000"], 1)]:
            with self.subTest(flags=flags):
                row = self.report(["--instance-types", "g5.xlarge", *flags])["rows"][0]
                self.assertEqual(row["host_gpus"], 4)
                self.assertEqual(row["packed_workers"], packed)
                self.assertIsNone(row["usd_per_1m_units"])

    def test_gpu_workers_skip_hosts_without_enough_gpus(self):
        rows = self.report(["--instance-types", "c7a.medium", "g5.xlarge", "--gpus", "1"])["rows"]
        self.assertEqual({row["instance_type"] for row in rows}, {"g5.xlarge"})
        for flags in (["--instance-types", "c7a.medium", "--gpus", "1"], ["--instance-types", "g5.xlarge", "--gpus", "2"]):
            with self.subTest(flags=flags):
                rc, output = self.execute([*flags, "--json"])
                self.assertEqual(rc, 1)
                self.assertEqual(json.loads(output)["rows"], [])

    def test_gpu_groups_and_replay_do_not_pool_different_allocations(self):
        self.record("a", kind="g5.xlarge", gpus=1)
        self.record("b", kind="g5.xlarge", gpus=0, units=1000)
        self.record("a", kind="g5.xlarge", gpus=1, attempt="failed", outcome="failed", elapsed=5)
        self.record("a", kind="g5.xlarge", gpus=0, attempt="cpu", outcome="failed", elapsed=100)
        flags = ["--run", "labels", "--instance-types", "g5.xlarge"]
        self.assertEqual(self.execute(flags)[0], 2)
        self.assertIn("0 GPU", self.last_stderr)
        self.assertIn("1 GPU", self.last_stderr)
        row = self.report([*flags, "--gpus", "1"])["rows"][0]
        self.assertEqual(row["units_per_second_per_worker"], 10)
        self.assertEqual(row["observations"], 1)
        self.assertEqual(row["replay_fraction"], 0.5)
        row = self.report([*flags, "--gpus", "0"])["rows"][0]
        self.assertEqual(row["units_per_second_per_worker"], 100)
        self.assertEqual(row["packed_workers"], 4)

    def test_missing_gpu_telemetry_means_zero_and_invalid_counts_are_skipped(self):
        self.record("a", gpus=None)
        for i, value in enumerate((-1, 0.5, True, "1")):
            self.record(f"bad{i}", gpus=value, units=100000)
        report = self.report(["--run", "labels"])
        self.assertEqual(report["sampling"]["shapes"], [{"vcpus": 1, "memory_mib": 1536, "gpus": 0}])
        self.assertEqual(report["rows"][0]["observations"], 1)
        self.assertEqual(report["rows"][0]["units_per_second_per_worker"], 10)

    def test_shape_inference_filters_types_arch_and_lane_before_ambiguity(self):
        self.record("a", kind="c6a.large", vcpus=2, memory=3000, lane="x86")
        self.record("b", kind="c7g.medium", arch="aarch64", lane="arm")
        for flags in (["--lane", "arm"], ["--arch", "arm64"], ["--instance-types", "c7g.medium"]):
            with self.subTest(flags=flags):
                report = self.report(["--run", "labels", *flags])
                self.assertEqual(report["sampling"]["shapes"], [{"vcpus": 1, "memory_mib": 1536, "gpus": 0}])
                row = next(row for row in report["rows"] if row["instance_type"] == "c7g.medium")
                self.assertEqual(row["units_per_second_per_worker"], 10)
                self.assertEqual(report["sampling"]["filtered_records"], 1)

    def test_lane_filter_separates_shared_types_and_discarded_observations(self):
        self.record("a", lane="x86")
        self.record("b", lane="other", vcpus=2, memory=3000)
        self.record("a", lane="other", attempt="f", outcome="failed", elapsed=100)
        row = self.report(["--run", "labels", "--lane", "x86"])["rows"][0]
        self.assertEqual((row["vcpus"], row["memory_mib"]), (1, 1536))
        self.assertEqual(row["replay_fraction"], 0)

    def test_no_matching_markers_falls_back_to_price_only_shape(self):
        self.record("a", kind="c6a.large", vcpus=2, memory=3000, lane="x86")
        report = self.report(["--run", "labels", "--lane", "arm"])
        self.assertEqual(report["sampling"]["shapes"], [])
        self.assertTrue(all(row["usd_per_1m_units"] is None for row in report["rows"]))
        self.assertTrue(all((row["vcpus"], row["memory_mib"], row["gpus"]) == (1, 1536, 0) for row in report["rows"]))

    def human_rows(self, flags=()):
        rc, out = self.execute(flags)
        self.assertEqual(rc, 0, out)
        return [line.split() for line in out.splitlines() if line.startswith(("c7a.medium", "c6a.large", "c7g.medium"))]

    def test_default_human_table_shows_cheapest_az_and_az_count(self):
        original = self.price
        self.price = lambda kind, az, hourly, stamp: original(kind, az, 0.01 if az == "b" else hourly, stamp)
        rows = self.human_rows()
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(row[1:3] == ["us-west-2b", "2"] for row in rows))
        self.assertAlmostEqual(float(next(row for row in rows if row[0] == "c6a.large")[6]), 0.005)

    def test_summary_chooses_cheapest_az_even_when_worker_does_not_fit(self):
        original = self.price
        self.price = lambda kind, az, hourly, stamp: original(kind, az, 0.01 if az == "b" else hourly, stamp)
        row = self.human_rows(["--instance-types", "c7a.medium", "--memory", "3000"])[0]
        self.assertEqual(row[1:3], ["us-west-2b", "2"])
        self.assertEqual(row[4], "0")
        self.assertEqual(row[6], "-")

    def test_summary_keeps_each_image_and_arch_separate(self):
        self.record("a")
        self.record("b", image="image:v2", units=1000)
        rows = self.human_rows(["--run", "labels", "--instance-types", "c7a.medium"])
        self.assertEqual(len(rows), 2)
        self.assertEqual({row[-1] for row in rows}, {"image:v1", "image:v2"})
        self.assertEqual(rows[0][-1], "image:v2")
        self.assertTrue(all(row[1:3] == ["us-west-2a", "2"] for row in rows))
        original = self.spec

        def dual_arch(kind):
            spec = original(kind)
            spec["ProcessorInfo"]["SupportedArchitectures"] = ["x86_64", "arm64"]
            return spec

        self.spec = dual_arch
        rows = self.human_rows(["--instance-types", "c7a.medium"])
        self.assertEqual(len(rows), 2)
        self.assertEqual({row[3] for row in rows}, {"x86_64", "arm64"})

    def test_by_az_restores_per_az_human_rows_and_json_stays_per_az(self):
        rows = self.human_rows(["--by-az"])
        self.assertEqual(len(rows), 6)
        self.assertEqual({row[1] for row in rows}, {"us-west-2a", "us-west-2b"})
        self.assertEqual(self.report()["rows"], self.report(["--by-az"])["rows"])
        self.assertEqual(len(self.report()["rows"]), 6)


if __name__ == "__main__":
    unittest.main()
