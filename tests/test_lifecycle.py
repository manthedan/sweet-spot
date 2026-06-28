from __future__ import annotations

import argparse
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import sweetspot.cli as cli

from sweetspot.lifecycle import (
    LIFECYCLE_STATE_REPORT_REQUIRED_FIELDS,
    LIFECYCLE_STATE_SCHEMA_V1,
    LIFECYCLE_STATES,
    REVIEW_REQUIRED_LIFECYCLE_STATES,
    TERMINAL_LIFECYCLE_STATES,
    evaluate_lifecycle_state,
    validate_lifecycle_state_report,
)


class LifecycleContractTests(unittest.TestCase):
    def test_canonical_state_list_matches_contract(self) -> None:
        self.assertEqual(
            LIFECYCLE_STATES,
            (
                "NEW",
                "PLANNING",
                "CANARY_MATERIALIZED",
                "CANARY_RUNNING",
                "CANARY_COLLECTING",
                "PLAN_READY",
                "PRODUCTION_ENQUEUED",
                "WORKERS_RUNNING",
                "DRAINING",
                "FINALIZING",
                "COMPLETE",
                "NEEDS_REPAIR",
                "REPAIR_RUNNING",
                "BLOCKED",
                "CANCELLED",
                "FAILED_REVIEW_REQUIRED",
            ),
        )
        self.assertEqual(TERMINAL_LIFECYCLE_STATES, frozenset({"COMPLETE", "CANCELLED"}))
        self.assertEqual(REVIEW_REQUIRED_LIFECYCLE_STATES, frozenset({"FAILED_REVIEW_REQUIRED"}))

    def test_required_report_fields_match_contract(self) -> None:
        self.assertEqual(
            LIFECYCLE_STATE_REPORT_REQUIRED_FIELDS,
            (
                "schema",
                "run_id",
                "artifact_dir",
                "state",
                "legacy_outcome",
                "terminal",
                "review_required",
                "generated_at",
                "known_facts",
                "missing_facts",
                "safe_actions",
                "unsafe_actions",
                "recommended_commands",
                "evidence",
                "warnings",
            ),
        )

    def test_validate_lifecycle_state_report_accepts_minimal_valid_report(self) -> None:
        report = {
            "schema": LIFECYCLE_STATE_SCHEMA_V1,
            "run_id": "run-123",
            "artifact_dir": "artifacts/run-123",
            "state": "PLAN_READY",
            "legacy_outcome": "ready_to_finish",
            "terminal": False,
            "review_required": False,
            "generated_at": "2026-06-27T00:00:00Z",
            "known_facts": {},
            "missing_facts": [],
            "safe_actions": [],
            "unsafe_actions": [],
            "recommended_commands": [],
            "evidence": [],
            "warnings": [],
        }

        self.assertEqual(validate_lifecycle_state_report(report), [])

    def test_validate_lifecycle_state_report_rejects_drift_from_contract(self) -> None:
        report = {
            "schema": "sweetspot.lifecycle_state.v0",
            "run_id": "run-123",
            "artifact_dir": "artifacts/run-123",
            "state": "plan_ready",
            "legacy_outcome": None,
            "terminal": "false",
            "review_required": "false",
            "generated_at": "2026-06-27T00:00:00Z",
            "known_facts": [],
            "missing_facts": {},
            "safe_actions": {},
            "unsafe_actions": {},
            "recommended_commands": {},
            "evidence": {},
            "warnings": {},
        }

        errors = validate_lifecycle_state_report(report)

        self.assertIn("schema must be sweetspot.lifecycle_state.v1", errors)
        self.assertTrue(any(error.startswith("state must be one of") for error in errors))
        self.assertIn("terminal must be a boolean", errors)
        self.assertIn("review_required must be a boolean", errors)
        self.assertIn("known_facts must be an object", errors)
        self.assertIn("missing_facts must be a list", errors)
        self.assertIn("safe_actions must be a list", errors)
        self.assertIn("unsafe_actions must be a list", errors)
        self.assertIn("recommended_commands must be a list", errors)
        self.assertIn("evidence must be a list", errors)
        self.assertIn("warnings must be a list", errors)


class LifecycleEvaluatorTests(unittest.TestCase):
    def _write_json(self, path: Path, payload: dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def _write_jsonl(self, path: Path, rows: list[dict[str, object]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

    def _run_state(self, artifact_dir: Path, *, plan_status: str = "ready", phases: list[dict[str, object]] | None = None) -> None:
        self._write_json(
            artifact_dir / "run_state.json",
            {
                "run_id": "run-123",
                "job_spec_sha256": "abc123",
                "plan": {"status": plan_status, "tasks": [{"id": "task-1"}, {"id": "task-2"}]},
                "controller": {
                    "deployment_sha256": "deploy123",
                    "run_queue": {"queue_url": "https://sqs.example.invalid/q", "dlq_url": "https://sqs.example.invalid/dlq"},
                    "production_binding": {"target": {"batch_job_queue": "queue"}},
                },
                "artifacts": {"production_tasks_jsonl": "production_tasks.jsonl"},
                "phases": phases or [],
            },
        )

    def assertValidReport(self, report: dict[str, object]) -> None:
        self.assertEqual(validate_lifecycle_state_report(report), [])

    def test_missing_run_state_is_new_with_non_mutating_recommendation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            report = evaluate_lifecycle_state(run_id="run-123", artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "NEW")
        self.assertFalse(report["terminal"])
        self.assertFalse(report["review_required"])
        self.assertIn("run_state_json", report["missing_facts"])
        self.assertEqual(report["recommended_commands"][0][:3], ["sweetspot", "run", "JOB_SPEC"])
        self.assertTrue(any(action["action"] == "finish" for action in report["unsafe_actions"]))

    def test_missing_default_run_state_is_new_not_review_required(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            old_cwd = Path.cwd()
            try:
                Path(tmp).mkdir(parents=True, exist_ok=True)
                import os

                os.chdir(tmp)
                report = evaluate_lifecycle_state(run_id="run-123", generated_at="2026-06-27T00:00:00Z")
            finally:
                os.chdir(old_cwd)

        self.assertValidReport(report)
        self.assertEqual(report["state"], "NEW")
        self.assertEqual(report["artifact_dir"], "artifacts/run-123")

    def test_human_lifecycle_command_formatting_shell_quotes_arguments(self) -> None:
        rendered = cli._format_lifecycle_value(["sweetspot", "status", "run id; rm -rf nope"])
        self.assertEqual(rendered, "sweetspot status 'run id; rm -rf nope'")

    def test_ready_plan_reports_known_facts_and_local_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir)
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}, {"id": "task-2"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "PLAN_READY")
        self.assertEqual(report["legacy_outcome"], "plan_ready")
        self.assertEqual(report["known_facts"]["job_spec_sha256"], "abc123")
        self.assertEqual(report["known_facts"]["deployment_sha256"], "deploy123")
        self.assertEqual(report["known_facts"]["plan_task_count"], 2)
        self.assertEqual(report["known_facts"]["production_task_count"], 2)
        self.assertTrue(report["known_facts"]["source_queue_url_recorded"])
        self.assertTrue(any(item["kind"] == "artifact" and item.get("field") == "production_task_count" for item in report["evidence"]))
        self.assertEqual(report["recommended_commands"][0][:4], ["sweetspot", "status", "run-123", "--from-state"])

    def test_canary_task_artifact_reports_canary_materialized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir)
            canary_tasks = artifact_dir / "canary_tasks.jsonl"
            self._write_jsonl(canary_tasks, [{"id": "canary-1"}])
            state = json.loads((artifact_dir / "run_state.json").read_text())
            state["artifacts"] = {"canary_tasks_jsonl": "canary_tasks.jsonl"}
            state["plan"] = {"status": "ready"}
            (artifact_dir / "run_state.json").write_text(json.dumps(state), encoding="utf-8")

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "CANARY_MATERIALIZED")
        self.assertEqual(report["known_facts"]["canary_task_count"], 1)
        self.assertNotIn("production_tasks_jsonl", report["missing_facts"])

    def test_canary_submit_complete_reports_collecting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "submit_canary_workers", "status": "completed"}])
            canary_tasks = artifact_dir / "canary_tasks.jsonl"
            self._write_jsonl(canary_tasks, [{"id": "canary-1"}])
            state = json.loads((artifact_dir / "run_state.json").read_text())
            state["artifacts"] = {"canary_tasks_jsonl": "canary_tasks.jsonl"}
            state["plan"] = {"status": "ready"}
            (artifact_dir / "run_state.json").write_text(json.dumps(state), encoding="utf-8")

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "CANARY_COLLECTING")
        self.assertIn("canary_summary_count", report["missing_facts"])

    def test_production_progress_takes_precedence_over_completed_canary_phase(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(
                artifact_dir,
                phases=[
                    {"name": "submit_canary_workers", "status": "completed"},
                    {"name": "submit_workers", "status": "completed", "job_name_prefix": "run-123-worker"},
                ],
            )
            canary_tasks = artifact_dir / "canary_tasks.jsonl"
            self._write_jsonl(canary_tasks, [{"id": "canary-1"}])
            production_tasks = artifact_dir / "production_tasks.jsonl"
            self._write_jsonl(production_tasks, [{"id": "task-1"}])
            state = json.loads((artifact_dir / "run_state.json").read_text())
            state["artifacts"] = {"canary_tasks_jsonl": "canary_tasks.jsonl", "production_tasks_jsonl": "production_tasks.jsonl"}
            (artifact_dir / "run_state.json").write_text(json.dumps(state), encoding="utf-8")

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "DRAINING")
        self.assertTrue(any(action["action"] == "finish" for action in report["safe_actions"]))

    def test_enqueue_phase_reports_production_enqueued(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "enqueue_tasks", "status": "completed"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "PRODUCTION_ENQUEUED")
        self.assertEqual(report["legacy_outcome"], "in_progress")
        self.assertEqual(report["known_facts"]["enqueue_tasks_status"], "completed")
        self.assertIn("submit_workers_status", report["missing_facts"])
        self.assertEqual(report["recommended_commands"][0][:4], ["sweetspot", "status", "run-123", "--from-state"])
        self.assertTrue(any(action["action"] == "replan" for action in report["unsafe_actions"]))

    def test_submit_phase_reports_workers_running_before_drain_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "submit_workers", "status": "in_progress"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "WORKERS_RUNNING")
        self.assertEqual(report["legacy_outcome"], "in_progress")
        self.assertEqual(report["known_facts"]["submit_workers_status"], "in_progress")
        self.assertIn("source_queue_depth", report["missing_facts"])
        self.assertIn("active_worker_count", report["missing_facts"])
        self.assertTrue(any(action["action"] == "finish" for action in report["unsafe_actions"]))

    def test_submit_complete_with_status_is_draining_and_finish_dry_run_recommended(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "submit_workers", "status": "completed"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_jsonl(artifact_dir / "task_status.jsonl", [{"id": "task-1", "status": "done"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "DRAINING")
        self.assertIn("active_worker_count", report["missing_facts"])
        self.assertIn("final_manifest_complete", report["missing_facts"])
        self.assertIn("--dry-run", report["recommended_commands"][0])
        self.assertEqual(report["known_facts"]["submit_workers_status"], "completed")

    def test_partial_finalizer_artifact_reports_finalizing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "submit_workers", "status": "completed"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_jsonl(artifact_dir / "task_status.jsonl", [{"id": "task-1", "status": "done"}])
            self._write_json(artifact_dir / "finish_report.json", {"started_at": "2026-06-27T00:00:00Z"})

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "FINALIZING")
        self.assertEqual(report["legacy_outcome"], "in_progress")
        self.assertIn("finish_report_ok", report["missing_facts"])
        self.assertEqual(report["recommended_commands"][0][:4], ["sweetspot", "status", "run-123", "--from-state"])
        self.assertTrue(any(item["kind"] == "report" and item.get("field") == "exists" for item in report["evidence"]))

    def test_legacy_finish_report_without_dry_run_flag_is_terminal_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "submit_workers", "status": "completed"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "finish_report.json", {"ok": True, "blocked": False, "blockers": []})

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "COMPLETE")
        self.assertTrue(report["terminal"])
        self.assertTrue(any(action["action"] == "cleanup" for action in report["safe_actions"]))

    def test_dry_run_finish_report_is_not_terminal_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "submit_workers", "status": "completed"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "finish_report.json", {"ok": True, "dry_run": True, "blocked": False, "blockers": []})

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "DRAINING")
        self.assertFalse(report["terminal"])
        self.assertTrue(any(action["action"] == "finish" for action in report["safe_actions"]))
        self.assertTrue(any(action["action"] == "cleanup" for action in report["unsafe_actions"]))

    def test_statusless_submit_phase_reports_draining_for_live_finish_check(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "submit_workers", "job_name_prefix": "run-123-worker"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "DRAINING")
        self.assertIn("submit_workers_status", report["missing_facts"])
        self.assertTrue(any(action["action"] == "finish" for action in report["safe_actions"]))

    def test_stale_blocked_finish_report_returns_to_draining_for_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "submit_workers", "status": "completed"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_jsonl(artifact_dir / "task_status.jsonl", [{"id": "task-1", "status": "done"}])
            self._write_json(artifact_dir / "finish_report.json", {"ok": False, "blockers": [{"code": "batch_jobs_active"}]})

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "DRAINING")
        self.assertTrue(any(action["action"] == "finish" for action in report["safe_actions"]))
        self.assertTrue(any(warning["code"] == "previous_finish_blocked" for warning in report["warnings"]))

    def test_retryable_finalizer_failure_returns_to_draining_for_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "submit_workers", "status": "completed"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "finish_report.json", {"ok": False, "blockers": [{"code": "finalizer_failed"}]})

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "DRAINING")
        self.assertTrue(any(action["action"] == "finish" for action in report["safe_actions"]))

    def test_non_transient_blocked_finish_report_stays_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "submit_workers", "status": "completed"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "finish_report.json", {"ok": False, "blockers": [{"code": "invalid_job_name_prefix"}]})

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "BLOCKED")
        self.assertTrue(any(action["action"] == "finish" for action in report["unsafe_actions"]))

    def test_finalizing_actions_guard_finish_and_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "submit_workers", "status": "completed"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "finish_report.json", {"started_at": "2026-06-27T00:00:00Z"})

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        safe_by_action = {action["action"]: action for action in report["safe_actions"]}
        unsafe_by_action = {action["action"]: action for action in report["unsafe_actions"]}
        self.assertIn("finish_dry_run", safe_by_action)
        self.assertIn("finish", safe_by_action)
        self.assertIn("--dry-run", safe_by_action["finish_dry_run"]["command"])
        self.assertEqual(unsafe_by_action["cleanup"]["required_state"], "COMPLETE")
        self.assertEqual(report["recommended_commands"][0][:4], ["sweetspot", "status", "run-123", "--from-state"])

    def test_complete_final_manifest_is_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir)
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "final_manifest.json", {"complete": True})

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "COMPLETE")
        self.assertTrue(report["terminal"])
        self.assertFalse(report["review_required"])
        self.assertEqual(report["known_facts"]["final_manifest_complete"], True)
        self.assertEqual(report["recommended_commands"][0][:3], ["sweetspot", "cleanup", "run-123"])
        self.assertNotIn("--apply", report["recommended_commands"][0])

    def test_incomplete_final_manifest_needs_repair(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir)
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "final_manifest.json", {"complete": False})
            self._write_jsonl(artifact_dir / "repair_tasks.jsonl", [{"id": "task-1"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "NEEDS_REPAIR")
        self.assertEqual(report["legacy_outcome"], "repair_needed")
        self.assertEqual(report["known_facts"]["repair_task_count"], 1)
        self.assertEqual(report["recommended_commands"][0][:3], ["sweetspot", "repair-plan", "run-123"])

    def test_needs_repair_actions_allow_guarded_finish_only_as_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir)
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "final_manifest.json", {"complete": False})

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        safe_by_action = {action["action"]: action for action in report["safe_actions"]}
        unsafe_by_action = {action["action"]: action for action in report["unsafe_actions"]}
        self.assertIn("repair_plan", safe_by_action)
        self.assertIn("finish_dry_run", safe_by_action)
        self.assertIn("--dry-run", safe_by_action["finish_dry_run"]["command"])
        self.assertEqual(unsafe_by_action["mark_complete"]["required_state"], "COMPLETE")
        self.assertEqual(unsafe_by_action["cleanup"]["required_state"], "COMPLETE")

    def test_completed_repair_enqueue_phase_reports_draining_for_finalization(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "enqueue_repair_tasks", "status": "completed"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_jsonl(artifact_dir / "repair_tasks.jsonl", [{"id": "task-1"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "DRAINING")
        self.assertEqual(report["legacy_outcome"], "in_progress")
        self.assertEqual(report["known_facts"]["repair_task_count"], 1)
        self.assertEqual(report["known_facts"]["repair_enqueue_status"], "completed")
        self.assertIn("repair_queue_depth", report["missing_facts"])
        self.assertIn("active_repair_worker_count", report["missing_facts"])
        self.assertTrue(any(action["action"] == "finish" for action in report["safe_actions"]))

    def test_active_repair_enqueue_phase_reports_repair_running(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "enqueue_repair_tasks", "status": "in_progress"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_jsonl(artifact_dir / "repair_tasks.jsonl", [{"id": "task-1"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "REPAIR_RUNNING")
        self.assertEqual(report["legacy_outcome"], "repair_running")
        self.assertIn("repair_queue_depth", report["missing_facts"])

    def test_blocked_marker_reports_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir)
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "blocked.json", {"reason": "missing permission"})

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "BLOCKED")
        self.assertEqual(report["legacy_outcome"], "blocked")
        self.assertEqual(report["recommended_commands"][0][:3], ["sweetspot", "explain", "run-123"])
        self.assertTrue(any(item.get("path", "").endswith("blocked.json") for item in report["evidence"]))

    def test_cancellation_marker_reports_cancelled_terminal_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir)
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "cancelled.json", {"reason": "operator cancelled"})

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "CANCELLED")
        self.assertEqual(report["legacy_outcome"], "cancelled")
        self.assertTrue(report["terminal"])
        self.assertFalse(report["review_required"])
        self.assertEqual(report["recommended_commands"][0][:3], ["sweetspot", "cleanup", "run-123"])
        self.assertNotIn("--apply", report["recommended_commands"][0])
        self.assertTrue(any(action["action"] == "resume_workers" for action in report["unsafe_actions"]))

    def test_malformed_finalizer_artifact_requires_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir)
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            (artifact_dir / "final_manifest.json").write_text("{not json", encoding="utf-8")

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "FAILED_REVIEW_REQUIRED")
        self.assertTrue(report["review_required"])
        self.assertIn("valid_finalizer_artifacts", report["missing_facts"])
        self.assertTrue(any(warning["code"] == "invalid_final_manifest" for warning in report["warnings"]))
        self.assertEqual(report["recommended_commands"][0][:3], ["sweetspot", "explain", "run-123"])

    def test_malformed_blocked_marker_requires_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir)
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            (artifact_dir / "blocked.json").write_text("{not json", encoding="utf-8")

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "FAILED_REVIEW_REQUIRED")
        self.assertTrue(report["review_required"])
        self.assertIn("valid_local_side_path_artifacts", report["missing_facts"])
        self.assertTrue(any(warning["code"] == "invalid_blocked_report" for warning in report["warnings"]))

    def test_complete_run_allows_stale_repair_task_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir)
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "final_manifest.json", {"complete": True})
            self._write_jsonl(artifact_dir / "repair_tasks.jsonl", [{"id": "task-1"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "COMPLETE")
        self.assertFalse(report["review_required"])
        self.assertEqual(report["known_facts"]["repair_task_count"], 1)

    def test_complete_run_with_completed_repair_phase_is_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "enqueue_repair_tasks", "status": "completed"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "final_manifest.json", {"complete": True})
            self._write_jsonl(artifact_dir / "repair_tasks.jsonl", [{"id": "task-1"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "COMPLETE")
        self.assertTrue(report["terminal"])

    def test_active_repair_work_with_complete_manifest_requires_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "enqueue_repair_tasks", "status": "in_progress"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "final_manifest.json", {"complete": True})
            self._write_jsonl(artifact_dir / "repair_tasks.jsonl", [{"id": "task-1"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "FAILED_REVIEW_REQUIRED")
        self.assertTrue(report["review_required"])
        self.assertIn("consistent_terminal_artifacts", report["missing_facts"])
        self.assertTrue(any(warning["code"] == "contradictory_lifecycle_artifacts" for warning in report["warnings"]))

    def test_active_production_phase_with_complete_manifest_requires_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "submit_workers", "status": "in_progress"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "final_manifest.json", {"complete": True})

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "FAILED_REVIEW_REQUIRED")
        self.assertTrue(report["review_required"])
        self.assertIn("consistent_terminal_artifacts", report["missing_facts"])

    def test_failed_production_phase_reports_blocked_not_plan_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "submit_workers", "status": "failed"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "BLOCKED")
        self.assertEqual(report["legacy_outcome"], "blocked")
        self.assertTrue(any(warning["code"] == "production_phase_failed" for warning in report["warnings"]))

    def test_mismatched_run_id_requires_review_without_raising(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir)

            report = evaluate_lifecycle_state(run_id="other-run", artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "FAILED_REVIEW_REQUIRED")
        self.assertTrue(report["review_required"])
        self.assertIn("valid_run_state_json", report["missing_facts"])
        self.assertTrue(any(warning["code"] == "run_context_load_failed" for warning in report["warnings"]))


class LifecycleCliGuardTests(unittest.TestCase):
    def _write_json(self, path: Path, payload: dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def _write_jsonl(self, path: Path, rows: list[dict[str, object]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

    def _write_run_state(self, artifact_dir: Path, *, phases: list[dict[str, object]] | None = None) -> None:
        self._write_json(
            artifact_dir / "run_state.json",
            {
                "run_id": "run-123",
                "plan": {"status": "ready", "tasks": [{"id": "task-1"}], "job": {"output_prefix": "s3://bucket/runs/run-123"}},
                "controller": {
                    "run_queue": {"queue_url": "https://sqs.example.invalid/q", "dlq_url": "https://sqs.example.invalid/dlq"},
                    "production_binding": {"target": {"batch_job_queue": "queue", "region": "us-east-1"}},
                },
                "artifacts": {"production_tasks_jsonl": "production_tasks.jsonl"},
                "phases": phases or [],
            },
        )
        self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])

    def test_finish_from_state_allows_safe_mutating_action_after_guard(self) -> None:
        class FakeSQS:
            def get_queue_attributes(self, **kwargs):
                return {"Attributes": {"ApproximateNumberOfMessages": "0", "ApproximateNumberOfMessagesNotVisible": "0", "ApproximateNumberOfMessagesDelayed": "0"}}

        class FakePaginator:
            def paginate(self, **kwargs):
                return [{"jobSummaryList": []}]

        class FakeBatch:
            def get_paginator(self, name):
                return FakePaginator()

        class FakeS3:
            def list_objects_v2(self, **kwargs):
                return {"Contents": [{"Key": "runs/run-123/done/task-1.done.json"}], "IsTruncated": False}

        class FakeSession:
            def __init__(self, profile_name=None, region_name=None):
                self.region_name = region_name

            def client(self, service, region_name=None):
                if service == "sqs":
                    return FakeSQS()
                if service == "batch":
                    return FakeBatch()
                if service == "s3":
                    return FakeS3()
                raise AssertionError(service)

        def fake_finalizer(args, **kwargs):
            print(json.dumps({"complete": True}))
            return 0

        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._write_run_state(artifact_dir, phases=[{"name": "submit_workers", "status": "completed", "job_name_prefix": "run-123-worker"}])
            self._write_jsonl(artifact_dir / "task_status.jsonl", [{"id": "task-1", "status": "done"}])
            args = argparse.Namespace(run_id="run-123", artifact_dir=artifact_dir, from_state=True, dry_run=False, region=None, profile=None)

            stdout = io.StringIO()
            with mock.patch.object(cli.boto3, "Session", FakeSession), mock.patch.object(cli, "_run_finalizer_service", side_effect=fake_finalizer):
                with contextlib.redirect_stdout(stdout):
                    rc = cli.cmd_finish(args)

        report = json.loads(stdout.getvalue())
        self.assertEqual(rc, 0)
        self.assertEqual(report["schema"], "sweetspot.finish.v1")
        self.assertFalse(report["blocked"])
        self.assertEqual(report["finalizer"]["complete"], True)

    def test_explain_from_state_reports_lifecycle_when_context_load_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._write_json(artifact_dir / "run_state.json", {"run_id": "other-run"})
            args = argparse.Namespace(run_id="run-123", artifact_dir=artifact_dir, from_state=True, format="json")

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = cli.cmd_explain(args)

        report = json.loads(stdout.getvalue())
        self.assertEqual(rc, 0)
        self.assertEqual(report["schema"], "sweetspot.lifecycle_explain.v1")
        self.assertEqual(report["state"], "FAILED_REVIEW_REQUIRED")
        self.assertIn("valid_run_state_json", report["missing_facts"])

    def test_cleanup_from_state_dry_run_reports_blockers_for_active_run(self) -> None:
        class FakeSQS:
            def get_queue_attributes(self, **kwargs):
                return {"Attributes": {"ApproximateNumberOfMessages": "1", "ApproximateNumberOfMessagesNotVisible": "0", "ApproximateNumberOfMessagesDelayed": "0"}}

        class FakePaginator:
            def paginate(self, **kwargs):
                return [{"jobSummaryList": []}]

        class FakeBatch:
            def get_paginator(self, name):
                return FakePaginator()

        class FakeSession:
            def __init__(self, profile_name=None, region_name=None):
                self.region_name = region_name

            def client(self, service, region_name=None):
                if service == "sqs":
                    return FakeSQS()
                if service == "batch":
                    return FakeBatch()
                raise AssertionError(service)

        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._write_run_state(artifact_dir)
            args = argparse.Namespace(run_id="run-123", artifact_dir=artifact_dir, from_state=True, apply=False, region=None, profile=None)

            stdout = io.StringIO()
            with mock.patch.object(cli.boto3, "Session", FakeSession):
                with contextlib.redirect_stdout(stdout):
                    rc = cli.cmd_cleanup(args)

        report = json.loads(stdout.getvalue())
        self.assertEqual(rc, 2)
        self.assertEqual(report["schema"], "sweetspot.cleanup_plan.v1")
        self.assertTrue(report["blocked"])
        self.assertEqual(report["blockers"][0]["code"], "source_queue_not_empty")


if __name__ == "__main__":
    unittest.main()
