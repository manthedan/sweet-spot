from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

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

    def test_ready_plan_reports_known_facts_and_local_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir)
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}, {"id": "task-2"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "PLAN_READY")
        self.assertEqual(report["legacy_outcome"], "ready_to_finish")
        self.assertEqual(report["known_facts"]["job_spec_sha256"], "abc123")
        self.assertEqual(report["known_facts"]["deployment_sha256"], "deploy123")
        self.assertEqual(report["known_facts"]["plan_task_count"], 2)
        self.assertEqual(report["known_facts"]["production_task_count"], 2)
        self.assertTrue(report["known_facts"]["source_queue_url_recorded"])
        self.assertTrue(any(item["kind"] == "artifact" and item.get("field") == "production_task_count" for item in report["evidence"]))
        self.assertEqual(report["recommended_commands"][0][:4], ["sweetspot", "status", "run-123", "--from-state"])

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
        self.assertIn("--dry-run", report["recommended_commands"][0])

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

    def test_repair_tasks_with_repair_enqueue_phase_reports_repair_running(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir, phases=[{"name": "enqueue_repair_tasks", "status": "completed"}])
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_jsonl(artifact_dir / "repair_tasks.jsonl", [{"id": "task-1"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "REPAIR_RUNNING")
        self.assertEqual(report["legacy_outcome"], "repair_running")
        self.assertEqual(report["known_facts"]["repair_task_count"], 1)
        self.assertEqual(report["known_facts"]["repair_enqueue_status"], "completed")
        self.assertIn("repair_queue_depth", report["missing_facts"])
        self.assertIn("active_repair_worker_count", report["missing_facts"])
        self.assertEqual(report["recommended_commands"][0][:4], ["sweetspot", "status", "run-123", "--from-state"])

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
        self.assertIn("--dry-run", report["recommended_commands"][0])
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

    def test_contradictory_complete_and_repair_artifacts_require_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            artifact_dir = Path(tmp) / "run-123"
            self._run_state(artifact_dir)
            self._write_jsonl(artifact_dir / "production_tasks.jsonl", [{"id": "task-1"}])
            self._write_json(artifact_dir / "final_manifest.json", {"complete": True})
            self._write_jsonl(artifact_dir / "repair_tasks.jsonl", [{"id": "task-1"}])

            report = evaluate_lifecycle_state(artifact_dir=artifact_dir, generated_at="2026-06-27T00:00:00Z")

        self.assertValidReport(report)
        self.assertEqual(report["state"], "FAILED_REVIEW_REQUIRED")
        self.assertTrue(report["review_required"])
        self.assertIn("consistent_terminal_artifacts", report["missing_facts"])
        self.assertTrue(any(warning["code"] == "contradictory_lifecycle_artifacts" for warning in report["warnings"]))

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


if __name__ == "__main__":
    unittest.main()
