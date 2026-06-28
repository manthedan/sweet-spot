from __future__ import annotations

import unittest

from sweetspot.lifecycle import (
    LIFECYCLE_STATE_REPORT_REQUIRED_FIELDS,
    LIFECYCLE_STATE_SCHEMA_V1,
    LIFECYCLE_STATES,
    REVIEW_REQUIRED_LIFECYCLE_STATES,
    TERMINAL_LIFECYCLE_STATES,
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


if __name__ == "__main__":
    unittest.main()
