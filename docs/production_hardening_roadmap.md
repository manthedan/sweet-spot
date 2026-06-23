# SweetSpot / SweetSpot production-hardening roadmap

This roadmap tracks the static-review findings that separate the current runner from a production-quality, portfolio-ready distributed work system.

Current positioning:

> SweetSpot is a durable, at-least-once AWS Batch Spot runner for trusted, idempotent, embarrassingly parallel workloads.

Do **not** claim arbitrary-job safety, exactly-once side effects, or globally optimal cost until the relevant milestones below are complete and measured.

## Milestone 1 — correctness and trust-boundary hotfixes

Goal: remove the sharpest correctness hazards without redesigning the commit protocol.

- [x] Cap task execution below SQS's 12-hour visibility ceiling, preferably at 11 hours or less.
- [x] Validate `0 < heartbeat_seconds < visibility_timeout <= 43200`.
- [x] Reject default or per-task timeouts above the safe cap.
- [x] Emit structured heartbeat/lease-renewal failure events instead of swallowing errors silently.
- [x] Document that the queue is a trusted control plane and tasks must be idempotent.
- [x] Reject task environment keys that can override framework/AWS/ECS state (`SWEETSPOT_*`, `AWS_*`, `ECS_*`).
- [x] Fix the README/log tooling mismatch around the OpenTofu log group.
- [x] Add focused tests for timeout caps, heartbeat validation, reserved env rejection, and log-group behavior.

## Milestone 2 — atomic attempt/commit protocol

Goal: tolerate duplicate delivery and concurrent execution without canonical output/marker overwrite.

- [x] Define a canonical task hash over stable task fields.
- [x] Assign every execution an immutable attempt ID.
- [x] Support attempt-scoped output, summary, and log paths.
- [x] Record output size and SHA-256 in the completion marker.
- [x] Publish the canonical done marker with S3 conditional write (`If-None-Match: *`).
- [x] Treat a 412/precondition failure as "another attempt won"; read and validate the winning marker before deleting SQS.
- [x] Validate existing marker schema, run ID, task ID, task hash, output URI, size, and checksum before skipping.
- [x] Add tests for duplicate delivery, stale/corrupt markers, and commit-race interruption behavior.

## Milestone 3 — IAM and input model hardening

Goal: make the trust boundary explicit and least-privileged.

- [x] Add a versioned task model/schema for enqueue and worker validation.
- [x] Detect duplicate task IDs before enqueue/finalize.
- [x] Enforce allowed S3 input/output prefixes.
- [x] Narrow worker IAM to required queue and configured bucket/prefix resources.
- [x] Remove unused worker permissions, including unnecessary `sqs:SendMessage` and invalid `s3:HeadObject` action.
- [x] Consider named workload profiles (`job_type`) for common trusted commands instead of unrestricted command arrays. Decision: keep `command` in v1 for generic trusted workloads; reserve optional `job_type` metadata for future profile enforcement.
- [x] Use separate queues/task roles for workloads with different data access. The OpenTofu module is now least-privilege per instantiation; deploy one module instance per data-access boundary.

## Milestone 4 — live observability and diagnostics

Goal: preserve diagnostics during Spot interruption and make operators faster.

- [x] Stream child stdout/stderr to container logs for CloudWatch visibility.
- [x] Keep bounded ring buffers for summary tails instead of reading whole log files.
- [x] Cap per-task log bytes and support redaction patterns.
- [x] Emit structured JSON events for lease renewal, retry, timeout, upload, commit, and skip decisions.
- [x] Add `sweetspot doctor` to validate queue/DLQ/S3 permissions, job definition, log group, timeouts, architecture, and quotas. Quota validation is currently advisory because AWS Batch quota codes vary by account/Region.
- [x] Add CloudWatch dashboard and alarms for queue age, DLQ depth, Batch failures, and stalled runnable jobs.

## Milestone 5 — scale/cost correctness

Goal: make finalization and cleanup scale and make cost claims defensible.

- [x] Stream finalizer JSONL instead of loading all tasks/records in memory.
- [x] Reject duplicate task IDs during finalization and repair generation.
- [x] Validate marker contents and output checksums during finalization.
- [x] Reduce HEAD-per-task cost with partitioned manifests, listings, or a durable state index. Implemented with `--use-listing-index`, `--preload-s3-prefix`, bounded inline outputs, and complete `outputs.jsonl` / `task_status.jsonl` manifests.
- [x] Add explicit versioned-bucket cleanup mode (`--include-versions`) plus lifecycle-policy guidance.
- [x] Replace manual DLQ send-then-delete redrive with native SQS redrive APIs where possible. Whole-DLQ redrive uses `StartMessageMoveTask`; filtered small repairs keep manual redrive.

## Milestone 6 — reproducible production deployment

Goal: make the infrastructure demonstrably reproducible and safer by default.

- [x] Add CI for tests, Ruff formatting/linting, typing, `tofu fmt`, `tofu validate`, and provider-lock verification.
- [x] Commit the OpenTofu provider lock file and constrain provider versions.
- [x] Pin Python dependencies and base image digest.
- [x] Run worker containers as an unprivileged user.
- [x] Add image provenance/SBOM generation and container scanning.
- [x] Provide a production topology option with dedicated no-ingress security group, explicit subnet mode, IMDSv2, encryption, cost tags, budget ceiling, and automatic teardown guidance.
- [x] Set DLQ retention longer than source queue retention and narrow the redrive allow policy.

## Milestone 7 — measured cost-optimization proof

Goal: turn the Spot scout/lane manager into a measured controller and publish an honest case study.

- [x] Have workers emit instance type, architecture, Region/AZ, image digest, startup delay, useful units completed, useful compute seconds, bytes transferred, retry/interruption status, attempt count, and discarded compute seconds.
- [x] Teach `scout` and lane manager to optimize expected total cost, not just instance price.
- [x] Include compute, interruption replay, startup overhead, cross-region transfer, NAT/endpoints, S3/SQS/CloudWatch, storage, and On-Demand repair costs.
- [x] Publish an anonymized machine-readable run manifest and short case study comparing Spot vs On-Demand under a fixed deadline and completeness target.

## Identity cleanup

Pick one public identity and make the others subordinate. Preferred framing:

> **SweetSpot — a cost-aware AWS Batch Spot work runner.**

Public identity is now SweetSpot end-to-end: distribution/import package, schemas, environment variables, container command, and CLI all use `sweetspot` / `SWEETSPOT_*`.
