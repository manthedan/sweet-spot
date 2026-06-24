# Changelog

All notable changes to this project are documented here. This project uses human-readable release notes; tag names should use the chosen public package/repo name plus a semantic version when releases begin.

## Unreleased

### Added

- Contributor guide covering local development, release checks, dependency pins, and SweetSpot's trusted-workload boundary.
- PyPI-oriented project metadata for repository URLs, keywords, classifiers, and SPDX license expression.
- `enqueue-and-submit` command that waits for SQS visible-message depth before sizing/submitting a worker wave.
- `repair-plan` command that combines finalizer task status with Batch job/log state to avoid repairing tasks still owned by active workers.
- `estimate-runtime` command for canary-derived wall-time, timeout-risk, and compute-cost estimates.
- `cleanup-stale-messages` command for dry-run/apply deletion of visible duplicate SQS messages whose done markers already exist.
- README guidance for small idempotent chunks, canary sizing, safe repair planning, and stale-message cleanup.
- Consistent `--profile`/`--region` options for AWS-touching CLI commands.
- `sweetspot version` command for checking the installed package version.
- `--queue-url` alias for worker-submission commands that previously exposed only `--sqs-queue-url`.
- Short argparse help examples for high-traffic production workflow commands.
- `sweetspot status` command for a one-shot AWS identity, queue-depth, DLQ-depth, and active-worker overview.
- JSON `--config` / `SWEETSPOT_CONFIG` support for prepopulating common command defaults and required workflow flags.
- `sweetspot scout` and `sweetspot lane-manager` subcommands, while preserving the standalone entry points.
- Opt-in human-readable `sweetspot status --format table` output while keeping JSON as the default.
- Clearer `sweetspot logs` aliases: `--max-events` for `--limit` and `--last` for `--tail`.
- Opt-in `--format table` output for read-oriented operator commands: `jobs`, `describe-job`, `logs`, `watch-job`, `doctor`, and `dlq`.
- `sweetspot finalize --dry-run` for previewing upload/READY targets while skipping S3 mutations.
- Guarded `sweetspot cancel-jobs` command with dry-run default, `--apply`, and explicit `--terminate-running` for active Batch jobs.
- Mixed-architecture cost guidance, including opt-in ARM/Graviton scouting docs, per-lane lane-manager instance type overrides, and a mixed x86/ARM lane example.
- `sweetspot cancel-jobs --format table` plus `--sqs-queue-url` aliases/config defaults for enqueue workflows.
- `sweetspot.job.v1` / `sweetspot.plan.v1` contract validation foundations with examples and stable planner reason codes.
- Initial `sweetspot plan JOB_SPEC` command that validates the declarative job contract and emits JSON plan status/reason codes without mutating AWS resources.
- `sweetspot scout --worker-memory` for memory-aware worker packing and cost estimates.
- Machine-readable `sweetspot scout` reason codes for default throughput, unobserved replay, and omitted optional cost components.
- Worker telemetry now records requested worker vCPU/memory, task-reported peak memory when available, and best-effort IMDS instance type/AZ fallback.
- Scout observed-performance telemetry now exposes `ApproximateReceiveCount` replay lower bounds and warns when replay cost is lower-bound-only or only partially captured by discarded-compute summaries.
- Optional OpenTofu ARM/Graviton Spot queue and worker job definition for canary-gated x86/ARM lane separation.
- Worker telemetry now records best-effort EC2 Spot interruption and rebalance notices from IMDS when available.
- Internal adaptive shard-sizing helper for growing canaries toward replay-safe task durations without agent-supplied shard sizes.
- `sweetspot plan --canary-summary-jsonl` to embed adaptive shard-sizing decisions from local canary summaries in the Plan JSON envelope.
- `sweetspot plan --input-manifest-jsonl` can combine a local logical-unit manifest copy with canary sizing to report adaptive production shard counts without mutating AWS resources.
- `sweetspot plan --out-production-tasks-jsonl` for explicitly writing calibrated production `sweetspot.task.v1` shards as a local review/enqueue artifact.

### Changed

- `sweetspot scout` now emits JSON to stdout by default; human table output is opt-in with `--format table`.

### Fixed

- `sweetspot scout` now labels placement scores as configuration-scoped on per-pool rows instead of implying per-instance/AZ placement guarantees.
- Operator-facing CLI failures for SQS batch sends, S3 bulk deletes, DLQ queue ARN lookup, and doctor validation now report clean errors instead of uncaught `RuntimeError` tracebacks.
- `scripts/verify_release.sh` now works on systems that provide `python3` but not `python`, and reports missing release-check dependencies with install guidance.

## sweetspot-v0.1.0 - 2026-06-23

### Added

- Atomic attempt/commit protocol with attempt-scoped output and summary artifacts.
- Hardened task validation, reserved environment namespaces, S3 prefix allow-listing, and least-privilege infrastructure defaults.
- Live worker observability: redacted stdout/stderr streaming, capped S3 attempt logs, job/log inspection commands, dashboards, and alarms.
- Streaming finalization artifacts for large runs, repair-task generation, S3 listing indexes, and guarded/version-aware S3 prefix cleanup.
- Reproducible deployment hardening: pinned dependencies, pinned GitHub Actions, committed OpenTofu provider lock, unprivileged worker container, SBOM/provenance build, and Trivy scan of the exact uploaded OCI artifact.
- Measured cost optimization: worker telemetry, expected-total-cost Spot scouting, cost-aware lane ordering, and anonymized manifest/case-study templates.
- Local `scripts/verify_release.sh` closeout script matching CI-critical checks.
- Optional doctor validation for live AWS/Batch CloudWatch metric dimensions.

### Changed

- Renamed the public project, Python distribution, import package, schemas, environment variables, Docker worker identity, and CLI to SweetSpot / `sweetspot`.
- Source SQS retention defaults to 13 days and DLQ retention to 14 days to avoid destructive retention shrinkage during upgrades.
- Spot pool ranking now prefers expected total cost per unit over raw Spot price when telemetry/assumptions are available.
- Legacy v1 done markers now require explicit migration mode; default worker/finalizer validation expects hash-bound v2 markers.
- Lane placement-score gating now fails closed when `min_placement_score` is set and AWS returns no score, unless explicitly allowed per lane.

### Fixed

- Upgraded boto3/botocore locks so S3 `PutObject` supports `IfNoneMatch`, and added a contract test for the service model.
- S3 conditional done-marker commits now retry transient 409 conflicts while treating 412 as an existing marker.
- DLQ canary probes now generate valid task payloads with explicit done markers.
- Runtime S3 prefix validation now matches OpenTofu IAM `${prefix}/*` object semantics.
- Duplicate attempts that lose the done-marker race now persist corrected summaries with discarded compute telemetry.
- Cost telemetry aggregation excludes commit-lost attempts from useful-throughput denominators.
- Lane manager pre-counts active workers across all lanes before cost-ordered allocation to avoid oversubmission.
- CI Trivy action pin updated to a commit whose internal `setup-trivy` dependency resolves to an existing pinned version.
