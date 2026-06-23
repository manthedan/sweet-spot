# Changelog

All notable changes to this project are documented here. This project uses human-readable release notes; tag names should use the chosen public package/repo name plus a semantic version when releases begin.

## Unreleased

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
