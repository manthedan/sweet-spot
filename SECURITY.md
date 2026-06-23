# Security Policy

## Supported versions

This project is pre-1.0. Security fixes are made on `main` until a formal release branch policy exists.

## Threat model

SweetSpot is designed for **trusted, idempotent AWS Batch workloads**. It is not a sandbox for arbitrary untrusted code.

The SQS work queue is a trusted control plane: any principal that can enqueue a task can choose the command executed by the worker job role. Production deployments should therefore:

- restrict SQS producers to trusted identities only;
- set `SWEETSPOT_ALLOWED_S3_PREFIXES` or pass `--allowed-s3-prefix` so task payload S3 URIs remain inside approved prefixes;
- keep task commands idempotent and bind external side effects to `task_id` / `task_hash`;
- run workers with the least-privilege IAM role generated or reviewed for the specific bucket/prefix/queue set;
- keep Batch security groups no-ingress by default;
- keep IMDSv2 required and hop limit at `1` unless you deliberately need container metadata access and have reviewed the credential exposure tradeoff;
- avoid putting secrets in task payloads, command arguments, or logs; use scoped runtime secret mechanisms where needed and configure redaction regexes.

## Supply chain

CI and release checks intentionally use:

- Python dependency lock files (`requirements.lock`, `requirements-dev.lock`);
- a Docker base image pinned by digest;
- GitHub Actions pinned to immutable commit SHAs;
- an OpenTofu provider lock file;
- one attested OCI tar that is built, scanned, and uploaded without rebuilding between scan and artifact upload.

When updating third-party actions, resolve the tag to a full commit SHA and verify any composite-action nested `uses:` references still resolve. The Trivy action is pinned to a version whose internal `aquasecurity/setup-trivy` reference exists and is itself pinned.

## Reporting vulnerabilities

Until GitHub private vulnerability reporting is enabled for the repository, please open a minimal public issue without sensitive exploit details, or contact the maintainer out-of-band if you have an established channel. Include:

- affected commit/version;
- whether the issue requires untrusted task producer access, worker role compromise, or only normal trusted workload execution;
- reproduction steps with synthetic/non-sensitive S3/SQS/Bucket names;
- suggested mitigation, if known.
