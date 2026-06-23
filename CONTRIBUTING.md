# Contributing to SweetSpot

Thank you for helping improve SweetSpot, a cost-aware AWS Batch Spot runner for trusted, idempotent workloads.

## Scope and trust boundary

SweetSpot intentionally treats the SQS queue as a trusted control plane. A producer that can enqueue a task can choose the command run by the worker role. Contributions should preserve this model:

- keep task producers trusted;
- keep tasks idempotent and bind side effects to `task_id` / `task_hash`;
- prefer explicit S3 prefix allow-lists for production paths;
- do not describe SweetSpot as an arbitrary-code sandbox or exactly-once transaction system.

Security-sensitive changes should also update `SECURITY.md` and `docs/reliability_contract.md` when the operator contract changes.

## Local development

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install --constraint requirements-dev.lock -e '.[dev]'

ruff format .
ruff check .
mypy sweetspot
python -m unittest discover -s tests -v
```

`python3` is fine on systems where `python` is not installed. The release verifier auto-detects either command, or you can set `PYTHON_BIN` explicitly:

```bash
PYTHON_BIN=python3 scripts/verify_release.sh
```

## Release and CI checks

Before opening a release PR or tagging, run:

```bash
scripts/verify_release.sh
```

The script runs Python format/lint/type/test checks and, when `tofu` is installed, OpenTofu formatting, initialization, validation, and provider-lock drift checks. It also verifies CI workflow invariants around the single OCI image artifact that is built, scanned, and uploaded.

See `docs/release_checklist.md` for the full release checklist, action-pin policy, and branch-protection recommendations.

## Dependency policy

Runtime and development dependencies are pinned in:

- `requirements.lock`
- `requirements-dev.lock`
- `infra/opentofu/.terraform.lock.hcl`
- `docker/Dockerfile.worker`
- `.github/workflows/ci.yml`

When updating dependencies, update the relevant lock/pin and describe why in the PR. GitHub Actions should remain pinned to full commit SHAs with a comment naming the human-readable version.

## Tests for AWS-facing behavior

Prefer deterministic unit tests with fake S3/SQS/Batch/CloudWatch clients. Live AWS smoke tests are useful, but they should not be required for ordinary CI unless credentials, cost limits, and teardown are explicit.

For worker/finalizer changes, include tests for duplicate delivery, corrupt markers, prefix allow-list behavior, and incomplete-run repair paths where applicable.
