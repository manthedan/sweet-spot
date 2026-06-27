# SweetSpot setup and first run handoff

This guide is for a cold human or agent starting from an existing SweetSpot checkout. It explains how to create a contained local `.sweetspot/` starter bundle, what each generated artifact means, how AWS authentication is represented safely, and how to validate the project before continuing to runtime-first commands.

`SweetSpot init` is local setup only. It does not create AWS queues, buckets, roles, Batch job definitions, container images, Terraform state, deployments, or live AWS checks. Treat the generated files as reviewable starter context for later milestones, not as provisioned infrastructure.

## Quick path from example config to validation

Use the example config when you want repeatable, noninteractive setup:

```bash
sweetspot init --config examples/setup.example.yaml
sweetspot plan --job .sweetspot/job.json --format json
sweetspot doctor project --format json
```

Expected outcome:

- `.sweetspot/` exists and contains the starter bundle described below.
- `sweetspot plan` can read the generated `.sweetspot/job.json` and produce a planning result.
- `sweetspot doctor project --format json` emits a local setup-health report with schema `sweetspot.project.doctor.v1`.

If `.sweetspot/` already exists, `sweetspot init` fails closed rather than silently overwriting generated context. Review the existing bundle first; only use the CLI overwrite option when replacing those local starter files is intentional.

## Config-driven init

`examples/setup.example.yaml` uses schema `sweetspot.project.v1` and declares four kinds of setup intent:

- `project`: a human-readable project name and description.
- `workload`: the S3 input manifest, S3 output prefix, command tokens, and target architecture.
- `aws`: the intended region and an auth reference such as a named profile, SSO, environment-provided auth, or role reference.
- `bootstrap`: the contained `.sweetspot/` output paths for generated starter artifacts.

Run config-driven init when an agent, CI-like local script, or repeatable onboarding path needs deterministic output:

```bash
sweetspot init --config examples/setup.example.yaml
```

The config must be valid YAML, must use `schema: sweetspot.project.v1`, must keep bootstrap paths under `.sweetspot/`, and must not contain secret-looking keys or values. Auth is configuration intent only; keep real credentials in your normal AWS tooling outside this repository.

## Interactive init

Run interactive init when a human wants the CLI to prompt for setup intent:

```bash
sweetspot init
```

The prompts collect the same project, workload, region, architecture, and auth-reference information that the YAML config contains. Interactive init still writes the same contained `.sweetspot/` bundle and follows the same safety rules: no AWS resources are created, no live AWS checks are run, and no credential material belongs in generated files.

## Complete `.sweetspot/` layout

After init, the starter bundle contains these files:

| Path | Role | How to review or customize |
|---|---|---|
| `.sweetspot/sweetspot.yaml` | Canonical local setup config generated from your init inputs. | Confirm project name, workload S3 URIs, command tokens, architecture, region, and auth method/reference are correct. Keep only references to external auth configuration. |
| `.sweetspot/SWEETSPOT.md` | Human/agent summary of setup status, workload intent, AWS auth intent, and bootstrap artifact paths. | Read this first when resuming a project. It should explain that the bundle is initialized but not deployed. |
| `.sweetspot/job.json` | Starter SweetSpot job spec for planning. | Review `run_id`, image placeholder, command, input/output locations, constraints, architecture, region, and validation contract. Use `sweetspot plan --job .sweetspot/job.json --format json` to check planner compatibility. |
| `.sweetspot/deployment.template.json` | Review-only deployment template with placeholder queue, job-definition, image, and SQS values. | Replace placeholders only during later bootstrap work. Do not treat this file as deployable infrastructure after init. |
| `.sweetspot/worker/README.md` | Notes for adapting the worker scaffold to the workload. | Use it to align the starter command, done-marker expectation, and auth-reference boundary before touching worker code. |
| `.sweetspot/worker/worker.py` | Review-only Python worker scaffold that writes a local summary and done marker. | Replace the scaffold body with real workload logic later. Keep AWS auth values outside the worker source. |
| `.sweetspot/infra/terraform.tfvars.json` | Review-only Terraform variable stub. | Treat `ready_for_apply: false` and placeholder TODOs as a hard stop. It is not ready for `terraform apply` after M001 setup. |
| `.sweetspot/next_steps.md` | Generated checklist for reviewing and customizing the starter bundle. | Use it as the next local handoff after init, especially before planning or future bootstrap work. |

## AWS authentication boundary

SweetSpot setup records AWS auth intent as references only:

- profile name
- role ARN reference
- SSO session configured outside SweetSpot
- process environment supplied outside SweetSpot

Do not paste access-key values, secret-key values, session tokens, generated credentials, private keys, passwords, or bearer tokens into YAML, Terraform variable files, worker source, README files, or environment files in this project. `sweetspot init` validates setup data before writing, and `sweetspot doctor project` scans generated artifacts for secret-looking material. If such material appears, treat it as a failure that must be removed rather than suppressed.

## Validate the local project state

### Planner validation

Run the planner against the generated job spec:

```bash
sweetspot plan --job .sweetspot/job.json --format json
```

This verifies that the starter `job.json` still conforms to the planner's job schema. If you customize the job file and planning fails, fix `job.json` before moving on; later runtime commands depend on a planner-compatible job spec.

### Project doctor validation

Run project doctor for an agent-readable local setup report:

```bash
sweetspot doctor project --format json
```

`doctor project` is read-only and local. It accepts either the project root or the `.sweetspot/` directory, resolves the contained bundle, reads files, validates schemas, scans local artifacts, and does not contact AWS.

The practical JSON contract is:

```json
{
  "schema": "sweetspot.project.doctor.v1",
  "ok": true,
  "status": "pass",
  "project_dir": "/path/to/project/.sweetspot",
  "root_dir": "/path/to/project",
  "summary": {
    "checks": {"pass": 4, "warning": 1, "fail": 0},
    "findings": {"error": 0, "warning": 1, "info": 0},
    "total_checks": 5,
    "total_findings": 1
  },
  "checks": []
}
```

Agents should consume these fields:

- `schema`: must be `sweetspot.project.doctor.v1` for this setup-health contract.
- `ok`: `true` means no failure checks were found. Warnings can still be present.
- `status`: aggregate status: `pass`, `warning`, or `fail`.
- `summary`: counts by check status and finding severity.
- `checks`: ordered check entries with `name`, `status`, `path`, and `findings`.

Current check IDs include:

| Check | Meaning | Warning vs failure semantics |
|---|---|---|
| `setup_config` | `.sweetspot/sweetspot.yaml` exists and validates as setup schema `sweetspot.project.v1`. | Missing, corrupt, invalid schema, invalid region, invalid auth reference, invalid S3 URI, or unsafe bootstrap path is a failure. |
| `generated_artifacts` | All expected generated files exist and are regular files. | Missing or non-file artifacts fail closed. |
| `planner_job` | `.sweetspot/job.json` is loadable by the SweetSpot planner. | Planner-incompatible JSON is a failure. |
| `secret_scan` | Generated setup artifacts do not contain secret-looking keys or values. | Any finding is a failure and should be removed from the artifact. Diagnostic messages are sanitized. |
| `placeholder_review` | Review-only placeholders are still present. | Placeholders are warning-tolerant in M001 because deployment bootstrap is future work. Review them before deployment. |

A useful first-run interpretation is: `ok: true` with `status: warning` is acceptable when the only warnings are review placeholders in deployment or infra starter artifacts. `ok: false` means a fail-closed safety finding exists and the bundle should not be used until fixed.

## M002 boundary

M001 setup stops at local project context, starter artifacts, planner compatibility, and local doctor observability. M002 is the boundary for AWS bootstrap work such as creating or wiring queues, buckets, IAM roles, Batch compute environments, Batch job queues, Batch job definitions, container images, Terraform state, or live AWS validation.

Do not infer that init has provisioned infrastructure. It has only captured intent and generated files for review.

## Troubleshooting

### `.sweetspot/` already exists

Symptom: `sweetspot init` reports that SweetSpot project context files already exist.

What to do:

1. Run `sweetspot doctor project --format json` to inspect the existing bundle.
2. Review `.sweetspot/SWEETSPOT.md` and `.sweetspot/next_steps.md` to understand what was generated.
3. If replacement is intentional, rerun init with the CLI overwrite option after confirming the generated files are safe to replace.

### Missing or corrupt setup config

Symptom: `doctor project` reports `setup_config` failure with `missing_setup_config` or `invalid_setup_config`.

What to do:

1. Restore or regenerate `.sweetspot/sweetspot.yaml` from a valid setup config.
2. Ensure the schema is `sweetspot.project.v1`.
3. Ensure workload input/output values are S3 URIs, command is a non-empty token list, architecture is supported, region looks like an AWS region, and auth is a supported reference method.
4. Rerun `sweetspot doctor project --format json`.

### Missing or corrupt generated artifacts

Symptom: `generated_artifacts` fails for a missing path or a path that is not a regular file.

What to do:

1. Compare the bundle to the layout table above.
2. Remove directories or other non-file objects that occupy expected file paths.
3. Regenerate the bundle from valid setup input, or restore the missing generated artifact from source control if it is intentionally tracked.
4. Rerun project doctor.

### Planner-incompatible `job.json`

Symptom: `planner_job` fails or `sweetspot plan --job .sweetspot/job.json --format json` exits with a schema error.

What to do:

1. Inspect `.sweetspot/job.json` for invalid JSON or unsupported fields.
2. Confirm `schema`, `run_id`, `image`, `command`, `input_manifest`, `output_prefix`, `constraints`, and `validation` still match planner expectations.
3. Recreate the starter job spec from setup config if local edits made it invalid.
4. Rerun planner validation and project doctor.

### Placeholder warnings

Symptom: `placeholder_review` returns `warning` findings for TODO, replacement, or placeholder material.

What to do:

- For M001 local setup, this is expected in review-only deployment and infra artifacts.
- Do not remove placeholders by inventing live AWS resource IDs.
- Replace placeholders only when M002 bootstrap work creates or confirms real AWS resources.

### Secret-scan failures

Symptom: `secret_scan` fails with a secret-looking key or value finding.

What to do:

1. Remove the secret-looking material from the reported generated artifact.
2. Replace it with a profile name, role reference, SSO note, or external environment-auth intent as appropriate.
3. Rotate any real credential that was accidentally written.
4. Rerun `sweetspot doctor project --format json` and continue only after `secret_scan` passes.

## Handoff checklist

Before moving from setup into runtime-first commands or future bootstrap work, confirm:

- `sweetspot init` has produced the complete `.sweetspot/` layout.
- `.sweetspot/job.json` passes `sweetspot plan --job .sweetspot/job.json --format json`.
- `sweetspot doctor project --format json` returns schema `sweetspot.project.doctor.v1`.
- `ok` is `true`, or every failure finding has been fixed.
- Any placeholder warnings are understood as review-only M001 artifacts.
- AWS auth remains reference-only and no generated file contains credential material.
- You understand that M002, not M001 init, owns AWS bootstrap and live resource validation.
