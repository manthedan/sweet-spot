# SweetSpot setup and first run handoff

This guide is for a cold human or agent starting from an installed SweetSpot CLI or an existing checkout. It explains how to create a contained local `.sweetspot/` starter bundle, what each generated artifact means, how AWS authentication is represented safely, and how to validate the project before continuing to runtime-first commands.

`SweetSpot init` is local setup only. It does not create AWS queues, buckets, roles, Batch job definitions, container images, Terraform state, deployments, or live AWS checks. Treat the generated files as reviewable starter context for later milestones, not as provisioned infrastructure.

## Install and first safe success

The recommended install shape is an isolated CLI tool. First success should be local, safe, and fast:

```bash
uv tool install sweetspot-runner
sweetspot init
sweetspot doctor project --format json
```

For a one-shot, npx-like run without persistent install:

```bash
uvx --from sweetspot-runner sweetspot init
```

Until the package is published, use GitHub instead:

```bash
uvx --from git+https://github.com/manthedan/sweet-spot.git sweetspot init
uv tool install git+https://github.com/manthedan/sweet-spot.git
# or: pipx install git+https://github.com/manthedan/sweet-spot.git
```

AWS bootstrap remains a separate explicit stage:

```bash
sweetspot bootstrap plan --format json
sweetspot bootstrap apply --confirm apply:<token> --format json
```

## Quick path from example config to validation

Use the example config when you want repeatable, noninteractive setup. `--project-dir` is the containing project root where `.sweetspot/` will be written; do not pass `.sweetspot` itself.

```bash
sweetspot init --config examples/setup.example.yaml --project-dir .
sweetspot plan .sweetspot/job.json --format json
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
sweetspot init --config examples/setup.example.yaml --project-dir .
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
| `.sweetspot/job.json` | Starter SweetSpot job spec for planning. | Review `run_id`, image placeholder, command, input/output locations, constraints, architecture, region, and validation contract. Use `sweetspot plan .sweetspot/job.json --format json` to check planner compatibility. |
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
sweetspot plan .sweetspot/job.json --format json
```

This verifies that the starter `job.json` still conforms to the planner's job schema. If you customize the job file and planning fails, fix `job.json` before moving on; later runtime commands depend on a planner-compatible job spec.

### Project doctor validation

Run project doctor for an agent-readable local setup report:

```bash
sweetspot doctor project --format json
```

`doctor project` is read-only and local. It accepts either the project root or the `.sweetspot/` directory, resolves the contained bundle, reads files, validates schemas, scans local artifacts, and does not contact AWS.

### Bootstrap doctor validation

Run bootstrap doctor when you want a machine-readable lifecycle report for bootstrap recovery:

```bash
sweetspot doctor bootstrap --format json
```

By default, `sweetspot doctor bootstrap` is artifact-only, read-only, local, and non-mutating. It accepts either the project root or the `.sweetspot/` directory, normalizes that input to the project root, reads existing files, classifies lifecycle state, and returns JSON with schema `sweetspot.bootstrap.doctor.v1`. It does not render a new plan, invoke OpenTofu, run subprocesses, contact AWS, create resources, validate live credentials, probe IAM, write recovery artifacts, or store credential material.

Use the default command first when you need to recover from an interrupted or stale bootstrap flow without making the situation worse. It reads these local evidence files when present:

| Evidence file | What the doctor uses it for |
|---|---|
| `.sweetspot/sweetspot.yaml` and generated setup artifacts | Local setup readiness through the same setup-status surface used by bootstrap planning. |
| `.sweetspot/bootstrap-plan.json` | Reviewed plan presence, schema `sweetspot.bootstrap.plan.v1`, and whether plan status is `ready`. |
| `.sweetspot/bootstrap/state.json` | Last guarded apply state such as `blocked`, `applying`, `failed`, or `output_written`. |
| `.sweetspot/bootstrap/failure.json` | Sanitized failure category, command summaries, messages, and recovery hints from blocked or failed apply attempts. |
| `.sweetspot/deployment.json` | Deployment-output validity through schema `sweetspot.deployment.v1`; this is required before downstream runtime handoff. |

The report includes `classification`, `status`, `exit_code`, `local_status`, ordered `evidence`, `next_actions`, and, only when requested, sanitized `aws_diagnostics`. `exit_code` is `0` for `not_started`, `planned`, and `applied`; it is non-zero for `drift_error` and `missing_permission` so scripts can stop on states that require repair.

Bootstrap doctor classifications:

| Classification | Meaning | Exit semantics | Recovery |
|---|---|---|---|
| `not_started` | No usable local setup or ready bootstrap plan exists yet. `.sweetspot/` may be missing, incomplete, or not ready for bootstrap. | `exit_code: 0`, `status: action_required`. | Run or repair local setup, then rerun `sweetspot bootstrap plan --project-dir .sweetspot --format json`. |
| `planned` | Local setup is ready and `.sweetspot/bootstrap-plan.json` is present with schema `sweetspot.bootstrap.plan.v1` and status `ready`, but guarded apply has not produced valid deployment outputs. | `exit_code: 0`, `status: action_required`. | Review the current plan, use its current confirmation token, and run guarded apply only when mutation is intentional. Regenerate the plan first if setup or generated OpenTofu files are stale. |
| `applied` | Apply state reports `output_written` and `.sweetspot/deployment.json` validates as `sweetspot.deployment.v1`. | `exit_code: 0`, `status: ok`. | Continue to downstream runtime or worker-container work using the validated deployment output. |
| `drift_error` | Local artifacts disagree or are unreadable: malformed plan/state/failure JSON, invalid plan schema, apply state saying `output_written` without a valid deployment output, or another sanitized error evidence item. | Non-zero `exit_code`, `status: error`. | Inspect `evidence` and `next_actions`; fix corrupt files, regenerate the bootstrap plan, or retry from the last safe lifecycle step. Do not invent deployment outputs or rerun mutation commands just to clear the error. |
| `missing_permission` | Guarded apply failure diagnostics or opt-in AWS diagnostics indicate missing AWS/IAM permission. | Non-zero `exit_code`, `status: error`. | Read sanitized `failure.json`, command summaries, AWS diagnostics, and recovery hints; update the user-owned profile, SSO session, role, or IAM policy outside `.sweetspot/`, then retry from a freshly reviewed plan. |

Command safety boundaries:

| Command or action | Safety class | Notes |
|---|---|---|
| `sweetspot init`, `sweetspot plan`, `sweetspot doctor project`, default `sweetspot doctor bootstrap`, and `sweetspot bootstrap plan` without validation | Safe/local. | Reads and writes only contained local review artifacts according to each command's contract; no live AWS calls or resource mutation. |
| `sweetspot bootstrap plan --validate` | Local subprocess validation. | May run local `tofu init -backend=false` and `tofu validate`; still does not apply resources or contact AWS backends. |
| `sweetspot doctor bootstrap --check-aws --format json` | Live/read-only and opt-in. | May construct a boto3 session and call STS/IAM diagnostics. It returns sanitized JSON and does not mutate AWS. |
| `sweetspot bootstrap apply --format json --confirm apply:<first-16-sha256>` | Guarded mutation. | May call OpenTofu apply and create or update AWS resources only after the reviewed plan and exact confirmation token pass the guard. |

AWS credentials remain user-owned at all times. Use normal AWS profile, environment, SSO, or role-reference mechanisms outside this repository. Do not store access keys, secret keys, session tokens, generated credentials, private keys, bearer tokens, raw profile credentials, or unredacted AWS errors in `.sweetspot/` files, logs, docs, worker code, Terraform variables, or repository-local environment files.

Common bootstrap-doctor recovery flows:

- **Stale plan:** if `classification` is `planned` but the plan identity, generated artifacts, setup intent, or confirmation token no longer matches what the operator reviewed, discard the stale review decision, rerun `sweetspot bootstrap plan --project-dir .sweetspot --format json`, review the new `.sweetspot/bootstrap-plan.json`, and use only the new confirmation token for guarded apply.
- **Failed apply:** if `local_status.apply` is `failed` or `classification` is `missing_permission`/`drift_error`, inspect `.sweetspot/bootstrap/failure.json`, `evidence`, and `next_actions` first. Fix the named OpenTofu, AWS permission, or output-extraction problem before retrying; rerun from a freshly reviewed plan rather than invoking OpenTofu directly.
- **Missing permissions:** keep the fix outside `.sweetspot/` by updating the referenced AWS profile, SSO session, role, or IAM policy. Then rerun default `sweetspot doctor bootstrap --format json`; use `--check-aws` only when a live read-only confirmation is intentional.
- **Invalid deployment output:** if apply state says `output_written` but `.sweetspot/deployment.json` is missing or invalid, treat the project as not handed off. Repair by rerunning the guarded apply/output-extraction path from a current reviewed plan; do not create or edit deployment output JSON by hand just to satisfy runtime consumers.

M003 worker-container work may proceed only when the doctor reports `classification: "applied"` with a valid `.sweetspot/deployment.json` using schema `sweetspot.deployment.v1`. That deployment registry must include bootstrap outputs needed by runtime and worker-container code, including queue/job-definition/image values and worker task role data such as `worker_task_role_arn`.

### Bootstrap plan review

Run bootstrap plan when you want a versioned, machine-readable OpenTofu-backed review artifact from the same local bootstrap intent:

```bash
sweetspot bootstrap plan --project-dir .sweetspot --format json
```

The command accepts either the generated `.sweetspot/` directory or the containing project root. It writes `.sweetspot/bootstrap-plan.json` and returns the same JSON report on stdout; guarded apply consumes that exact path, so alternate `--out` paths are rejected for this lifecycle.

`bootstrap plan` is a review-before-apply surface. It renders deterministic OpenTofu configuration files and a deployment-output template, but it does not run `tofu apply`, create AWS resources, create Terraform/OpenTofu state, build or push images, write deployment outputs, contact AWS, or store secrets. Treat the generated files as exact starter infrastructure intent to review before guarded apply, not as provisioned infrastructure. The generated OpenTofu is a single-account Spot starter; production deployments should still review lane topology, IAM scope, budgets, alarms, and capacity limits before use. The starter worker task role scopes S3 access to the reviewed `input_prefix` and `output_prefix`; widen those prefixes only when the workload command demonstrably needs additional trusted inputs or outputs.

Generated plan output includes:

| Artifact | Role | Review notes |
|---|---|---|
| `.sweetspot/bootstrap-plan.json` | Versioned plan report with `schema: sweetspot.bootstrap.plan.v1`. | Review `status`, `findings`, `resource_inventory`, `generated_artifacts`, `command_attempts`, `stderr_summary`, and `next_actions` before any later mutation work. |
| `.sweetspot/infra/versions.tf` | Provider and Terraform/OpenTofu version constraints for the starter bootstrap. | Review provider source/version before validation or apply. |
| `.sweetspot/infra/variables.tf` | Variable schema for the starter bootstrap. | Review which values remain placeholders before apply. |
| `.sweetspot/infra/main.tf` | Deterministic OpenTofu configuration for IAM roles, Batch Spot compute environment, Batch queue, Batch job definition, SQS queue, ECR repository, S3 bucket references, CloudWatch logs, and outputs. | Review resource names, tags, region, IAM trust/policy shape, and Spot settings. This file is not applied by `bootstrap plan`. |
| `.sweetspot/infra/outputs.tf` | Deployment output definitions consumed by guarded apply. | Review output names before accepting generated deployment output. |
| `.sweetspot/infra/terraform.tfvars.json` | Sanitized variable values derived from setup intent. | It must not contain access keys, secret keys, session tokens, passwords, private keys, or bearer tokens. Auth remains reference-only. |
| `.sweetspot/deployment.template.json` | Review-only deployment-output skeleton using schema `sweetspot.deployment.v1`. | Guarded apply owns writing real `.sweetspot/deployment.json`; do not use this skeleton as proof that resources exist. |

Starter apply operator permissions should be reviewed as an explicit allow list before any human runs OpenTofu. At minimum the applying principal needs the Terraform state/backend permissions you choose plus the create/describe/update permissions for the generated Batch, SQS, ECR, CloudWatch Logs, EC2 networking lookups, IAM role/profile/policy attachments, and S3 object-prefix checks. Keep `iam:PassRole` scoped to only the generated Batch service role, ECS instance role/profile role, Spot Fleet role, and worker task role, with an `iam:PassedToService` condition limited to the AWS services that consume them (`batch.amazonaws.com`, `ec2.amazonaws.com`, `ecs-tasks.amazonaws.com`, and `spotfleet.amazonaws.com`). Do not grant broad `iam:PassRole` or bucket-wide S3 access just to make the starter apply pass. The PassRole statement should look like this shape after replacing account/project placeholders with the reviewed generated names:

```json
{
  "Effect": "Allow",
  "Action": "iam:PassRole",
  "Resource": [
    "arn:aws:iam::<ACCOUNT_ID>:role/<project>-<arch>-compute-batch-service-role",
    "arn:aws:iam::<ACCOUNT_ID>:role/<project>-<arch>-compute-ecs-instance-role",
    "arn:aws:iam::<ACCOUNT_ID>:role/<project>-<arch>-compute-spot-fleet-role",
    "arn:aws:iam::<ACCOUNT_ID>:role/<project>-worker-task-role"
  ],
  "Condition": {
    "StringEquals": {
      "iam:PassedToService": [
        "batch.amazonaws.com",
        "ec2.amazonaws.com",
        "ecs-tasks.amazonaws.com",
        "spotfleet.amazonaws.com"
      ]
    }
  }
}
```

Plan report statuses are intentionally reviewable even when setup is not yet deployable:

| Status | Meaning | Recovery |
|---|---|---|
| `ready` | Local setup intent is sufficient to render the OpenTofu review files. | Review generated artifacts and keep them as handoff input for S04. If `--validate` was used, also review the OpenTofu validation status. |
| `incomplete` | Required bootstrap inputs are missing or fixable, such as missing local setup fields or placeholder deployment values. | Fix `.sweetspot/sweetspot.yaml` through setup input or regenerate local setup, then rerun `sweetspot bootstrap plan`. The artifact is still useful for seeing what is missing. |
| `invalid` | The setup bundle or output target is not safe to turn into a plan, such as corrupt config, unsafe output paths, or artifact write failures. | Fix the reported finding before continuing. Do not hand the artifact to S04 until the report is no longer invalid. |

OpenTofu validation is optional and local:

```bash
sweetspot bootstrap plan --project-dir . --validate --format json
```

Without `--validate`, the report records OpenTofu status as not requested. With `--validate`, SweetSpot may run local `tofu init -backend=false` and `tofu validate` against the generated directory. If `tofu` is unavailable, install OpenTofu or rerun without validation and document that validation was not performed. If validation fails, review the sanitized `command_attempts`, `stderr_summary`, and `next_actions` fields, fix the generated contract or setup intent, and rerun. Validation failure is a local review finding, not a reason to run apply manually from SweetSpot-generated directories.

### Guarded bootstrap apply

Run guarded apply only after the bootstrap plan has been generated and reviewed. This is the first bootstrap command in this lifecycle that may mutate AWS, so it fails closed unless the reviewed plan artifact is present, has `status: "ready"`, has no blocking findings, includes the generated OpenTofu artifacts, and the caller supplies the exact confirmation token derived from the reviewed plan bytes.

Minimal flow:

```bash
sweetspot bootstrap plan --project-dir .sweetspot --format json
# Review .sweetspot/bootstrap-plan.json, including findings, generated_artifacts,
# resource_inventory, expected_deployment, and reviewed_plan.confirmation_token.
sweetspot bootstrap apply --format json --confirm apply:<first-16-sha256>
```

Use the current confirmation token from the reviewed plan identity. The token format is `apply:<first-16-sha256>`, and changing `.sweetspot/bootstrap-plan.json` changes the expected token. Do not reuse a token copied from an older plan.

`bootstrap apply` writes machine-readable state for cold-start recovery:

| Artifact | Written when | How to use it |
|---|---|---|
| `.sweetspot/bootstrap/state.json` | Every guarded apply attempt, including blocked, applying, failed, and output-written states. | Inspect `schema`, `status`, `category`, `message`, `reviewed_plan`, `confirmation`, `output_completeness`, `command_summaries`, and `recovery_hints` before retrying. |
| `.sweetspot/bootstrap/failure.json` | Blocked or failed attempts. | Use the sanitized `category`, `message`, command summaries, and recovery hints to fix the refusal or failure without rerunning a mutating command first. |
| `.sweetspot/deployment.json` | Only after OpenTofu apply and output extraction both succeed. | Runtime commands load this `sweetspot.deployment.v1` registry. Treat its absence as evidence that deployment outputs were not completed. |

Apply statuses are intentionally explicit:

| Status | Meaning | Recovery |
|---|---|---|
| `blocked` | The guard refused to call OpenTofu apply. Common categories include `missing_reviewed_plan`, `invalid_reviewed_plan`, `reviewed_plan_not_ready`, `blocking_plan_finding`, `missing_generated_artifact`, `confirmation_missing`, and `confirmation_mismatched`. | Fix the reviewed plan, generated artifact, or confirmation token named in `state.json`/`failure.json`, then rerun from the plan-review step. |
| `applying` | The guard passed and the OpenTofu apply runner has been invoked, but deployment outputs have not yet been written. | If this is the last persisted state after an interruption, inspect local OpenTofu state outside SweetSpot's JSON summaries before retrying, then rerun only with a freshly reviewed plan and exact token. |
| `failed` | OpenTofu apply or output extraction failed after the guard passed. Categories include `missing_permission`, `apply_failed`, and `output_extraction_failed`. | Read sanitized `command_summaries` and `recovery_hints`; fix AWS/OpenTofu permissions, command failure, or missing outputs before retrying. |
| `output_written` | Apply succeeded and `.sweetspot/deployment.json` was written from complete OpenTofu outputs. | Continue to runtime validation using the deployment registry. |

Live AWS apply requires user-owned AWS credentials configured outside `.sweetspot/`, such as AWS CLI profiles, SSO, role assumption, or environment credentials supplied by the user's shell. Prefer short-lived credentials. Never paste access keys, secret keys, session tokens, bearer tokens, private keys, passwords, or generated credential files into `.sweetspot/` artifacts, docs, worker code, Terraform variables, or repository-local environment files.

The regression tests use mocked OpenTofu runners and mocked AWS diagnostics so the guarded contract can be verified without live credentials or resource mutation. S05 will expand doctor and recovery documentation that classifies these persisted state/failure artifacts for operators and agents.

Use the AWS diagnostics mode only when a human or agent explicitly wants a live, read-only AWS check:

```bash
sweetspot doctor bootstrap --check-aws --format json
```

`--check-aws` is opt-in because it may construct a boto3 session and call AWS. It performs only read-only diagnostics: STS `GetCallerIdentity` and best-effort IAM `SimulatePrincipalPolicy`. It does not create queues, buckets, roles, Batch resources, container images, Terraform state, deployments, or write credential material. The diagnostics path is injectable and covered by stdlib unittest mocks, so the contract can be tested without live AWS credentials.

When enabled, the bootstrap report includes an `aws_diagnostics` object with schema `sweetspot.bootstrap.aws_diagnostics.v1`:

```json
{
  "schema": "sweetspot.bootstrap.aws_diagnostics.v1",
  "ok": true,
  "status": "warning",
  "region": "us-west-2",
  "auth": {
    "method": "profile",
    "reference": "[REDACTED_AUTH_REFERENCE]",
    "supported": true
  },
  "caller_identity": {
    "account": "[REDACTED_ACCOUNT_ID]",
    "arn": "[REDACTED_ARN]",
    "user_id": "[REDACTED_USER_ID]"
  },
  "checks": [
    {
      "name": "iam_simulate_principal_policy",
      "status": "warn",
      "severity": "warning",
      "details": {
        "classification": "simulation_unavailable",
        "missing_permission": "iam:SimulatePrincipalPolicy"
      },
      "error": {
        "classification": "simulation_unavailable",
        "type": "AccessDenied",
        "message": "[REDACTED_AWS_ERROR]"
      }
    }
  ],
  "required_actions": ["sts:GetCallerIdentity", "iam:SimulatePrincipalPolicy"],
  "redactions": ["account_id", "arn", "aws_error_message"]
}
```

AWS diagnostics fields are designed for cold agents to triage from JSON alone:

- `schema`: must be `sweetspot.bootstrap.aws_diagnostics.v1` for the live read-only diagnostics contract.
- `ok`: `true` means no failing diagnostics check remains. Warnings can still be present.
- `status`: aggregate status: `ready` means identity and permission simulation passed, `warning` means the local intent and caller identity are usable but a non-blocking permission or simulation caveat exists, and `blocked` means AWS readiness cannot be established.
- `region`: the intended AWS region from setup intent, sanitized before output.
- `auth.method`: one of the supported reference methods (`env`, `profile`, `sso`) when configured.
- `auth.reference`: always redacted when present. Store and rotate credentials in AWS-supported tooling outside the repository; keep `.sweetspot/` files reference-only.
- `auth.supported`: `false` means the configured auth method is not one of the supported reference methods.
- `caller_identity`: sanitized STS identity. Account IDs, ARNs, and user IDs are redacted even on success.
- `checks`: ordered diagnostic checks. Each check has `name`, `status`, `severity`, sanitized `details`, and, for failures or AWS exceptions, sanitized `error`.
- `required_actions`: the action names evaluated by the IAM simulation. This list is operational intent only; it is not a permission grant.
- `redactions`: marker names explaining what sensitive categories were removed from the report.

Current AWS diagnostic checks include:

| Check | Meaning | Status and recovery semantics |
|---|---|---|
| `region` | Confirms setup intent includes an AWS region. | `fail` with `missing_region` blocks diagnostics. Add a valid region to `.sweetspot/sweetspot.yaml` through setup input, then rerun. |
| `auth` | Confirms auth intent is configured, supported, and can construct a session. | `fail` classifications include `missing_auth_method`, `unsupported_auth`, `incomplete_auth`, and `profile_not_found`. Use `env`, `profile`, or `sso` references; configure the named profile or SSO session in normal AWS tooling outside the repo. |
| `sts_get_caller_identity` | Calls STS to identify the effective caller. | `pass` means identity is available. `fail` classifications include `missing_credentials`, `partial_credentials`, `access_denied`, `throttled`, `endpoint_unavailable`, `client_error`, and `unknown_exception`. Fix AWS credential source, network/endpoint access, or caller policy, then rerun. |
| `iam_simulate_principal_policy` | Best-effort IAM simulation for the bootstrap action set. | `pass` with `simulation_allowed` means all simulated actions were allowed. `warn` with `simulation_denied` means one or more required actions appear denied. `warn` with `simulation_unavailable` usually means the caller cannot call `iam:SimulatePrincipalPolicy`; grant that diagnostic permission to improve the report or manually review required actions. `skipped` with `simulation_skipped` means the caller identity did not include a principal ARN to simulate. |

Permission simulation is a diagnostic hint, not a deployment guarantee. IAM simulation can be unavailable, can be denied to otherwise valid operators, and may not model every resource condition or future bootstrap action. Treat `status: warning` as "needs review" rather than "ready to deploy" when simulation is denied, skipped, or unavailable.

Redaction is fail-closed: account IDs, ARNs, AWS access key IDs, secret-like strings, request IDs, profile or role names, principal user IDs, and raw AWS error messages are replaced before JSON is returned. If a report contains unexpected sensitive material, treat that as a defect and remove the source material rather than storing it in `.sweetspot/` files.

The project doctor JSON contract is:

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

M001 setup stops at local project context, starter artifacts, planner compatibility, and local doctor observability. M002 adds explicit read-only AWS diagnostics and OpenTofu plan rendering paths for bootstrap review, but provisioning remains future bootstrap work. Creating or wiring queues, buckets, IAM roles, Batch compute environments, Batch job queues, Batch job definitions, container images, Terraform/OpenTofu state, deployment outputs, or deployments is still not performed by init, project doctor, default bootstrap doctor, `doctor bootstrap --check-aws`, or `bootstrap plan`.

Do not infer that init, diagnostics, or plan rendering have provisioned infrastructure. They have only captured intent, generated files for review, and optionally reported sanitized live AWS identity/permission signals. S03 stops at a reviewable plan artifact; S04 owns guarded mutation, apply controls, deployment output writing, and any proof that live resources were created.

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

Symptom: `planner_job` fails or `sweetspot plan .sweetspot/job.json --format json` exits with a schema error.

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

### Bootstrap plan incomplete or invalid

Symptom: `sweetspot bootstrap plan --format json` reports `status: "incomplete"` or `status: "invalid"`.

What to do:

1. Read `findings` and `next_actions` in `.sweetspot/bootstrap-plan.json` or stdout.
2. For `incomplete`, fix missing local setup intent in `.sweetspot/sweetspot.yaml` through setup input or regenerate the bundle from valid config, then rerun the plan command.
3. For `invalid`, fix unsafe paths, corrupt config, or artifact write problems before continuing.
4. Do not run OpenTofu apply or invent deployment outputs to bypass the status; S04 owns guarded mutation after the review artifact is valid.

### OpenTofu unavailable or validation failed

Symptom: `sweetspot bootstrap plan --validate --format json` reports an OpenTofu status such as executable unavailable, init failed, validation failed, or timed out.

What to do:

1. Confirm the local `tofu` executable is installed and on PATH, or provide `--tofu-executable` for a local validation smoke.
2. Inspect `command_attempts`, `stderr_summary`, and `next_actions`; stderr is summarized and sanitized for review.
3. Fix setup intent or generated-contract defects and rerun `sweetspot bootstrap plan --validate --format json`.
4. If OpenTofu is unavailable on the machine, rerun without `--validate` and keep the rendered plan as review input; CI and user machines are not required to have OpenTofu installed.
5. Never use a failed validation as a reason to run `tofu apply` manually from the generated directory.

### AWS diagnostics not configured

Symptom: `doctor bootstrap --check-aws --format json` includes `aws_diagnostics.status: "blocked"` with `missing_region`, `missing_auth_method`, `unsupported_auth`, or `incomplete_auth`.

What to do:

1. Keep real credential values out of `.sweetspot/` files.
2. Update setup intent to use a supported auth reference: `env`, `profile`, or `sso`.
3. For `profile` or `sso`, configure the named source with AWS CLI or SSO tooling outside this repository.
4. Rerun default `sweetspot doctor bootstrap --format json` first, then rerun `--check-aws` only when a live read-only check is intentional.

### AWS credentials missing or profile not found

Symptom: `sts_get_caller_identity` fails with `missing_credentials`, `partial_credentials`, or `profile_not_found`.

What to do:

1. Configure the referenced profile, SSO session, or environment credentials in normal AWS tooling outside SweetSpot.
2. Prefer short-lived credentials, SSO, or role assumption over static access keys.
3. Do not copy access keys, secret keys, or session tokens into `.sweetspot/sweetspot.yaml`, `.sweetspot/infra/terraform.tfvars.json`, worker files, docs, or local environment files in this project.
4. Rerun `doctor bootstrap --check-aws --format json` and confirm the report remains redacted.

### AWS identity access denied, throttled, or unreachable

Symptom: `sts_get_caller_identity` fails with `access_denied`, `throttled`, `endpoint_unavailable`, `client_error`, or `unknown_exception`.

What to do:

1. Confirm the selected AWS caller is allowed to call `sts:GetCallerIdentity`.
2. Confirm local network, proxy, endpoint, and region configuration allow STS calls.
3. Retry throttled diagnostics later; the command is read-only and safe to rerun.
4. Use the sanitized `classification`, `type`, and `redactions` fields for triage instead of enabling unsafe debug logs.

### AWS permission simulation denied or unavailable

Symptom: `iam_simulate_principal_policy` returns `simulation_denied`, `simulation_unavailable`, or `simulation_skipped`.

What to do:

1. Review `required_actions` in the diagnostics report to see which bootstrap actions were evaluated.
2. For `simulation_denied`, inspect the per-action `evaluations` and update the caller's intended bootstrap permissions before provisioning work.
3. For `simulation_unavailable`, either grant `iam:SimulatePrincipalPolicy` for diagnostics or manually review the listed required actions.
4. For `simulation_skipped`, rerun with an auth method that yields a principal ARN if IAM simulation is required.
5. Treat the warning as a review gate, not as proof that provisioning will succeed or fail.

## Handoff checklist

Before moving from setup into runtime-first commands or future bootstrap work, confirm:

- `sweetspot init` has produced the complete `.sweetspot/` layout.
- `.sweetspot/job.json` passes `sweetspot plan .sweetspot/job.json --format json`.
- `sweetspot doctor project --format json` returns schema `sweetspot.project.doctor.v1`.
- `ok` is `true`, or every failure finding has been fixed.
- Any placeholder warnings are understood as review-only M001 artifacts.
- AWS auth remains reference-only and no generated file contains credential material.
- `sweetspot bootstrap plan --project-dir .sweetspot --format json` has produced a reviewable `.sweetspot/bootstrap-plan.json` when preparing the bootstrap handoff.
- The plan report is `ready`, or every `incomplete`/`invalid` finding has an explicit recovery note.
- Generated OpenTofu and deployment plan artifacts have been reviewed as intent only, with no apply, state, resource creation, credential storage, or deployment output writing performed.
- Optional `doctor bootstrap --check-aws` diagnostics were run only when live read-only AWS checks were intentional.
- Default `sweetspot doctor bootstrap --format json` classifies the lifecycle state, and any `drift_error` or `missing_permission` evidence has been recovered before runtime handoff.
- Before M003 worker-container work, bootstrap doctor reports `classification: "applied"` and `.sweetspot/deployment.json` validates as `sweetspot.deployment.v1` with bootstrap outputs such as Batch queue/job definition/image references and worker task role data.
- You understand that S03 owns reviewable bootstrap plan artifacts, S04 owns guarded apply, live resource mutation, and deployment output writing, and M003 consumes only the validated deployment registry produced by that lifecycle.
