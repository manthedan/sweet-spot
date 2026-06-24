# SweetSpot CLI and UX Audit (Re-audit)

> Re-audited after commit `590fee6` which addresses the original findings.
> Original audit items are marked with their resolution status.

## Resolution summary

| Original item | Status | Commit(s) |
|---|---|---|
| A. No help epilog/examples | **RESOLVED** | `3b3bf58` |
| B. No --output-format option | **PARTIALLY RESOLVED** | `5fefa27` (status only) |
| C. Three separate entry points | **RESOLVED** | `3381903` |
| D. No version/status command | **RESOLVED** | `3790549`, `8b061c2` |
| E. No --profile/--region on AWS commands | **RESOLVED** | `4297dd5` |
| F. Argument duplication | **NOT ADDRESSED** | - |
| G. No config file support | **RESOLVED** | `6b4cfb8` |
| H. finalize --dry-run | **NOT ADDRESSED** | - |
| I. No progress output | **NOT ADDRESSED** | - |
| J. No sweetspot init | **NOT ADDRESSED** | - |
| K. logs --limit/--tail confusion | **RESOLVED** | `590fee6` |
| L. No cancel/drain | **NOT ADDRESSED** | - |
| M. Mixed SystemExit/RuntimeError | **RESOLVED** | `b336960` |
| N. No shell completion | **NOT ADDRESSED** | - |
| O. 2226-line cli.py monolith | **WORSENED** (now 2692 lines) | - |
| P. Inconsistent queue URL flag | **RESOLVED** | `2e7126d` |
| (New) Repair-plan log scanning | **IMPROVED** | `4999b0a` |
| (New) Enqueue-and-submit sizing | **IMPROVED** | `4999b0a` |

## 1. CLI Surface Overview

Three entry points (`pyproject.scripts`):

- `sweetspot` - the main CLI (now 2692 lines, 22 subcommands including `version`, `status`, `scout`, `lane-manager`)
- `sweetspot-scout` - Spot pool ranking tool (`scout.py`, 533 lines), also available as `sweetspot scout`
- `sweetspot-lane-manager` - multi-region lane allocator (`lane_manager.py`, 224 lines), also available as `sweetspot lane-manager`

22 subcommands under `sweetspot`:

| Command | Purpose |
|---|---|
| `version` | Print installed package version |
| `status` | Show AWS identity, queue depth, DLQ depth, active workers |
| `scout` | Rank Spot pools (forwards to sweetspot-scout) |
| `lane-manager` | Multi-lane allocation (forwards to sweetspot-lane-manager) |
| `worker` | Run an SQS worker inside AWS Batch |
| `enqueue-jsonl` | Validate and optionally submit tasks to SQS |
| `enqueue-and-submit` | Atomic enqueue + wait-for-visible + submit workers |
| `derive-canary` | Derive a deterministic canary subset |
| `submit-workers` | Size and submit Batch workers (dry-run by default) |
| `supervise-workers` | Multi-loop bounded worker pool supervisor |
| `finalize` | Stream tasks, check done markers, write manifests |
| `repair-plan` | Build repair JSONL excluding active-worker tasks |
| `cleanup-stale-messages` | Dry-run/apply deletion of stale SQS messages |
| `estimate-runtime` | Estimate wall time/cost from telemetry |
| `describe-job` | Inspect a Batch job |
| `jobs` | List/filter Batch jobs |
| `logs` | Fetch CloudWatch logs for a job/stream |
| `watch-job` | Poll a Batch job until terminal |
| `s3-delete-prefix` | Dry-run/apply bulk S3 prefix deletion |
| `dlq` | Inspect/redrive DLQ messages |
| `doctor` | Preflight AWS prerequisites |

---

## 2. Strengths

1. **Dry-run-by-default culture**: `submit-workers`, `supervise-workers`, `s3-delete-prefix`, `dlq`, `cleanup-stale-messages` all require explicit `--submit`/`--apply`/`--delete`. Excellent UX for a destructive-operations CLI.

2. **Structured JSON output everywhere**: Every command emits a versioned JSON schema (`sweetspot.*.v1`). Makes the CLI scriptable and machine-parseable.

3. **Environment variable fallbacks**: Queue URLs, S3 prefixes, timeouts, etc. all have `SWEETSPOT_*` env var defaults. Good for container/ECS use.

4. **Well-considered safety model**: `--confirm-prefix` for S3 deletion, `min-prefix-chars` guard, reserved env namespaces, S3 prefix allow-listing, conditional done-marker writes.

5. **Comprehensive production loop**: The canary -> estimate -> enqueue-and-submit -> supervise -> finalize -> repair-plan -> cleanup-stale-messages workflow is a well-thought-out production lifecycle.

---

## 3. Usability Issues and Improvement Recommendations

### A. No `--help` epilog or examples in argparse

Every subparser is created with minimal `help=` strings. There are no usage examples in `--help` output. The README has a good "CLI quickstart" but `sweetspot enqueue-jsonl --help` won't show you the canary -> finalize workflow.

**Recommendation**: Add `epilog=` with usage examples to key subparsers, or add a `sweetspot workflow` command that prints the recommended production loop.

### B. No `--output-format` option (JSON-only)

All output is JSON. For interactive terminal use, a human-readable table format would be much better for `jobs`, `dlq`, `describe-job`, `doctor`, `finalize` summary. JSON is great for scripting but hostile for quick operator checks.

**Recommendation**: Add `--format json|table|yaml` (or at least `--quiet`/`--summary-only`) to read-oriented commands (`jobs`, `dlq`, `describe-job`, `doctor`, `logs`, `watch-job`).

### C. Three separate entry points create confusion

`sweetspot`, `sweetspot-scout`, and `sweetspot-lane-manager` are three binaries. An operator has to know which binary to call. The README documents them separately.

**Recommendation**: Make `scout` and `lane-manager` subcommands of `sweetspot` (e.g., `sweetspot scout ...`, `sweetspot lane-manager ...`). Keep the standalone entry points as backwards-compatible shims.

### D. No `sweetspot version` or `sweetspot status` command

There's no way to check the installed version from the CLI, and no single command that shows "what's my current AWS identity, region, queue depth, active workers, DLQ depth" in one shot.

**Recommendation**: Add `sweetspot version` (reads from `importlib.metadata`). Add `sweetspot status` that calls `sts:GetCallerIdentity` + queue depth + active worker count.

### E. No `--profile`/`--region` on several commands

`enqueue-jsonl`, `enqueue-and-submit`, `submit-workers`, `finalize`, `cleanup-stale-messages`, and `dlq` all call `boto3.client(...)` without profile/region. If an operator uses named profiles (common for multi-account), these commands silently use the default chain.

**Recommendation**: Add `--profile` and `--region` to every command that makes AWS calls, or document that these commands rely on the default credential chain and env vars.

### F. Massive argument duplication across submit-workers/supervise-workers/enqueue-and-submit

These three commands share ~20 identical arguments (`--visibility-timeout`, `--heartbeat-seconds`, `--vcpus`, `--memory`, `--env`, `--allowed-s3-prefix`, `--redact-regex`, etc.). This is ~120 lines of repeated `add_argument` calls.

**Recommendation**: Extract a shared argument group function (e.g., `_add_worker_args(p)`) to reduce duplication and prevent drift.

### G. No config file support

For repeated operations with the same queue URL, job queue, job definition, S3 prefixes, etc., there's no way to save defaults. Every invocation needs 8-15 flags.

**Recommendation**: Support `--config config.json` or `~/.sweetspot/config.toml` that pre-populates defaults. This would dramatically improve operator ergonomics.

### H. `finalize` has no `--dry-run` mode

Every other mutating command has dry-run semantics, but `finalize --upload` immediately writes manifests to S3. An operator can't preview what finalize would do without running it (which writes local files at minimum).

**Recommendation**: Add `--dry-run` to `finalize` that validates tasks and checks done markers but doesn't write manifests or upload.

### I. No progress output during long operations

`finalize` has `--progress-interval` (default 1000) but only prints to stderr. `supervise-workers` writes to JSONL but prints nothing to stdout per loop. For long-running operations, there's no visual feedback.

**Recommendation**: Add optional progress bars or at least periodic single-line status updates for `finalize`, `supervise-workers`, `s3-delete-prefix`.

### J. No `sweetspot init` or guided setup

A new user has to read the README, create infra with OpenTofu, build a Docker image, create a tasks.jsonl, and then know which commands to chain. There's no bootstrap helper.

**Recommendation**: Add `sweetspot init` that prompts for queue URL, job queue, job definition, S3 bucket, and generates a config file + example tasks.jsonl.

### K. `logs` command has confusing `--limit` + `--tail` interaction

`--limit` controls how many events to fetch from CloudWatch, then `--tail` slices the last N from that. If both are set, behavior is non-obvious. The naming is inconsistent with the `--tail 50` idiom (which usually means "follow").

**Recommendation**: Rename to `--max-events` and `--last N`, or document the interaction clearly. Consider a `--follow` mode for live tailing.

### L. No `sweetspot cancel` or `sweetspot drain` command

There's no way to cancel in-flight Batch jobs or drain a queue gracefully from the CLI. An operator has to use the AWS Console or raw `aws batch terminate-job` commands.

**Recommendation**: Add `sweetspot cancel-jobs --job-queue X --name-regex Y` and `sweetspot drain-queue --queue-url X`.

### M. Error messages mix `SystemExit` and `RuntimeError`

Some errors raise `SystemExit(msg)` (good, clean exit), others raise `RuntimeError` (uncaught, stack trace). For example, `_send_tasks_to_sqs` raises `RuntimeError` on SQS batch failures.

**Recommendation**: Standardize on `SystemExit` for all operator-facing errors, or catch `RuntimeError`/`ClientError` at the top level and print clean messages.

### N. No shell completion

No `sweetspot completion --shell bash` command to generate shell completions for the 18 subcommands and their flags.

**Recommendation**: If migrating to a framework like `click` or `typer`, this comes for free. With argparse, libraries like `argcomplete` can be used.

### O. 2226-line `cli.py` monolith

The CLI, finalizer logic, repair logic, S3 cleanup logic, doctor checks, and all argparse setup are in a single 2226-line file. This makes the code hard to navigate and test.

**Recommendation**: Split into `cli/` package: `cli/__init__.py` (entry point), `cli/args.py` (argparse setup), `cli/enqueue.py`, `cli/finalize.py`, `cli/jobs.py`, `cli/dlq.py`, `cli/doctor.py`, etc.

### P. No `--queue` alias for `--queue-url` / `--sqs-queue-url`

Different commands use different flag names for the SQS queue URL: `enqueue-jsonl` uses `--queue-url`, `submit-workers` uses `--sqs-queue-url`, `finalize` doesn't need it. This inconsistency makes muscle memory impossible.

**Recommendation**: Standardize on `--queue-url` everywhere, with `--sqs-queue-url` as a deprecated alias.

---

## 4. Feature Improvement Recommendations

| Priority | Feature | Rationale |
|---|---|---|
| High | `sweetspot version` | Basic CLI expectation |
| High | `sweetspot status` | One-shot operational overview |
| High | Config file support (`--config`) | Eliminates flag fatigue for repeated ops |
| High | `--format table` for read commands | Interactive usability |
| High | `sweetspot scout` / `lane-manager` as subcommands | Single entry point |
| Medium | `sweetspot init` guided setup | Onboarding |
| Medium | `--profile`/`--region` everywhere | Multi-account support |
| Medium | `finalize --dry-run` | Safety parity with other commands |
| Medium | `sweetspot cancel-jobs` / `drain-queue` | Operational completeness |
| Medium | `sweetspot logs --follow` | Live log tailing |
| Medium | Shell completion | Power-user UX |
| Low | Migrate to `click`/`typer` | Easier subcommand composition, help text, completion |
| Low | Split `cli.py` into package | Maintainability |
| Low | `sweetspot workflow` guide | Embedded workflow docs |

---

## 5. Re-audit: Detailed findings per item

### A. Help epilog/examples -- RESOLVED

`_add_parser_with_examples()` helper added, used on 10 high-traffic subcommands (`enqueue-jsonl`, `enqueue-and-submit`, `derive-canary`, `submit-workers`, `supervise-workers`, `finalize`, `repair-plan`, `cleanup-stale-messages`, `estimate-runtime`, `doctor`, `status`). Each now shows a concrete usage example in `--help` output. Test `test_high_traffic_help_includes_examples` verifies the epilogs are present.

### B. --output-format -- PARTIALLY RESOLVED

`status` now supports `--format json|table`. The table output is clean and readable (`_print_status_table`). However, other read-oriented commands (`jobs`, `dlq`, `describe-job`, `doctor`, `logs`) are still JSON-only.

**Remaining**: Extend `--format table` to `jobs`, `dlq`, `doctor`, and `describe-job`.

### C. Three separate entry points -- RESOLVED

`sweetspot scout` and `sweetspot lane-manager` now forward to the respective `main()` functions. The standalone `sweetspot-scout` and `sweetspot-lane-manager` entry points are preserved for backwards compatibility. The `scout.main()` and `lane_manager.main()` functions accept `argv` and `prog` parameters for clean forwarding. Tests verify the forwarding works.

### D. version/status command -- RESOLVED

`sweetspot version` reads from `importlib.metadata` and outputs `sweetspot.version.v1` JSON. Falls back to `"0+unknown"` when package metadata is unavailable.

`sweetspot status` calls `sts:GetCallerIdentity`, optionally checks SQS queue depth, DLQ depth, and active Batch worker counts. Supports both `--format json` and `--format table`. Includes active job status breakdown.

### E. --profile/--region -- RESOLVED

All AWS-calling commands now accept `--profile` and `--region`. A shared `_aws_client(args, service)` helper creates session-aware clients when profile/region are set, falling back to `boto3.client` otherwise. `run_worker()` also accepts `profile` and `region` parameters. Tests verify session-based client creation (`test_enqueue_uses_profile_region_session_when_supplied`).

### F. Argument duplication -- NOT ADDRESSED

`submit-workers`, `supervise-workers`, and `enqueue-and-submit` still repeat ~20 identical `add_argument` calls. No shared argument group helper was extracted.

**Remaining**: Extract `_add_worker_args(parser)` to reduce duplication and prevent flag drift.

### G. Config file support -- RESOLVED

Comprehensive implementation via `--config <path.json>` and `SWEETSPOT_CONFIG` env var. Supports `defaults` section plus per-command sections (e.g., `"submit-workers": {...}`). Uses `CONFIG_COMMAND_KEYS` allowlist to prevent injection of irrelevant flags, and `CONFIG_FLAG_MAP` to translate JSON keys to CLI flags. Handles repeatable flags (`--allowed-s3-prefix`, `--s3-prefix`) and boolean flags correctly. Explicit CLI flags always override config values. Tests verify config defaults, per-command overrides, and that non-configurable commands are unaffected.

### H. finalize --dry-run -- NOT ADDRESSED

`finalize` still writes local artifacts on every invocation. No `--dry-run` flag.

### I. Progress output -- NOT ADDRESSED

`finalize` still only emits progress to stderr at `--progress-interval`. `supervise-workers` only prints its final summary. No visual progress feedback.

### J. sweetspot init -- NOT ADDRESSED

No guided setup command.

### K. logs --limit/--tail -- RESOLVED

`--limit` now has `--max-events` as an alias, and `--tail` now has `--last` as an alias. Help text clarifies both. Test `test_logs_accepts_clearer_limit_aliases` verifies the aliases work.

### L. cancel/drain -- NOT ADDRESSED

No `cancel-jobs` or `drain-queue` commands.

### M. Error handling -- RESOLVED

Operator-facing errors are now consistently `SystemExit`. Key changes:
- `_send_tasks_to_sqs`: `RuntimeError` changed to `SystemExit`
- `_queue_arn`: `RuntimeError` changed to `SystemExit`
- `cmd_s3_delete_prefix` flush: `RuntimeError` changed to `SystemExit`
- `cmd_doctor` checks: Internal `RuntimeError` changed to `ValueError` (caught by `_doctor_check`)
- Only remaining `RuntimeError` is a finalizer internal assertion (submitted != checked), which is genuinely a programming error, not an operator error.

Test `test_enqueue_reports_sqs_batch_failure_without_traceback` verifies clean error output.

### N. Shell completion -- NOT ADDRESSED

No completion command or argcomplete integration.

### O. cli.py monolith -- WORSENED

The file grew from 2226 to 2692 lines (466 lines added). The config system, version/status/scout/lane-manager commands, and example helpers were all added to the same file. No package split was done.

**Remaining**: Split into `cli/` package. The config system (`CONFIG_COMMAND_KEYS`, `CONFIG_FLAG_MAP`, `_extract_config_arg`, `_load_config`, `_apply_config_defaults`) alone is ~200 lines that could live in `cli/config.py`.

### P. Queue URL flag inconsistency -- RESOLVED

`submit-workers` and `supervise-workers` now accept `--queue-url` as an alias for `--sqs-queue-url` (both set `dest="sqs_queue_url"`). The config system maps `sqs_queue_url` to `--queue-url`. Test `test_submit_workers_accepts_queue_url_alias` verifies the alias.

### Additional improvements beyond the original audit

1. **enqueue-and-submit dry-run sizing** (`4999b0a`): When not submitting, the command now sizes workers from the task count that would be sent, not just the current SQS depth. This prevents undersized dry-run output when the queue is empty.

2. **enqueue-and-submit backlog floor** (`4999b0a`): After sending messages, the backlog floor accounts for sent messages so SQS approximate-depth lag doesn't cause undersized worker counts.

3. **repair-plan log scanning** (`4999b0a`): `_job_task_ids_from_logs` now uses `filter_log_events` with `filterPattern='"task_id"'` and paginates through results (up to `--log-tail` events, default raised from 100 to 50000). Falls back to `get_log_events` when FilterLogEvents is denied. This makes repair-plan much more reliable for excluding active tasks.

4. **Doctor error cleanup** (`b336960`): Internal checks use `ValueError` instead of `RuntimeError`, caught uniformly by `_doctor_check`.

---

## 6. Remaining improvement priorities (updated)

| Priority | Feature | Rationale |
|---|---|---|
| Medium | `--format table` on `jobs`, `dlq`, `doctor`, `describe-job` | Extend the pattern started by `status` |
| Medium | `finalize --dry-run` | Safety parity with all other mutating commands |
| Medium | `sweetspot cancel-jobs` / `drain-queue` | Operational completeness |
| Medium | Extract shared worker argument helper | Reduce ~120 lines of flag duplication across 3 commands |
| Low | Split `cli.py` (now 2692 lines) into a package | Maintainability is degrading with each addition |
| Low | `sweetspot init` guided setup | Onboarding |
| Low | `sweetspot logs --follow` | Live log tailing |
| Low | Progress output during `finalize`/`supervise-workers` | Visual feedback for long ops |
| Low | Shell completion | Power-user UX |
| Low | `sweetspot workflow` guide | Embedded workflow docs |
