---
name: sweetspot-finalize
description: 'Advanced/admin workflow for manual finalization, done-marker verification, repair planning, READY publishing, stale SQS cleanup, and guarded S3 prefix inspection/deletion. For new runs, prefer sweetspot-run.'
---

# Skill: sweetspot-finalize

Advanced/admin guide for finalizing SweetSpot runs, planning repairs, and cleaning up stale resources.

## When to use

Invoke this skill for advanced/operator workflows. For normal run lifecycle checks, prefer `sweetspot-run` plus `sweetspot status RUN_ID` and `sweetspot repair RUN_ID`.

Use this skill when an agent explicitly needs to:
- Check completion status of a run (which tasks are done vs incomplete)
- Generate repair tasks for incomplete/failed tasks
- Upload finalization manifests to S3
- Publish a READY marker when a run is complete
- Clean up stale SQS messages
- Plan repairs that avoid duplicating work owned by active workers
- Clean up S3 prefixes from old runs

## Finalization workflow

```
1. Run finalize to check all done markers
2. If incomplete: examine repair_tasks.jsonl
3. Run repair-plan to exclude tasks owned by active workers
4. Enqueue repair tasks and submit repair workers
5. Re-run finalize to confirm completion
6. Optionally publish READY marker
7. Clean up stale SQS messages
8. Clean up old S3 prefixes
```

## CLI commands

### Finalize a run

Basic finalization (local artifacts only):
```bash
sweetspot finalize \
  --run-id my-run-001 \
  --output-prefix s3://my-bucket/runs/my-run-001 \
  --tasks-jsonl artifacts/my-run-001/tasks.jsonl \
  --workers 32 \
  --use-listing-index \
  --write-repair-jsonl artifacts/my-run-001/repair_tasks.jsonl
```

Upload manifests and publish READY:
```bash
sweetspot finalize \
  --run-id my-run-001 \
  --output-prefix s3://my-bucket/runs/my-run-001 \
  --tasks-jsonl artifacts/my-run-001/tasks.jsonl \
  --workers 32 \
  --use-listing-index \
  --upload \
  --publish-ready \
  --require-complete
```

Key arguments:
- `--workers`: Parallelism for S3 existence checks (default 32)
- `--use-listing-index`: Preload S3 prefixes with ListObjectsV2 to reduce HEAD calls
- `--preload-s3-prefix`: Additional S3 prefixes to preload, repeatable
- `--write-repair-jsonl`: Custom path for repair tasks output
- `--upload`: Upload manifests to S3 under output_prefix/manifests/
- `--publish-ready`: Write a READY marker when complete (requires --upload)
- `--ready-key`: S3 key name for READY marker (default "READY")
- `--require-complete`: Exit code 2 if any tasks are incomplete
- `--allow-incomplete-ready`: Unsafe: publish READY even when incomplete
- `--allow-legacy-done-markers`: Migration mode for v1 markers
- `--progress-interval`: Print progress every N tasks to stderr (default 1000)
- `--max-inline-outputs`: Max output URIs inlined in final_manifest.json (default 1000)

### Build a repair plan

Excludes tasks that are still owned by active workers:
```bash
sweetspot repair-plan \
  --tasks-jsonl artifacts/my-run-001/tasks.jsonl \
  --task-status-jsonl artifacts/my-run-001/finalizer/task_status.jsonl \
  --out-jsonl artifacts/my-run-001/repair_plan.jsonl \
  --job-queue my-batch-spot-queue \
  --job-name-regex 'my-run-001-worker'
```

Key arguments:
- `--include-active`: Unsafe: include missing tasks even if active workers own them
- `--only-known-failed`: Only repair tasks seen in FAILED job logs
- `--log-group`: CloudWatch log group for task ID extraction
- `--log-tail`: Maximum CloudWatch log events to scan per job for `task_id` patterns via `filter_log_events` (default 50000); falls back to `get_log_events` when FilterLogEvents is denied

### Clean up stale SQS messages

Removes visible SQS messages whose S3 done marker already exists:
```bash
# Dry-run (default)
sweetspot cleanup-stale-messages \
  --queue-url https://sqs.us-west-2.amazonaws.com/123456789012/my-work-queue \
  --run-id my-run-001

# Apply deletion
sweetspot cleanup-stale-messages \
  --queue-url https://sqs.us-west-2.amazonaws.com/123456789012/my-work-queue \
  --run-id my-run-001 \
  --apply
```

### S3 prefix cleanup

Inspect and optionally delete an S3 prefix:
```bash
# Dry-run inspection
sweetspot s3-delete-prefix \
  --prefix s3://my-bucket/runs/old-run-001/ \
  --artifact-dir artifacts/old-run-001/delete-dryrun

# Actual deletion (requires exact confirmation)
sweetspot s3-delete-prefix \
  --prefix s3://my-bucket/runs/old-run-001/ \
  --delete \
  --confirm-prefix s3://my-bucket/runs/old-run-001/ \
  --include-versions
```

Key arguments:
- `--min-prefix-chars`: Safety guard, minimum key length (default 8)
- `--batch-size`: Objects per DeleteObjects call (default 1000)
- `--include-versions`: Delete all versions and delete markers
- `--completion-marker-s3`: Write a completion marker after deletion

## Output interpretation

### finalize output
```json
{
  "schema": "sweetspot.final_manifest.v1",
  "run_id": "my-run-001",
  "task_count": 1000,
  "done_count": 998,
  "output_count": 998,
  "missing_count": 2,
  "missing_done_count": 2,
  "complete": false,
  "missing_task_ids": ["task-000042", "task-000733"],
  "final_manifest": "artifacts/my-run-001/finalizer/final_manifest.json",
  "repair_tasks": "artifacts/my-run-001/finalizer/repair_tasks.jsonl",
  "task_status": "artifacts/my-run-001/finalizer/task_status.jsonl",
  "outputs_manifest": "artifacts/my-run-001/finalizer/outputs.jsonl"
}
```

Critical fields:
- `complete`: true only if all tasks have valid done markers and outputs
- `missing_count`: Number of tasks needing repair
- `missing_task_ids`: Sample of incomplete task IDs (up to 1000)
- `missing_output_task_ids`: Tasks with done markers but missing output objects

If `complete` is false and `--require-complete` was set, exit code is 2.

### Task states in task_status.jsonl

Each line has a `state` field:
- `done`: Valid done marker and output exists
- `incomplete`: No done marker found
- `missing_output`: Done marker exists but output object is missing
- `output_without_done`: Output exists but done marker is missing or invalid
- `invalid_done_marker`: Done marker exists but failed validation

### repair-plan output
```json
{
  "schema": "sweetspot.repair_plan.v1",
  "task_count": 1000,
  "state_counts": {"done": 998, "incomplete": 2},
  "missing_count": 2,
  "active_job_count": 5,
  "blocked_active_count": 0,
  "repair_task_count": 2,
  "repair_task_ids": ["task-000042", "task-000733"]
}
```

Key fields:
- `blocked_active_count`: Tasks excluded from repair because active workers own them
- `repair_task_count`: Number of tasks to actually repair

### cleanup-stale-messages output
```json
{
  "schema": "sweetspot.stale_message_cleanup.v1",
  "scanned": 50,
  "done_messages": 48,
  "deleted": 0,
  "kept": 2,
  "invalid": 0
}
```

## Artifacts produced

Finalize creates these files in the artifact directory:

| File | Purpose |
|---|---|
| `final_manifest.json` | Summary with counts, completion status, sample missing IDs |
| `task_status.jsonl` | Per-task status record (state, done_exists, output_exists, etc.) |
| `repair_tasks.jsonl` | Tasks needing repair (enqueue this to retry) |
| `outputs.jsonl` | All completed output URIs |

When `--upload` is used, these are uploaded to:
```
s3://<output_prefix>/manifests/final_manifest.json
s3://<output_prefix>/manifests/task_status.jsonl
s3://<output_prefix>/manifests/outputs.jsonl
s3://<output_prefix>/manifests/repair_tasks.jsonl  (only if missing tasks)
```

## Repair workflow

1. Run finalize to identify incomplete tasks
2. If `missing_count > 0`:
   a. Run repair-plan to exclude tasks owned by active workers
   b. Enqueue the repair plan output: `sweetspot enqueue-jsonl --tasks-jsonl repair_plan.jsonl --queue-url <url> --submit`
   c. Submit repair workers or use an On-Demand repair queue
   d. Wait for repair workers to finish
   e. Re-run finalize to confirm completion
3. If `output_without_done_count > 0`: These tasks have output but no/invalid done markers. The repair plan will fix them.
4. If `invalid_marker_count > 0`: Done markers failed validation. Repair tasks use a `.repair-<timestamp>` suffix to avoid colliding with the invalid marker.

## Common pitfalls

1. **Not using `--use-listing-index` for large runs**: Without it, each task requires a separate HeadObject call. For 10k+ tasks, this is slow and expensive.
2. **Forgetting `--require-complete`**: Without it, finalize exits 0 even if tasks are incomplete. Use this in CI to detect incomplete runs.
3. **Repairing while workers are still active**: Use repair-plan with `--job-queue` to exclude tasks that active workers are processing.
4. **Not cleaning up stale messages**: After a run completes, visible messages in SQS with existing done markers should be cleaned up to avoid phantom work.
5. **Publishing READY too early**: `--publish-ready --require-complete` exits 2 and does NOT publish READY if any task is incomplete (unless `--allow-incomplete-ready` is set).
