# Cost model and measured run manifest

Milestone 7 makes SweetSpot cost decisions evidence-driven rather than price-only.

## Worker telemetry

Each task summary includes `telemetry`:

- `instance_type`, `architecture`, `region`, `availability_zone`, `image`, `image_digest` when available from env/ECS metadata.
- `startup_delay_seconds` from SQS `SentTimestamp` to command start.
- `receive_count` and `retry` from SQS receive attributes.
- `completed_units`, `useful_compute_seconds`, and `units_per_second` when the task writes them to `SWEETSPOT_METRICS_PATH`.
- `input_bytes`, `output_bytes`, `log_bytes`, and `bytes_transferred` from task metrics plus framework-observed output/log bytes.
- `interruption_status` (`none`, `failed`, or `timeout`) and `discarded_compute_seconds` for failed/timed-out attempts. Duplicate attempts that lose the done-marker race emit `commit_lost` with discarded compute.

Commands can write:

```json
{
  "completed_units": 100000,
  "useful_compute_seconds": 3600,
  "input_bytes": 1048576,
  "output_bytes": 524288
}
```

## Expected total cost

`sweetspot-scout` still reads placement scores and Spot prices, but its top pool ranking is now:

```text
expected total $/1M units = compute $/1M
  × (1 + replay_fraction + startup_overhead_seconds / useful_task_seconds)
  + noncompute $/1M
```

Where non-compute cost can include:

- cross-region transfer
- NAT data processing
- CloudWatch log ingest
- S3 storage
- a caller-supplied extra cost term

Observed summaries provide median units/sec by instance type plus retry/discarded-compute fractions. Operators can override the replay fraction and overhead assumptions for what-if planning.

## Lane allocation

`sweetspot-lane-manager` allocates eligible lanes by `expected_total_cost_per_1m_units` / `expected_cost_per_1m_units` first, then placement score, then config order. Lanes without cost annotations remain valid but are ranked after costed lanes.

## Run manifest and case study

Publish a sanitized manifest like `examples/run_manifest.example.json` with every substantial run:

- fixed deadline/completeness target
- units completed and useful compute seconds
- observed retry/discarded compute
- Spot and On-Demand comparison assumptions
- compute and non-compute cost components
- scout JSON and lane allocation artifacts

This keeps public claims reproducible without exposing private task payloads or S3 keys.
