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
- `spot_interruption_notice`, `spot_interruption_action`, `spot_interruption_time`, and `spot_rebalance_recommendation_time` when EC2 IMDS exposes Spot interruption or rebalance notices before the worker exits.

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

`sweetspot-scout` still reads placement scores and Spot prices, and `sweetspot plan` embeds the same shared model when it has only conservative default pricing. Pool ranking and Plan estimates use:

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

Observed summaries provide median units/sec by instance type plus retry/discarded-compute fractions. `ApproximateReceiveCount` is also surfaced as a replay lower bound: it proves at least prior receives occurred, but it cannot recover compute seconds from missing/killed attempts. If receive-count telemetry proves more replay receives than discarded-compute summaries captured, scout emits `replay_fraction_partially_observed` so operators know the replay cost is still a lower bound. Operators can override the replay fraction and overhead assumptions for what-if planning. Scout JSON includes `reasons` and `cost_model.omitted_cost_components` when throughput, replay, or optional non-compute costs are defaulted/omitted, so agents do not mistake a partial estimate for a complete bill. Use `--worker-memory` / `--worker-memory-mib` when Batch jobs request meaningful memory so scout caps packed workers by both vCPU and memory fit instead of under-costing memory-heavy workloads. Scout subtracts a conservative per-instance reserve (`--instance-memory-reserve-mib`, default 512 MiB) before computing memory fit because raw EC2 instance memory is not the same as schedulable Batch/ECS memory. Use `sweetspot scout --preset smallest` first for cheap 1 vCPU lanes (`c7a.medium`, `c7g.medium`, `c6g.medium`), then `--preset mixed` for broader capacity. Treat ARM as opt-in until a workload canary proves the image and native dependencies are compatible; for 2 GiB medium instances, start with a Batch memory reservation around 1536 MiB instead of 2048 MiB.

## Lane allocation

`sweetspot-lane-manager` allocates eligible lanes by `expected_total_cost_per_1m_units` / `expected_cost_per_1m_units` first, then placement score, then config order. Lanes without cost annotations remain valid but are ranked after costed lanes. Lane configs may set per-lane `instance_types`, which is useful when comparing separate x86 and ARM Batch queues without making ARM the default for unknown workloads.

## Run manifest and case study

Publish a sanitized manifest like `examples/run_manifest.example.json` with every substantial run:

- fixed deadline/completeness target
- units completed and useful compute seconds
- observed retry/discarded compute
- Spot and On-Demand comparison assumptions
- compute and non-compute cost components
- scout JSON and lane allocation artifacts

This keeps public claims reproducible without exposing private task payloads or S3 keys.
