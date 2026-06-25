---
name: sweetspot-scout
description: 'Advanced/admin read-only Spot cost/capacity investigation: rank AWS Spot pools by expected total cost, use telemetry for pool choice, and allocate workers across lanes. For new runs, prefer sweetspot-run.'
---

# Skill: sweetspot-scout

Advanced/admin guide for Spot pool ranking, cost estimation, and multi-lane worker allocation.

## When to use

Invoke this skill for advanced/operator workflows. For new manifest-based runs, prefer `sweetspot-run`; `sweetspot plan` consumes measured canary telemetry for architecture/resource selection, while scout remains a read-only capacity/cost investigation tool.

Use this skill when an agent explicitly needs to:
- Rank AWS Spot regions/instance pools by expected total cost
- Use observed telemetry to inform pool selection
- Allocate workers across multiple regions/lanes
- Configure and run the lane manager

## Concepts

### Expected total cost ranking

SweetSpot's scout ranks Spot pools by **expected total cost per useful unit**, not just Spot price. The formula:

```
expected_total_$/1M units = compute_$/1M
  x (1 + replay_fraction + startup_overhead_seconds / useful_task_seconds)
  + noncompute_$/1M
```

Where:
- `replay_fraction`: Fraction of compute lost to Spot interruption/retry (from telemetry)
- `startup_overhead_seconds`: Fixed overhead per task (container pull, SQS poll, etc.)
- `useful_task_seconds`: Median useful compute seconds per task
- `noncompute_$/1M`: Transfer, NAT, CloudWatch, S3, and caller-supplied costs

### Telemetry inputs

Worker summaries include telemetry under the `telemetry` key:
- `instance_type`, `architecture`, `region`, `availability_zone`
- `startup_delay_seconds` from SQS SentTimestamp
- `receive_count` and `retry` status
- `completed_units`, `useful_compute_seconds`, `units_per_second`
- `input_bytes`, `output_bytes`, `bytes_transferred`
- `interruption_status` (`none`, `failed`, `timeout`)
- `discarded_compute_seconds` for failed/commit-lost attempts

Scout uses these to compute pool-specific replay fractions and throughput rates.

## CLI commands

### sweetspot scout / sweetspot-scout

Read-only Spot pool ranking tool. Does not submit jobs or mutate resources. Prefer the nested `sweetspot scout` command; the standalone `sweetspot-scout` entry point remains available for compatibility.

```bash
sweetspot scout \
  --preset mixed \
  --regions us-west-2 us-east-2 eu-north-1 \
  --target-vcpus 256 512 \
  --bucket my-data-bucket \
  --observed-summaries artifacts/my-run-001/summaries \
  --startup-overhead-seconds 90 \
  --cross-region-gb-per-1m-units 2 \
  --nat-gb-per-1m-units 0 \
  --json-out artifacts/my-run-001/scout.json
```

Key arguments:
- `--preset x86|arm|mixed`: Instance type preset to evaluate. The CLI default remains `x86` for compatibility safety; use `mixed` during scouting to surface ARM/Graviton savings before deciding whether to opt in.
  - `x86`: c5/c5a/c6i/c6a/c7i/c7a/c8i/c8a/m5/m5a/m6i/m6a/m7i/m7a/m8i/m8a families
  - `arm`: c6g/c7g/c8g/m6g/m7g/m8g families (Graviton)
- `--regions`: AWS regions to evaluate, repeatable
- `--target-vcpus`: vCPU targets for placement score queries, repeatable
- `--bucket`: S3 bucket for cross-region transfer cost estimation
- `--observed-summaries`: Directory of summary JSON files from prior runs
- `--startup-overhead-seconds`: Per-task overhead assumption
- `--cross-region-gb-per-1m-units`: Cross-region transfer cost per 1M units
- `--nat-gb-per-1m-units`: NAT data processing cost per 1M units
- `--json-out`: Write JSON results to this path

### sweetspot lane-manager / sweetspot-lane-manager

Multi-region lane allocator. Reads a config file and allocates workers across pre-existing lanes. Prefer the nested `sweetspot lane-manager` command; the standalone `sweetspot-lane-manager` entry point remains available for compatibility.

```bash
sweetspot lane-manager --config lanes.json
```

Lane config format:
```json
{
  "sqs_queue_url": "https://sqs.us-west-2.amazonaws.com/123456789012/sweetspot-work",
  "instance_types": ["c6i.large", "c6a.large", "m6i.large", "m6a.large"],
  "lanes": [
    {
      "name": "us-west-2-x86",
      "region": "us-west-2",
      "batch_job_queue": "arn:aws:batch:us-west-2:123456789012:job-queue/sweetspot-cpu-spot-queue",
      "job_definition": "arn:aws:batch:us-west-2:123456789012:job-definition/sweetspot-worker:1",
      "job_name_prefix": "my-run-worker",
      "max_workers": 128,
      "messages_per_worker": 4,
      "vcpus": 2,
      "memory": 4096,
      "min_placement_score": 7,
      "expected_total_cost_per_1m_units": 0.42
    },
    {
      "name": "us-west-2-arm",
      "region": "us-west-2",
      "instance_types": ["c7g.large", "m7g.large"],
      "batch_job_queue": "arn:aws:batch:us-west-2:123456789012:job-queue/sweetspot-arm-spot-queue",
      "job_definition": "arn:aws:batch:us-west-2:123456789012:job-definition/sweetspot-worker-arm64:1",
      "job_name_prefix": "my-run-arm-worker",
      "max_workers": 64,
      "messages_per_worker": 4,
      "vcpus": 2,
      "memory": 4096,
      "min_placement_score": 7,
      "expected_total_cost_per_1m_units": 0.32
    }
  ]
}
```

Use top-level `instance_types` for homogeneous lane files. In mixed-architecture configs, set per-lane `instance_types` so placement-score checks match each Batch queue's real x86 or ARM capacity pool.

Lane fields:
- `name`: Lane identifier for reporting
- `region`: AWS region for this lane
- `batch_job_queue`: Batch job queue ARN
- `job_definition`: Batch job definition ARN (with revision)
- `instance_types`: Optional per-lane placement-score instance list; use this for separate x86/ARM lanes
- `job_name_prefix`: Prefix for submitted job names
- `max_workers`: Maximum concurrent workers in this lane
- `messages_per_worker`: SQS messages per worker (optional)
- `vcpus` / `memory`: Container overrides (optional)
- `min_placement_score`: Minimum Spot placement score (1-10); lane is ineligible if score is below this
- `allow_unknown_placement_score`: If true, lane is eligible even when AWS returns no score (default: false, fails closed)
- `expected_total_cost_per_1m_units`: Cost annotation for cheapest-first allocation; lanes without this are ranked after costed lanes

### Allocation logic

Lanes are allocated in this order:
1. Filter by placement score eligibility (if `min_placement_score` is set)
2. Sort by `expected_total_cost_per_1m_units` ascending (cheapest first)
3. Lanes without cost annotations come after costed lanes
4. Allocate workers up to each lane's `max_workers` until total demand is met
5. Pre-count active workers across all lanes before allocation to avoid oversubscription

## Output interpretation

### scout output
The scout JSON contains ranked pools with:
- Region, instance type, AZ
- Spot placement score
- Spot price
- Expected total cost per 1M units
- Replay fraction (from telemetry if available)
- Median units per second (from telemetry if available)

### lane-manager output
The lane manager prints allocation results showing:
- Per-lane worker counts (active, desired, to-submit)
- Queue depth
- Total allocation across all lanes
- Lane eligibility and ranking rationale

## Choosing instance presets

### x86 preset
Use for workloads that require x86-compatible binaries or have not been tested on ARM:
- Standard CPU compute (c5/c6i/c7i/c8i families)
- General purpose (m5/m6i/m7i/m8i families)
- AMD variants (c5a/c6a/c7a/c8a, m5a/m6a/m7a/m8a)

### arm preset (Graviton)
Use for workloads that are ARM-compatible or container-based:
- Graviton instances are often materially cheaper than equivalent x86 for CPU-heavy retryable work
- c6g/c7g/c8g, m6g/m7g/m8g families
- Test with a canary first; some libraries have ARM-specific issues
- Keep ARM opt-in: if compatibility is unknown, scout with `mixed` but deploy x86 until an ARM canary passes
- Prefer separate x86 and ARM lanes/Batch queues unless the image is verified multi-arch and dependencies are architecture-neutral

## Cost optimization tips

1. **Use observed telemetry**: Pass `--observed-summaries` from prior runs. Without telemetry, scout falls back to price-only ranking which overvalues cheap-but-unreliable pools.
2. **Cross-region is viable**: Multi-region lanes can dramatically reduce cost if transfer costs are low relative to compute savings.
3. **Check ARM explicitly**: Run `--preset mixed` to see whether Graviton pools are cheaper for your telemetry, then promote ARM lanes only after a compatibility canary.
4. **Placement scores matter**: A cheap pool with placement score 2 will waste time on `STARTING`/`RUNNABLE` stalls. Use `min_placement_score` of 6-7 in production.
5. **Startup overhead dominates short tasks**: For tasks under 5 minutes, startup overhead (container pull, SQS poll) is a significant fraction of total cost. Batch more work per task.
6. **Replay fraction from Spot interruptions**: If 10% of tasks are interrupted and fully replayed, effective cost is 11% higher than Spot price alone suggests.

## Common pitfalls

1. **Running scout without telemetry**: Price-only ranking is misleading. Always try to provide `--observed-summaries` from canary or prior runs.
2. **No placement score gating**: Without `min_placement_score`, lanes may be allocated to pools where AWS cannot fulfill capacity, causing `RUNNABLE` stalls.
3. **Lane manager dry-run**: Like other SweetSpot commands, `sweetspot lane-manager` / `sweetspot-lane-manager` is dry-run by default. Check the output before adding `--submit`.
4. **Forgetting transfer costs**: Cross-region or NAT costs can erase Spot savings for data-intensive workloads. Use `--cross-region-gb-per-1m-units` and `--nat-gb-per-1m-units`.
5. **ARM compatibility**: Test workloads on Graviton with a canary before committing to ARM-only lanes.
