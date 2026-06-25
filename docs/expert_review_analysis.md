# Expert Review Analysis

Analysis of the external expert review against the SweetSpot codebase as of commit `26202cd`.

## Overall assessment

The expert review is technically accurate, well-referenced, and identifies a genuine architectural gap between what SweetSpot claims to be (agent-native cost-aware controller) and what it actually is (an AWS Batch operator toolkit with strong reliability internals). The recommendations are directionally correct, though some are more urgent than others.

---

## Points where the expert is correct

### 1. The reliability core is sound

Verified: The worker loop in `worker.py:766-813` receives one message at a time (`MaxNumberOfMessages=1`), processes it, uploads attempt-scoped output/summary, writes a conditional done marker (`IfNoneMatch=*` via `s3_upload_text_if_absent` in `s3util.py`), validates the winning marker on 412, and only deletes the SQS message after successful commit. The task hash, attempt ID, and marker validation chain is correct. This is the right foundation.

### 2. The sizing decisions are pushed to the agent

Verified: `submit-workers` computes `desired_worker_count(backlog, messages_per_worker, min_workers, max_workers)` which is `ceil(visible / messages_per_worker)` clamped to min/max. It does not consider measured throughput, deadline, budget, memory, or interruption risk. The agent skills document these as manual choices. The expert's core critique is valid.

### 3. `derive-canary` does not determine shard size

Verified: `derive-canary` selects a deterministic subset of already-defined tasks by index. It does not create or resize tasks. The agent skill's example shows the agent inventing `payload: {"count": 1000}` per task with no basis.

### 4. `estimate-runtime` is a passive calculator

Verified: It takes `--target-units` or `--task-count`/`--units-per-task` as caller inputs, computes `median_rate / parallelism`, and emits warnings. It does not rewrite tasks or select resource shapes.

### 5. Scout placement score is applied at the wrong granularity

Verified: `scout.py:394` calls `placement_scores()` once per `(region, target_vcpus)` pair. The result is a per-region score. Line 457 attaches that same region-level score to every `instance_type/AZ` row. The output makes it appear that the score is per-pool, when AWS documents it as a recommendation for the full configuration. This is a real misleading-output issue.

### 6. Scout packing ignores memory

Verified: `scout.py:437` computes `packed_workers = max(1, vcpus // args.worker_vcpus)`. There is no `DescribeInstanceTypes` call for memory, no `worker_memory` parameter, and no memory-based packing constraint. For memory-bound workloads this overestimates packing density.

### 7. Replay fraction is global

Verified: `scout.py:296` computes `observed_replay_fraction = discarded_compute_seconds / useful_compute_seconds` aggregated across all summaries. This single global fraction is then used for every instance-type pool at line 444. The expert is correct that pool-specific replay rates would be more accurate, but this requires more telemetry data than a typical early-stage run produces.

### 8. Discarded compute is undercounted for Spot interruptions

Verified: The worker writes summaries and telemetry after the task process exits. An abruptly terminated host (Spot reclaim, hardware failure) cannot write the elapsed-but-discarded seconds. The subsequent retry attempt records `retry: True` but the killed attempt's lost work is absent. This makes observed replay fraction a lower bound. The expert's observation is architecturally correct.

### 9. Instance type is not injected by the default deployment

Verified: `worker.py:83` reads `env.get("SWEETSPOT_INSTANCE_TYPE") or env.get("EC2_INSTANCE_TYPE")`. The OpenTofu `batch.tf` job definition does not inject either variable. The default deployment cannot build per-instance-type throughput history without additional configuration. This is a real telemetry gap.

### 10. The `messages_per_worker` skill guidance is wrong

Verified: The agent skill (`sweetspot-workers.md:174`) says "Higher values mean fewer Batch jobs but more work lost on Spot interruption." But the worker code (`worker.py:770-771`) receives exactly one message at a time (`MaxNumberOfMessages=1`) and deletes it after completion before receiving the next. A Spot interruption only loses the currently executing task, not the remaining `messages_per_worker - 1` messages (which are still in SQS). The expert caught a genuine factual error in the skills.

### 11. OpenTofu defaults are x86-only with no ARM compute environment

Verified: `variables.tf:106-118` defaults to x86 instance types only (c5/c6i/c6a/c7i/m6i/m6a/m7i). There is no paired ARM compute environment or job definition. AWS Batch now supports `default_x86_64` and `default_arm64` instance bundles. The expert's suggestion to provision paired environments is valid.

### 12. Scout defaults to human-readable stdout

Verified: `scout.py:513-527` prints a formatted human table to stdout. JSON output only appears when `--json-out` is passed, and it goes to a file, not stdout. For an agent-facing tool, JSON on stdout should be the default. Correct observation.

---

## Points where the expert is partially right but overstated

### 1. "The agent surface is far too broad"

The expert recommends collapsing to 5 commands (`plan`, `run`, `status`, `repair`, `cancel`). This is directionally correct for a future state, but the current command set exists because each maps to a real operational phase that operators may need to invoke independently. The problem is not that the commands exist, but that there is no higher-level orchestrator. A `run` command that calls them internally is the right next step, but hiding the primitives would reduce flexibility for advanced operators without adding value for agents in the short term.

### 2. "Documentation is already drifting"

The expert cites the re-audit saying finalize --dry-run was "not addressed" while the changelog says it was added. This is accurate but reflects a process issue (the audit was written before the implementation, then updated), not a permanent doc-code gap. The more substantive drift claim is the `messages_per_worker` skill error, which is a genuine bug in the documentation.

### 3. "Multi-region lane manager should be last"

This is a reasonable prioritization opinion. However, the lane manager already exists and works. Deferring its use is fine, but the code is not a maintenance burden or complexity source for the reliability core.

---

## Points where the expert may be wrong

### 1. "Increasing `messages_per_worker` means more work lost on interruption"

The expert says this skill claim is incorrect, which we verified. However, the expert implies this is a fundamental design flaw. In practice, `messages_per_worker` controls how many sequential SQS messages a single Batch job processes before exiting. If a Spot host is terminated mid-task, only the current message is lost (it returns to the queue via visibility timeout). The remaining messages the worker would have processed are never received, so they remain visible in SQS for other workers. The skill text is indeed wrong, but the practical impact is minimal because the behavior is safe regardless.

### 2. The proposed `plan` command is harder than it looks

The expert proposes adaptive shard sizing, resource lattice testing, and ARM/x86 auto-selection. These are valuable but represent significant engineering effort (canary execution, OOM detection, architecture compatibility testing, cost-per-unit optimization under deadline constraints). The current CLI is a toolkit; the proposed `plan` command is an autonomous controller. The gap between these is substantial and should not be underestimated.

---

## Recommended action items (prioritized)

### Immediate (factual fixes)

1. **Fix the `messages_per_worker` skill text**: The claim about work loss on interruption is incorrect. The worker processes one message at a time and deletes after completion. Only the currently executing task is at risk.

2. **Fix scout placement score representation**: Either rename the field from `placement_score` to `region_placement_score` on per-pool rows, or document that the score applies to the full configuration, not the individual pool.

### Short-term (engineering gaps)

3. **Add memory to scout packing**: Add a `--worker-memory` parameter to scout and use `DescribeInstanceTypes` to compute memory-constrained packing alongside vCPU-constrained packing.

4. **Inject instance type into worker environment**: Update the OpenTofu Batch job definition to set `SWEETSPOT_INSTANCE_TYPE` from the ECS task metadata, or use IMDS to detect it in the worker.

5. **Make scout JSON-on-stdout the default**: For agent consumption, `--json-out` should be optional and JSON should go to stdout by default. Human table output should be `--format table`.

6. **Add paired ARM/x86 infrastructure**: Provision two optional Batch compute environments using `default_x86_64` and `default_arm64` instance bundles.

### Medium-term (product direction)

7. **Define `JobSpec` and `Plan` schemas**: A declarative job specification (image, command, input manifest, output prefix, constraints) and a machine-readable plan output are prerequisites for the proposed `plan`/`run` flow.

8. **Build a `run` controller**: An orchestrator that chains derive-canary, estimate-runtime, enqueue, submit-workers, supervise, and finalize internally. This is the single highest-value feature for agent UX.

9. **Add deadline-aware concurrency sizing**: Use the formula `workers = ceil(remaining_units / (p10_rate * seconds_remaining))` with budget and quota caps. This removes the most dangerous agent decision.

### Long-term (architectural)

10. **Implement adaptive shard sizing**: Geometric-shard canary to determine safe task duration, then generate appropriately sized tasks from the input manifest.

11. **Auto-select architecture**: Run paired x86/ARM canaries, reject ARM on compatibility failure, select by measured cost per unit.

12. **Correct replay accounting**: Use SQS `ApproximateReceiveCount` as a lower bound on replay events, or add Spot interruption signal handling to capture lost compute before termination.

---

## The expert's key insight

The most important observation in the review is:

> "Agentic" does not mean writing detailed instructions that teach an agent to operate a complex CLI. It means encoding the decisions and guardrails in the software so the agent cannot make the expensive choices badly.

This is correct and is the gap between SweetSpot's current state and its stated goal. The agent skills we wrote are detailed procedural instructions for operating a complex CLI. They are useful given the current architecture, but the better path is to reduce the decision surface so the skills become thinner. The `plan`/`run` proposal is the right direction.
