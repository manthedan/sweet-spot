# Cost case study template

Use this template to publish a reproducible, anonymized SweetSpot cost claim without exposing private payloads, S3 keys, account IDs, or billing data.

Pair every case study with a machine-readable manifest shaped like `examples/run_manifest.example.json`.

## Summary

- Workload: `<batch inference / ETL / simulation / scraping / other>`
- Region(s): `<us-west-2, ...>`
- Unit definition: `<records / positions / files / simulations>`
- Completed units: `<N>`
- Deadline / completeness target: `<e.g. finish 100M units within 24h; require 99.9% completed before READY>`
- Worker image digest: `<sha256:...>`
- SweetSpot commit: `<git sha>`

## Reliability result

- Source tasks: `<N>`
- Completed tasks: `<N>`
- DLQ tasks after repair: `<N>`
- Duplicate/commit-lost attempts: `<N>`
- Replay fraction: `<discarded useful-equivalent compute / successful useful compute>`
- Finalizer status: `<complete / repaired / incomplete>`
- Finalizer artifacts:
  - `final_manifest.json`
  - `task_status.jsonl`
  - `outputs.jsonl`
  - `repair_tasks.jsonl` if any

## Cost assumptions

Use public price references or explicitly stated internal rates. If AWS Cost Explorer is unavailable, label the numbers as estimated.

| Component | Assumption | Source |
| --- | --- | --- |
| Spot compute | `<$/instance-hour per pool>` | `<spot scout JSON timestamp / AWS public pricing>` |
| On-Demand compute | `<$/instance-hour>` | `<AWS public pricing>` |
| Startup overhead | `<seconds/task>` | `<worker telemetry median>` |
| Replay/interruption overhead | `<fraction>` | `<worker telemetry>` |
| Cross-region transfer | `<GB/1M units and $/GB>` | `<assumption/source>` |
| NAT data processing | `<GB/1M units and $/GB>` | `<assumption/source>` |
| CloudWatch logs | `<GB ingest/1M units and $/GB>` | `<assumption/source>` |
| S3 storage | `<GB-month/1M units and retention>` | `<assumption/source>` |
| SQS/API requests | `<requests/1M units>` | `<assumption/source>` |

## Result

| Scenario | Compute | Replay/startup | Non-compute | Total | Cost / 1M units |
| --- | ---: | ---: | ---: | ---: | ---: |
| SweetSpot observed | `$...` | `$...` | `$...` | `$...` | `$...` |
| On-Demand baseline | `$...` | `$...` | `$...` | `$...` | `$...` |

Estimated savings: `<X%>`

## Artifacts to publish

- Sanitized run manifest JSON.
- Spot scout JSON with prices, placement scores, assumptions, and `expected_total_cost_per_1m_units`.
- Lane manager dry-run/allocation JSON if multiple lanes were used.
- Finalizer manifest summary.
- Notes on excluded costs and known sources of estimation error.

## Disclosure checklist

- [ ] No account IDs, private bucket names, private S3 keys, queue URLs, job IDs, or logs with secrets.
- [ ] Rates are timestamped and labeled as observed, public-price estimate, or manual assumption.
- [ ] Unit definition is unambiguous.
- [ ] Case study states that SweetSpot runs trusted/idempotent jobs and is not an untrusted-code sandbox.
