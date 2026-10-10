# Changelog

## 0.2.0 (unreleased): rewrite

0.1 had grown to ~16k lines and 35 commands (setup wizard, guarded bootstrap apply, a lifecycle state machine, a
planner that refused to plan without canaries). 0.2 keeps the parts that earned their place (the SQS lease +
conditional S3 done-marker protocol, measured $/unit lane selection, the OpenTofu stack) and rebuilds the rest
around one file format and nine commands.

- Tasks are `{"id", "command", "inputs", ...}` with `{in}`/`{out}` placeholders: existing scripts run unchanged,
  inputs are staged for them and everything they write to `{out}` is the output. No per-task S3 paths.
- `sweetspot tasks` generates task files from a command template (`--inputs`, `--inputs-from`, `--count`, `--chunks`).
- `sweetspot local` runs tasks on your machine with the same runner and commit protocol as the cloud.
- `sweetspot submit` creates a run (its own SQS queue + DLQ, a registered job definition, one array job of
  workers) and is the single command to resume, scale, or `--retry` it. Run state lives in S3, not on the
  machine that submitted it.
- `status` reports done/failing/queued/workers and the next command to run; `logs` shows every attempt of a task.
- `finish` verifies every done marker, repairs interrupted promotions, publishes a deterministic `manifest.jsonl`
  and a conditional `READY`, then releases queues and job definitions.
- Outputs land at stable paths (`<prefix>/<run>/out/<task id>/`); duplicate attempts never mix files.
- Workers renew a 5-minute lease instead of holding a long visibility timeout, hand tasks back on Spot interruption
  notices or SIGTERM (exit 75, retried by Batch), keep one sweeper alive while work is in flight, and stop only on
  repeated infrastructure errors, not on task failures.
- Done markers record instance type, image, shape, units, elapsed time, peak RSS and input ETags; `scout --run`
  turns them into $ per million units by instance type.
- Removed: init/bootstrap/doctor, the planner and adaptive canary controller, lane-manager, monitor/explain/
  postmortem/cleanup, the admin command family, the YAML setup bundle, agent skill files, redaction regexes.
  The task and marker schemas are not compatible with 0.1.
