# Security

SweetSpot runs **trusted** commands. It is not a sandbox.

- Anyone who can send messages to a run's SQS queue chooses the commands its workers run, with the worker role's
  permissions. Keep queue access (the operator policy from `infra/opentofu`) to people and systems you trust, and
  never build task commands from untrusted input.
- The worker role from `infra/opentofu` can read and write only the runs prefix, read the `extra_input_uris` you
  list, and use the run queues. Keep it that narrow.
- `imds_hop_limit = 2` (the default) lets containers read instance metadata, which records instance type and AZ for
  cost telemetry, and also exposes the EC2 instance role's credentials to task commands. Set it to 1 if your
  tasks must not see those; telemetry then lacks instance type.
- Don't put secrets in task JSON: it is stored in S3 (tasks.jsonl, summaries) and logged.
- Batch instances get a no-ingress security group and encrypted root volumes by default.

Report vulnerabilities privately to the maintainer via GitHub security advisories on
https://github.com/manthedan/sweet-spot.
