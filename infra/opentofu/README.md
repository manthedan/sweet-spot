# OpenTofu AWS infra

Creates the AWS primitives used by `SweetSpot`:

- SQS work queue + DLQ with SSE, longer DLQ retention, and a narrow redrive allow policy
- AWS Batch x86 Spot compute environment + queue
- optional separate ARM/Graviton Spot compute environment + queue
- optional On-Demand repair queue
- no-ingress Batch security group by default, unless explicit security groups are supplied
- Batch launch template requiring IMDSv2 and encrypted gp3 root volumes
- generic x86 worker job definition that explicitly runs `sweetspot worker`
- optional ARM worker job definition for a verified ARM or multi-arch image
- IAM roles for Batch/ECS/worker task
- optional CloudWatch dashboard and baseline alarms
- optional monthly AWS Budget alert

## Example

```hcl
project_name     = "my-sweetspot"
aws_region       = "us-west-2"
worker_image_uri  = "ACCOUNT.dkr.ecr.us-west-2.amazonaws.com/my-sweetspot-worker:latest"
worker_s3_bucket  = "my-work-bucket"
worker_s3_prefixes = ["runs/hello-001"]
max_vcpus_spot      = 256
subnet_ids           = ["subnet-aaa", "subnet-bbb"]
require_explicit_subnets = true
cost_tags = {
  CostCenter = "batch-research"
}
monthly_budget_limit_usd   = 500
budget_notification_emails = ["ops@example.com"]
alarm_sns_topic_arns = ["arn:aws:sns:us-west-2:ACCOUNT:sweetspot-alerts"]
```

```bash
tofu init
tofu plan -var-file=example.tfvars
tofu apply -var-file=example.tfvars
```

## Notes

- Default Spot allocation strategy is `SPOT_PRICE_CAPACITY_OPTIMIZED`.
- Default Spot instance types are x86-only for workload compatibility. To evaluate ARM/Graviton savings, run `sweetspot scout --preset mixed --observed-summaries ...`, then opt into the separate ARM queue only after a canary proves the worker image and native dependencies are compatible.
- The committed `.terraform.lock.hcl` is part of the reproducibility contract; CI runs `tofu init -lockfile=readonly`, `tofu fmt`, and `tofu validate`.
- For production, pass explicit private `subnet_ids` and set `require_explicit_subnets = true` so the module does not silently use every subnet in the selected/default VPC.
- If `security_group_ids` is empty, the module creates a dedicated no-ingress security group. Set `create_no_ingress_security_group = false` only when intentionally falling back to the VPC default security group.
- Batch instances use a launch template with IMDSv2 required, metadata response hop limit 1, and encrypted gp3 root volumes. Set `ebs_kms_key_id` to use a customer-managed KMS key.
- SQS SSE is enabled. The DLQ defaults to the SQS maximum 14-day retention while the source queue defaults to 13 days to avoid destructive retention shrinkage during upgrades, and the DLQ redrive allow policy only permits the module's source queue.
- `cost_tags` are merged onto resources for cost allocation. Set `monthly_budget_limit_usd` and `budget_notification_emails` to create an account-scoped AWS Budget alert as a guardrail.
- The worker task role is scoped to the work queue plus `worker_s3_bucket`/`worker_s3_prefixes`. Set prefixes to the run roots that contain inputs, outputs, summaries, logs, and done markers; object access is granted as `${prefix}/*`, and runtime validation uses the same under-prefix semantics.
- The job definition injects matching `SWEETSPOT_ALLOWED_S3_PREFIXES` so workers reject task payloads that reference S3 URIs outside the configured prefixes. For SSE-KMS encrypted S3 objects, set `worker_kms_key_arns` and ensure each key policy permits the generated worker task role.
- `create_observability` defaults to true and creates a CloudWatch dashboard plus alarms for work-queue age, DLQ depth, Batch failures, and runnable-job stalls. Set `alarm_sns_topic_arns` to wire notifications. Validate Batch metric dimensions in your account after first launch; if `AWS/Batch` dimensions differ, use the worker `sweetspot.worker_event.v1` logs/EventBridge as the authoritative alarm source.
- The dashboard includes a Logs Insights widget over structured `sweetspot.worker_event.v1` events emitted by the worker.
- The reliability contract depends on SQS visibility timeout + deterministic S3 done markers, not Batch retries.
- Attempt-scoped outputs/logs/summaries make duplicate attempts safe but can grow quickly under interruptions. Add bucket lifecycle rules for run prefixes (for example, retain canonical manifests/done markers longer than `*.attempts/*` objects) and document the retention window for reproducibility.
- For S3 buckets with versioning enabled, pair run prefixes with lifecycle rules that expire noncurrent versions/delete markers, or use `sweetspot s3-delete-prefix --include-versions` for explicit teardown. Deleting current objects only is not a complete cost cleanup on versioned buckets.
- Automatic teardown guidance: set low `max_vcpus_*` for tests, keep `monthly_budget_limit_usd` nonzero, tag every run prefix, finalize/repair before deleting SQS messages, run version-aware S3 cleanup, then `tofu destroy` idle stacks rather than leaving Batch queues and log/storage resources behind.

## Opt-in ARM / Graviton lanes

ARM is not the module default because many user workloads or container images are x86-only. When a canary proves ARM compatibility, enable the separate ARM queue and job definition:

```hcl
create_arm_spot_queue = true

# Optional. Leave empty only if worker_image_uri is a verified multi-arch image.
worker_image_uri_arm = "ACCOUNT.dkr.ecr.us-west-2.amazonaws.com/my-sweetspot-worker-arm64:latest"

spot_arm_instance_types = [
  "c7g.large", "c7g.xlarge", "c7g.2xlarge",
  "m7g.large", "m7g.xlarge", "m7g.2xlarge",
]
max_vcpus_spot_arm = 64
```

The existing `batch_spot_queue` / `worker_job_definition` outputs remain the default x86 lane. Use `batch_spot_arm_queue` and `worker_arm_job_definition` for ARM canaries or ARM production lanes, then model x86 and ARM as separate `sweetspot lane-manager` lanes with per-lane `instance_types`. Only place x86 and ARM types in the same Batch compute environment when the worker image is verified multi-arch and all native dependencies work on both architectures.
