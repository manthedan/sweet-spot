# OpenTofu AWS infra

Creates the AWS primitives used by `SpotBatch`:

- SQS work queue + DLQ
- AWS Batch Spot compute environment + queue
- optional On-Demand repair queue
- generic worker job definition that explicitly runs `spotbatch worker`
- IAM roles for Batch/ECS/worker task
- optional CloudWatch dashboard and baseline alarms

## Example

```hcl
project_name     = "my-spotbatch"
aws_region       = "us-west-2"
worker_image_uri  = "ACCOUNT.dkr.ecr.us-west-2.amazonaws.com/my-spotbatch-worker:latest"
worker_s3_bucket  = "my-work-bucket"
worker_s3_prefixes = ["runs/hello-001"]
max_vcpus_spot      = 256
alarm_sns_topic_arns = ["arn:aws:sns:us-west-2:ACCOUNT:spotbatch-alerts"]
```

```bash
tofu init
tofu plan -var-file=example.tfvars
tofu apply -var-file=example.tfvars
```

## Notes

- Default Spot allocation strategy is `SPOT_PRICE_CAPACITY_OPTIMIZED`.
- The worker task role is scoped to the work queue plus `worker_s3_bucket`/`worker_s3_prefixes`. Set prefixes to the run roots that contain inputs, outputs, summaries, logs, and done markers.
- The job definition injects matching `SPOTBATCH_ALLOWED_S3_PREFIXES` so workers reject task payloads that reference S3 URIs outside the configured prefixes.
- `create_observability` defaults to true and creates a CloudWatch dashboard plus alarms for work-queue age, DLQ depth, Batch failures, and runnable-job stalls. Set `alarm_sns_topic_arns` to wire notifications.
- The dashboard includes a Logs Insights widget over structured `spotbatch.worker_event.v1` events emitted by the worker.
- The reliability contract depends on SQS visibility timeout + deterministic S3 done markers, not Batch retries.
- For S3 buckets with versioning enabled, pair run prefixes with lifecycle rules that expire noncurrent versions/delete markers, or use `spotbatch s3-delete-prefix --include-versions` for explicit teardown. Deleting current objects only is not a complete cost cleanup on versioned buckets.
