# OpenTofu AWS infra

Creates the AWS primitives used by `aws-spot-batch-runner`:

- SQS work queue + DLQ
- AWS Batch Spot compute environment + queue
- optional On-Demand repair queue
- generic worker job definition
- IAM roles for Batch/ECS/worker task

## Example

```hcl
project_name     = "my-spotbatch"
aws_region       = "us-west-2"
worker_image_uri = "ACCOUNT.dkr.ecr.us-west-2.amazonaws.com/my-spotbatch-worker:latest"
max_vcpus_spot   = 256
```

```bash
tofu init
tofu plan -var-file=example.tfvars
tofu apply -var-file=example.tfvars
```

## Notes

- Default Spot allocation strategy is `SPOT_PRICE_CAPACITY_OPTIMIZED`.
- The worker task role has broad S3 access in the starter module; narrow it for production if your buckets are known.
- The reliability contract depends on SQS visibility timeout + deterministic S3 done markers, not Batch retries.
