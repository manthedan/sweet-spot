# SweetSpot v2 infrastructure

One managed Batch compute environment and queue per lane, a worker role, an
optional operator policy, a log group, and an IMDSv2 launch template. The CLI
owns each run's SQS queue/DLQ and registers a job definition for each launch;
this module manages neither. No dashboard or alarms are created. An optional
account-wide monthly budget alert remains.

Example `stack.tfvars`:

```hcl
project_name = "sweetspot"
aws_region   = "us-west-2"
bucket_name  = "my-existing-bucket"
runs_prefix  = "sweetspot"
subnet_ids   = ["subnet-aaa", "subnet-bbb"]
extra_input_uris = ["s3://my-input-bucket/shards/"]
# create_bucket = true  # optional: private, encrypted bucket; false by default
# monthly_budget_limit_usd = 500
# budget_notification_emails = ["ops@example.com"]
```

```bash
tofu init -lockfile=readonly
tofu fmt -check
tofu validate
tofu plan -var-file=stack.tfvars
# Review the plan and approve the cloud changes before applying it.
tofu apply -var-file=stack.tfvars
tofu output -raw sweetspot_config > ../../sweetspot.json
tofu output -raw operator_policy_arn
```

The operator policy is created by default but must be attached to the operator's
identity by an administrator. It permits run queue operations on `<project>-*`,
S3 read/write/list within `runs_prefix`, registration and submission using this
project's job definitions and lane queues, and the two EC2 scout reads. Batch
list/describe APIs require `Resource="*"`; these reads are region constrained.
`sqs:SendMessage` covers `SendMessageBatch` too. The CLI must tag both registered
job definitions and submitted jobs with `sweetspot-project=<project_name>` (in
addition to `sweetspot-run`); job termination requires that project tag.
Terminate the array parent to terminate its children. `iam:PassRole` permits only
the worker role with `iam:PassedToService=ecs-tasks.amazonaws.com`, the final
service assuming it. See [Batch IAM actions/resources](https://docs.aws.amazon.com/service-authorization/latest/reference/list_batch.html)
and [PassedToService semantics](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_iam-condition-keys.html#ck_PassedToService).
This policy covers the CLI, not infrastructure provisioning or pushing images.
It also covers running a worker locally and `sweetspot local` with S3 prefixes, including reads from `extra_input_uris`.

Batch ownership limitation: tagged `SubmitJob` needs `batch:TagResource`, with
no documented create-only condition. Job ARNs contain job IDs, not job names,
so a project job-name ARN pattern cannot constrain termination. The policy
denies retagging jobs already tagged for another `sweetspot-project`, but an
operator can still tag any **untagged** regional/account job with this project
and then terminate it (or terminate foreign jobs already carrying this tag).
This is not a complete isolation boundary; use a separate AWS account or an
administrator-controlled launch/termination service when that is required.

The worker role receives/deletes/extends visibility on `<project>-*`, and reads
and lists the run tree. It writes only `attempts/`, `done/`, `out/`, `failures/`
within each run; deletes only `attempts/`; and requires `If-None-Match: *` on
done-marker creates via [S3's `s3:if-none-match` condition](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html).
It cannot write `run.json`, `tasks.jsonl`, `READY`, `manifest.jsonl`, `launches/`,
`enqueues/`, or `control/`; those are operator control/publication records.
Both roles read extra input objects, but only the operator lists those prefixes.
The operator keeps full run-tree access for cloud and local operations.
An existing bucket is the default.
Encrypted inputs using a customer KMS key require a separate key grant/policy.
Keep attempt outputs until `finish` has repaired promotions and published READY.

The default lane is x86 with c7a/c6a/c7i/m7a medium through xlarge types and
`SPOT_PRICE_CAPACITY_OPTIMIZED`. ARM is opt-in so existing x86 images work by
default. To enable it, replace `lanes` with both lanes (map values replace the
default rather than merging):

```hcl
lanes = {
  x86 = {
    instance_types = ["c7a.medium", "c7a.large", "c6a.large", "c7i.large", "m7a.large"]
    arch = "x86_64"
    max_vcpus = 256
  }
  arm = {
    instance_types = ["c7g.medium", "c6g.medium", "c7g.large", "c6g.large", "m7g.medium", "m7g.large", "m7g.xlarge"]
    arch = "arm64"
    max_vcpus = 64
  }
}
default_lane = "x86"
```

Use an ARM image or a verified multi-arch image before submitting to `arm`.
GPU lanes set `gpu = true` (default false), e.g. `instance_types = ["g5.xlarge"]`
with `arch = "x86_64"`, to select `ECS_AL2023_NVIDIA`; other lanes use `ECS_AL2023`.
The flag is exported per lane in `sweetspot_config`; the CLI must reject GPU
workers on non-GPU lanes. Validation checks g/p family names and excludes
g3/p3, AMD g4ad and ARM g5g; verify the chosen types provide NVIDIA GPUs and are
available in your region. [Batch's AMI contract](https://docs.aws.amazon.com/batch/latest/APIReference/API_Ec2Configuration.html)
excludes g3/p3 and Graviton from this NVIDIA image. Scout uses the EC2-reported
GPU count to constrain packing and skips types that cannot fit a GPU worker.
An on-demand lane uses `type="EC2"`; its default strategy is
`BEST_FIT_PROGRESSIVE`. Specify concrete instance types of a single architecture
per lane so scout can price them. `max_vcpus` is a scaling target; Batch can exceed
it by one instance with these allocation strategies. Scout's 512 MiB per-host
reserve is a conservative estimate for small instances, not the AMI's configured
reservation; start a 2 GiB host's worker at 1536 MiB and verify telemetry.
Actual [ECS-registered memory](https://docs.aws.amazon.com/batch/latest/userguide/memory-management.html)
is below EC2's nominal memory.

IMDSv2 is required. `imds_hop_limit=2` lets containers fetch instance type/AZ for
telemetry, but also makes the host instance-role credentials accessible to
container code; use trusted workloads and keep that role limited to ECS duties.
Set it to 1 to block container IMDS access, accepting missing host telemetry.
The encrypted gp3 root disk defaults to 50 GiB (`batch_root_volume_size_gib`):
the container image, every packed worker's inputs/scratch/staged outputs, and
logs share this disk. Size for their combined peak, not one worker's footprint.

Provide explicit subnets with outbound ECS/image/S3/SQS/log access for production.
Otherwise the default/selected VPC's subnets are discovered. Empty security group
IDs create a no-ingress group. This is a v2 schema replacement: old tfvars and
outputs are removed; an existing v1 state can propose resource replacements and
queue/job-definition deletion. Review that migration separately from a new stack.
