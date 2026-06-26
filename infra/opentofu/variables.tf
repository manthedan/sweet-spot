variable "project_name" {
  type        = string
  description = "Name prefix for AWS resources."
  default     = "sweetspot"
}

variable "aws_region" {
  type        = string
  description = "AWS region."
}

variable "vpc_id" {
  type        = string
  description = "VPC id. If empty, default VPC is used."
  default     = ""
}

variable "subnet_ids" {
  type        = list(string)
  description = "Subnet ids. If empty, default VPC subnets are used."
  default     = []
}

variable "security_group_ids" {
  type        = list(string)
  description = "Security groups for Batch instances. If empty, a no-ingress security group is created by default."
  default     = []
}

variable "create_no_ingress_security_group" {
  type        = bool
  description = "Create and use a dedicated no-ingress security group when security_group_ids is empty. Set false only if you intentionally want the VPC default security group."
  default     = true
}

variable "require_explicit_subnets" {
  type        = bool
  description = "Fail planning when subnet_ids is empty instead of discovering all subnets in the selected/default VPC. Recommended for production."
  default     = false
}

variable "worker_image_uri" {
  type        = string
  description = "ECR image URI for the generic sweetspot worker."
}

variable "worker_s3_bucket" {
  type        = string
  description = "S3 bucket containing worker inputs, outputs, summaries, logs, and done markers. Use a bucket name, not an s3:// URI."
}

variable "worker_s3_prefixes" {
  type        = list(string)
  description = "Object key prefixes within worker_s3_bucket that workers may read/write. Empty list means the whole bucket."
  default     = []
}

variable "worker_allowed_s3_prefixes" {
  type        = list(string)
  description = "Optional s3:// prefixes injected into SWEETSPOT_ALLOWED_S3_PREFIXES for runtime task validation. Defaults to worker_s3_prefixes in worker_s3_bucket."
  default     = []
}

variable "worker_kms_key_arns" {
  type        = list(string)
  description = "Optional KMS key ARNs workers need for SSE-KMS encrypted S3 inputs/outputs/logs/done markers. The key policy must also allow the worker role."
  default     = []
}

variable "worker_vcpus" {
  type    = number
  default = 2
}

variable "worker_memory_mib" {
  type    = number
  default = 4096
}

variable "max_vcpus_spot" {
  type    = number
  default = 256
}

variable "max_vcpus_spot_arm" {
  type        = number
  description = "Maximum vCPUs for the optional ARM/Graviton Spot compute environment. Used only when create_arm_spot_queue=true."
  default     = 64
}

variable "max_vcpus_ondemand" {
  type    = number
  default = 16
}

variable "create_ondemand_queue" {
  type    = bool
  default = true
}

variable "spot_allocation_strategy" {
  type        = string
  description = "AWS Batch Spot allocation strategy. SPOT_PRICE_CAPACITY_OPTIMIZED is usually best for broad retryable CPU work."
  default     = "SPOT_PRICE_CAPACITY_OPTIMIZED"
}

variable "spot_bid_percentage" {
  type    = number
  default = 100
}

variable "spot_instance_types" {
  type        = list(string)
  description = "Broad x86-compatible instance list for the default Spot queue. Keep this x86-only unless your worker image and dependencies are verified multi-arch."
  default = [
    # c7a.medium is a cheap 1 vCPU / 2 GiB x86 lane. Workers on 2 GiB
    # instances must request less than full host memory, for example 1536 MiB.
    "c7a.medium",
    "c5.large", "c5.xlarge", "c5.2xlarge", "c5.4xlarge",
    "c6i.large", "c6i.xlarge", "c6i.2xlarge", "c6i.4xlarge",
    "c6a.large", "c6a.xlarge", "c6a.2xlarge", "c6a.4xlarge",
    "c7i.large", "c7i.xlarge", "c7i.2xlarge", "c7i.4xlarge",
    "m6i.large", "m6i.xlarge", "m6i.2xlarge", "m6i.4xlarge",
    "m6a.large", "m6a.xlarge", "m6a.2xlarge", "m6a.4xlarge",
    "m7i.large", "m7i.xlarge", "m7i.2xlarge", "m7i.4xlarge",
  ]
}

variable "create_arm_spot_queue" {
  type        = bool
  description = "Create a separate opt-in ARM/Graviton Spot compute environment, queue, and worker job definition. Keep false until a canary proves image/dependency compatibility."
  default     = false
}

variable "spot_arm_instance_types" {
  type        = list(string)
  description = "Broad ARM/Graviton instance list for the optional ARM Spot queue. Used only when create_arm_spot_queue=true."
  default = [
    # Medium Graviton lanes are often the cheapest useful first canaries.
    # Use a multi-arch/ARM-verified image and reserve less than 2 GiB memory.
    "c7g.medium", "c6g.medium",
    "c6g.large", "c6g.xlarge", "c6g.2xlarge", "c6g.4xlarge",
    "c7g.large", "c7g.xlarge", "c7g.2xlarge", "c7g.4xlarge",
    "m6g.large", "m6g.xlarge", "m6g.2xlarge", "m6g.4xlarge",
    "m7g.large", "m7g.xlarge", "m7g.2xlarge", "m7g.4xlarge",
  ]
}

variable "worker_image_uri_arm" {
  type        = string
  description = "Optional ARM-specific worker image URI. Empty means reuse worker_image_uri, which must then be a verified multi-arch image before create_arm_spot_queue is enabled."
  default     = ""
}

variable "sqs_visibility_timeout_seconds" {
  type    = number
  default = 1800
}

variable "sqs_message_retention_seconds" {
  type        = number
  description = "Source work-queue message retention. Keep shorter than sqs_dlq_message_retention_seconds so failed messages remain inspectable after redrive."
  default     = 1123200
}

variable "sqs_dlq_message_retention_seconds" {
  type        = number
  description = "DLQ retention. Defaults to the SQS maximum and should exceed source queue retention."
  default     = 1209600
}

variable "sqs_max_receive_count" {
  type    = number
  default = 10
}

variable "create_observability" {
  type        = bool
  description = "Create a CloudWatch dashboard and baseline alarms for SQS/Batch health."
  default     = true
}

variable "alarm_sns_topic_arns" {
  type        = list(string)
  description = "SNS topic ARNs for alarm/OK actions. Empty list creates alarms without actions."
  default     = []
}

variable "alarm_evaluation_periods" {
  type        = number
  description = "Evaluation periods for baseline CloudWatch alarms."
  default     = 5
}

variable "queue_age_alarm_seconds" {
  type        = number
  description = "Alarm threshold for ApproximateAgeOfOldestMessage on the work queue."
  default     = 1800
}

variable "dlq_depth_alarm_threshold" {
  type        = number
  description = "Alarm threshold for visible messages in the DLQ."
  default     = 1
}

variable "batch_failed_jobs_alarm_threshold" {
  type        = number
  description = "Alarm threshold for AWS Batch failed jobs on the Spot queue."
  default     = 1
}

variable "runnable_jobs_alarm_threshold" {
  type        = number
  description = "Alarm threshold for runnable AWS Batch jobs on the Spot queue."
  default     = 1
}

variable "runnable_jobs_alarm_evaluation_periods" {
  type        = number
  description = "Evaluation periods before alarming on stalled runnable AWS Batch jobs."
  default     = 30
}

variable "batch_root_device_name" {
  type        = string
  description = "Root block device name used by the Batch ECS-optimized AMI launch template."
  default     = "/dev/xvda"
}

variable "batch_root_volume_size_gib" {
  type        = number
  description = "Encrypted gp3 root volume size for Batch EC2 instances."
  default     = 30
}

variable "ebs_kms_key_id" {
  type        = string
  description = "Optional KMS key ID/ARN for encrypted Batch instance root volumes. Empty uses the account default EBS key."
  default     = ""
}

variable "monthly_budget_limit_usd" {
  type        = number
  description = "Optional AWS Budgets monthly cost ceiling. Set to 0 to disable. Budgets are account-scoped unless you customize outside this module."
  default     = 0
}

variable "monthly_budget_alarm_threshold_percent" {
  type        = number
  description = "Budget alert threshold as a percentage of monthly_budget_limit_usd."
  default     = 80
}

variable "budget_notification_emails" {
  type        = list(string)
  description = "Email subscribers for the optional monthly budget alert."
  default     = []
}

variable "cost_tags" {
  type        = map(string)
  description = "Additional cost-allocation tags merged onto resources after the module's default tags."
  default     = {}
}

variable "tags" {
  type    = map(string)
  default = {}
}
