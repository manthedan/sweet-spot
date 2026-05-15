variable "project_name" {
  type        = string
  description = "Name prefix for AWS resources."
  default     = "spotbatch"
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
  description = "Security groups for Batch instances. If empty, default VPC SG is used."
  default     = []
}

variable "worker_image_uri" {
  type        = string
  description = "ECR image URI for the generic spotbatch worker."
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
  description = "Broad compatible instance list for Spot."
  default = [
    "c5.large", "c5.xlarge", "c5.2xlarge", "c5.4xlarge",
    "c6i.large", "c6i.xlarge", "c6i.2xlarge", "c6i.4xlarge",
    "c6a.large", "c6a.xlarge", "c6a.2xlarge", "c6a.4xlarge",
    "c7i.large", "c7i.xlarge", "c7i.2xlarge", "c7i.4xlarge",
    "m6i.large", "m6i.xlarge", "m6i.2xlarge", "m6i.4xlarge",
    "m6a.large", "m6a.xlarge", "m6a.2xlarge", "m6a.4xlarge",
    "m7i.large", "m7i.xlarge", "m7i.2xlarge", "m7i.4xlarge",
  ]
}

variable "sqs_visibility_timeout_seconds" {
  type    = number
  default = 1800
}

variable "sqs_message_retention_seconds" {
  type    = number
  default = 1209600
}

variable "sqs_max_receive_count" {
  type    = number
  default = 10
}

variable "tags" {
  type    = map(string)
  default = {}
}
