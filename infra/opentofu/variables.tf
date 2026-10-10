variable "project_name" {
  type        = string
  description = "Resource prefix; leaves room for a 48-character run name and -dlq in SQS's 80-character limit."
  default     = "sweetspot"
  validation {
    condition     = can(regex("^[A-Za-z0-9][A-Za-z0-9_-]{0,25}$", var.project_name))
    error_message = "project_name must be 1-26 letters, digits, underscores or hyphens, starting with a letter or digit."
  }
}

variable "aws_region" {
  type = string
}

variable "bucket_name" {
  type        = string
  description = "Existing bucket for runs, or globally unique name when create_bucket=true."
}

variable "runs_prefix" {
  type        = string
  default     = "sweetspot"
  description = "Nonempty S3 key prefix containing all run directories."
  validation {
    condition     = length(trim(var.runs_prefix, "/")) > 0 && !can(regex("[?*]", var.runs_prefix))
    error_message = "runs_prefix must be nonempty and cannot contain IAM wildcards."
  }
}

variable "create_bucket" {
  type    = bool
  default = false
}

variable "extra_input_uris" {
  type        = list(string)
  default     = []
  description = "Additional s3://bucket/prefix inputs; both roles read objects, only operators list them."
  validation {
    condition     = alltrue([for uri in var.extra_input_uris : can(regex("^s3://[a-z0-9][a-z0-9.-]+(/[^?*]*)?$", uri))])
    error_message = "extra_input_uris must be s3:// bucket/prefix URIs without IAM wildcards."
  }
}

variable "lanes" {
  type = map(object({
    instance_types      = list(string)
    arch                = string
    gpu                 = optional(bool, false)
    max_vcpus           = number
    type                = optional(string, "SPOT")
    allocation_strategy = optional(string)
  }))
  description = "One compute environment and queue per lane. ARM is opt-in; use only types matching the lane architecture."
  default = {
    x86 = {
      instance_types = ["c7a.medium", "c7a.large", "c7a.xlarge", "c6a.large", "c6a.xlarge", "c7i.large", "c7i.xlarge", "m7a.medium", "m7a.large", "m7a.xlarge"]
      arch           = "x86_64"
      max_vcpus      = 256
    }
  }
  validation {
    condition = length(var.lanes) > 0 && alltrue([for name, lane in var.lanes :
      can(regex("^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$", name)) && contains(["x86_64", "arm64"], lane.arch) &&
      lane.max_vcpus >= 1 && floor(lane.max_vcpus) == lane.max_vcpus && length(lane.instance_types) > 0 &&
      contains(["SPOT", "EC2"], lane.type) && (lane.allocation_strategy == null || contains(
        lane.type == "SPOT" ? ["SPOT_CAPACITY_OPTIMIZED", "SPOT_PRICE_CAPACITY_OPTIMIZED"] : ["BEST_FIT_PROGRESSIVE"],
      coalesce(lane.allocation_strategy, lane.type == "SPOT" ? "SPOT_PRICE_CAPACITY_OPTIMIZED" : "BEST_FIT_PROGRESSIVE")))
    ])
    error_message = "lanes need safe names, a supported arch/type, positive integer max_vcpus, explicit instance types, and a capacity strategy compatible with the type."
  }
  validation {
    condition = alltrue([for lane in values(var.lanes) : !lane.gpu || (lane.arch == "x86_64" && alltrue([
      for kind in lane.instance_types : can(regex("^[gp][0-9][a-z0-9]*\\.[a-z0-9]+$", kind)) && !can(regex("^([gp]3|g4ad|g5g)\\.", kind))
    ]))])
    error_message = "GPU lanes need x86_64 NVIDIA GPU (g/p family) types; g3/p3, AMD g4ad and ARM g5g are unsupported."
  }
}

variable "default_lane" {
  type    = string
  default = "x86"
}

variable "create_operator_policy" {
  type    = bool
  default = true
}

variable "vpc_id" {
  type        = string
  default     = ""
  description = "Empty selects the default VPC."
}

variable "subnet_ids" {
  type        = list(string)
  default     = []
  description = "Empty discovers subnets in the selected VPC; provide explicit subnets for production."
}

variable "security_group_ids" {
  type        = list(string)
  default     = []
  description = "Empty creates a no-ingress security group with outbound access."
}

variable "imds_hop_limit" {
  type        = number
  default     = 2
  description = "2 permits container telemetry and exposes the host instance role via IMDS; 1 blocks container access."
  validation {
    condition     = var.imds_hop_limit >= 1 && var.imds_hop_limit <= 64 && floor(var.imds_hop_limit) == var.imds_hop_limit
    error_message = "imds_hop_limit must be an integer from 1 to 64."
  }
}

variable "batch_root_volume_size_gib" {
  type        = number
  default     = 50
  description = "Encrypted gp3 root disk shared by the image, all packed workers' scratch, logs and staged outputs."
  validation {
    condition     = var.batch_root_volume_size_gib >= 30 && floor(var.batch_root_volume_size_gib) == var.batch_root_volume_size_gib
    error_message = "batch_root_volume_size_gib must be an integer of at least 30."
  }
}

variable "monthly_budget_limit_usd" {
  type        = number
  default     = 0
  description = "Optional account-wide monthly budget alert; zero disables."
}

variable "monthly_budget_alarm_threshold_percent" {
  type    = number
  default = 80
}

variable "budget_notification_emails" {
  type    = list(string)
  default = []
}

variable "tags" {
  type    = map(string)
  default = {}
}
