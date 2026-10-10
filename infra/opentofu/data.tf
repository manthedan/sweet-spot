data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

data "aws_vpc" "selected" {
  default = var.vpc_id == "" ? true : null
  id      = var.vpc_id != "" ? var.vpc_id : null
}

data "aws_subnets" "selected" {
  count = length(var.subnet_ids) == 0 ? 1 : 0
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.selected.id]
  }
}

locals {
  partition          = data.aws_partition.current.partition
  account            = data.aws_caller_identity.current.account_id
  subnet_ids         = length(var.subnet_ids) > 0 ? var.subnet_ids : data.aws_subnets.selected[0].ids
  security_group_ids = length(var.security_group_ids) > 0 ? var.security_group_ids : [aws_security_group.batch_no_ingress[0].id]
  tags               = merge(var.tags, { Project = var.project_name, ManagedBy = "opentofu" })
  runs_key           = trim(var.runs_prefix, "/")
  bucket_arn         = "arn:${local.partition}:s3:::${var.bucket_name}"
  run_objects        = "${local.bucket_arn}/${local.runs_key}/*"
  queue_arn          = "arn:${local.partition}:sqs:${var.aws_region}:${local.account}:${var.project_name}-*"
  batch_arn          = "arn:${local.partition}:batch:${var.aws_region}:${local.account}"
  inputs = { for uri in distinct(var.extra_input_uris) : uri => {
    bucket = split("/", trimprefix(uri, "s3://"))[0]
    key    = trim(join("/", slice(split("/", trimprefix(uri, "s3://")), 1, length(split("/", trimprefix(uri, "s3://"))))), "/")
  } }
}
