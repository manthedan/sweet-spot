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

data "aws_security_group" "default" {
  count  = length(var.security_group_ids) == 0 && !var.create_no_ingress_security_group ? 1 : 0
  vpc_id = data.aws_vpc.selected.id
  name   = "default"
}

locals {
  subnet_ids         = length(var.subnet_ids) > 0 ? var.subnet_ids : data.aws_subnets.selected[0].ids
  security_group_ids = length(var.security_group_ids) > 0 ? var.security_group_ids : (var.create_no_ingress_security_group ? [aws_security_group.batch_no_ingress[0].id] : [data.aws_security_group.default[0].id])
  tags               = merge({ Project = var.project_name, ManagedBy = "opentofu", CostManagedBy = "sweetspot" }, var.tags, var.cost_tags)

  worker_s3_prefixes_normalized = length(var.worker_s3_prefixes) == 0 ? [""] : [for p in var.worker_s3_prefixes : trim(p, "/")]
  worker_s3_object_resources = [
    for p in local.worker_s3_prefixes_normalized :
    p == "" ? "arn:aws:s3:::${var.worker_s3_bucket}/*" : "arn:aws:s3:::${var.worker_s3_bucket}/${p}/*"
  ]
  worker_s3_list_prefixes = distinct(flatten([
    for p in local.worker_s3_prefixes_normalized :
    p == "" ? ["*"] : [p, "${p}/*"]
  ]))
  worker_allowed_s3_prefixes_effective = length(var.worker_allowed_s3_prefixes) > 0 ? var.worker_allowed_s3_prefixes : [
    for p in local.worker_s3_prefixes_normalized :
    p == "" ? "s3://${var.worker_s3_bucket}/" : "s3://${var.worker_s3_bucket}/${p}"
  ]
}
