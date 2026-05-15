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
  count  = length(var.security_group_ids) == 0 ? 1 : 0
  vpc_id = data.aws_vpc.selected.id
  name   = "default"
}

locals {
  subnet_ids         = length(var.subnet_ids) > 0 ? var.subnet_ids : data.aws_subnets.selected[0].ids
  security_group_ids = length(var.security_group_ids) > 0 ? var.security_group_ids : [data.aws_security_group.default[0].id]
  tags               = merge(var.tags, { Project = var.project_name, ManagedBy = "opentofu" })
}
