resource "aws_cloudwatch_log_group" "batch" {
  name              = "/aws/batch/${var.project_name}"
  retention_in_days = 14
  tags              = local.tags
}

resource "aws_batch_compute_environment" "lane" {
  for_each                 = var.lanes
  compute_environment_name = "${var.project_name}-${each.key}"
  type                     = "MANAGED"
  service_role             = aws_iam_role.batch_service.arn

  compute_resources {
    type                = each.value.type
    allocation_strategy = coalesce(each.value.allocation_strategy, each.value.type == "SPOT" ? "SPOT_PRICE_CAPACITY_OPTIMIZED" : "BEST_FIT_PROGRESSIVE")
    spot_iam_fleet_role = each.value.type == "SPOT" ? aws_iam_role.spot_fleet.arn : null
    min_vcpus           = 0
    max_vcpus           = each.value.max_vcpus
    desired_vcpus       = 0
    instance_type       = each.value.instance_types
    subnets             = local.subnet_ids
    security_group_ids  = local.security_group_ids
    instance_role       = aws_iam_instance_profile.ecs_instance.arn
    ec2_configuration {
      image_type = each.value.gpu ? "ECS_AL2023_NVIDIA" : "ECS_AL2023"
    }
    launch_template {
      launch_template_id = aws_launch_template.batch.id
      version            = tostring(aws_launch_template.batch.latest_version)
    }
  }
  tags = local.tags
  depends_on = [aws_iam_role_policy_attachment.batch_service, aws_iam_role_policy_attachment.ecs_instance,
  aws_iam_role_policy_attachment.spot_fleet]
  lifecycle {
    ignore_changes = [compute_resources[0].desired_vcpus]
  }
}

resource "aws_batch_job_queue" "lane" {
  for_each = var.lanes
  name     = "${var.project_name}-${each.key}"
  state    = "ENABLED"
  priority = 100
  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.lane[each.key].arn
  }
  tags = local.tags
}
