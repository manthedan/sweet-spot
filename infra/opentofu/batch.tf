resource "aws_cloudwatch_log_group" "batch" {
  name              = "/aws/batch/${var.project_name}"
  retention_in_days = 14
  tags              = local.tags
}

resource "aws_batch_compute_environment" "spot" {
  compute_environment_name = "${var.project_name}-cpu-spot"
  type                     = "MANAGED"
  service_role             = aws_iam_role.batch_service.arn

  compute_resources {
    type                = "SPOT"
    allocation_strategy = var.spot_allocation_strategy
    bid_percentage      = var.spot_bid_percentage
    min_vcpus           = 0
    max_vcpus           = var.max_vcpus_spot
    desired_vcpus       = 0
    instance_type       = var.spot_instance_types
    subnets             = local.subnet_ids
    security_group_ids  = local.security_group_ids
    instance_role       = aws_iam_instance_profile.ecs_instance.arn

    launch_template {
      launch_template_id = aws_launch_template.batch.id
      version            = "$Latest"
    }
  }

  tags = local.tags

  lifecycle {
    precondition {
      condition     = !var.require_explicit_subnets || length(var.subnet_ids) > 0
      error_message = "require_explicit_subnets=true requires non-empty subnet_ids."
    }
    precondition {
      condition     = var.sqs_dlq_message_retention_seconds > var.sqs_message_retention_seconds
      error_message = "sqs_dlq_message_retention_seconds must be greater than sqs_message_retention_seconds."
    }
  }
}

resource "aws_batch_job_queue" "spot" {
  name     = "${var.project_name}-cpu-spot-queue"
  state    = "ENABLED"
  priority = 100
  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.spot.arn
  }
  tags = local.tags
}

resource "aws_batch_compute_environment" "ondemand" {
  count                    = var.create_ondemand_queue ? 1 : 0
  compute_environment_name = "${var.project_name}-cpu-ondemand"
  type                     = "MANAGED"
  service_role             = aws_iam_role.batch_service.arn

  compute_resources {
    type               = "EC2"
    min_vcpus          = 0
    max_vcpus          = var.max_vcpus_ondemand
    desired_vcpus      = 0
    instance_type      = ["optimal"]
    subnets            = local.subnet_ids
    security_group_ids = local.security_group_ids
    instance_role      = aws_iam_instance_profile.ecs_instance.arn

    launch_template {
      launch_template_id = aws_launch_template.batch.id
      version            = "$Latest"
    }
  }

  tags = local.tags
}

resource "aws_batch_job_queue" "ondemand" {
  count    = var.create_ondemand_queue ? 1 : 0
  name     = "${var.project_name}-cpu-ondemand-queue"
  state    = "ENABLED"
  priority = 10
  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.ondemand[0].arn
  }
  tags = local.tags
}

resource "aws_batch_job_definition" "worker" {
  name = "${var.project_name}-worker"
  type = "container"

  container_properties = jsonencode({
    image      = var.worker_image_uri
    vcpus      = var.worker_vcpus
    memory     = var.worker_memory_mib
    jobRoleArn = aws_iam_role.worker_task.arn
    command    = ["sweetspot", "worker"]
    environment = [
      { name = "SWEETSPOT_SQS_QUEUE_URL", value = aws_sqs_queue.work.url },
      { name = "SWEETSPOT_MAX_MESSAGES", value = "1" },
      { name = "SWEETSPOT_ALLOWED_S3_PREFIXES", value = join(",", local.worker_allowed_s3_prefixes_effective) }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.batch.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "sweetspot"
      }
    }
  })

  tags = local.tags
}
