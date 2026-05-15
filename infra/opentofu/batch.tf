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
  }

  tags = local.tags
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
    command    = ["spotbatch", "worker"]
    environment = [
      { name = "SPOTBATCH_SQS_QUEUE_URL", value = aws_sqs_queue.work.url },
      { name = "SPOTBATCH_MAX_MESSAGES", value = "1" }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.batch.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "spotbatch"
      }
    }
  })

  tags = local.tags
}
