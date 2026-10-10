resource "aws_iam_role" "batch_service" {
  name = "${var.project_name}-batch-service"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "batch.amazonaws.com" }
  }] })
  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "batch_service" {
  role       = aws_iam_role.batch_service.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSBatchServiceRole"
}

resource "aws_iam_role" "ecs_instance" {
  name = "${var.project_name}-ecs-instance"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "ec2.amazonaws.com" }
  }] })
  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "ecs_instance" {
  role       = aws_iam_role.ecs_instance.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_instance_profile" "ecs_instance" {
  name = "${var.project_name}-ecs-instance"
  role = aws_iam_role.ecs_instance.name
  tags = local.tags
}

resource "aws_iam_role" "spot_fleet" {
  name = "${var.project_name}-spot-fleet"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "spotfleet.amazonaws.com" }
  }] })
  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "spot_fleet" {
  role       = aws_iam_role.spot_fleet.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole"
}

resource "aws_iam_role" "worker_task" {
  name = "${var.project_name}-worker"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "ecs-tasks.amazonaws.com" }
  }] })
  tags = local.tags
}

# Both roles read and list the run tree; only the operator controls it.
data "aws_iam_policy_document" "runs_read" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [local.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [local.runs_key, "${local.runs_key}/*"]
    }
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = [local.run_objects]
  }
}

data "aws_iam_policy_document" "runs" {
  source_policy_documents = [data.aws_iam_policy_document.runs_read.json]
  statement {
    actions   = ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [local.run_objects]
  }
}

data "aws_iam_policy_document" "inputs_read" {
  dynamic "statement" {
    for_each = local.inputs
    content {
      actions = ["s3:GetObject"]
      resources = statement.value.key == "" ? ["arn:${local.partition}:s3:::${statement.value.bucket}/*"] : [
        "arn:${local.partition}:s3:::${statement.value.bucket}/${statement.value.key}",
        "arn:${local.partition}:s3:::${statement.value.bucket}/${statement.value.key}/*"
      ]
    }
  }
}

data "aws_iam_policy_document" "worker" {
  source_policy_documents = [data.aws_iam_policy_document.runs_read.json, data.aws_iam_policy_document.inputs_read.json]
  statement {
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes"]
    resources = [local.queue_arn]
  }
  statement {
    actions = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = [for directory in ["attempts", "out", "failures"] :
      "${local.bucket_arn}/${local.runs_key}/*/${directory}/*"
    ]
  }
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${local.bucket_arn}/${local.runs_key}/*/done/*"]
    condition {
      test     = "StringEquals"
      variable = "s3:if-none-match"
      values   = ["*"] # Markers use a single conditional PutObject, never multipart or CopyObject.
    }
  }
  statement {
    actions   = ["s3:DeleteObject"]
    resources = ["${local.bucket_arn}/${local.runs_key}/*/attempts/*"]
  }
}

resource "aws_iam_role_policy" "worker" {
  name   = "${var.project_name}-worker"
  role   = aws_iam_role.worker_task.id
  policy = data.aws_iam_policy_document.worker.json
}

data "aws_iam_policy_document" "operator" {
  source_policy_documents = [data.aws_iam_policy_document.runs.json, data.aws_iam_policy_document.inputs_read.json]
  statement {
    actions = ["sqs:CreateQueue", "sqs:DeleteQueue", "sqs:GetQueueUrl", "sqs:GetQueueAttributes", "sqs:SetQueueAttributes",
    "sqs:SendMessage", "sqs:TagQueue", "sqs:ListQueueTags", "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"] # SendMessage also authorizes SendMessageBatch.
    resources = [local.queue_arn]
  }
  dynamic "statement" {
    for_each = local.inputs
    content {
      actions   = ["s3:ListBucket"]
      resources = ["arn:${local.partition}:s3:::${statement.value.bucket}"]
      condition {
        test     = "StringLike"
        variable = "s3:prefix"
        values   = statement.value.key == "" ? ["*"] : [statement.value.key, "${statement.value.key}/*"]
      }
    }
  }
  statement {
    actions   = ["batch:RegisterJobDefinition"]
    resources = ["${local.batch_arn}:job-definition/${var.project_name}-*"]
  }
  statement {
    actions   = ["batch:DeregisterJobDefinition"]
    resources = ["${local.batch_arn}:job-definition/${var.project_name}-*:*"]
  }
  statement {
    actions = ["batch:SubmitJob"]
    resources = concat([for queue in aws_batch_job_queue.lane : queue.arn], [
      "${local.batch_arn}:job-definition/${var.project_name}-*:*", "${local.batch_arn}:job/*"
    ])
    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/sweetspot-project"
      values   = [var.project_name]
    }
  }
  statement {
    actions   = ["batch:TagResource"]
    resources = ["${local.batch_arn}:job-definition/${var.project_name}-*:*", "${local.batch_arn}:job/*"]
    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/sweetspot-project"
      values   = [var.project_name]
    }
  }
  # TagResource is required for tagged SubmitJob; Batch has no create-only condition.
  # Block adoption of jobs tagged for another project, but untagged jobs remain adoptable.
  # Job ARNs contain IDs, not names: see README for the residual isolation limit.
  statement {
    effect    = "Deny"
    actions   = ["batch:TagResource"]
    resources = ["${local.batch_arn}:job/*"]
    condition {
      test     = "Null"
      variable = "aws:ResourceTag/sweetspot-project"
      values   = ["false"]
    }
    condition {
      test     = "StringNotEquals"
      variable = "aws:ResourceTag/sweetspot-project"
      values   = [var.project_name]
    }
  }
  statement {
    actions   = ["batch:TerminateJob"]
    resources = ["${local.batch_arn}:job/*"]
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/sweetspot-project"
      values   = [var.project_name]
    }
  }
  # These read APIs do not support resource-level authorization (including ListJobs).
  statement {
    actions   = ["batch:DescribeJobDefinitions", "batch:ListJobs", "batch:DescribeJobs", "ec2:DescribeSpotPriceHistory", "ec2:DescribeInstanceTypes"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "aws:RequestedRegion"
      values   = [var.aws_region]
    }
  }
  statement {
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.worker_task.arn]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["ecs-tasks.amazonaws.com"] # Final assuming service; Batch is the intermediary.
    }
  }
}

resource "aws_iam_policy" "operator" {
  count  = var.create_operator_policy ? 1 : 0
  name   = "${var.project_name}-operator"
  policy = data.aws_iam_policy_document.operator.json
  tags   = local.tags
}
