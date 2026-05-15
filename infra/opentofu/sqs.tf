resource "aws_sqs_queue" "dlq" {
  name                      = "${var.project_name}-work-dlq"
  message_retention_seconds = var.sqs_message_retention_seconds
  tags                      = local.tags
}

resource "aws_sqs_queue" "work" {
  name                       = "${var.project_name}-work"
  visibility_timeout_seconds = var.sqs_visibility_timeout_seconds
  message_retention_seconds  = var.sqs_message_retention_seconds
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.sqs_max_receive_count
  })
  tags = local.tags
}
