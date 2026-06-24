resource "aws_cloudwatch_dashboard" "sweetspot" {
  count          = var.create_observability ? 1 : 0
  dashboard_name = "${var.project_name}-sweetspot"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title  = "SQS work queue depth and age"
          region = var.aws_region
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.work.name, { label = "work visible" }],
            [".", "ApproximateNumberOfMessagesNotVisible", ".", ".", { label = "work in-flight" }],
            [".", "ApproximateAgeOfOldestMessage", ".", ".", { label = "oldest age seconds", yAxis = "right" }]
          ]
          stat   = "Maximum"
          period = 60
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "DLQ depth"
          region  = var.aws_region
          metrics = [["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.dlq.name, { label = "dlq visible" }]]
          stat    = "Maximum"
          period  = 60
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "AWS Batch job states (best-effort namespace metrics)"
          region = var.aws_region
          metrics = concat(
            [
              ["AWS/Batch", "FailedJobs", "JobQueue", aws_batch_job_queue.spot.name, { label = "x86 spot failed" }],
              [".", "RunnableJobs", ".", ".", { label = "x86 spot runnable" }],
              [".", "RunningJobs", ".", ".", { label = "x86 spot running" }]
            ],
            var.create_arm_spot_queue ? [
              ["AWS/Batch", "FailedJobs", "JobQueue", aws_batch_job_queue.spot_arm[0].name, { label = "arm64 spot failed" }],
              [".", "RunnableJobs", ".", ".", { label = "arm64 spot runnable" }],
              [".", "RunningJobs", ".", ".", { label = "arm64 spot running" }]
            ] : []
          )
          stat   = "Maximum"
          period = 60
        }
      },
      {
        type   = "log"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "Recent structured worker events"
          region = var.aws_region
          query  = "SOURCE '${aws_cloudwatch_log_group.batch.name}' | fields @timestamp, @message | filter @message like /sweetspot.worker_event.v1/ | sort @timestamp desc | limit 50"
          view   = "table"
        }
      }
    ]
  })
}

resource "aws_cloudwatch_metric_alarm" "work_queue_age" {
  count               = var.create_observability ? 1 : 0
  alarm_name          = "${var.project_name}-work-queue-oldest-message-age"
  alarm_description   = "Oldest visible SweetSpot work-queue message age is above the configured threshold. Workers may be stalled or underprovisioned."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  dimensions          = { QueueName = aws_sqs_queue.work.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = var.alarm_evaluation_periods
  threshold           = var.queue_age_alarm_seconds
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags
}

resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  count               = var.create_observability ? 1 : 0
  alarm_name          = "${var.project_name}-dlq-depth"
  alarm_description   = "SweetSpot DLQ has visible messages. Inspect with `sweetspot dlq` before submitting more workers."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = var.dlq_depth_alarm_threshold
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags
}

resource "aws_cloudwatch_metric_alarm" "batch_failed_jobs" {
  count               = var.create_observability ? 1 : 0
  alarm_name          = "${var.project_name}-batch-failed-jobs"
  alarm_description   = "AWS Batch reports failed jobs for the Spot queue. Check worker structured events and CloudWatch logs."
  namespace           = "AWS/Batch"
  metric_name         = "FailedJobs"
  dimensions          = { JobQueue = aws_batch_job_queue.spot.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = var.alarm_evaluation_periods
  threshold           = var.batch_failed_jobs_alarm_threshold
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags
}

resource "aws_cloudwatch_metric_alarm" "batch_runnable_jobs" {
  count               = var.create_observability ? 1 : 0
  alarm_name          = "${var.project_name}-batch-runnable-jobs"
  alarm_description   = "AWS Batch runnable jobs remain above threshold. This can indicate insufficient capacity, invalid compute resources, or Spot scarcity."
  namespace           = "AWS/Batch"
  metric_name         = "RunnableJobs"
  dimensions          = { JobQueue = aws_batch_job_queue.spot.name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = var.runnable_jobs_alarm_evaluation_periods
  threshold           = var.runnable_jobs_alarm_threshold
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags
}

resource "aws_cloudwatch_metric_alarm" "batch_arm_failed_jobs" {
  count               = var.create_observability && var.create_arm_spot_queue ? 1 : 0
  alarm_name          = "${var.project_name}-batch-arm64-failed-jobs"
  alarm_description   = "AWS Batch reports failed jobs for the optional ARM/Graviton Spot queue. Check canary compatibility, worker structured events, and CloudWatch logs."
  namespace           = "AWS/Batch"
  metric_name         = "FailedJobs"
  dimensions          = { JobQueue = aws_batch_job_queue.spot_arm[0].name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = var.alarm_evaluation_periods
  threshold           = var.batch_failed_jobs_alarm_threshold
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags
}

resource "aws_cloudwatch_metric_alarm" "batch_arm_runnable_jobs" {
  count               = var.create_observability && var.create_arm_spot_queue ? 1 : 0
  alarm_name          = "${var.project_name}-batch-arm64-runnable-jobs"
  alarm_description   = "AWS Batch runnable jobs remain above threshold on the optional ARM/Graviton Spot queue. This can indicate insufficient capacity, invalid compute resources, image incompatibility, or Spot scarcity."
  namespace           = "AWS/Batch"
  metric_name         = "RunnableJobs"
  dimensions          = { JobQueue = aws_batch_job_queue.spot_arm[0].name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = var.runnable_jobs_alarm_evaluation_periods
  threshold           = var.runnable_jobs_alarm_threshold
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.alarm_sns_topic_arns
  ok_actions          = var.alarm_sns_topic_arns
  tags                = local.tags
}
