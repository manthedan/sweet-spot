output "sqs_work_queue_url" {
  value = aws_sqs_queue.work.url
}

output "sqs_dlq_url" {
  value = aws_sqs_queue.dlq.url
}

output "batch_spot_queue" {
  value = aws_batch_job_queue.spot.arn
}

output "batch_ondemand_queue" {
  value = var.create_ondemand_queue ? aws_batch_job_queue.ondemand[0].arn : null
}

output "worker_job_definition" {
  value = aws_batch_job_definition.worker.arn
}

output "worker_task_role_arn" {
  value = aws_iam_role.worker_task.arn
}

output "cloudwatch_dashboard_name" {
  value = var.create_observability ? aws_cloudwatch_dashboard.sweetspot[0].dashboard_name : null
}

output "cloudwatch_log_group" {
  value = aws_cloudwatch_log_group.batch.name
}

output "batch_security_group_ids" {
  value = local.security_group_ids
}

output "batch_launch_template_id" {
  value = aws_launch_template.batch.id
}

output "monthly_budget_name" {
  value = var.monthly_budget_limit_usd > 0 ? aws_budgets_budget.monthly[0].name : null
}
