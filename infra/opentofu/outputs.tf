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
