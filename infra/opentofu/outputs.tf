output "sweetspot_config" {
  description = "Write with tofu output -raw sweetspot_config > sweetspot.json."
  value = jsonencode({
    region       = var.aws_region
    prefix       = "s3://${var.bucket_name}/${local.runs_key}"
    name         = var.project_name
    job_role_arn = aws_iam_role.worker_task.arn
    log_group    = aws_cloudwatch_log_group.batch.name
    lanes = { for name, lane in var.lanes : name => {
      job_queue      = aws_batch_job_queue.lane[name].arn
      arch           = lane.arch
      gpu            = lane.gpu
      instance_types = lane.instance_types
    } }
    default_lane = var.default_lane
  })
  precondition {
    condition     = contains(keys(var.lanes), var.default_lane)
    error_message = "default_lane must name a configured lane."
  }
}

output "operator_policy_arn" {
  value = var.create_operator_policy ? aws_iam_policy.operator[0].arn : null
}
