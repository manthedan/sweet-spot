resource "aws_security_group" "batch_no_ingress" {
  count       = var.create_no_ingress_security_group && length(var.security_group_ids) == 0 ? 1 : 0
  name_prefix = "${var.project_name}-batch-no-ingress-"
  description = "No-ingress security group for SweetSpot Batch instances"
  vpc_id      = data.aws_vpc.selected.id

  egress {
    description      = "Outbound access for ECS agent, ECR, S3, SQS, and CloudWatch Logs"
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = merge(local.tags, { Name = "${var.project_name}-batch-no-ingress" })
}

resource "aws_launch_template" "batch" {
  name_prefix            = "${var.project_name}-batch-"
  update_default_version = true

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  block_device_mappings {
    device_name = var.batch_root_device_name
    ebs {
      delete_on_termination = true
      encrypted             = true
      kms_key_id            = var.ebs_kms_key_id != "" ? var.ebs_kms_key_id : null
      volume_size           = var.batch_root_volume_size_gib
      volume_type           = "gp3"
    }
  }

  tag_specifications {
    resource_type = "instance"
    tags          = merge(local.tags, { Name = "${var.project_name}-batch" })
  }

  tag_specifications {
    resource_type = "volume"
    tags          = local.tags
  }

  tags = local.tags
}

