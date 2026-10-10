resource "aws_security_group" "batch_no_ingress" {
  count       = length(var.security_group_ids) == 0 ? 1 : 0
  name_prefix = "${var.project_name}-batch-"
  description = "No ingress; outbound access for ECS, images, S3, SQS and logs"
  vpc_id      = data.aws_vpc.selected.id
  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
  tags = local.tags
}

resource "aws_launch_template" "batch" {
  name_prefix            = "${var.project_name}-batch-"
  update_default_version = true
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = var.imds_hop_limit
  }
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      delete_on_termination = true
      encrypted             = true
      volume_size           = var.batch_root_volume_size_gib
      volume_type           = "gp3"
    }
  }
  tag_specifications {
    resource_type = "instance"
    tags          = local.tags
  }
  tag_specifications {
    resource_type = "volume"
    tags          = local.tags
  }
  tags = local.tags
}
