resource "aws_s3_bucket" "runs" {
  count  = var.create_bucket ? 1 : 0
  bucket = var.bucket_name
  tags   = local.tags
}

resource "aws_s3_bucket_public_access_block" "runs" {
  count                   = var.create_bucket ? 1 : 0
  bucket                  = aws_s3_bucket.runs[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "runs" {
  count  = var.create_bucket ? 1 : 0
  bucket = aws_s3_bucket.runs[0].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
