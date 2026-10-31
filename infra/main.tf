data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

data "aws_subnet" "private" {
  for_each = toset(var.private_subnet_ids)
  id       = each.value
}

locals {
  name = "${var.project_name}-${var.environment}"
}

resource "aws_s3_bucket" "data" {
  for_each      = toset(["raw", "canonical", "artifacts"])
  bucket        = "${local.name}-${data.aws_caller_identity.current.account_id}-${each.key}"
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "data" {
  for_each                = aws_s3_bucket.data
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  rule { object_ownership = "BucketOwnerEnforced" }
}

resource "aws_s3_bucket_versioning" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_policy" "tls" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [each.value.arn, "${each.value.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
      }], each.key == "canonical" ? [{
      Sid       = "RequireConditionalReleaseWrites"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:PutObject"
      Resource  = "${each.value.arn}/releases/*"
      Condition = { StringNotEquals = { "s3:if-none-match" = "*" } }
      }] : [], each.key == "raw" ? [{
      Sid       = "RequireImmutableSourceWrites"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:PutObject"
      Resource  = "${each.value.arn}/*"
      Condition = { StringNotEquals = { "s3:if-none-match" = "*" } }
    }] : [])
  })
}

resource "aws_iam_role" "glue" {
  name = "${local.name}-glue"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "glue" {
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [for bucket in aws_s3_bucket.data : bucket.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = ["${aws_s3_bucket.data["raw"].arn}/*", "${aws_s3_bucket.data["artifacts"].arn}/*", "${aws_s3_bucket.data["canonical"].arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = ["${aws_s3_bucket.data["canonical"].arn}/normalized/*", "${aws_s3_bucket.data["canonical"].arn}/glue-temp/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = ["${aws_s3_bucket.data["canonical"].arn}/releases/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = flatten([for group in aws_cloudwatch_log_group.glue : [group.arn, "${group.arn}:*"]])
      },
      {
        Effect    = "Allow"
        Action    = ["cloudwatch:PutMetricData"]
        Resource  = "*"
        Condition = { StringEquals = { "cloudwatch:namespace" = "Glue" } }
      },
      {
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartition", "glue:GetPartitions"]
        Resource = ["arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog", aws_glue_catalog_database.canonical.arn, "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.canonical.name}/*"]
      }
    ]
  })
}

resource "aws_cloudwatch_log_group" "glue" {
  for_each          = toset(["error", "output"])
  name              = "/aws-glue/jobs/${local.name}/${each.key}"
  retention_in_days = var.log_retention_days
}

resource "aws_glue_catalog_database" "canonical" {
  name = replace("${local.name}_canonical", "-", "_")
}

resource "aws_s3_object" "glue_script" {
  bucket      = aws_s3_bucket.data["artifacts"].id
  key         = "jobs/normalize.py"
  source      = var.glue_script_path
  source_hash = fileexists(var.glue_script_path) ? filesha256(var.glue_script_path) : null
}

resource "aws_s3_object" "glue_package" {
  bucket      = aws_s3_bucket.data["artifacts"].id
  key         = "python/logistics_platform.zip"
  source      = var.glue_package_path
  source_hash = fileexists(var.glue_package_path) ? filesha256(var.glue_package_path) : null
}

resource "aws_glue_job" "normalize" {
  name              = "${local.name}-normalize"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30
  max_retries       = 0
  execution_class   = "STANDARD"
  execution_property { max_concurrent_runs = 1 }
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data["artifacts"].id}/${aws_s3_object.glue_script.key}"
  }
  default_arguments = {
    "--additional-python-modules"        = "boto3==1.40.40,botocore==1.40.76"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.data["artifacts"].id}/${aws_s3_object.glue_package.key}"
    "--TempDir"                          = "s3://${aws_s3_bucket.data["canonical"].id}/glue-temp/"
    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--custom-logGroup-prefix"           = "/aws-glue/jobs/${local.name}"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-job-insights"              = "false"
  }
}

resource "aws_security_group" "redshift" {
  name        = "${local.name}-redshift"
  description = "No inbound socket access; authenticated Data API only. Outbound routing supplied by VPC owner."
  vpc_id      = var.vpc_id
  egress {
    description = "HTTPS COPY to S3 through VPC routes"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_iam_role" "redshift" {
  name = "${local.name}-redshift-copy"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"] }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "redshift" {
  role = aws_iam_role.redshift.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"], Resource = aws_s3_bucket.data["canonical"].arn },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:GetObjectVersion"], Resource = "${aws_s3_bucket.data["canonical"].arn}/releases/*" }
    ]
  })
}

resource "aws_redshiftserverless_namespace" "warehouse" {
  namespace_name       = local.name
  db_name              = "logistics"
  iam_roles            = [aws_iam_role.redshift.arn]
  default_iam_role_arn = aws_iam_role.redshift.arn
  log_exports          = ["userlog", "connectionlog", "useractivitylog"]
}

resource "aws_redshiftserverless_workgroup" "warehouse" {
  workgroup_name       = local.name
  namespace_name       = aws_redshiftserverless_namespace.warehouse.namespace_name
  base_capacity        = var.redshift_base_capacity
  max_capacity         = var.redshift_base_capacity
  publicly_accessible  = false
  enhanced_vpc_routing = true
  security_group_ids   = [aws_security_group.redshift.id]
  subnet_ids           = var.private_subnet_ids
  lifecycle {
    precondition {
      condition     = alltrue([for subnet in data.aws_subnet.private : subnet.vpc_id == var.vpc_id && !subnet.map_public_ip_on_launch]) && length(toset([for subnet in data.aws_subnet.private : subnet.availability_zone])) >= 3
      error_message = "Subnets must belong to the specified VPC, span three AZs, and not auto-assign public IPs. Independently verify private routes."
    }
  }
}

resource "aws_redshiftserverless_usage_limit" "monthly" {
  resource_arn  = aws_redshiftserverless_workgroup.warehouse.arn
  usage_type    = "serverless-compute"
  amount        = var.monthly_rpu_hour_limit
  period        = "monthly"
  breach_action = "deactivate"
}
