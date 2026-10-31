variable "aws_region" {
  type    = string
  default = "eu-west-1"
}

variable "project_name" {
  type    = string
  default = "logistics-canonical"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,30}$", var.project_name))
    error_message = "Use 3-31 lowercase alphanumeric characters or hyphens, starting with a letter."
  }
}

variable "environment" {
  type    = string
  default = "portfolio"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,15}$", var.environment))
    error_message = "Use a short lowercase environment identifier."
  }
}

variable "vpc_id" {
  type        = string
  description = "Existing VPC with DNS support and working S3 routing."
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "At least three private subnets in three AZs, with sufficient free IPs and S3 routes."
  validation {
    condition     = length(distinct(var.private_subnet_ids)) >= 3
    error_message = "Supply at least three distinct private subnet IDs."
  }
}

variable "redshift_base_capacity" {
  type    = number
  default = 8
  validation {
    condition     = contains([8, 16, 32, 64], var.redshift_base_capacity)
    error_message = "Portfolio capacity must be explicitly chosen from 8, 16, 32, 64 RPUs. Verify regional support."
  }
}

variable "glue_script_path" {
  type        = string
  description = "Local tested Glue entrypoint. Upload is tracked by file SHA-256."
  default     = "../jobs/normalize.py"
}

variable "glue_package_path" {
  type        = string
  description = "Local Python package zip built with scripts/build_glue_zip.py."
  default     = "../dist/logistics_platform.zip"
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "monthly_rpu_hour_limit" {
  type        = number
  default     = 24
  description = "Serverless usage guardrail; reaching this monthly RPU-hour limit deactivates user queries. This is not a price guarantee."
}
