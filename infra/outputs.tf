output "buckets" {
  value = { for purpose, bucket in aws_s3_bucket.data : purpose => bucket.id }
}

output "glue_job_name" {
  value = aws_glue_job.normalize.name
}

output "catalog_database" {
  value = aws_glue_catalog_database.canonical.name
}

output "redshift_workgroup" {
  value = aws_redshiftserverless_workgroup.warehouse.workgroup_name
}

output "redshift_copy_role_arn" {
  value = aws_iam_role.redshift.arn
}

output "redshift_database" {
  value = aws_redshiftserverless_namespace.warehouse.db_name
}
