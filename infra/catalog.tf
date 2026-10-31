locals {
  evidence_columns = {
    source_system = "string", entity_type = "string", source_id = "string",
    business_key  = "string", revision = "bigint", operation = "string",
    effective_at  = "string", updated_at = "string", attributes = "string",
    payload_hash  = "string", raw_hash = "string", raw_record = "string", row_number = "bigint"
  }
}

resource "aws_glue_catalog_table" "normalized_evidence" {
  name          = "normalized_evidence"
  database_name = aws_glue_catalog_database.canonical.name
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
    contract       = "normalized-staging-not-canonical-products"
  }
  partition_keys {
    name = "run_id"
    type = "string"
  }
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data["canonical"].id}/normalized/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    dynamic "columns" {
      for_each = local.evidence_columns
      content {
        name = columns.key
        type = columns.value
      }
    }
  }
}
