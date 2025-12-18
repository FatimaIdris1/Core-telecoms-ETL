 resource "aws_iam_role" "snowflake_user_role" {
  name = "snowflake_s3_reader_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::<account-id>:role/<role-name>"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = ""
          }
        }
      }
    ]
  })
}
data "aws_iam_policy_document" "snowflake_role_policy" {
  # Bucket-level permissions
  statement {
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:GetObject"
    ]

    resources = [
      "arn:aws:s3:::coretelecomms-dl"
    ]
  }

  # Object-level permissions
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListMultipartUploadParts"
    ]

    resources = [
      "arn:aws:s3:::coretelecomms-dl/*"
    ]
  }
}

resource "aws_iam_policy" "s3_read_policy" {
  name   = "snowflake_dedicated_policy"
  policy = data.aws_iam_policy_document.snowflake_role_policy.json
}


resource "aws_iam_role_policy_attachment" "attach" {
  role       = aws_iam_role.snowflake_user_role.name
  policy_arn = aws_iam_policy.s3_read_policy.arn
}



resource "snowflake_storage_integration" "integration" {
  name                      = "S3_WAREHOUSE_INTEGRATION"
  type                      = "EXTERNAL_STAGE"
  enabled                   = true
  storage_allowed_locations = ["s3://coretelecomms-dl/"]
  storage_provider     = "S3"
  storage_aws_role_arn = "arn:aws:iam::343935324582:role/snowflake_s3_reader_role"
}

resource "snowflake_database" "coretelecom_db" {
  name = "CORETELECOM_DB"
}

resource "snowflake_schema" "staging" {
  name     = "CORETELECOM_STAGING"
  database = snowflake_database.coretelecom_db.name
}

resource "snowflake_file_format" "parquet" {
  name     = "MY_PARQUET_FORMAT"
  database = snowflake_database.coretelecom_db.name
  schema   = snowflake_schema.staging.name
  format_type = "PARQUET"
  null_if = ["NULL", ""]
  compression = "AUTO"
}



resource "snowflake_stage" "coretelecomms_stage" {
  name       = "CORETELECOMMS_STAGE"
  database   = snowflake_database.coretelecom_db.name
  schema     = snowflake_schema.staging.name
  url        = "s3://coretelecomms-dl/staging/"
  storage_integration = snowflake_storage_integration.integration.name
  file_format = "format_name = 'CORETELECOM_DB.CORETELECOM_STAGING.MY_PARQUET_FORMAT'"
  depends_on = [
    snowflake_storage_integration.integration,
    snowflake_file_format.parquet
  ]

}
