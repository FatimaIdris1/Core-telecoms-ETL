# AWS provider (needed if you interact with AWS resources like S3)
provider "aws" {
  region = "us-west-2"
}

# Terraform configuration
terraform {
  backend "s3" {
    bucket = "ct-terraform-bucket"
    key    = "terraform.tfstate"
    region = "us-west-2"
  }

  required_providers {

    snowflake = {
      source  = "snowflakedb/snowflake"
      version = ">= 1.0.0"
    }
  }
}
provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.account_name
  user              = var.user
  password          = var.password
  role              = var.snowflake_admin_role
  preview_features_enabled  = ["snowflake_storage_integration_resource", "snowflake_file_format_resource", "snowflake_stage_resource"]
}
