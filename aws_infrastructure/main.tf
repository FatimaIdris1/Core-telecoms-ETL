provider "aws" {
  region = "us-west-2"
}



terraform {
  backend "s3" {
    bucket = "ct-terraform-bucket"
    key    = "terraform.tfstate"
    region = "us-west-2"
  }
}



