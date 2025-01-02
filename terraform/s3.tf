resource "aws_s3_bucket" "extracted_data" {
  bucket_prefix = var.extracted_data_bucket_name
}

resource "aws_s3_bucket" "transformed_data" {
  bucket_prefix = var.transformed_data_bucket_name
}