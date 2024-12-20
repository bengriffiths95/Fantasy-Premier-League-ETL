resource "aws_s3_bucket" "extracted_data" {
  bucket_prefix = var.extracted_data_bucket_name
}