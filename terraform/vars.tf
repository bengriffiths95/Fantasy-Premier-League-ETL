variable "extracted_data_bucket_name" {
  type    = string
  default = "fpl-etl-extracted-data"
}

variable "transformed_data_bucket_name" {
  type    = string
  default = "fpl-etl-transformed-data"
}

variable "region" {
  type    = string
  default = "eu-west-2"
}

variable "instance_id" {
  type    = string
  default = "i-09a5211bae0615273"
}