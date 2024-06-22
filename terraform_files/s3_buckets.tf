resource "aws_s3_bucket" "mituca-repo1-raw-data" {
  bucket = var.raw_data_bucket_name
}

resource "aws_s3_bucket" "mituca-repo1-base-data" {
  bucket = var.base_dataframe_bucket_name
}
