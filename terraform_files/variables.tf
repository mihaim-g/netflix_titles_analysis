variable "raw_data_bucket_name" {
  description = "S3 bucket where original csv file is uploaded"
  default     = "mituca-repo1-raw-data"
}

variable "base_dataframe_bucket_name" {
  description = "S3 bucket where we store dataframe files"
  default     = "mituca-repo1-base-data"
}

