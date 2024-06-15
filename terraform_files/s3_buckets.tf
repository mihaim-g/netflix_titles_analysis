resource "aws_s3_bucket" "mituca-repo1-utils" {
  bucket = var.utils_bucket_name
}

resource "aws_s3_bucket" "mituca-repo1-raw-data" {
  bucket = var.raw_data_bucket_name
}

resource "aws_s3_object" "fetch_dataset_to_s3_object" {
  bucket = aws_s3_bucket.mituca-repo1-utils.id

  key    = "${var.fetch_dataset_to_s3_name}.zip"
  source = data.archive_file.fetch_dataset_to_s3_archive.output_path

  depends_on = [
        data.archive_file.fetch_dataset_to_s3_archive
    ]

}