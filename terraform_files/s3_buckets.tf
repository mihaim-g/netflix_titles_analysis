resource "aws_s3_bucket" "mituca-repo1-utils" {
  bucket = "mituca-repo1-utils"
}

resource "aws_s3_object" "fetch_dataset_to_s3_object" {
  bucket = aws_s3_bucket.mituca-repo1-utils.id

  key    = "fetch_dataset_to_s3.zip"
  source = data.archive_file.fetch_dataset_to_s3_archive.output_path

  depends_on = [
        data.archive_file.fetch_dataset_to_s3_archive
    ]

}