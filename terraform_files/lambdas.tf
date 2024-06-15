data "archive_file" "fetch_dataset_to_s3_archive" {
  type = "zip"

  source_dir  = "${path.module}/src/lambdas/fetch_dataset_to_s3"
  output_path = "${path.module}/fetch_dataset_to_s3.zip"
}

resource "aws_lambda_function" "fetch_dataset_to_s3" {
  function_name    = "fetch_dataset_to_s3"
  role             = aws_iam_role.fetch_dataset_to_s3_role.arn
  handler          = "lambda_handler"
  source_code_hash = filebase64sha256(data.archive_file) // or filebase64sha256(handler.zip)
  runtime          = "python3.12"

  s3_bucket = aws_s3_bucket.mituca-repo1-utils.id
  s3_key    = aws_s3_object.fetch_dataset_to_s3_object.key

  depends_on = [
    aws_s3_object.fetch_dataset_to_s3_object
  ]
}

resource "aws_cloudwatch_log_group" "lambda_cloudwatch_log" {
  name              = "/aws/lambda/${aws_lambda_function.fetch_dataset_to_s3.function_name}"
  retention_in_days = 14
}


resource "aws_iam_role" "fetch_dataset_to_s3_role" {
  name = "fetch_dataset_to_s3-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "lambda_role_policy" {
  role       = aws_iam_role.fetch_dataset_to_s3_role.name
  policy_arn = var.iam_role_policy_arn
}

variable "iam_role_policy_arn" {
  description = "ARN of the IAM role policy to attach to the lambda role."

  type = string
  default = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}