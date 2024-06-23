#!/bin/bash

source ~/.netflix_titles_analysis_local_env_setup.txt

bash ~/start_spark.sh

localstack start -d
kaggle datasets download -d rahulvyasm/"${DATASET}"
unzip "${DOWNLOADED_FILE_NAME}"
gzip "${UNZIPPED_FILE_NAME}"

aws s3 mb s3://"${BUCKET}" --profile "${AWS_PROFILE}" --endpoint "${LOCAL_ENDPOINT}"
aws s3 mb s3://"${DATAFRAME_DESTINATION_BUCKET}" --profile "${AWS_PROFILE}" --endpoint "${LOCAL_ENDPOINT}"
aws s3 cp "${GZIPPED_FILE_NAME}" s3://"${BUCKET}"/raw_input/"${GZIPPED_FILE_NAME}" --profile "${AWS_PROFILE}" --endpoint "${LOCAL_ENDPOINT}"

rm "${GZIPPED_FILE_NAME}"
rm "${DOWNLOADED_FILE_NAME}"