#!/bin/bash

source ~/.netflix_titles_analysis_local_env_setup.txt

unset "${LOCAL_ENDPOINT}"

kaggle datasets download -d rahulvyasm/"${DATASET}"

unzip "${DOWNLOADED_FILE_NAME}"
gzip "${UNZIPPED_FILE_NAME}"

aws s3 cp "${GZIPPED_FILE_NAME}" s3://"${BUCKET}"/raw_input/"${GZIPPED_FILE_NAME}" --profile "${AWS_PROFILE}"

rm "${GZIPPED_FILE_NAME}"
rm "${DOWNLOADED_FILE_NAME}"