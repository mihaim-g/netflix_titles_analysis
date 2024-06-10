#!/bin/bash

AWS_PROFILE=repo1-admin
DATASET=netflix-movies-and-tv-shows
DOWNLOADED_FILE_NAME=${DATASET}.zip
UNZIPPED_FILE_NAME=netflix_titles.csv
GZIPPED_FILE_NAME=${UNZIPPED_FILE_NAME}.gz
BUCKET=mituca-repo1-raw-data-bucket

kaggle datasets download -d rahulvyasm/${DATASET}

unzip ${DOWNLOADED_FILE_NAME}
gzip ${UNZIPPED_FILE_NAME}

aws s3 cp ${GZIPPED_FILE_NAME} s3://${BUCKET}/raw_input/${GZIPPED_FILE_NAME} --profile ${AWS_PROFILE}

#rm ${GZIPPED_FILE_NAME}
#rm ${DOWNLOADED_FILE_NAME}