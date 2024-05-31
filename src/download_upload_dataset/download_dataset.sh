#!/bin/bash

AWS_PROFILE=repo1-admin
DATASET=netflix-movies-and-tv-shows
FILE_NAME=${DATASET}.zip
BUCKET=mituca-repo1-raw-data

kaggle datasets download -d rahulvyasm/${DATASET}

aws s3 cp ${FILE_NAME} s3://${BUCKET}/raw_input/${FILE_NAME} --profile ${AWS_PROFILE}

rm ${FILE_NAME}