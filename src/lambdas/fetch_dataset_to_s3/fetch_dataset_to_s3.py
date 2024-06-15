import os
import kaggle
import boto3
from botocore.exceptions import ClientError


def get_secret(region):
    secret_name = "kaggle_key"
    region_name = region
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    return get_secret_value_response['SecretString']


def lambda_handler(event, context):
    region = os.environ['aws_region']
    key = get_secret(region)
    print(key)
