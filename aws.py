import os

import boto3

session = boto3.session.Session(
    aws_access_key_id=os.environ["AWS_KEY"],
    aws_secret_access_key=os.environ["AWS_SECRET"],
    region_name=os.environ["AWS_REGION"],
)