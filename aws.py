import os

import boto3

if "APFDASH_AWS_KEY" in os.environ:
    session = boto3.session.Session(
        aws_access_key_id=os.environ["APFDASH_AWS_KEY"],
        aws_secret_access_key=os.environ["APFDASH_AWS_SECRET"],
        region_name=os.environ["APFDASH_AWS_REGION"],
    )
else:
    # Try to use se ~/.aws/credentials file
    session = boto3.session.Session()