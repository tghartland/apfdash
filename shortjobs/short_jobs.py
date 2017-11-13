import io
import time
import csv
from datetime import datetime, timedelta

import matplotlib.pyplot as plt

import boto3

s3 = boto3.client("s3")
athena = boto3.client("athena")

short_jobs = athena.get_named_query(NamedQueryId="cb3f97bc-861d-4505-a37e-ade33e2219aa")

response = athena.start_query_execution(
			QueryString=short_jobs["NamedQuery"]["QueryString"],
			QueryExecutionContext={
				"Database":"apfhistory",
			},
			ResultConfiguration={
				"OutputLocation": "s3://aws-athena-query-results-384617194507-us-east-1/",
				"EncryptionConfiguration": {"EncryptionOption":"SSE_S3"},
			}
)

execution = athena.get_query_execution(QueryExecutionId=response["QueryExecutionId"])

time.sleep(3)
bucket, key = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"][5:].split("/", 1)
print("Bucket {}, key {}".format(bucket, key))

file_data = io.BytesIO()
s3.download_fileobj(bucket, key, file_data)
file_data.seek(0)
data = str(file_data.read(), "utf8")

def d(date_string):
    return datetime.strptime(date_string, "%Y-%m-%d")
    
reader = csv.reader(data.split("\n"),  delimiter=",")
valid_rows = [row for row in reader if len(row) == 3]
valid_rows = [row for row in valid_rows if (len(row[0]) > 0 and len(row[1]) > 0 and len(row[2]) > 0)]
valid_rows = [[d(a), int(b), int(c)] for (a, b, c) in valid_rows[1:]]
print(valid_rows[125])
