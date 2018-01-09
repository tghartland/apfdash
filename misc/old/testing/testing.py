import io
import time
import csv
from datetime import datetime, timedelta
from collections import OrderedDict

import matplotlib.pyplot as plt

import boto3

s3 = boto3.client("s3")
athena = boto3.client("athena")

#print("Available buckets:\n")
#for bucket in s3.buckets.all():
#	print(bucket.name)

print("\n\nAvailable queries:\n")
response = athena.list_named_queries()
print(response)

for query_id in response["NamedQueryIds"]:
	query = athena.get_named_query(NamedQueryId=query_id)
	print(query_id, query["NamedQuery"]["Name"])



jobs_per_day = athena.get_named_query(NamedQueryId="74225e6d-ebda-4e10-b6af-a33e7d8fbef0")
print(jobs_per_day)
print()

response = athena.start_query_execution(
			QueryString=jobs_per_day["NamedQuery"]["QueryString"],
			QueryExecutionContext={
				"Database":"apfhistory",
			},
			ResultConfiguration={
				"OutputLocation": "s3://aws-athena-query-results-384617194507-us-east-1/",
				"EncryptionConfiguration": {"EncryptionOption":"SSE_S3"},
			}
)

print(response)	
print()


execution = athena.get_query_execution(QueryExecutionId=response["QueryExecutionId"])

print(execution)
print()
time.sleep(3)
bucket, key = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"][5:].split("/", 1)
print("Bucket {}, key {}".format(bucket, key))

file_data = io.BytesIO()
s3.download_fileobj(bucket, key, file_data)
file_data.seek(0)
data = str(file_data.read(), "utf8")
#print(data)

def d(date_string):
	return datetime.strptime(date_string, "%Y-%m-%d")

reader = csv.reader(data.split("\n"),  delimiter=",")
valid_rows = [row for row in reader if len(row) == 2]
valid_rows = [row for row in valid_rows if (len(row[0]) > 0 and len(row[1]) > 1)]
valid_rows = [[d(a), int(b)] for (a, b) in valid_rows[1:]]
#for row in valid_rows:
#	print(row)

dates, jobs = zip(*valid_rows)
"""dates_and_jobs = OrderedDict(zip(dates, jobs))

all_dates = {}
for i in range(0, (max(dates)-min(dates)).days):
	date = min(dates) + timedelta(i)
	if date in dates_and_jobs:
		all_dates[date] = dates_and_jobs[date]
	else:
		all_dates[date] = 0

print(len(all_dates.keys()), len(all_dates.values()))
"""

#jobs = [jobs[dates.index(date)] if date in jobs else 0 for date in []]
dates2 = []
jobs2 = []

for i in range(0, (max(dates)-min(dates)).days):
	date = min(dates)+timedelta(i)
	dates2.append(date)
	if date in dates:
		jobs2.append(jobs[dates.index(date)])
	else:
		jobs2.append(0)

plt.plot(dates2[-60:], jobs2[-60:])
plt.xlabel("Date")
plt.ylabel("Jobs started per day")
plt.show()
