import time
import pprint
from collections import namedtuple
from datetime import datetime
from dateutil.tz import tzutc
import io

import pandas as pd

import boto3

s3 = boto3.client("s3")
athena = boto3.client("athena")

def most_recent_object_in_bucket(bucket_name):
    BucketItem = namedtuple("BucketItem", ["name", "modified"])
    all_items = []
    response = s3.list_objects(
        Bucket=bucket_name,
    )
    for obj in response["Contents"]:
        if ".metadata" in obj["Key"]: continue
        all_items.append(BucketItem(obj["Key"], obj["LastModified"]))
        
    return max(all_items, key=lambda obj: obj.modified)

class TrackedData:
    def __init__(self, bucket_name):
        pass

class Datasources:
    queue_comparison_30d = None
    individual_queues_24h = None
    
    query_data = {}

    @staticmethod
    def get_latest_data_for(bucket_name):
        if bucket_name in Datasources.query_data:
            if (datetime.now(tzutc())-Datasources.query_data[bucket_name]["checked_for_update"]).seconds <= 5*60:
                return Datasources.query_data[bucket_name]["data"].copy()
                
        name, date = most_recent_object_in_bucket(bucket_name)
        Datasources.query_data[bucket_name] = Datasources.query_data.get(bucket_name, {"modified": datetime(1, 1, 1, tzinfo=tzutc())})
        if date > Datasources.query_data[bucket_name]["modified"]:
            file_data = io.BytesIO()
            s3.download_fileobj(bucket_name, name, file_data)
            file_data.seek(0)
            bytes_data = file_data.read()
            string_data = str(bytes_data, "utf8")
            string_file = io.StringIO(string_data)
            string_file.seek(0)
            Datasources.query_data[bucket_name]["data"] = pd.read_csv(string_file)
            Datasources.query_data[bucket_name]["filename"] = name
            Datasources.query_data[bucket_name]["modified"] = date
            Datasources.query_data[bucket_name]["downloaded"] = datetime.now(tzutc())
        
        Datasources.query_data[bucket_name]["checked_for_update"] = datetime.now(tzutc())
        return Datasources.query_data[bucket_name]["data"].copy()
    



from apscheduler.schedulers.background import BackgroundScheduler
from functools import partial

scheduler = BackgroundScheduler()

QueryExecutionResult = namedtuple("QueryExecutionResult", ["name", "query_id", "result_file", "time", "status", "bytes_scanned", "execution_duration"])

class QueryHistory:
    history = []

def run_query(query_id, bucket, database="apfhistorylong"):
    query = athena.get_named_query(NamedQueryId=query_id)
    
    response = athena.start_query_execution(
        QueryString=query["NamedQuery"]["QueryString"],
        QueryExecutionContext={
            "Database": database,
        },
        ResultConfiguration={
            "OutputLocation": "s3://{}/".format(bucket),
        }
    )
    
    execution_id = response["QueryExecutionId"]
    
    execution = athena.get_query_execution(QueryExecutionId=execution_id)
    
    wait_start_time = time.time()
    timeout = 30
    
    while execution["QueryExecution"]["Status"]["State"] == "RUNNING":
        if time.time()-wait_start_time > timeout:
            execution["QueryExecution"]["Status"]["State"] = "*TIMEOUT"
            break
        time.sleep(1)
        execution = athena.get_query_execution(QueryExecutionId=execution_id)
    
    QueryHistory.history.append(
        QueryExecutionResult(
            query["NamedQuery"]["Name"],
            query_id,
            execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"],
            datetime.now(tzutc()),
            execution["QueryExecution"]["Status"]["State"],
            execution["QueryExecution"]["Statistics"]["DataScannedInBytes"],
            execution["QueryExecution"]["Statistics"]["EngineExecutionTimeInMillis"],
        )
    )
    
    
# queue comparison 30d
job1 = scheduler.add_job(
    partial(run_query,
            "00bb4f20-25b0-4d48-a16a-57870c7cbc2c",
            "aws-athena-query-results-lancs-30d"
            ),
    "interval",
    seconds=600,
)

# jobs per hour in past 24 hours
job2 = scheduler.add_job(
    partial(run_query,
            "5e1549f7-f2a5-40ee-9345-cb488c0feabc",
            "aws-athena-query-results-lancs-24h"
            ),
    "interval",
    seconds=600,
)

# jobs per day in past 30 days
job3 = scheduler.add_job(
    partial(run_query,
            "c47f5e49-6861-46b6-9fc5-faa44048ee9d",
            "aws-athena-query-results-lancs-history-30d"
            ),
    "interval",
    seconds=600,
)

scheduler.start()


#run_query("00bb4f20-25b0-4d48-a16a-57870c7cbc2c",
#          "aws-athena-query-results-lancs-30d")

#run_query("5e1549f7-f2a5-40ee-9345-cb488c0feabc",
#          "aws-athena-query-results-lancs-24h")

#run_query("c50de2b4-dc45-4f1d-af4b-ee10b5561bfa",
#            "aws-athena-query-results-lancs-history-30d")