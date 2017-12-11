import time
from collections import namedtuple
from datetime import datetime
from dateutil.tz import tzutc

import boto3

athena = boto3.client("athena")

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


queries = [
     # query id                               bucket
    ("00bb4f20-25b0-4d48-a16a-57870c7cbc2c", "aws-athena-query-results-lancs-30d"),         # queue comparison 30d
    ("5e1549f7-f2a5-40ee-9345-cb488c0feabc", "aws-athena-query-results-lancs-24h"),         # jobs per hour in past 24 hours
    ("c50de2b4-dc45-4f1d-af4b-ee10b5561bfa", "aws-athena-query-results-lancs-history-30d"), # jobs per day in past 30 days
]

for query_id, bucket in queries:
    scheduler.add_job(run_query, "interval", seconds=600, args=(query_id, bucket))

scheduler.start()

def update_now():
    for query_id, bucket in queries:
        run_query(query_id, bucket)