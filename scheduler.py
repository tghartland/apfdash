import time
from collections import namedtuple
from datetime import datetime, timedelta
from dateutil.tz import tzutc

from aws import session
athena = session.client("athena")

from datasources import Datasources

from apscheduler.schedulers.background import BackgroundScheduler
scheduler = BackgroundScheduler()

QueryExecutionResult = namedtuple("QueryExecutionResult", ["name", "query_id", "result_file", "time", "status", "bytes_scanned", "execution_duration"])

class QueryHistory:
    history = []

def run_query(query_id, bucket, database="apfhistorypanda", timeout=30):
    """
    Run a given query ID on a database and store the result into a named bucket.
    
    Bucket name should not include an s3:// prefix.
    This function waits until the query has finished running
    and then adds a job to the scheduler to download the result
    file after waiting 15 seconds, if the query succeeded.
    
    If the timeout argument is exceeded when waiting for the query
    to complete, it sets the status to "*TIMEOUT", which can be
    seen in the debug page query history list.
    It is marked with an asterisk to differentiate it from an
    actual athena result status string, as is a
    *TooManyRequestsException, if one should occur.
    """
    query = athena.get_named_query(NamedQueryId=query_id)
    
    try:
        response = athena.start_query_execution(
            QueryString=query["NamedQuery"]["QueryString"],
            QueryExecutionContext={
                "Database": database,
            },
            ResultConfiguration={
                "OutputLocation": "s3://{}/".format(bucket),
            }
        )
    except athena.exceptions.TooManyRequestsException:
        QueryHistory.history.append(
            QueryExecutionResult(
                query["NamedQuery"]["Name"],
                query_id,
                "-",
                datetime.now(tzutc()),
                "*TooManyRequestsException",
                0,
                0,
            )
        )
        if len(QueryHistory.history) > 30:
            QueryHistory.history.pop(0)
        return
    
    
    execution_id = response["QueryExecutionId"]
    
    execution = athena.get_query_execution(QueryExecutionId=execution_id)
    
    wait_start_time = time.time()
    
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
    
    # Download the latest data
    # Leaving a short length of time before doing so because I don't know
    # if the result file is available immediately.
    if execution["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
        scheduler.add_job(Datasources.download_latest_data_for, "date", run_date=datetime.now()+timedelta(seconds=15), args=(bucket,))
    
    # Don't want to keep all history, just the most recent. 30 is enough to keep.
    if len(QueryHistory.history) > 30:
        QueryHistory.history.pop(0)



# new queries using apfhistorypanda database
queries = [
    # query id                                bucket                                          query name                    description
    ("f7001c25-4b29-427f-906f-62d0d6d3cce9", "aws-athena-apfdash-index"),               # pnd_index_24h                 Aggregate data in past 24 hours
    ("ec94f454-9bad-492a-a10b-d1d862445a1f", "aws-athena-apfdash-queue-history"),       # pnd_jobs_hourly_48h           hourly jobs data over past 48 hours
    ("e5d4a76f-7bfe-423a-b50e-e66340ea8bdb", "aws-athena-apfdash-queue-history-30d"),   # pnd_jobs_daily_30d            daily jobs data over 30 days per queue
    ("c8eb3753-78d3-4680-b7f1-4ac17d33558c", "aws-athena-apfdash-scatter"),             # pnd_all_4h                    all columns for jobs in past 4 hours
    ("e75782ef-f233-4445-b3fb-c74934886e20", "aws-athena-apfdash-dist-binned1s"),       # pnd_wc_pandacount_binned_48h  all wallclock, pandacount for wc<1200 past 48 hours
    ("15e14204-589a-4864-acb3-ac01674ad51d", "aws-athena-apfdash-dist-binned1m"),       # pnd_mins_total_empty_48h      all wc time binned into minutes for past 48 hours
    ("81e5dde6-ea6a-4857-b176-c6f35c51c0c8", "aws-athena-apfdash-queue-binned1s"),      # pnd_q_wcbinned_jobs_empty     Jobs in past 48 hours binned by wc time per queue
    ("4b263d23-01e0-4364-b58b-ef5ab3d245da", "aws-athena-apfdash-queue-binned10m"),     # pnd_q_wcbinned10m_jobs_empty  Jobs in past 48 hours binned by 10 minutes wc time per queue
]

def add_repeating_query_to_scheduler(query_id, bucket):
    print("Adding query to scheduler", query_id, bucket)
    scheduler.add_job(run_query, "interval", minutes=20, args=(query_id, bucket))

for i, (query_id, bucket) in enumerate(queries):
    # Use single job to stagger adding repeating update every 20 minutes
    # If all queries run at the same time, it will sometimes trigger a TooManyRequests exception
    # Wait 20*i seconds before adding the proper job to the scheduler
    scheduler.add_job(add_repeating_query_to_scheduler, "date", run_date=datetime.now()+timedelta(seconds=5+20*i), args=(query_id, bucket))
    
    # Run one update as the server starts, staggered so that they do not all run at the same time
    scheduler.add_job(run_query, "date", run_date=datetime.now()+timedelta(seconds=5+i*5), args=(query_id, bucket))

scheduler.start()