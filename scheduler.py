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

def run_query(query_id, bucket, database="apfhistorypanda"):
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
                "TooManyRequestsException",
                0,
                0,
            )
        )
        return
    
    
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
    
    if len(QueryHistory.history) > 30:
        QueryHistory.history = list(reversed(reversed(QueryHistory.history)[:30]))



# new queries using apfhistorypanda database
queries = [
    # query id                                bucket                                          query name                    description
    ("d712c576-1b6b-42ab-8958-d4dd50ab2b32", "aws-athena-apfdash-index"),               # pnd_index_24h                 Aggregate data in past 24 hours
    ("a3052d5c-77cb-44f7-8b71-d17470a33e36", "aws-athena-apfdash-queue-history"),       # pnd_jobs_hourly_48h           hourly jobs data over past 48 hours
    ("0865d653-ab36-42a0-8c7a-1e5f6a983e40", "aws-athena-apfdash-queue-history-30d"),   # pnd_jobs_daily_30d            daily jobs data over 30 days per queue
    ("61922f3a-6cc4-4e21-9eec-cce51cab9fbf", "aws-athena-apfdash-scatter"),             # pnd_all_4h                    all columns for jobs in past 4 hours
    ("db6956ba-15cc-4e2a-91d1-edb849e12504", "aws-athena-apfdash-dist-binned1s"),       # pnd_wc_pandacount_binned_48h  all wallclock, pandacount for wc<1200 past 48 hours
    ("c15475b6-a226-494a-abf3-a2b8862c5416", "aws-athena-apfdash-dist-binned1m"),       # pnd_mins_total_empty_48h      all wc time binned into minutes for past 48 hours
    ("d99ddd95-c0a1-4879-8f88-68a35b446158", "aws-athena-apfdash-queue-binned1s"),      # pnd_q_wcbinned_jobs_empty     Jobs in past 48 hours binned by wc time per queue
    ("2cc20e2f-93dd-4f3e-9ff0-542483663df9", "aws-athena-apfdash-queue-binned10m"),     # pnd_q_wcbinned10m_jobs_empty  Jobs in past 48 hours binned by 10 minutes wc time per queue
]


# for each query
for i, (query_id, bucket) in enumerate(queries):
    # add repeating update every 20 minutes
    scheduler.add_job(run_query, "interval", minutes=20, args=(query_id, bucket))
    
    # run one update as the server starts, staggered so that they do not all run at the same time
    scheduler.add_job(run_query, "date", run_date=datetime.now()+timedelta(seconds=5+i*5), args=(query_id, bucket))
    
    # download the result of the first update after it has completed
    scheduler.add_job(Datasources.get_latest_data_for, "date", run_date=datetime.now()+timedelta(seconds=15+i*5), args=(bucket,))

scheduler.start()