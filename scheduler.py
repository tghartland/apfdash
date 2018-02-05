import time
import threading
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



# new queries using apfhistorypanda database
queries = [
    # query id                                bucket                                          query name                    description
    #("d9070f40-ba13-453e-a4be-43cf67fa083d", "aws-athena-query-results-lancs-30d"),         # pnd_dur_30d                   jobs/duration data over 30 days
    #("e0fa8b88-5718-4b02-acfe-b184786c7808", "aws-athena-query-results-lancs-30d"),         # pnd_index_24h                 Aggregate data in past 24 hours
    ("d712c576-1b6b-42ab-8958-d4dd50ab2b32", "aws-athena-apfdash-index"),         # pnd_index_24h                 Aggregate data in past 24 hours
    #("d2ee5403-6772-4cc7-b446-d4cab42ca8c4", "aws-athena-query-results-lancs-24h"),         # pnd_jobs_hourly_24h           hourly jobs data over past 24 hours
    ("a3052d5c-77cb-44f7-8b71-d17470a33e36", "aws-athena-apfdash-queue-history"),         # pnd_jobs_hourly_48h           hourly jobs data over past 48 hours
    ("0865d653-ab36-42a0-8c7a-1e5f6a983e40", "aws-athena-apfdash-queue-history-30d"), # pnd_jobs_daily_30d            daily jobs data over 30 days per queue
    ("efab784b-a182-4acf-80bf-b6e7ddaa3e79", "aws-athena-apfdash-scatter"),          # pnd_all_4h                    all columns for jobs in past 4 hours
    #("80545946-0d70-4589-8c7d-dc2eb31a80ed", "aws-athena-query-results-lancs-all-48h"),     # pnd_q_dur_wc_pandacount_48h   all queue, duration, wallclock, pandacount for past 48 hours
    ("db6956ba-15cc-4e2a-91d1-edb849e12504", "aws-athena-apfdash-dist-binned1s"),     # pnd_wc_pandacount_binned_48h         all wallclock, pandacount for wc<1200 past 48 hours
    ("c15475b6-a226-494a-abf3-a2b8862c5416", "aws-athena-apfdash-dist-binned1m"),  # pnd_mins_total_empty_48h      all wc time binned into minutes for past 48 hours
    ("d99ddd95-c0a1-4879-8f88-68a35b446158", "aws-athena-apfdash-queue-binned1s"),          # pnd_q_wcbinned_jobs_empty     Jobs in past 48 hours binned by wc time per queue
    ("2cc20e2f-93dd-4f3e-9ff0-542483663df9", "aws-athena-apfdash-queue-binned10m"),         # pnd_q_wcbinned10m_jobs_empty  Jobs in past 48 hours binned by 10 minutes wc time per queue
]


for i, (query_id, bucket) in enumerate(queries):
    scheduler.add_job(run_query, "interval", minutes=20, args=(query_id, bucket))
    scheduler.add_job(run_query, "date", run_date=datetime.now()+timedelta(seconds=5+i*5), args=(query_id, bucket))
    scheduler.add_job(Datasources.get_latest_data_for, "date", run_date=datetime.now()+timedelta(seconds=15+i*5), args=(bucket,))

scheduler.start()


def update_now(threaded=False):
    if threaded:
        threads = []
        for query_id, bucket in queries:
            t = threading.Thread(target=run_query, args=(query_id, bucket))
            t.start()
            threads.append(t)
            
        for thread in threads:
            thread.join()
    else:
        for query_id, bucket in queries:
            run_query(query_id, bucket)

start_time = time.time()


#print("Running queries now")
#update_now()

#print("Preloading latest csv data")
# Preload data
threads = []
#for query_id, bucket in queries:
#    t = threading.Thread(target=Datasources.get_latest_data_for, args=(bucket,))
#    t.start()
#    threads.append(t)

#for thread in threads:
#    thread.join()

#print("Done ({:.02f}s)".format(time.time()-start_time))
