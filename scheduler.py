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



# new queries using apfhistorypanda database
queries = [
    # query id                                bucket                                          query name                    description
    ("9c6c3a70-b9e5-40c4-bbe1-ad776d820760", "aws-athena-query-results-lancs-30d"),         # pnd_dur_30d                   jobs/duration data over 30 days
    ("af8f85f3-ec5e-4513-89f3-d9bcdf9af29f", "aws-athena-query-results-lancs-24h"),         # pnd_jobs_hourly_24h           hourly jobs data over past 24 hours
    ("73ddc08f-79ae-4502-a440-aa97ba73a10c", "aws-athena-query-results-lancs-history-30d"), # pnd_jobs_daily_30d            daily jobs data over 30 days per queue
    ("efab784b-a182-4acf-80bf-b6e7ddaa3e79", "aws-athena-query-results-lancs-4h"),          # pnd_all_4h                    all columns for jobs in past 4 hours
    #("80545946-0d70-4589-8c7d-dc2eb31a80ed", "aws-athena-query-results-lancs-all-48h"),     # pnd_q_dur_wc_pandacount_48h   all queue, duration, wallclock, pandacount for past 48 hours
    ("6a33eb91-d745-4a99-bbfa-7d19d522eaee", "aws-athena-query-results-lancs-all-48h"),     # pnd_wc_pandacount_48h         all wallclock, pandacount for wc<1200 past 48 hours
]



for query_id, bucket in queries:
    scheduler.add_job(run_query, "interval", minutes=20, args=(query_id, bucket))
    scheduler.add_job(run_query, "date", run_date=datetime.now()+timedelta(seconds=30), args=(query_id, bucket))

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
