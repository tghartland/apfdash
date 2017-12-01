import pprint
import time

from datetime import datetime, date, timedelta
from matplotlib import pyplot as plt
from matplotlib.dates import DateFormatter

import numpy as np

from execute_query import execute_query_by_id


query_id = "5e1549f7-f2a5-40ee-9345-cb488c0feabc"
database = "apfhistorylong"
output_location = "aws-athena-query-results-lancs"

result = execute_query_by_id(query_id, database, output_location)

bytes_scanned = result.query_metadata["QueryExecution"]["Statistics"]["DataScannedInBytes"]
execution_duration = result.query_metadata["QueryExecution"]["Statistics"]["EngineExecutionTimeInMillis"]

def plot_for_queue(queue, times, total_jobs, short_jobs):

    long_jobs = total_jobs-short_jobs

    fig, ax = plt.subplots()

    ax.bar(times, short_jobs, color="red", width=1/24, align="edge", label="Short jobs")
    ax.bar(times, long_jobs, color="blue", width=1/24, align="edge", bottom=short_jobs, label="Long jobs")
    
    current_time = datetime.now()
    x_axis_start = current_time-timedelta(1)
    ax.set_xlim(left=x_axis_start, right=datetime.now())
    
    # Borders are only being drawn on the first bar because of a bug in matplotlib 2.1.0, waiting for next minor release to fix it
    # https://github.com/matplotlib/matplotlib/issues/9351

    #ax.bar(times, short_jobs, width=1/24, align="edge", edgecolor="black", linewidth=1, facecolor="None", ls="solid")
    #ax.bar(times, long_jobs,  width=1/24, align="edge", edgecolor="black", linewidth=1, facecolor="None", bottom=short_jobs, ls="solid")

    ax.set_xlabel("Time")
    ax.set_ylabel("Jobs")
    ax.set_title(queue)

    ax.xaxis.set_major_formatter(DateFormatter("%H:%M"))

    ax.legend()
    plt.show()

for queue in set(result.match_apf_queue):
    if not "LANCS" in queue: continue
    times = [datetime.strptime(time, "%Y-%m-%d %H:%M:%S.%f") for q, time in zip(result.match_apf_queue, result.job_time) if q == queue]
    total_jobs = np.array([int(jobs) for q, jobs in zip(result.match_apf_queue, result.total_jobs) if q == queue])
    
    short_jobs = np.array([int(jobs) for q, jobs in zip(result.match_apf_queue, result.short_jobs) if q == queue])
    if sum(total_jobs) > 1000:
        plot_for_queue(queue, times, total_jobs, short_jobs)

