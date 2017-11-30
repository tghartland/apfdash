from datetime import datetime, date, timedelta
from matplotlib import pyplot as plt
import matplotlib.ticker as ticker

import numpy as np

from execute_query import execute_query_by_id

query_id = "00bb4f20-25b0-4d48-a16a-57870c7cbc2c"
database = "apfhistorylong"
output_location = "aws-athena-query-results-lancs"

result = execute_query_by_id(query_id, database, output_location)

bytes_scanned = result.query_metadata["QueryExecution"]["Statistics"]["DataScannedInBytes"]
execution_duration = result.query_metadata["QueryExecution"]["Statistics"]["EngineExecutionTimeInMillis"]

fig, ax = plt.subplots()

total_jobs = np.array(list(map(int, result.total_jobs)))
short_jobs = np.array(list(map(int, result.short_jobs)))
long_jobs = total_jobs-short_jobs

display_limit = 10

ax.barh(range(0, display_limit), short_jobs[:display_limit],
        color="red", label="Short jobs")
ax.barh(range(0, display_limit), total_jobs[:display_limit],
        color="blue", label="Long jobs", left=short_jobs[:display_limit])

ax.yaxis.set_major_locator(ticker.FixedLocator(range(0, display_limit)))
ax.yaxis.set_major_formatter(ticker.FixedFormatter(result.match_apf_queue[:display_limit]))

ax.invert_yaxis()

ax.set_ylabel("Queue")
ax.set_xlabel("# jobs")
ax.set_title("Top {} queues with most short jobs in past 30 days".format(display_limit))

ax.set_xlim(left=0)

plt.legend()

plt.show()

