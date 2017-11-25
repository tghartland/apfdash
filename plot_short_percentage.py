from datetime import datetime, timedelta

import numpy as np
from matplotlib import pyplot as plt

from execute_query import execute_query_by_id

query_id = "cb3f97bc-861d-4505-a37e-ade33e2219aa" # get percentage of short jobs per day
database = "apfhistory"
output_location = "aws-athena-query-results-lancs"

result = execute_query_by_id(query_id, database, output_location)


lim = -60
dates = [datetime.strptime(date, "%Y-%m-%d") for date in result.day][lim:]
percentage_short = list(map(int, result.percentage_short))[lim:]
total_jobs = np.array([int(jobs) for jobs in result.total_jobs][lim:])
total_jobs_normalised = 100*total_jobs/max(total_jobs)

total_short = (np.array(percentage_short)/100) * total_jobs


trim_xrange = False
if trim_xrange:
    dates = dates[-60:]
    percentage_short = percentage_short[-60:]
    total_jobs = total_jobs[-60:]
    total_short = total_short[-60:]



# bar chart percentage of short jobs
plt.bar(dates, percentage_short)
plt.ylim(top=100)
plt.gca().yaxis.grid(True)
plt.xlabel("Date")
plt.ylabel("% short jobs per day")
plt.plot(dates, total_jobs_normalised, "r-")
plt.show()

plt.clf()
plt.cla()
plt.close()

# line chart showing total and short jobs
plt.plot(dates, total_jobs, label="Total jobs")
plt.plot(dates, total_short, "r", label="Short jobs")
plt.xlabel("Date")
plt.ylabel("Jobs")
plt.legend()
plt.show()

plt.clf()
plt.cla()
plt.close()

# stacked bar charts
total_long = total_jobs-total_short
plt.bar(dates, total_short, color="red", width=1)
plt.bar(dates, total_long, color="blue", width=1, bottom=total_short)
plt.xlabel("Date")
plt.ylabel("Jobs")
plt.legend()
plt.show()
