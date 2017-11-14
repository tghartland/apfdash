from datetime import datetime, timedelta

from matplotlib import pyplot as plt

from execute_query import execute_query_by_id

query_id = "cb3f97bc-861d-4505-a37e-ade33e2219aa" # get percentage of short jobs per day
database = "apfhistory"
output_location = "aws-athena-query-results-lancs"

result = execute_query_by_id(query_id, database, output_location)


dates = [datetime.strptime(date, "%Y-%m-%d") for date in result.day]
percentage_short = list(map(int, result.percentage_short))

plt.plot(dates, percentage_short)
plt.xlabel("Date")
plt.ylabel("% short jobs per day")
plt.show()
