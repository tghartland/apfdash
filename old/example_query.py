from execute_query import execute_query_by_id

query_id = "cb3f97bc-861d-4505-a37e-ade33e2219aa" # get percentage of short jobs per day
database = "apfhistory"
output_location = "aws-athena-query-results-lancs"

result = execute_query_by_id(query_id, database, output_location)

print(result.day[10])
print(result.total_jobs[10])
print(result.percentage_short[10])
