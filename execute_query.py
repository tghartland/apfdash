import io
import csv
import time
from collections import namedtuple
import pprint

import boto3
import botocore

# Requires AWS credentials to be set up
# boto looks in ~/.aws, which is where the command
# $ aws configure
# writes its credentials file
s3 = boto3.client("s3")
athena = boto3.client("athena")


class QueryExecutionException:
    pass


def execute_query_by_id(query_id, database, to_bucket, timeout=10):
    query = athena.get_named_query(NamedQueryId=query_id)
    
    response = athena.start_query_execution(
        QueryString=query["NamedQuery"]["QueryString"],
        QueryExecutionContext={
            "Database": database,
        },
        ResultConfiguration={
            "OutputLocation": "s3://{}/".format(to_bucket),
        }
    )
    
    execution = athena.get_query_execution(QueryExecutionId=response["QueryExecutionId"])
    
    # Returns an s3://.../... formatted location (which is useless for downloading the file)
    # So convert that to bucket name, file name pair
    bucket, key = execution["QueryExecution"]["ResultConfiguration"]["OutputLocation"][5:].split("/", 1)

    # Keep checking the state of the execution until it is no longer running
    # (or we reach the timeout)
    
    wait_start_time = time.time()
    
    while execution["QueryExecution"]["Status"]["State"] == "RUNNING":
        if time.time()-wait_start_time > timeout:
            raise QueryExecutionException("Results object has not been created within timeout limit")
        time.sleep(1)
        execution = athena.get_query_execution(QueryExecutionId=response["QueryExecutionId"])
    
    if not execution["QueryExecution"]["Status"]["State"] == "SUCCEEDED":
        raise QueryExecutionException("Execution final state: {}".format(execution["QueryExecution"]["Status"]["State"]))
    
    
    # Download the object into a file-like object
    file_data = io.BytesIO()
    s3.download_fileobj(bucket, key, file_data)
    
    # read the data from the file
    file_data.seek(0)
    data = str(file_data.read(), "utf8")
    
    # parse csv data with csv.reader
    reader = list(csv.reader(data.split("\n"), delimiter=","))
    column_headers = reader.pop(0)
    
    # Strip empty rows
    # I think it makes one at the end of the file
    rows = [row for row in reader if len(row) == len(column_headers)]
    # And remove any rows with blank data
    rows = [row for row in rows if min(map(len, row)) > 0]

    QueryResults = namedtuple("QueryResults", column_headers)
    
    columns = zip(*rows)

    return QueryResults(*columns)
