import pprint
from collections import namedtuple
from datetime import datetime
from dateutil.tz import tzutc
import io

import pandas as pd

import boto3

s3 = boto3.client("s3")

def most_recent_object_in_bucket(bucket_name):
    BucketItem = namedtuple("BucketItem", ["name", "modified"])
    all_items = []
    response = s3.list_objects(
        Bucket=bucket_name,
    )
    for obj in response["Contents"]:
        if ".metadata" in obj["Key"]: continue
        all_items.append(BucketItem(obj["Key"], obj["LastModified"]))
        
    return max(all_items, key=lambda obj: obj.modified)

class TrackedData:
    def __init__(self, bucket_name):
        pass

class Datasources:
    queue_comparison_30d = None
    individual_queues_24h = None
    
    query_data = {}

    @staticmethod
    def get_latest_data_for(bucket_name):
        if bucket_name in Datasources.query_data:
            if (datetime.now(tzutc())-Datasources.query_data[bucket_name]["checked_for_update"]).seconds <= 5*60:
                return Datasources.query_data[bucket_name]["data"].copy()
                
        name, date = most_recent_object_in_bucket(bucket_name)
        Datasources.query_data[bucket_name] = Datasources.query_data.get(bucket_name, {"modified": datetime(1, 1, 1, tzinfo=tzutc())})
        if date > Datasources.query_data[bucket_name]["modified"]:
            file_data = io.BytesIO()
            s3.download_fileobj(bucket_name, name, file_data)
            file_data.seek(0)
            bytes_data = file_data.read()
            string_data = str(bytes_data, "utf8")
            string_file = io.StringIO(string_data)
            string_file.seek(0)
            Datasources.query_data[bucket_name]["data"] = pd.read_csv(string_file)
            Datasources.query_data[bucket_name]["filename"] = name
            Datasources.query_data[bucket_name]["modified"] = date
            Datasources.query_data[bucket_name]["downloaded"] = datetime.now(tzutc())
        
        Datasources.query_data[bucket_name]["checked_for_update"] = datetime.now(tzutc())
        return Datasources.query_data[bucket_name]["data"].copy()
    
    @staticmethod
    def update_data_sources():
        return
        print(most_recent_object_in_bucket("aws-athena-query-results-lancs"))
        """
        Here:
        Select most recent files from bucket for each query TYPE
        and then store a pandas dataframe for each to use.
        Update the dataframe every 10 minutes or so, after
        checking if there is a new file to use.
        Use apschedular as in
        https://github.com/H4rtland/rpi_muons/blob/master/analysis/scheduler.py
        
        for bucket, variable in Datasources.tracked_buckets:
            file = most_recent_object_in_bucket(bucket_name)
            
        """
    
    @staticmethod
    def track_bucket(bucket_name, variable):
        Datasources.tracked_buckets.append([bucket_name, variable])
        
Datasources.update_data_sources()
