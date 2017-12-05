import pprint
from collections import namedtuple

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
    
    tracked_buckets = []
    
    @staticmethod
    def update_data_sources():
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