import time
import pprint
from collections import namedtuple
from datetime import datetime
from dateutil.tz import tzutc
import io

import pandas as pd

from aws import session

s3 = session.client("s3")
athena = session.client("athena")

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
