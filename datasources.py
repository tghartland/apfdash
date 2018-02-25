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
    """Get most recent result filename / modified date from bucket.
    
    list_objects returns maximum 1000 results,
    but if lifetime rules are used for the bucket
    this should never be an issue. Only the most
    recent result needs to be kept at all.
    """
    
    BucketItem = namedtuple("BucketItem", ["name", "modified"])
    all_items = []
    response = s3.list_objects(
        Bucket=bucket_name,
    )
    for obj in response["Contents"]:
        if ".metadata" in obj["Key"]: continue
        all_items.append(BucketItem(obj["Key"], obj["LastModified"]))
        
    return max(all_items, key=lambda obj: obj.modified)


class Datasources:
    queue_comparison_30d = None
    individual_queues_24h = None
    
    query_data = {}


    @staticmethod
    def get_latest_data_for(bucket_name):
        """Return a copy of the stored results dataframe for a given bucket name."""
        if bucket_name not in Datasources.query_data:
            Datasources.download_latest_data_for(bucket_name)
        
        return Datasources.query_data[bucket_name]["data"].copy()


    @staticmethod
    def download_latest_data_for(bucket_name):
        """Download most recent result file and parse into a pandas dataframe.
        
        Stores the dataframe and other metadata into Dataframe.query_data
        dictionary keyed by the bucket name.
        """
        name, date = most_recent_object_in_bucket(bucket_name)
        stored_query_result = {}
        
        file_data = io.BytesIO()
        s3.download_fileobj(bucket_name, name, file_data)
        file_data.seek(0)
        bytes_data = file_data.read()
        string_data = str(bytes_data, "utf8")
        string_file = io.StringIO(string_data)
        string_file.seek(0)
        stored_query_result["data"] = pd.read_csv(string_file)
        stored_query_result["filename"] = name
        stored_query_result["modified"] = date
        stored_query_result["downloaded"] = datetime.now(tzutc())
        
        stored_query_result["checked_for_update"] = datetime.now(tzutc())
        Datasources.query_data[bucket_name] = stored_query_result
