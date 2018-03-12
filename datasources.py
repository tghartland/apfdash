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

def get_last_object_per_day(bucket_name):
    """Get the last object per day from a bucket
    
    Used to construct the past 30d history from old results files.
    """
    BucketItem = namedtuple("BucketItem", ["name", "modified"])
    all_items = []
    response = s3.list_objects(
        Bucket=bucket_name,
    )
    for obj in response["Contents"]:
        if ".metadata" in obj["Key"]: continue
        all_items.append(BucketItem(obj["Key"], obj["LastModified"]))
    
    while response["IsTruncated"]:
        # Keep listing objects until returned list is no longer truncated
        next_marker = response["NextMarker"]
        response = s3.list_objects(
            Bucket=bucket_name,
            Marker=next_marker,
        )
        for obj in response["Contents"]:
            if ".metadata" in obj["Key"]: continue
            all_items.append(BucketItem(obj["Key"], obj["LastModified"]))
    
    # get list of unique last modified dates in bucket
    all_dates = set(map(lambda b_obj: b_obj.modified.date(), all_items))
    
    last_obj_per_day = []
    
    # get last modified object for each date
    for date in all_dates:
        objs_from_this_day = [obj for obj in all_items if obj.modified.date() == date]
        
        # Need to force use of this file that contains 30d history from before the switch to 24 hour bucket.
        # If you're reading this and it's after 12/04/18, you can delete this block of code.
        master_file = [obj for obj in objs_from_this_day if obj.name == "a3ffeb24-6b75-46dd-9732-2ddeac4236d1.csv"]
        if len(master_file) > 0:
            last_obj_per_day.append(master_file[0])
            continue
        
        last_this_day = max(objs_from_this_day, key=lambda obj: obj.modified)
        last_obj_per_day.append(last_this_day)
    
    return last_obj_per_day
    

def construct_30d_history(bucket_name):
    """Construct 30d history from multiple results files in bucket
    
    Used so that 30d history only needs 48 hour data bucket
    """
    last_obj_per_day = get_last_object_per_day(bucket_name)
    
    # dict keyed by (queue_name, date) pairs
    # used to store best data for each queue/date pair
    # best data is the version with most total jobs
    queue_date_data = {}
    
    # column names for rebuilding dataframe
    keys = None
    
    for obj in last_obj_per_day:
        file_data = io.BytesIO()
        s3.download_fileobj(bucket_name, obj.name, file_data)
        file_data.seek(0)
        bytes_data = file_data.read()
        string_data = str(bytes_data, "utf8")
        string_file = io.StringIO(string_data)
        string_file.seek(0)
        csv_data = pd.read_csv(string_file)
        keys = csv_data.keys()
        rows = [list(row[1]) for row in csv_data.iterrows()]
        
        for queue_name, date, total_jobs, empty, empty3, empty4 in rows:
            # add this queue/data pair to data if it doesn't already exist there
            if (queue_name, date) not in queue_date_data:
                queue_date_data[(queue_name, date)] = [queue_name, date, total_jobs, empty, empty3, empty4]
                continue
            
            # update queue/data pair with this data if new total_jobs is higher
            if total_jobs > queue_date_data[(queue_name, date)][2]:
                queue_date_data[(queue_name, date)] = [queue_name, date, total_jobs, empty, empty3, empty4]
    
    # remake dataframe from queue/data data and column names from downloaded csvs
    history_data = pd.DataFrame(list(queue_date_data.values()), columns=keys)
    
    return history_data, last_obj_per_day


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
        
        # Override for this bucket as the 30d history needs to be constructed from multiple results files.
        # Allows use of 48h bucket for 30d queries.
        if bucket_name == "aws-athena-apfdash-queue-history-30d-2":
            df, last_obj_per_day = construct_30d_history("aws-athena-apfdash-queue-history-30d-2")
            stored_query_result = {"data": df}
            
            most_recent_obj = max(last_obj_per_day, key=lambda obj: obj.modified)
            
            stored_query_result["filename"] = most_recent_obj.name + " (+ {} older files)".format(len(last_obj_per_day)-1)
            stored_query_result["modified"] = most_recent_obj.modified
            stored_query_result["downloaded"] = datetime.now(tzutc())
            stored_query_result["checked_for_update"] = datetime.now(tzutc())
            Datasources.query_data[bucket_name] = stored_query_result
            return
            
        
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
