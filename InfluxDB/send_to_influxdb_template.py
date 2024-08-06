"""
================================================================================
Author        : Teow Si Hao Nicholas 
Copyright     : DSO National Laboratories
Date          : 26 July 2024
Purpose       : Script migrate time-series data from MongoDB to InfluxDB,  
                ensures that the data structure is appropriate for InfluxDB, 
                handling timestamps correctly and distinguishing 
                between fields and tags.
================================================================================
Revision History
Version:	Date:			By:		Description
1.0			22-Jun-2024		TSHN	Initial creation	
================================================================================
"""

import pandas as pd
import zmq
import json
from datetime import datetime, timezone
import joblib
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import traceback
import os
from dotenv import load_dotenv
import pymongo
from pymongo import MongoClient

load_dotenv("secrets.env")

# MongoDB connection details
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['qw_16_unit_oper_data']

# InfluxDB connection details
influx_url = "http://localhost:8086"
influx_token = os.environ.get("INFLUX_TOKEN")
influx_org = os.environ.get("INFLUX_ORG")
influx_bucket = "qw_16_unit_oper_data"

# Connect to InfluxDB
influx_client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

def document_to_point(collection_name, document):
    point = Point(collection_name)
    
    # Convert Date_Time to UTC datetime if it's a string
    if isinstance(document.get('Date_Time'), str):
        timestamp = datetime.fromisoformat(document['Date_Time']).astimezone(timezone.utc)
    else:
        timestamp = document.get('Date_Time', datetime.utcnow())
    
    point.time(timestamp, WritePrecision.NS)  # Set the timestamp with nanosecond precision

    # Handle fields
    for key, value in document.items():
        if key == '_id' or key == 'Date_Time':
            continue
        elif isinstance(value, (int, float)):
            point.field(key, value)
        elif isinstance(value, str):
            point.tag(key, value)
    
    return point

# Process specific collections
collections_to_process = ["final_lxx_data", "final_psu_data"]

for collection_name in collections_to_process:
    collection = mongo_db[collection_name]
    
    # Iterate through all documents in the collection
    for document in collection.find():
        point = document_to_point(collection_name, document)
        print(point)
        # Write the point to InfluxDB
        write_api.write(bucket=influx_bucket, org=influx_org, record=point)

print("Data import completed.")

# Close connections
mongo_client.close()
influx_client.close()
