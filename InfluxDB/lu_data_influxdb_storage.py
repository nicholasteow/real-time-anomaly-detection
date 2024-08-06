"""
================================================================================
Author        : Teow Si Hao Nicholas 
Copyright     : DSO National Laboratories
Date          : 26 July 2024
Purpose       : Script subscribes to a ZeroMQ publisher to receive real-time 
                data, processes the data to detect anomalies, and simply stores 
                it in a InfluxDB database. Template code for future 
                inmplementation.
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

load_dotenv("secrets.env")

# InfluxDB connection details
influx_url = "http://localhost:8086"
influx_token = "SdZwyWH03sbN9RjUmlPeDaiIkBpE2TPJk8Eq3CCBd8eSq3nbTKEhs5LhVCAYq8MKoc6pNJrde0K2vcPGcUYp3Q"
influx_org = "DSO"
influx_bucket = "data_analysis"


# Load the pre-trained model
model = joblib.load('../Created_files/best_isolation_forest_model.pkl')

# Initialize InfluxDB client
influx_client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

def insert_into_influxdb(data):
    try:
        points = []
        for lcc_status_obj in data:
            lcc_desc = lcc_status_obj['desc']
            lu_status_arr = lcc_status_obj.get('lu_status_arr', [])
            timestamp = lcc_status_obj.get('timestamp')
            timestamp = datetime.now(timezone.utc).isoformat()
            print(f"Processing {len(lu_status_arr)} items for {lcc_desc}")

            for item in lu_status_arr:
                rename_mapping = {
                    'psu_curr': 'I_MEAS',
                    'ld_temp': 'TC_LD',
                    'cmb_temp': 'TC_CMB',
                    'cps_temp': 'TC_CPS',
                    'pd2': 'PD2'
                }

                # Apply the renaming to the item
                item_renamed = {rename_mapping.get(k, k): v for k, v in item.items()}

                try:
                    # Anomaly detection
                    features = ['I_MEAS', 'TC_LD', 'TC_CMB', 'TC_CPS', 'PD2']
                    new_X = pd.DataFrame([item_renamed])[features]
                    new_X_preprocessed = model.named_steps['preprocessor'].transform(new_X)
                    new_anomaly_label = model.named_steps['model'].predict(new_X_preprocessed)[0]

                    item['is_anomaly_pred'] = int(new_anomaly_label)

                    # Create InfluxDB point
                    point = Point(lcc_desc)
                    point.time(timestamp, WritePrecision.NS)  # Use current time as timestamp

                    float_features = ['psu_curr', 'ld_temp', 'cmb_temp', 'cps_temp', 'pd1', 'pd2']
                    
                    for key, value in item.items():
                        if key in float_features:
                            point.field(key, float(value))
                        elif key == 'is_anomaly_pred':
                            point.field(key, int(value))
                        else:
                            point.tag(key, str(value))
                    
                    points.append(point)
                    print(f"Prepared point for measurement {lcc_desc}: {item}")

                except Exception as e:
                    print(f"Error processing item {item}: {e}")
                    print(traceback.format_exc())

        # Write all points to InfluxDB
        if points:
            write_api.write(bucket=influx_bucket, org=influx_org, record=points)
            print(f"Inserted {len(points)} points into InfluxDB")
        else:
            print("No points to insert into InfluxDB")

    except Exception as e:
        print(f"Error storing data in InfluxDB: {e}")
        print(traceback.format_exc())

def main():
    # ZeroMQ context
    context = zmq.Context()

    # Socket to talk to the server
    socket = context.socket(zmq.SUB)

    # Connect to the publisher address
    socket.connect("tcp://127.0.0.1:5556")

    socket.setsockopt_string(zmq.SUBSCRIBE, "data/lcc_status_arr/")

    print("Waiting for messages...")

    try:
        while True:
            # Receive message
            curr_time = str(datetime.now())
            message = socket.recv_string()
            print(f"Message received at {curr_time}")
            
            try:
                payload = json.loads(message.split("data/lcc_status_arr/", 1)[1].strip())
                print(f"Parsed payload: {payload[:100]}...")  # Print first 100 characters of payload
                insert_into_influxdb(payload)
                print('\n')

            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing JSON message: {e}")
                print(f"Raw message: {message[:100]}...")  # Print first 100 characters of raw message

    except KeyboardInterrupt:
        print("Interrupted, closing subscriber...")
    finally:
        socket.close()
        context.term()
        influx_client.close()

if __name__ == "__main__":
    main()