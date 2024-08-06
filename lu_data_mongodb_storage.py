"""
================================================================================
Author        : Teow Si Hao Nicholas 
Copyright     : DSO National Laboratories
Date          : 26 July 2024
Purpose       : Script subscribes to a ZeroMQ publisher to receive real-time 
                data, processes the data to detect anomalies, and simply stores 
                it in a MongoDB database.
================================================================================
Revision History
Version:	Date:			By:		Description
1.0			09-Jul-2024		TSHN	Initial creation	
================================================================================
"""

import os
import signal
import time
import pandas as pd
import zmq
import pymongo
from datetime import datetime
import json
import joblib

# Global variables for MongoDB URI and database name
mongo_uri = "mongodb://localhost:27017"
database_name = "casa_lcc_unit_data"
script_dir = os.path.dirname(os.path.abspath(__file__))
model = joblib.load(os.path.join(script_dir, 'Created_files/best_isolation_forest_model.pkl'))

# Global flag for exiting
exit_flag = False

# Function to handle termination signal
def signal_handler(signum, frame):
    global exit_flag
    print("Interrupt received. Exiting gracefully...")
    exit_flag = True

# Function to insert real-time data into MongoDB collections with anomaly predictions 
def insert_into_mongodb(data):
    client = pymongo.MongoClient(mongo_uri)
    timestamp = data[0]['timestamp']

    try:
        if database_name not in client.list_database_names():
            client[database_name]
            print("Initialising the database...\n")

        db = client[database_name]
        for lcc_status_obj in data:
            lu_status_arr = lcc_status_obj.get('lu_status_arr', [])

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
                collection_name = f"{item_renamed['desc']}"  # Adjust collection naming as needed

                # Check if collection exists, create if not
                if collection_name not in db.list_collection_names():
                    db.create_collection(collection_name)

                collection = db[collection_name]
                main_collection = db["full_lu_data"]

                try:
                    # Anomaly detection
                    features = ['I_MEAS', 'TC_LD', 'TC_CMB', 'TC_CPS', 'PD2']
                    new_X = pd.DataFrame([item_renamed])[features]
                    new_X_preprocessed = model.named_steps['preprocessor'].transform(new_X)
                    new_anomaly_label = model.named_steps['model'].predict(new_X_preprocessed)[0]

                    # Add the anomaly prediction to the row
                    item['is_anomaly_pred'] = int(new_anomaly_label)
                    item['timestamp'] = datetime.fromisoformat(timestamp)
                
                    # Insert into MongoDB
                    collection.insert_one(item)
                    main_collection.insert_one(item)
                    print(f"Inserted into collection {collection_name}: {item}")

                except Exception as e:
                    print(f"Error processing item {item}: {e}")

    except Exception as e:
        print(f"Error storing data in MongoDB: {e}")
    finally:
        if client:
            client.close()


def main():
    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)

    # ZeroMQ context
    context = zmq.Context()

    # Socket to talk to the server
    socket = context.socket(zmq.SUB)

    # Connect to the publisher address
    socket.connect("tcp://127.0.0.1:5556")

    socket.setsockopt_string(zmq.SUBSCRIBE, "data/lcc_status_arr/")

    try:
        while not exit_flag:
            try:
                # Receive message with a timeout
                curr_time = str(datetime.now())
                message = socket.recv_string(flags=zmq.NOBLOCK)
                payload = json.loads(message.split("data/lcc_status_arr/", 1)[1].strip())

                print(f"Message Received At " + curr_time)
                insert_into_mongodb(payload)
                print('\n')

            except zmq.Again:
                # No message received, wait before retrying
                time.sleep(0.1)

            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing JSON message: {e}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        print("Closing socket and terminating context...")
        socket.close()
        context.term()
        print("Cleanup complete. Exiting.")

if __name__ == "__main__":
    main()
