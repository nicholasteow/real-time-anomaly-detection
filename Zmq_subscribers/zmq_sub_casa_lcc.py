"""
================================================================================
Author        : Teow Si Hao Nicholas 
Copyright     : DSO National Laboratories
Date          : 26 July 2024
Purpose       : Script sets up a ZeroMQ subscriber to receive data, performs 
                anomaly detection using a pre-trained model, and publishes 
                detected anomalies back.
================================================================================
Revision History
Version:	Date:			By:		Description
1.0			26-Jun-2024		TSHN	Initial creation	
================================================================================
"""

import os
import time
import pandas as pd
import zmq
from datetime import datetime
import json
import joblib
import signal

# Global variables
mongo_uri = "mongodb://localhost:27017"
database_name = "casa_lcc_unit_data"
script_dir = os.path.dirname(os.path.abspath(__file__))
model = joblib.load(os.path.join(script_dir, '../Created_files/best_isolation_forest_model.pkl'))
exit_flag = False

def write_anomalies_to_file(anomalies_dict, timestamp):
    filepath = "../File_Storage/anomalies_records.json"
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    record = {timestamp: anomalies_dict}
    
    # Read existing records
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            try:
                existing_records = json.load(f)
            except json.JSONDecodeError:
                existing_records = {}
    else:
        existing_records = {}
    
    # Append new record
    existing_records.update(record)
    
    # Write updated records back to file
    with open(filepath, 'w') as f:
        json.dump(existing_records, f, indent=4)
    
    print(f"Anomalies appended to file: {filepath}")
    return filepath

# Handles termination signal to exit the script gracefully
def signal_handler(signum, frame):
    global exit_flag
    print("Ctrl+C received. Exiting gracefully...")
    exit_flag = True

# Detects anomalies in the received data and publishes the results.
def anomaly_detection_and_publish(data, publisher, curr_time):
    anomalies_dict = {}
    timestamp = curr_time

    try:
        for lcc_status_obj in data:
            lcc_desc = lcc_status_obj.get('desc')
            lu_status_arr = lcc_status_obj.get('lu_status_arr', [])
            lu_anomalies = []

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

                    # Add the anomaly prediction to the row
                    item['is_anomaly_pred'] = int(new_anomaly_label)
                    
                    # if anomaly is suspected, add the unit's index with reference to the data
                    if new_anomaly_label == -1:
                        lu_anomalies.append(item['idx'])

                except Exception as e:
                    print(f"Error processing item {item}: {e}")
            anomalies_dict[lcc_desc] = lu_anomalies

        # Print out the unit names with anomalies detected
        filepath = write_anomalies_to_file(anomalies_dict, timestamp)
        message = json.dumps(anomalies_dict)
        socket_message = ("data/anomaly/" + message)
        publisher.send_string(socket_message)
        print(f"Published message: {socket_message}")

    except Exception as e:
        print(f"Error Data Not Found: {e}")
    return filepath


def main():
    # Main function to set up ZeroMQ contexts and sockets, handle messages, and manage anomalies.
    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)

    # ZeroMQ context
    context = zmq.Context()

    # Socket to talk to the server
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://127.0.0.1:5556")
    socket.setsockopt_string(zmq.SUBSCRIBE, "data/lcc_status_arr/")
    print("Subscriber connected to tcp://127.0.0.1:5556\n")

    publisher = context.socket(zmq.PUB)
    publisher.connect("tcp://127.0.0.1:5555")
    print(f"Publisher connected to tcp://127.0.0.1:5555\n")

    try:
        while not exit_flag:
            try:
                # Receive message with a timeout
                curr_time = str(datetime.now())
                message = socket.recv_string(flags=zmq.NOBLOCK)
                print(message)
                payload = json.loads(message.split("data/lcc_status_arr/", 1)[1].strip())

                print(f"Message Received At " + curr_time)
                anomaly_detection_and_publish(payload, publisher, curr_time)
                print('\n')

            except zmq.Again:
                # No message received, wait before retrying
                time.sleep(0.1)

            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing JSON message: {e}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        print("Closing sockets and terminating context...")
        socket.close()
        publisher.close()
        context.term()
        print("Cleanup complete. Exiting.")

if __name__ == "__main__":
    main()
