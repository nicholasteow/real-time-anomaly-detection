"""
================================================================================
Author        : Teow Si Hao Nicholas 
Copyright     : DSO National Laboratories
Date          : 26 July 2024
Purpose       : Script simulates real-time data generation and serves the data
                via a Flask RESTful API. It creates a Flask app to simulate 
                real-time data feed with continuous updates and serve the 
                latest payload via a RESTful API endpoint.
================================================================================
Revision History
Version:	Date:			By:		Description
1.0			09-Jul-2024		TSHN	Initial creation	
================================================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime
import time
from flask import Flask, jsonify
import json
import threading
import signal
import sys
import random

app = Flask(__name__)

# Global variable to store the last payload
stored_payload = None
exit_flag = threading.Event()
counters = {"lcc1": 0, "lcc2": 0}

# Load the dataframe and drop the 'is_anomaly_pred' and 'is_anomaly_truth' column temporarily for simulation
df_simulate = pd.read_pickle("Created_files/no_psu_with_fake_data_df_train.pkl")
df_simulate = df_simulate.drop(columns=['is_anomaly_pred', 'is_anomaly_truth'])

# For now, create another 16 units by changing the unit_names column
new_unit_names = ['L01'] + [f'L{str(i).zfill(2)}' for i in range(17, 33)]

# Select all from the current DataFrame
new_created_df = df_simulate.sample(frac=1, random_state=42)

# Assign a random new unit_name to these rows
new_created_df['unit_names'] = np.random.choice(new_unit_names, size=len(new_created_df))

# Combine the new and original DataFrames
final_df = pd.concat([df_simulate, new_created_df])

final_df = final_df.rename(columns={
    'unit_names': 'desc',
    'I_MEAS': 'psu_curr',
    'TC_LD': 'ld_temp',
    'TC_CMB': 'cmb_temp',
    'TC_CPS': 'cps_temp',
    'PD1': 'pd1',
    'PD2': 'pd2'
})

final_df = final_df[['desc', 'ld_temp', 'cmb_temp', 'cps_temp', 'pd2', 'psu_curr']]

# Group by unit_names and convert to a dictionary of DataFrames
grouped_df_dict = {unit_name: group for unit_name, group in final_df.groupby('desc')}

# Function to generate real-time feed DataFrame
def generate_realtime_feed():
    # Create a DataFrame for real-time feed
    realtime_feed_df = pd.DataFrame()

    default_values = {
        "counter": 0,
        "lu_state": 0,
        "lu_power": 0.0,
        "lu_power_state": 0,
        "mon_state": 0,
        "usage_s": 0,
        "seed_status": 0,
        "psu_status": 0,
        "psu_volt": 0.0,
        "flow": 0
    }

    # Iterate over each group and select one row, update Date_Time, and predict anomaly
    for unit_name, group in grouped_df_dict.items():
        row = group.sample(n=1).copy()  # Randomly select one row and copy
        row.insert(row.columns.get_loc('pd2'), 'pd1', 9.99)
        row.insert(row.columns.get_loc('desc'), 'idx', row['desc'].str.replace('L', '').astype(int) - 1)

        for key, value in default_values.items():
            row[key] = value
        
        row['lu_state'] = random.randint(0, 9)
        row['lu_power'] = round(random.uniform(0, 1000), 2)
        row['seed_status'] = random.choice([0, 1, 2])
        row['psu_status'] = random.choice([0, 1, 2, 3])
        row['flow'] = random.choice([0, 1])

        # Append to the real-time feed DataFrame
        realtime_feed_df = pd.concat([realtime_feed_df, row])

    return realtime_feed_df

# Function to update the global stored_payload with the latest real-time data.
def update_stored_payload():
    global stored_payload
    global counters

    timestamp = datetime.now().isoformat()
    realtime_feed_df_1 = generate_realtime_feed()
    realtime_feed_df_2 = generate_realtime_feed()
    realtime_feed_df_2 = realtime_feed_df_2[realtime_feed_df_2['desc'].apply(lambda x: int(x[1:]) <= 23)]

    # Convert to JSON with ISO date format and orient='records'
    json_data1 = realtime_feed_df_1.to_json(date_format='iso', orient='records')
    json_data2 = realtime_feed_df_2.to_json(date_format='iso', orient='records')

    # Create the payload dictionary
    payload = [
        {
            "idx": 0,
            "desc": "lcc1",
            "counter": counters["lcc1"],
            "lcc_state": 0,
            "lcc_power": 0.0,
            "lcc_power_state": 0,
            "mode": 0,
            "rmux_status_data": {
                    "dewpoint": 27.8,
                    "humidity": 73.2,
                    "leak1": "Open",
                    "leak2": "Open",
                    "temperature": 33.3
                },
            "timestamp": timestamp,
            "lu_status_arr": json.loads(json_data1)
        },
        {
            "idx": 1,
            "desc": "lcc2",
            "counter": counters["lcc2"],
            "lcc_state": 0,
            "lcc_power": 0.0,
            "lcc_power_state": 0,
            "mode": 0,
            "rmux_status_data": {
                    "dewpoint": 27.8,
                    "humidity": 73.2,
                    "leak1": "Open",
                    "leak2": "Open",
                    "temperature": 33.3
                },
            "timestamp": timestamp,
            "lu_status_arr": json.loads(json_data2)
        }
    ]


    # Store the payload
    stored_payload = payload
    print("Payload Updated!")


@app.route('/casa_lcc_data', methods=['GET'])

# Function that serves the latest stored payload via an API endpoint
def get_stored_payload():
    global stored_payload
    if stored_payload:
        return jsonify(stored_payload), 200
    else:
        return "No payload available", 404


# Continuous update loop
def continuous_update():
    while not exit_flag.is_set():
        update_stored_payload()
        time.sleep(0.5)
    print("Continuous update thread exiting...")

# Continuous counter increment loop
def continuous_counter_increment():
    while not exit_flag.is_set():
        counters["lcc1"] += 1
        counters["lcc2"] += 1
        time.sleep(1)
    print("Continuous counter increment thread exiting...")

# Signal handler for graceful exit
def signal_handler(signum, frame):
    print("Ctrl+C received. Exiting gracefully...")
    exit_flag.set()
    sys.exit(0)

# Change time.sleep to desired time increment (in seconds)
if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)

    continuous_update_thread = threading.Thread(target=continuous_update)
    continuous_counter_increment_thread = threading.Thread(target=continuous_counter_increment)
    continuous_update_thread.start()
    continuous_counter_increment_thread.start()

    try:
        # Run Flask app
        app.run(debug=False)
    finally:
        print("Flask app stopped. Cleaning up...")
        exit_flag.set()
        continuous_update_thread.join()
        print("Cleanup complete. Exiting.")
