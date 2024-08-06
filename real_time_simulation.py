"""
================================================================================
Author        : Teow Si Hao Nicholas 
Copyright     : DSO National Laboratories
Date          : 26 July 2024
Purpose       : Script to simulate the process of sending real-time machine 
                operational data to a MongoDB database and generating
                CSV and JSON files for further analysis.
================================================================================
Revision History
Version:	Date:			By:		Description
1.0			13-Jun-2024		TSHN	Initial creation	
================================================================================
"""


import os
import logging
from prometheus_client import start_http_server, Gauge
import pymongo
import time
import joblib
import pandas as pd
import pytz
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv("secrets.env")

# MongoDB connection details
mongo_uri = os.environ.get("LOCAL_MONGO_URI")
database_name = "qw_16_unit_oper_data"

# Connect to MongoDB
client = pymongo.MongoClient(mongo_uri)
db = client[database_name]

# Load the pre-trained model
model = joblib.load('Created_files/best_isolation_forest_model.pkl')

# Load the dataframe and sample
df_simulate = pd.read_pickle("Created_files/no_psu_with_fake_data_df_test.pkl")

# Drop rows with NaN values in 'Date_Time'
df_simulate = df_simulate.dropna(subset=['Date_Time'])

# Filter and select 50 rows for each unit
date_to_filter = "2024-04-16"
rows_to_send = []

# Filter the DataFrame by date
df_filtered = df_simulate[
    df_simulate['Date_Time'].dt.date == pd.to_datetime(date_to_filter).date()
]
# Sort by 'I_MEAS' in ascending order
df_filtered = df_filtered.sort_values(by='I_MEAS', ascending=True)
# Select the top 10 rows for each 'unit_name'
rows_to_send = df_filtered.groupby('unit_names').head(1000)
# If needed, reset index or perform other operations
rows_to_send = rows_to_send.reset_index(drop=True)

# Function to clear a collection
def clear_collection(collection):
    result = collection.delete_many({})
    print(f"Cleared {result.deleted_count} documents from the collection.")

# Function to send data to MongoDB and create CSV and JSON
def send_data_to_database_and_create_files(collection, rows_to_send):
    timezone = pytz.timezone('Asia/Singapore')
    
    # Group rows by 'unit_names'
    grouped = rows_to_send.groupby('unit_names')
    
    for unit_name, group in grouped:
        all_rows = []  # List to store all rows for CSV and JSON
        
        for index, row in group.iterrows():
            # Gets the current time in UTC and convert to UTC+8, real data will not need this line
            current_time_utc = pd.Timestamp.utcnow()
            current_time_utc_plus_8 = current_time_utc.tz_convert(timezone)

            # Replace the 'Date_Time' column with the current timestamp in UTC+8, again real data will not need this line
            row.loc['Date_Time'] = current_time_utc_plus_8
            
            # Convert the row to a dictionary and prepare it for insertion
            features = ['I_MEAS', 'TC_LD', 'TC_CMB', 'TC_CPS', 'PD2']
            
            # Ensure new_X is shaped correctly as a DataFrame with a single row
            new_X = pd.DataFrame([row[features]])
            new_X_preprocessed = model.named_steps['preprocessor'].transform(new_X)
            new_anomaly_label = model.named_steps['model'].predict(new_X_preprocessed)[0]
            
            # Add the anomaly prediction to the row
            row['is_anomaly_pred'] = int(new_anomaly_label)  # Ensure it's serializable
            
            # Insert the row into a dynamically named MongoDB collection
            dynamic_collection_name = f"real_time_simulation_data_{unit_name}_{row['Date_Time'].date()}"
            dynamic_collection = db[dynamic_collection_name]
            dynamic_collection.insert_one(row.to_dict())

            # Also insert the row into the large original MongoDB collection
            collection.insert_one(row.to_dict())
            
            all_rows.append(row)  # Append row to the list for CSV and JSON creation
            
            print(f"Data sent to the collection {dynamic_collection_name}:", row.to_dict())
            time.sleep(0.1)
        
        # Convert all_rows to DataFrame
        all_rows_df = pd.DataFrame(all_rows)

        # Create JSON with all rows for importing in Tableau
        json_file_path = f"File_storage/real_time_simulation_data_{unit_name}_{pd.Timestamp.now().date()}.json"
        if os.path.exists(json_file_path):
            # Read existing data
            with open(json_file_path, 'r') as json_file:
                existing_json = json_file.read().strip()
            
            # Check if the existing file has valid JSON content
            if existing_json:
                existing_json = existing_json.split('\n')
                existing_json = [json.loads(line) for line in existing_json]
            else:
                existing_json = []

            # Convert new DataFrame rows to JSON records
            all_rows_df['Date_Time'] = all_rows_df['Date_Time'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')  # Convert datetime to string
            new_json = all_rows_df.to_json(orient='records', lines=True).strip()
            new_json = new_json.split('\n')
            new_json = [json.loads(line) for line in new_json]

            # Write combined data back to the JSON file
            with open(json_file_path, 'w') as json_file:
                json_file.write('\n'.join([json.dumps(record) for record in new_json]))
        else:
            # Write new data if the file does not exist
            all_rows_df['Date_Time'] = all_rows_df['Date_Time'].dt.strftime('%Y-%m-%d %H:%M:%S.%f')  # Convert datetime to string
            all_rows_json = all_rows_df.to_json(orient='records', lines=True)
            with open(json_file_path, 'w') as json_file:
                json_file.write(all_rows_json)
        print(f"JSON file updated: {json_file_path}")

# Example usage: limit to the first 100 rows for testing
collection_name = "real_time_simulation_data"
# clear_collection(db[collection_name])
send_data_to_database_and_create_files(db[collection_name], rows_to_send)
