"""
================================================================================
Author        : Teow Si Hao Nicholas 
Copyright     : DSO National Laboratories
Date          : 26 July 2024
Purpose       : Script sets up a ZeroMQ subscriber to receive spectrometer data, 
                including intensity, timestamp, and wavelength information. It 
                inserts this data into MongoDB and saves it to a JSON file. The 
                script dynamically generates collection names and file names 
                based on the current timestamp..
================================================================================
Revision History
Version:	Date:			By:		Description
1.0			25-Jun-2024		TSHN	Initial creation	
================================================================================
"""

import zmq
import pymongo
from datetime import datetime
import json
import os

mongo_uri = "mongodb://localhost:27017"
database_name = "pubsub_data"

# Directory path for JSON file
json_directory = "../File_storage"

# Global variable for dynamic collection and file name
current_datetime = None

def insert_into_mongodb_and_save_json(intensity_data, timestamp_data, wavelength_data, name):
    try:
        # Connect to MongoDB
        client = pymongo.MongoClient(mongo_uri)
        db = client[database_name]

        # Always to store into the main spectromter collection
        main_collection_name = "spectrometer_data"
        # Use the provided name for new collection and file creation
        collection_name = f"{name}_spectrometer"

        # Create JSON file path using the same collection name
        json_file_path = os.path.join(json_directory, f"{collection_name}.json")

        # Access or create the collection
        main_collection = db[main_collection_name]
        collection = db[collection_name]

        # Use the first timestamp for the entire document
        first_timestamp = timestamp_data if timestamp_data else None

        if first_timestamp is not None:
            # Convert timestamp from seconds to datetime
            date_time_dt = datetime.fromtimestamp(first_timestamp)

            document = {
                "date_time": date_time_dt,  # Insert Date_Time as datetime object
                "intensity": intensity_data,  # Insert entire intensity array
                "timestamp": first_timestamp,  # Use the first timestamp
                "wavelength": wavelength_data  # Insert entire wavelength array
            }

            # Insert the document into both collections
            main_collection.insert_one(document)
            collection.insert_one(document)
            print(f"Inserted document into MongoDB: {document}")

            # Prepare the document for JSON file
            file_storage_document = {
                "date_time": date_time_dt.isoformat(),  # Convert datetime to ISO format string
                "intensity": intensity_data,
                "timestamp": first_timestamp,
                "wavelength": wavelength_data
            }

            # Save document to JSON file
            save_to_json_file(json_file_path, file_storage_document)
        else:
            print("No valid timestamp found in the data")

    except Exception as e:
        print(f"Error inserting into MongoDB: {e}")
    finally:
        if client:
            client.close()

def save_to_json_file(json_file_path, data):
    # Save data to JSON file, overwriting any existing file
    with open(json_file_path, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data saved to JSON file: {json_file_path}")

def main():
    global collection_and_file_name

    # Generate dynamic collection and file name once
    collection_and_file_name = datetime.now().strftime('%Y%m%d_%H%M%S')

    # ZeroMQ context
    context = zmq.Context()

    # Socket to talk to the server
    socket = context.socket(zmq.SUB)

    # Connect to the publisher address
    socket.connect("tcp://127.0.0.1:5556")

    # Subscribe to "spectrometer"
    socket.setsockopt_string(zmq.SUBSCRIBE, "data/spectrometer/")

    print("Subscriber connected to tcp://127.0.0.1:5556")

    try:
        while True:
            # Receive message
            message = socket.recv_string()
            print(message)
            
            # Extract JSON part safely and correctly
            try:
                json_part = message.split("data/spectrometer/", 1)[-1].strip()
                data = json.loads(json_part)

                intensity_data = data.get('intensity', [])
                timestamp_data = data.get('timestamp')
                wavelength_data = data.get('wavelength', [])

                # Insert into MongoDB
                insert_into_mongodb_and_save_json(intensity_data, timestamp_data, wavelength_data, collection_and_file_name)
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing JSON message: {e}")

    except KeyboardInterrupt:
        print("Interrupted, closing subscriber...")
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    main()
