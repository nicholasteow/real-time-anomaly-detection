"""
================================================================================
Author        : Teow Si Hao Nicholas 
Copyright     : DSO National Laboratories
Date          : 26 July 2024
Purpose       : Script sets up a ZeroMQ subscriber to receive power meter data 
                and timestamps, inserts the data into MongoDB and saves it to a 
                JSON file. It dynamically generates collection names and file 
                names based on current timestamp to organize data efficiently.
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

# Global variables for MongoDB URI and database name
mongo_uri = "mongodb://localhost:27017"
database_name = "pubsub_data"

# Directory path for JSON file
json_directory = "../File_storage"

# Global variable for dynamic collection and file name
collection_and_file_name = None

def insert_into_mongodb_and_save_json(power_data, timestamp_data, name):
    try:
        # Connect to MongoDB
        client = pymongo.MongoClient(mongo_uri)
        db = client[database_name]

        # Always to store into the main spectromter collection
        main_collection_name = "power_meter_data"
        # Use the provided name for new collection and file creation
        collection_name = f"{name}_power_meter"

        # Create JSON file path using the same name
        json_file_path = os.path.join(json_directory, f"{collection_name}.json")

        # Access or create the collection
        main_collection = db[main_collection_name]
        collection = db[collection_name]

        # Prepare list to store documents
        file_storage_documents = []

        # Insert documents with Date_Time field as datetime object
        for power, timestamp_ms in zip(power_data, timestamp_data):
            # Convert timestamp from milliseconds to seconds
            timestamp_sec = timestamp_ms / 1000.0
            
            # Create datetime object from timestamp in seconds
            date_time_dt = datetime.fromtimestamp(timestamp_sec)

            document = {
                "date_time": date_time_dt,  # Insert Date_Time as datetime object
                "power": power,
                "timestamp": timestamp_ms
            }

            # Insert into MongoDB
            main_collection.insert_one(document)
            collection.insert_one(document)
            print(f"Inserted document into MongoDB: {document}")

            # Add document to list for JSON saving
            file_storage_documents.append({
                "date_time": date_time_dt.isoformat(),  # Convert datetime to ISO format string
                "power": power,
                "timestamp": timestamp_ms
            })

        # Save documents to JSON file
        save_to_json_file(json_file_path, file_storage_documents)

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

    # Subscribe to "power_meter"
    socket.setsockopt_string(zmq.SUBSCRIBE, "data/power_meter/")

    print("Subscriber connected to tcp://127.0.0.1:5556")

    try:
        while True:
            # Receive message
            message = socket.recv_string()
            print(message)

            try:
                json_part = message.split("data/power_meter/", 1)[1].strip()
                data = json.loads(json_part)

                power_data = data.get('power', [])
                timestamp_data = data.get('timestamps', [])
                

                # Insert into MongoDB
                insert_into_mongodb_and_save_json(power_data, timestamp_data, collection_and_file_name)
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing JSON message: {e}")

    except KeyboardInterrupt:
        print("Interrupted, closing subscriber...")
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    main()
