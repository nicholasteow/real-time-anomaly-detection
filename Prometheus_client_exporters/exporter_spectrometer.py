"""
================================================================================
Author        : Teow Si Hao Nicholas 
Copyright     : DSO National Laboratories
Date          : 26 July 2024
Purpose       : Script collects and exposes spectrometer metrics from a MongoDB 
                database to Prometheus. Eventually, this script allows Grafana 
                to use Prometheus a data source to visualize metrics.
================================================================================
Revision History
Version:	Date:			By:		Description
1.0			26-Jun-2024		TSHN	Initial creation	
================================================================================
"""

import os
import logging
from prometheus_client import start_http_server, Gauge
import pymongo
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv("secrets.env")

# Update this to your local MongoDB URI
mongo_uri = os.environ.get("LOCAL_MONGO_URI")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create MongoDB client with increased timeouts
client = pymongo.MongoClient(mongo_uri, socketTimeoutMS=60000, connectTimeoutMS=60000)

# Function to get valid database from user, else prompt user to enter a valid database name
def get_database_name():
    while True:
        try:
            database_name = input("Enter the MongoDB database name you would like to scrape: ")
            if database_name not in client.list_database_names():
                logging.error(f"Database '{database_name}' does not exist.")
                print(f"Database '{database_name}' does not exist. Please enter a valid database name.")
            else:
                return database_name
        except Exception as e:
            logging.error(f"Error accessing MongoDB: {e}")
            print(f"Error accessing MongoDB: {e}. Please try again.")

# Function to get valid collection from user, else prompt user to enter a valid collection name
def get_collection_name(db):
    while True:
        try:
            collection_name = input("Enter the collection name you would like to scrape: ")
            if collection_name not in db.list_collection_names():
                logging.error(f"Collection '{collection_name}' does not exist in database '{db.name}'.")
                print(f"Collection '{collection_name}' does not exist in database '{db.name}'. Please enter a valid collection name.")
            else:
                return collection_name
        except Exception as e:
            logging.error(f"Error accessing MongoDB collection: {e}")
            print(f"Error accessing MongoDB collection: {e}. Please try again.")

# Get valid database and collection names
database_name = get_database_name()
db = client[database_name]
collection_name = get_collection_name(db)

collection = db[collection_name]

# Sanitize collection name for Prometheus metric names
collection_name_sanitized = collection_name.replace(".", "_").replace("-", "_")

# Prometheus metrics
mongo_up = Gauge(f'mongo_up_{collection_name_sanitized}', 'MongoDB is up')
document_count_metric_name = f'mongodb_{collection_name_sanitized}_document_count'
document_count = Gauge(document_count_metric_name, f'Total number of documents in the collection {collection_name}')
intensity_metric = Gauge(f'mongodb_{collection_name_sanitized}_intensity', 'Intensity value from MongoDB')
wavelength_metric = Gauge(f'mongodb_{collection_name_sanitized}_wavelength', 'Wavelength value from MongoDB')

def collect_metrics():
    try:
        # Check server status
        server_status = db.command("serverStatus")
        mongo_up.set(1)

        # Count the total number of documents in the collection
        total_documents = collection.count_documents({})
        document_count.set(total_documents)
        
        # Query MongoDB for recent data
        cursor = collection.find({}, {"intensity": 1, "wavelength": 1}).sort("date_time", pymongo.ASCENDING)
        
        for doc in cursor:
            intensity_value = doc.get('intensity', 0)
            wavelength_value = doc.get('wavelength', 0)
            
            # Update Prometheus metric
            intensity_metric.set(intensity_value)
            wavelength_metric.set(wavelength_value)
        
        logging.info("Metrics collected successfully")
    except pymongo.errors.ServerSelectionTimeoutError as e:
        mongo_up.set(0)
        logging.error(f"Connection timed out: {e}")
    except Exception as e:
        mongo_up.set(0)
        logging.error(f"Error collecting metrics: {e}")

if __name__ == '__main__':
    # Start Prometheus server
    start_http_server(9216)
    logging.info("Prometheus server started on port 9216")
    while True:
        collect_metrics()
        time.sleep(1)  # Scrape interval
