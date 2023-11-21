import logging
import json
import pymongo
import os

def get_latest_file(directory):
    # Initialize variables to store the latest file and its creation time
    latest_file = None
    latest_time = 0

    # Iterate over files in downloads directory
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)

        # Get the creation time of the file
        creation_time = os.path.getctime(file_path)

        # Check if this file is the latest
        if creation_time > latest_time:
            latest_time = creation_time
            latest_file = filename
    if latest_file:
        print(f"The latest file is: {latest_file}")
        input_file_path = directory+latest_file
        return input_file_path
    else:
        print("No .jsonl files found.")
        return False

# Configure logging
logging.basicConfig(level=logging.INFO)
jsonl_path = 'processed/cleaned_jsonl/'  

def connect_to_mongo(uri):
    client = pymongo.MongoClient(uri)
    db_name = 'nwo'
    client = client[db_name]
    return client

def load_jsonl_to_mongo(db, collection_name, jsonl_path):
    collection = db[collection_name]
    file_path = get_latest_file(jsonl_path)
    with open(file_path, 'r') as file:
            for line in file:
                try:
                    data = json.loads(line)
                    collection.insert_one(data)
                except Exception as e:
                    logging.error(f"Error inserting data from {file_path}: {e}")


# Connect to MongoDB
client = connect_to_mongo('mongodb://localhost:27017')

# Load data into MongoDB
load_jsonl_to_mongo(client,'submissions', jsonl_path)
