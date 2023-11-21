import os
import time
from datetime import datetime, timedelta
import pymongo
from bson.objectid import ObjectId

def check_mongo():
    client = pymongo.MongoClient('mongodb://localhost:27017')
    db_name = 'nwo'
    client = client[db_name]
    collection = client['submissions']
    if collection.count_documents({}) == 0:
        print("The collection is empty.")
        pass
    else:
        latest_document = collection.find().sort({'_id':-1}).limit(1)
        for doc in latest_document:
            timestamp = ObjectId(doc['_id']).generation_time
            payload = timestamp.date()

            return payload

def check_for_file(directory,cadence):
    #check for cadence
    now = datetime.now()
    file_found = None
    # Iterate over files in the directory
    if len(os.listdir(directory)) == 0:
        print('directory is empty')
        return False
    else:            
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)

            # Get the creation time of the file
            creation_time = os.path.getctime(file_path)
            creation_date = datetime.fromtimestamp(creation_time)
            
            # Check based on cadence
            if cadence == 'day' and creation_date.date() == now.date():
                file_found = filename
                print(f"A file was already created today: {file_found}")
                return file_found
            elif cadence == 'week' and now.isocalendar()[1] == creation_date.isocalendar()[1]:
                file_found = filename
                print(f"A file was already created this week: {file_found}")
                return file_found
            elif cadence == 'hour' and creation_date.hour == now.hour and creation_date.date() == now.date():
                file_found = filename
                print(f"A file was already created this hour: {file_found}")
                return file_found

            else:
                print(f"No .jsonl files created this {cadence} were found.")
                return False