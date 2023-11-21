import json
import pandas as pd
import logging
import fastjsonschema
import time 
import os 

def clean_string(s):
    """Remove double quotes from a string."""
    return s.replace(r'\n', '\\n').replace(r'"','\\"')


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

# File paths
timestr = time.strftime("%Y%m%d-%H%M%S")
output_file_path = f'processed/cleaned_jsonl/cleaned_submissions_pandas_{timestr}.jsonl'  
failed_submissions_file_path = f'processed/failed_jsonl/failed_submissions_{timestr}.jsonl'
input_directory = 'raw/jsonl_downloads/'


def process_data(input_directory, output_file_path, failed_submissions_file_path):
    #finds latest downloaded file to preprocess
    input_file_path = get_latest_file(input_directory)

    #load schema from json file and generate a validator to be used before loading data into final file
    with open('config/schema.json') as f:
        schema = json.load(f)
    validator = fastjsonschema.compile(schema)

    #cleans up characters that break JSON
    #unnests nested data field into individual columns
    #checks each json record against a json schema before loading
    #if a record does not pass schema checks then the misformed record is loaded into a failed_submissions file
    try:
        with open(input_file_path, 'r') as infile, open(output_file_path, 'w') as outfile,open(failed_submissions_file_path, 'w') as failed_outfile:
            for line_number,line in enumerate(infile,1):
                try:
                    data = json.loads(line)
                    cleaned_data = {key: clean_string(value) if isinstance(value, str) else value for key, value in data.items()}
                    # Flatten the data using json_normalize and write directly to the output file
                    df = pd.json_normalize(cleaned_data, max_level=1)
                    json_record = df.to_json(orient='records', lines=True, force_ascii=False)
                    try:
                        json_dict = json.loads(json_record)
                        validator(json_dict)
                        outfile.write(json_record)
                    except fastjsonschema.JsonSchemaException as e:
                        logging.error(f"Data failed validation on line {line_number}: {e}")
                        failed_outfile.write(json_record)
                except json.JSONDecodeError as e:
                    logging.error(f"JSON decoding error on line {line_number}: {e}")
                except Exception as e:
                    logging.error(f"Error processing data on line {line_number}: {e}")
    except IOError as e:
        logging.error(f"File operation error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    logging.info("Data processing completed.")

process_data(input_directory, output_file_path, failed_submissions_file_path)
