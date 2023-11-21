import subprocess
import functions as func
import yaml
from datetime import datetime
import os
import sys

def run_script(script):
    result = subprocess.run(['python', script], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error in {script}: {result.stderr}")
        exit(result.returncode)
    else:
        print(f"{script} executed successfully.")


#load yaml config file
with open('config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)
cadence = config['global']['cadence']



if func.check_for_file('raw/jsonl_downloads/',cadence) is False:
    run_script('data_processing/pull_data.py')

if func.check_for_file('processed/cleaned_jsonl/',cadence) is False:
    run_script('data_processing/preprocess.py')

if func.check_mongo() !=  datetime.now().date():
    run_script('data_processing/load_2_mongo.py')
else:
    print('files already loaded to mongo today')