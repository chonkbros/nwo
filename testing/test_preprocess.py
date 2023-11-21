import pytest
import json
from unittest.mock import mock_open, MagicMock, patch
import nwo.data_processing.preprocess
import pandas as pd

# Provided JSON record
with open('/home/coli/interviews/nwo.ai/nwo/testing/test_json_load.jsonl', 'r') as infile:
    for line in infile:
        record = line
        mock_cleaned_data = json.loads(line)

def test_clean_and_process_data(mocker):
    # Mocking file operations
    mocker.patch('builtins.open', mock_open(read_data=record))
    
    # Mocking external functions
    mocker.patch('nwo.data_processing.preprocess.get_latest_file', return_value='dummy_path')

    # Mocking data processing
    mocker.patch('json.loads', return_value=mock_cleaned_data)
    mocker.patch('pandas.json_normalize', return_value=pd.DataFrame([mock_cleaned_data]))

    # Mock schema validator creation
    mock_validator_function = MagicMock()
    mocker.patch('fastjsonschema.compile', return_value=mock_validator_function)

    # Run the preprocess script
    nwo.data_processing.preprocess.process_data('mock_input_directory', 'mock_output_path', 'mock_failed_path')

    # Prepare the expected JSON object
    expected_json_object = json.loads(pd.DataFrame([mock_cleaned_data]).to_json(orient='records', lines=True, force_ascii=False))

    # Assert the validator function was called with cleaned data
    mock_validator_function.assert_called_with(expected_json_object)
