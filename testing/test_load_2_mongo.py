# test_load_2_mongo.py
import pytest
import pymongo
from nwo.data_processing.load_2_mongo import connect_to_mongo,load_jsonl_to_mongo
from unittest.mock import MagicMock, mock_open

def test_connect_to_mongo():
    uri = 'mongodb://localhost:27017'  # Replace with a test URI if necessary
    db = connect_to_mongo(uri)
    assert isinstance(db, pymongo.database.Database)



@pytest.fixture
def mock_db():
    return MagicMock()

def test_load_jsonl_to_mongo(mocker, mock_db):
    mocker.patch('builtins.open', mock_open(read_data='{"test": "data"}'))
    mocker.patch('nwo.data_processing.load_2_mongo.json.loads', return_value={"test": "data"})
    mocker.patch('nwo.data_processing.load_2_mongo.connect_to_mongo', return_value=mock_db)
    mocker.patch('nwo.data_processing.load_2_mongo.get_latest_file', return_value='dummy_path')

    collection_name = 'test_collection'
    jsonl_path = 'test_path'
    load_jsonl_to_mongo(mock_db, collection_name, jsonl_path)

    # Assuming that insert_one is called on the collection
    assert mock_db[collection_name].insert_one.called
