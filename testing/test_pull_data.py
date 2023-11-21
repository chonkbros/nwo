import pytest
from unittest.mock import patch, MagicMock, call
from nwo.data_processing.pull_data import download_file_from_google_drive, save_response_content

class MockResponse:
    def __init__(self, json_data, status_code, cookies=None):
        self.json_data = json_data
        self.status_code = status_code
        self.cookies = cookies or {}

    def json(self):
        return self.json_data

    def iter_content(self, chunk_size=1):
        return iter([b'data'])

def mocked_requests_get(*args, **kwargs):
    cookies = {'download_warning': 'test_token'}  # Example cookie, adjust as needed
    return MockResponse({"fake_key": "fake_value"}, 200, cookies)

@patch('requests.Session.get', side_effect=mocked_requests_get)
def test_download_file_from_google_drive(mock_get):
    # Setup
    file_id = 'test_file_id'
    destination = 'test_destination'

    # Exercise
    download_file_from_google_drive(file_id, destination)

    # Verify
    assert mock_get.call_count == 2
    first_call = call('https://docs.google.com/uc?export=download', params={'id': file_id}, stream=True)
    second_call = call('https://docs.google.com/uc?export=download', params={'id': file_id, 'confirm': 'test_token'}, stream=True)
    mock_get.assert_has_calls([first_call, second_call])
