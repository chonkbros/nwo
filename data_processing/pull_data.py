import requests
import time
import zstandard  as zstd

#download files from download link 
timestr = time.strftime("%Y%m%d-%H%M%S")
zst_destination = f'raw/zst_downloads/data_pull_{timestr}.zst'
jsonl_destination = f'raw/jsonl_downloads/data_pull_{timestr}.jsonl'
file_id = '1E7iRwCp7IjvCjh_-owrt2NTMWnvgleZp'

def download_file_from_google_drive(file_id, destination):
    URL = "https://docs.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(URL, params = { 'id' : file_id }, stream = True)
    token = get_confirm_token(response)

    if token:
        params = { 'id' : file_id, 'confirm' : token }
        response = session.get(URL, params = params, stream = True)

    save_response_content(response, destination)    

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value

    return None

def save_response_content(response, destination):
    CHUNK_SIZE = 32768

    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)


#need a function to pull latest file from google drive once this goes into production
#currently just a single file so not able to test and no need
download_file_from_google_drive(file_id, zst_destination)


with open(zst_destination, 'rb') as compressed:
    with open(jsonl_destination, 'wb') as destination:
        dctx = zstd.ZstdDecompressor()
        dctx.copy_stream(compressed, destination)

print('data succesfully pulled from google drive today')