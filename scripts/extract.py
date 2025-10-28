import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi

def download_kaggle_dataset():
    os.environ['KAGGLE_CONFIG_DIR'] = os.path.expanduser('~/.kaggle')
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files('username/dataset-name', path='data/', unzip=False)


def unzip_file(zip_path, extract_to='data/'):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)