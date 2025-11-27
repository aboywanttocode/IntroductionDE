import os
import zipfile
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from airflow.decorators import task

@task
def extract_weather_history():
    """
    Extract Task:
    - Download dataset from Kaggle
    - unzip file ZIP
    - return file CSV path through XCom
    """
    # 1. Kaggle API Authentication
    os.environ["KAGGLE_CONFIG_DIR"] = os.path.expanduser("~/.kaggle")
    api = KaggleApi()
    api.authenticate()

    # 2. Download ZIP dataset
    zip_output_dir = "data/"
    zip_file_path = os.path.join(zip_output_dir, "weather_history.zip")

    api.dataset_download_files(
        "username/dataset-name",
        path=zip_output_dir,
        unzip=False
    )

    # 3. Unzip file
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(zip_output_dir)

    # 4. 
    csv_files = [f for f in os.listdir(zip_output_dir) if f.endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError("No CSV file found after extraction!")

    csv_path = os.path.join(zip_output_dir, csv_files[0])

    # 5. Pass file path to next task 
    return csv_path
