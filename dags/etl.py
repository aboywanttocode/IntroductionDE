import sys



sys.path.append('/opt/airflow/scripts')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

# Import from scripts
from validation import validate_data 
from load import load_data_to_sqlite
from transform import (
    clean_weather_data,
    engineer_features,
    calculate_monthly_aggregates,
    save_transformed_data
)


# -----------------------------
# 1. EXTRACT TASK (FIX FINAL - CREATE FILE AT .CONFIG/KAGGLE)
# -----------------------------
def extract_data_from_kaggle(**kwargs):
    import os
    import json
    import zipfile
    
    # --- Username information ---
    K_USERNAME = "hoangphucle82"
    K_KEY = "1ec000bf4791bbbd775a3b5e90ce7530" 
    # ---------------------------

    # 1. REMOVE ANY CONFUSING ENVIRONMENT VARIABLES
# Let the library naturally find its default directory
    env_vars_to_clear = ['KAGGLE_USERNAME', 'KAGGLE_KEY', 'KAGGLE_CONFIG_DIR']
    for var in env_vars_to_clear:
        if var in os.environ:
            del os.environ[var]

    # 2. CREATE FILE IN THE RIGHT PLACE LOG REQUIRES: /home/airflow/.config/kaggle
    home_dir = os.path.expanduser("~") # /home/airflow
    
    # position 1: new file (Log report error)
    config_dir_new = os.path.join(home_dir, ".config", "kaggle")
    # position 2: backup file
    config_dir_old = os.path.join(home_dir, ".kaggle")

    # def which create small file
    def create_kaggle_file(directory):
        if not os.path.exists(directory):
            os.makedirs(directory)
        json_path = os.path.join(directory, "kaggle.json")
        with open(json_path, 'w') as f:
            json.dump({"username": K_USERNAME, "key": K_KEY}, f)
        os.chmod(json_path, 0o600)
        print(f" create file at: {json_path}")

    # create 2 places to be sure
    create_kaggle_file(config_dir_new)
    create_kaggle_file(config_dir_old)

    # 3. IMPORT and RUN
    from kaggle.api.kaggle_api_extended import KaggleApi 
    
    api = KaggleApi()
    api.authenticate()
    print("login successfully!")

    # 4. DOWNLOAD
    download_path = '/opt/airflow/data/'
    os.makedirs(download_path, exist_ok=True)
    
    dataset_name = 'muthuj7/weather-dataset'
    print(f"Downloading {dataset_name}...")
    
    api.dataset_download_files(dataset_name, path=download_path, unzip=False)
    
    # 5. UNZIP
    zip_path = os.path.join(download_path, 'weather-dataset.zip')
    if os.path.exists(zip_path):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(download_path)
            
    file_path = os.path.join(download_path, 'weatherHistory.csv')
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"cannot see file at {file_path}")

    kwargs['ti'].xcom_push(key='file_path', value=file_path)
    print(f" Extracted data to: {file_path}")
# 2. TRANSFORM FUNCTION (fix data NULL)
# -----------------------------
def transform_function(**kwargs):
    ti = kwargs['ti']
    # take path from real extract task
    file_path = ti.xcom_pull(key='file_path', task_ids='extract_task')

    if not file_path:
        raise ValueError("File path not found in XCom!")

    # run logic transform
    df = clean_weather_data(file_path)
    df, daily_avg, monthly_mode_df = engineer_features(df)
    monthly_avg = calculate_monthly_aggregates(df)

    # ---------------------------------------------------------
    # MAPPING Board DAILY (fix error NULL)
    # ---------------------------------------------------------
    # database requires many columns and need to rename to be match
    daily_avg = daily_avg.rename(columns={
        'Date': 'formatted_date',
        'Temperature (C)': 'temperature_c',
        'Apparent Temperature (C)': 'apparent_temperature_c',
        'Humidity': 'humidity',
        'Wind Speed (km/h)': 'wind_speed_kmh',
        'Visibility (km)': 'visibility_km',
        'Pressure (millibars)': 'pressure_millibars',
        # 'wind_strength' and 'precip_type' remain names
    })

    # Database has column 'avg_...' but daily_avg does not have these columns
    # We copy the value to avoid NULL
    daily_avg['avg_temperature_c'] = daily_avg['temperature_c']
    daily_avg['avg_humidity'] = daily_avg['humidity']
    daily_avg['avg_wind_speed_kmh'] = daily_avg['wind_speed_kmh']

    # ---------------------------------------------------------
    # MAPPING Board MONTHLY
    # ---------------------------------------------------------
    monthly = monthly_avg.merge(monthly_mode_df, on='Month', how='left')
    
    monthly = monthly.rename(columns={
        'Month': 'month',
        'Temperature (C)': 'avg_temperature_c',
        'Apparent Temperature (C)': 'avg_apparent_temperature_c',
        'Humidity': 'avg_humidity',
        'Wind Speed (km/h)': 'avg_wind_speed_kmh', # notion
        'Visibility (km)': 'avg_visibility_km',
        'Pressure (millibars)': 'avg_pressure_millibars',
        'Mode': 'mode_precip_type'
    })

    # save file
    daily_path, monthly_path = save_transformed_data(daily_avg, monthly)

    # Push XCom
    ti.xcom_push(key='daily_path', value=daily_path)
    ti.xcom_push(key='monthly_path', value=monthly_path)
    print(f" Transformed data saved to: {daily_path} & {monthly_path}")

# -----------------------------
# DAG DEFINITION
# -----------------------------
with DAG(
    'kaggle_extraction_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['weather', 'etl']
) as dag:

    # 1. Extract Task 
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data_from_kaggle,
        provide_context=True
    )

    # 2. Transform Task
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_function,
        provide_context=True
    )

    # 3. Validate Task
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        provide_context=True
    )

    # 4. Load Task
    load_task = PythonOperator(
        task_id='load_to_sqlite',
        python_callable=load_data_to_sqlite,
        provide_context=True
    )

    # -----------------------------
    # TASK DEPENDENCIES
    # -----------------------------
    extract_task >> transform_task >> validate_task >> load_task
