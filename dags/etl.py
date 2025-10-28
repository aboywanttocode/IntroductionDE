from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

# Import các hàm từ thư mục scripts


from scripts.validation import validate_function
from scripts.load import load_data_to_sqlite
from scripts.transform import push_transformed_paths
from scripts.transform import clean_weather_data, engineer_features, calculate_monthly_aggregates, save_transformed_data
def transform_function(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='file_path', task_ids='push_task')
    df = clean_weather_data(file_path)
    df, daily_avg, monthly_mode_df = engineer_features(df)
    monthly_avg = calculate_monthly_aggregates(df)

    daily_avg['formatted_date'] = daily_avg['Date']
    daily_avg['avg_temperature_c'] = daily_avg['Temperature (C)']
    daily_avg['avg_humidity'] = daily_avg['Humidity']
    daily_avg['avg_wind_speed_kmh'] = daily_avg['Wind Speed (km/h)']

    monthly_avg['avg_temperature_c'] = monthly_avg['Temperature (C)']
    monthly_avg['avg_humidity'] = monthly_avg['Humidity']
    monthly_avg['avg_wind_speed_kmh'] = monthly_avg['Wind Speed (km/h)']
    monthly_avg['avg_visibility_km'] = monthly_avg['Visibility (km)']
    monthly_avg['avg_pressure_millibars'] = monthly_avg['Pressure (millibars)']

    monthly = monthly_avg.merge(monthly_mode_df, on='Month', how='left')
    monthly['mode_precip_type'] = monthly['Mode']

    daily_path, monthly_path = save_transformed_data(daily_avg, monthly)

    kwargs['ti'].xcom_push(key='daily_path', value=daily_path)
    kwargs['ti'].xcom_push(key='monthly_path', value=monthly_path)
def push_file_path_to_xcom(**kwargs):
    file_path = 'data/unzipped_file.csv'
    kwargs['ti'].xcom_push(key='file_path', value=file_path)

def pull_file_path_from_xcom(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='file_path', task_ids='push_task')
    print(f"Received file path: {file_path}")

with DAG('kaggle_extraction_pipeline', start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:

    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_file_path_to_xcom,
        provide_context=True
    )

    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_file_path_from_xcom,
        provide_context=True
    )

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_function,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_to_sqlite',
        python_callable=load_data_to_sqlite,
        provide_context=True
    )
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_function,
        provide_context=True
)
    # Kết nối các task
    push_task >> pull_task >> transform_task >> validate_task >> load_task
