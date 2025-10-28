import pandas as pd
import numpy as np

def clean_weather_data(file_path):
    df = pd.read_csv(file_path)

    # Convert Formatted Date to datetime
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], errors='coerce')

    # Drop rows with invalid dates
    df = df.dropna(subset=['Formatted Date'])

    # Handle missing/erroneous values in critical columns
    critical_cols = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']
    df[critical_cols] = df[critical_cols].apply(pd.to_numeric, errors='coerce')
    df = df.dropna(subset=critical_cols)

    # Remove duplicates
    df = df.drop_duplicates()

    return df
def engineer_features(df):
    df['Date'] = df['Formatted Date'].dt.date
    df['Month'] = df['Formatted Date'].dt.to_period('M')

    # Daily averages
    daily_avg = df.groupby('Date')[['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']].mean().reset_index()

    # Monthly mode for Precip Type
    def monthly_mode(series):
        mode = series.mode()
        return mode[0] if len(mode) == 1 else np.nan

    monthly_mode_df = df.groupby('Month')['Precip Type'].agg(monthly_mode).reset_index()
    monthly_mode_df.rename(columns={'Precip Type': 'Mode'}, inplace=True)

    # Wind strength categorization (convert km/h to m/s)
    def categorize_wind(speed_kmh):
        speed = speed_kmh / 3.6
        if speed <= 1.5: return 'Calm'
        elif speed <= 3.3: return 'Light Air'
        elif speed <= 5.4: return 'Light Breeze'
        elif speed <= 7.9: return 'Gentle Breeze'
        elif speed <= 10.7: return 'Moderate Breeze'
        elif speed <= 13.8: return 'Fresh Breeze'
        elif speed <= 17.1: return 'Strong Breeze'
        elif speed <= 20.7: return 'Near Gale'
        elif speed <= 24.4: return 'Gale'
        elif speed <= 28.4: return 'Strong Gale'
        elif speed <= 32.6: return 'Storm'
        else: return 'Violent Storm'

    df['wind_strength'] = df['Wind Speed (km/h)'].apply(categorize_wind)

    return df, daily_avg, monthly_mode_df
def calculate_monthly_aggregates(df):
    df['Month'] = df['Formatted Date'].dt.to_period('M')
    monthly_avg = df.groupby('Month')[['Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Visibility (km)', 'Pressure (millibars)']].mean().reset_index()
    return monthly_avg
def save_transformed_data(daily_avg, monthly_avg, output_dir='transformed_data'):
    import os
    os.makedirs(output_dir, exist_ok=True)
    daily_path = os.path.join(output_dir, 'daily_transformed.csv')
    monthly_path = os.path.join(output_dir, 'monthly_transformed.csv')
    daily_avg.to_csv(daily_path, index=False)
    monthly_avg.to_csv(monthly_path, index=False)
    return daily_path, monthly_path
from airflow.operators.python import PythonOperator

def push_transformed_paths(**kwargs):
    daily_path, monthly_path = kwargs['ti'].xcom_pull(task_ids='transform_task')
    kwargs['ti'].xcom_push(key='daily_path', value=daily_path)
    kwargs['ti'].xcom_push(key='monthly_path', value=monthly_path)