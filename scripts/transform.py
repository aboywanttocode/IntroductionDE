import pandas as pd
import numpy as np
import os
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------
# 1
# ---------------------------------------------------------
def clean_weather_data(file_path):
    df = pd.read_csv(file_path)

    
    if 'Formatted_Date' not in df.columns:
        raise KeyError("Missing 'Formatted_Date' column in input file.")

    # Convert Formatted_Date sang datetime
    df['Formatted_Date'] = pd.to_datetime(df['Formatted_Date'], errors='coerce')

    
    df = df.dropna(subset=['Formatted_Date'])

   
    critical_cols = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']
    for col in critical_cols:
        if col not in df.columns:
            df[col] = np.nan

    df[critical_cols] = df[critical_cols].apply(pd.to_numeric, errors='coerce')
    df = df.dropna(subset=critical_cols)

    # 
    df = df.drop_duplicates()

    return df


# ---------------------------------------------------------
# 2
# ---------------------------------------------------------
def engineer_features(df):
    
    df['Formatted_Date'] = pd.to_datetime(df['Formatted_Date'], errors='coerce')

   
    df = df.dropna(subset=['Formatted_Date'])

   
    df['Date'] = df['Formatted_Date'].dt.date
    df['Month'] = df['Formatted_Date'].dt.to_period('M')

    # --- Daily averages ---
    daily_avg = df.groupby('Date')[['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']].mean().reset_index()

    # --- Monthly mode của Precip Type ---
    def monthly_mode(series):
        mode = series.mode()
        return mode.iloc[0] if not mode.empty else np.nan

    if 'Precip Type' in df.columns:
        monthly_mode_df = df.groupby('Month')['Precip Type'].agg(monthly_mode).reset_index()
        monthly_mode_df.rename(columns={'Precip Type': 'Mode'}, inplace=True)
    else:
        monthly_mode_df = pd.DataFrame(columns=['Month', 'Mode'])

    # 
    def categorize_wind(speed_kmh):
        try:
            speed = float(speed_kmh) / 3.6
        except (ValueError, TypeError):
            return np.nan

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


# ---------------------------------------------------------
# 3️
# ---------------------------------------------------------
def calculate_monthly_aggregates(df):
    df['Formatted_Date'] = pd.to_datetime(df['Formatted_Date'], errors='coerce')
    df = df.dropna(subset=['Formatted_Date'])

    df['Month'] = df['Formatted_Date'].dt.to_period('M')

    # Check the exist column before group
    cols_to_avg = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Visibility (km)', 'Pressure (millibars)']
    existing_cols = [col for col in cols_to_avg if col in df.columns]

    if not existing_cols:
        raise KeyError("No numeric columns found for monthly aggregation.")

    monthly_avg = df.groupby('Month')[existing_cols].mean().reset_index()
    return monthly_avg


# ---------------------------------------------------------
# 4️
# ---------------------------------------------------------
def save_transformed_data(daily_avg, monthly_avg, output_dir='transformed_data'):
    os.makedirs(output_dir, exist_ok=True)
    daily_path = os.path.join(output_dir, 'daily_transformed.csv')
    monthly_path = os.path.join(output_dir, 'monthly_transformed.csv')
    daily_avg.to_csv(daily_path, index=False)
    monthly_avg.to_csv(monthly_path, index=False)
    return daily_path, monthly_path


# ---------------------------------------------------------
# 5
# ---------------------------------------------------------
def push_transformed_paths(**kwargs):
    ti = kwargs['ti']
    daily_path, monthly_path = ti.xcom_pull(task_ids='transform_task')
    ti.xcom_push(key='daily_path', value=daily_path)
    ti.xcom_push(key='monthly_path', value=monthly_path)
