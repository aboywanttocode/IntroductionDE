import pandas as pd
import numpy as np
import os

# Wind classification function (Common for Daily logic)
def categorize_wind(speed_kmh):
    try:
        speed = float(speed_kmh) / 3.6 # transform into m/s
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

# ---------------------------------------------------------
# 1. CLEAN DATA
# ---------------------------------------------------------
def clean_weather_data(file_path):
    df = pd.read_csv(file_path)

    # 1. Handling column names: Kaggle files are usually "Formatted Date" (with spaces)
    # We change everything to "Formatted_Date" (underscore) for easy coding
    if 'Formatted Date' in df.columns:
        df.rename(columns={'Formatted Date': 'Formatted_Date'}, inplace=True)

    if 'Formatted_Date' not in df.columns:
        raise KeyError("Missing 'Formatted Date' column in input file.")

    # Convert Formatted_Date sang datetime (UTC=True to fix timezone +0200)
    df['Formatted_Date'] = pd.to_datetime(df['Formatted_Date'], utc=True)
    df = df.dropna(subset=['Formatted_Date'])

    # 2. add 'Apparent Temperature (C)' to important columns
    # database will null if these columns are missing
    critical_cols = ['Temperature (C)', 'Apparent Temperature (C)', 'Humidity', 'Wind Speed (km/h)']
    
    for col in critical_cols:
        if col not in df.columns:
            df[col] = np.nan
        # transform to numeric, coerce errors to NaN
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Drop we are missing critical values
    df = df.dropna(subset=['Temperature (C)', 'Humidity'])
    df = df.drop_duplicates()

    return df


# ---------------------------------------------------------
# 2. ENGINEER FEATURES
# ---------------------------------------------------------
import pandas as pd
import numpy as np
import os

# ---------------------------------------------------------
# 
# ---------------------------------------------------------
def engineer_features(df):
    df['Formatted_Date'] = pd.to_datetime(df['Formatted_Date'], errors='coerce')
    df['Date'] = df['Formatted_Date'].dt.date
    df['Month'] = df['Formatted_Date'].dt.to_period('M')

    # 1. calculate daily average
    cols_daily = [
        'Temperature (C)', 'Apparent Temperature (C)', 'Humidity', 
        'Wind Speed (km/h)', 'Visibility (km)', 'Pressure (millibars)'
    ]
    # Filter which columns are actually in df
    cols_exist = [c for c in cols_daily if c in df.columns]
    
    daily_avg = df.groupby('Date')[cols_exist].mean().reset_index()

    # 2. calculate wind strength category
    if 'Wind Speed (km/h)' in daily_avg.columns:
        daily_avg['wind_strength'] = daily_avg['Wind Speed (km/h)'].apply(categorize_wind)
    else:
        daily_avg['wind_strength'] = np.nan

    # 3. FIX PRECIP TYPE NULL ERROR: Calculate Mode (most frequent) by DAY
    def get_mode(series):
        mode = series.mode()
        return mode.iloc[0] if not mode.empty else None

    if 'Precip Type' in df.columns:
        # Group by date and get the mode of Precip Type
        daily_precip = df.groupby('Date')['Precip Type'].agg(get_mode).reset_index()
        daily_precip.rename(columns={'Precip Type': 'precip_type'}, inplace=True)
        
        # Merge into daily_avg table
        daily_avg = daily_avg.merge(daily_precip, on='Date', how='left')
    else:
        daily_avg['precip_type'] = None

   # 4. Monthly Mode (Keep the old logic)
    if 'Precip Type' in df.columns:
        monthly_mode_df = df.groupby('Month')['Precip Type'].agg(get_mode).reset_index()
        monthly_mode_df.rename(columns={'Precip Type': 'Mode'}, inplace=True)
    else:
        monthly_mode_df = pd.DataFrame(columns=['Month', 'Mode'])
    
    monthly_mode_df['Month'] = monthly_mode_df['Month'].astype(str)

    return df, daily_avg, monthly_mode_df




# ---------------------------------------------------------
# 3. CALCULATE MONTHLY AGGREGATES
# ---------------------------------------------------------
def calculate_monthly_aggregates(df):
    df['Month'] = df['Formatted_Date'].dt.to_period('M')

    # FIX: Add Apparent Temperature to this list
    cols_to_avg = [
        'Temperature (C)', 
        'Apparent Temperature (C)', 
        'Humidity', 
        'Wind Speed (km/h)', 
        'Visibility (km)', 
        'Pressure (millibars)'
    ]
    existing_cols = [col for col in cols_to_avg if col in df.columns]

    if not existing_cols:
        raise KeyError("No numeric columns found for monthly aggregation.")

    monthly_avg = df.groupby('Month')[existing_cols].mean().reset_index()
    
    # --- FIX NULL MONTH ERROR IN DB ---
# Period('2006-04') must be converted to String '2006-04' to be saved to SQLite
    monthly_avg['Month'] = monthly_avg['Month'].astype(str)
    
    return monthly_avg


# ---------------------------------------------------------
# 4. SAVE TRANSFORMED DATA
# ---------------------------------------------------------
def save_transformed_data(daily_avg, monthly_avg, output_dir='transformed_data'):
    os.makedirs(output_dir, exist_ok=True)
    daily_path = os.path.join(output_dir, 'daily_transformed.csv')
    monthly_path = os.path.join(output_dir, 'monthly_transformed.csv')
    
    # Convert Date column in daily to string for sure
    if 'Date' in daily_avg.columns:
        daily_avg['Date'] = daily_avg['Date'].astype(str)

    daily_avg.to_csv(daily_path, index=False)
    monthly_avg.to_csv(monthly_path, index=False)
    
    return daily_path, monthly_path


# ---------------------------------------------------------
# 5. XCOM Helper (Optional)
# ---------------------------------------------------------
def push_transformed_paths(**kwargs):
    ti = kwargs['ti']
    daily_path, monthly_path = ti.xcom_pull(task_ids='transform_task')
    ti.xcom_push(key='daily_path', value=daily_path)
    ti.xcom_push(key='monthly_path', value=monthly_path)
