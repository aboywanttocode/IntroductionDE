import pandas as pd
import os

def validate_data(**kwargs):
    """
    Main validation function for Airflow.
    Pulls the transformed CSV path from XCom and runs all checks.
    """
    ti = kwargs['ti']
    path = ti.xcom_pull(key='daily_path', task_ids='transform_task')

    if not path or not os.path.exists(path):
        raise FileNotFoundError(f" Transformed file not found at path: {path}")

    df = pd.read_csv(path)
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    # Run validation checks
    check_missing_values(df)
    check_value_ranges(df)
    detect_outliers(df)

    print(" Data validation completed successfully.")
    return True


def check_missing_values(df):
    """
    Check for missing critical columns and NaN values.
    """
    critical_columns = ['avg_temperature_c', 'avg_humidity', 'avg_wind_speed_kmh', 'formatted_date']

    # 
    missing_cols = [col for col in critical_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f" Missing critical columns: {missing_cols}")

    # 
    missing_report = df[critical_columns].isnull().sum()
    missing = missing_report[missing_report > 0]
    if not missing.empty:
        raise ValueError(f" Missing values found:\n{missing}")


def check_value_ranges(df):
    """
    Validate physical value ranges.
    """
    # Temperature range
    if not df['avg_temperature_c'].between(-50, 50).all():
        raise ValueError(" Temperature values out of range (-50 to 50°C).")

    # Humidity range
    if not df['avg_humidity'].between(0, 1).all():
        raise ValueError(" Humidity values out of range (0 to 1).")

    # Wind Speed non-negative
    if (df['avg_wind_speed_kmh'] < 0).any():
        raise ValueError(" Wind Speed contains negative values.")


def detect_outliers(df):
    """
    Detect outliers in Temperature using IQR method.
    """
    temp = df['avg_temperature_c']
    Q1, Q3 = temp.quantile([0.25, 0.75])
    IQR = Q3 - Q1

    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    outliers = df[(temp < lower_bound) | (temp > upper_bound)]

    if not outliers.empty:
        print(f" Outliers detected in Temperature (first 5 rows):\n"
              f"{outliers[['formatted_date', 'avg_temperature_c']].head()}")
    else:
        print("No significant temperature outliers detected.")
