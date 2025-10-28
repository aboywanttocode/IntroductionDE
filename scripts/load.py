import sqlite3

def setup_database(db_path='weather_data.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create daily_weather table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            formatted_date TEXT,
            temperature_c REAL,
            apparent_temperature_c REAL,
            humidity REAL,
            wind_speed_kmh REAL,
            visibility_km REAL,
            pressure_millibars REAL,
            wind_strength TEXT,
            avg_temperature_c REAL,
            avg_humidity REAL,
            avg_wind_speed_kmh REAL
        )
    ''')

    # Create monthly_weather table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS monthly_weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT,
            avg_temperature_c REAL,
            avg_apparent_temperature_c REAL,
            avg_humidity REAL,
            avg_visibility_km REAL,
            avg_pressure_millibars REAL,
            mode_precip_type TEXT
        )
    ''')

    conn.commit()
    return conn
import pandas as pd

def load_data_to_sqlite(**kwargs):
    ti = kwargs['ti']
    daily_path = ti.xcom_pull(key='daily_path', task_ids='validate_task')
    monthly_path = ti.xcom_pull(key='monthly_path', task_ids='validate_task')

    daily_df = pd.read_csv(daily_path)
    monthly_df = pd.read_csv(monthly_path)

    conn = setup_database()
    cursor = conn.cursor()

    # Insert daily data
    for _, row in daily_df.iterrows():
        cursor.execute('''
            INSERT INTO daily_weather (
                formatted_date, temperature_c, apparent_temperature_c, humidity,
                wind_speed_kmh, visibility_km, pressure_millibars, wind_strength,
                avg_temperature_c, avg_humidity, avg_wind_speed_kmh
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row.get('formatted_date'),
            row.get('temperature_c'),
            row.get('apparent_temperature_c'),
            row.get('humidity'),
            row.get('wind_speed_kmh'),
            row.get('visibility_km'),
            row.get('pressure_millibars'),
            row.get('wind_strength'),
            row.get('avg_temperature_c'),
            row.get('avg_humidity'),
            row.get('avg_wind_speed_kmh')
        ))

    # Insert monthly data
    for _, row in monthly_df.iterrows():
        cursor.execute('''
            INSERT INTO monthly_weather (
                month, avg_temperature_c, avg_apparent_temperature_c,
                avg_humidity, avg_visibility_km, avg_pressure_millibars, mode_precip_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            row.get('month'),
            row.get('avg_temperature_c'),
            row.get('avg_apparent_temperature_c'),
            row.get('avg_humidity'),
            row.get('avg_visibility_km'),
            row.get('avg_pressure_millibars'),
            row.get('mode_precip_type')
        ))

    conn.commit()
    conn.close()
