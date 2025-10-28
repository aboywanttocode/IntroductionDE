def validate_data(**kwargs):
    path = kwargs['ti'].xcom_pull(key='transformed_path', task_ids='transform_task')
    df = pd.read_csv(path)

   
    if df[['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']].isnull().any().any():
        raise ValueError("Missing values detected.")

   
    if not df['Temperature (C)'].between(-50, 50).all():
        raise ValueError("Temperature out of range.")
def check_missing_values(df):
    critical_columns = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Formatted Date']
    missing_report = df[critical_columns].isnull().sum()
    if missing_report.any():
        raise ValueError(f"Missing values found:\n{missing_report}")
def check_value_ranges(df):
    if not df['Temperature (C)'].between(-50, 50).all():
        raise ValueError("Temperature values out of range (-50 to 50°C).")
    if not df['Humidity'].between(0, 1).all():
        raise ValueError("Humidity values out of range (0 to 1).")
    if not (df['Wind Speed (km/h)'] >= 0).all():
        raise ValueError("Wind Speed contains negative values.")
def detect_outliers(df):
    Q1 = df['Temperature (C)'].quantile(0.25)
    Q3 = df['Temperature (C)'].quantile(0.75)
    IQR = Q3 - Q1
    outliers = df[(df['Temperature (C)'] < Q1 - 1.5 * IQR) | (df['Temperature (C)'] > Q3 + 1.5 * IQR)]
    if not outliers.empty:
        print(f"Outliers detected in Temperature:\n{outliers[['Formatted Date', 'Temperature (C)']]}")
