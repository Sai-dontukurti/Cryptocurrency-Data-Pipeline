from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import pandas as pd

# Define the default args
default_args = {
    'owner': 'Sai_n',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'Crypto_daily_dag',
    default_args=default_args,
    description='Daily running DAG to extract raw crypto data',
    schedule_interval='@daily',  # Scheduled to run daily
)

# Define the function that will run the API call and save data
def api_calling_function():
    url = "https://api.coinpaprika.com/v1/tickers"
    
    print(f"Calling API: {url}")  # Log API call
    
    response = requests.get(url)
    if response.status_code == 200:
        print("API request successful")
        data = response.json()
    else:
        print(f"API request failed with status code {response.status_code}")
        return
    
    # Extract data
    coins = []
    for coin in data:
        coins.append({
            'name': coin['name'],
            'symbol': coin['symbol'],
            'price': coin['quotes']['USD']['price'],
            'updated_date': coin['last_updated'],
            'market_cap': coin['quotes']['USD']['market_cap'],
            'volume_24h': coin['quotes']['USD']['volume_24h'],
            'All_time_High': coin['quotes']['USD']['ath_price'],
            'All_time_high_date': coin['quotes']['USD']['ath_date'],
            'percent_change_24h': coin['quotes']['USD']['percent_change_24h']
        })

    # Save data to CSV
    df = pd.DataFrame(coins)
    
    # Generate timestamp
    time_stamp = datetime.now().strftime("%d-%m-%y")
    
    # Create folder if not exists
    folder_path = '/mnt/c/ES data/Projects/Crypto/data/raw_data'
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    # File path
    file_name = f'{folder_path}/crypto_bronze_data_{time_stamp}.csv'
    print(f"Saving the file to: {file_name}")
    
    df.to_csv(file_name, index=False)
    print("File saved successfully")

# Define the task
run_api_task = PythonOperator(
    task_id='run_api_calling_script',
    python_callable=api_calling_function,
    dag=dag,
)

# Set task dependencies (if any)
run_api_task
