from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta
import pandas as pd
import sys
sys.path.append('/home/azureuser/test/')
from config import OPENWEATHERMAP_API_KEY

def kelvin_to_celsius(k):
    return k - 273.15

def kelvin_to_fahrenheit(k):
    return (k - 273.15) * 9/5 + 32

def format_unix_timestamp(ts):
    return datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

def transform_load_data(**kwargs):
    task_instance = kwargs['ti']
    data= task_instance.xcom_pull(task_ids="extract_data_from_openweather")
    new_data = {
    'city_name': [data['name']],
    'weather_description': [data['weather'][0]['description']],
    'temperature_celsius': [kelvin_to_celsius(data['main']['temp'])],
    'temperature_fahrenheit': [kelvin_to_fahrenheit(data['main']['temp'])],
    'city_id': [data['id']],
    'feels_like_celsius': [kelvin_to_celsius(data['main']['feels_like'])],
    'feels_like_fahrenheit': [kelvin_to_fahrenheit(data['main']['feels_like'])],
    'min_temp_celsius': [kelvin_to_celsius(data['main']['temp_min'])],
    'min_temp_fahrenheit': [kelvin_to_fahrenheit(data['main']['temp_min'])],
    'max_temp_celsius': [kelvin_to_celsius(data['main']['temp_max'])],
    'max_temp_fahrenheit': [kelvin_to_fahrenheit(data['main']['temp_max'])],
    'pressure': [data['main']['pressure']],
    'humidity': [data['main']['humidity']],
    'wind_speed': [data['wind']['speed']],
    'time_of_record': [format_unix_timestamp(data['dt'])],
    'sunrise_time': [format_unix_timestamp(data['sys']['sunrise'])],
    'sunset_time': [format_unix_timestamp(data['sys']['sunset'])]
    }
    df = pd.DataFrame(new_data)
    



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


dag = DAG(
    'weather_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

check_weather_api_ready =HttpSensor(
    task_id='check_weather_api_ready',
    http_conn_id='weather_api',
    endpoint=f'/data/2.5/weather?q=chicago%20&appid={OPENWEATHERMAP_API_KEY}',
    dag=dag
)

extract_data_from_openweather= SimpleHttpOperator(
    task_id='extract_data_from_openweather',
    http_conn_id='weather_api',
    endpoint=f'/data/2.5/weather?q=chicago%20&appid={OPENWEATHERMAP_API_KEY}',
    method='GET',
    response_filter=lambda response:response.json(),
    log_response=True,
    dag=dag
) 

transform_weather_data = PythonOperator(
    task_id='transform_load_weather_data',
    python_callable=transform_load_data
) 


check_weather_api_ready >> extract_data_from_openweather >> transform_weather_data


  