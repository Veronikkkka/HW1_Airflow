from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import requests
from airflow.operators.sqlite_operator import SqliteOperator
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_table():
    b_create = SqliteOperator(
    task_id="create_table_sqlite",
    sqlite_conn_id="airflow_conn",
    sql="""
    CREATE TABLE IF NOT EXISTS measures
    (
    timestamp TIMESTAMP,
    temp FLOAT,
    humidity FLOAT,
    clouds FLOAT,
    wind_speed FLOAT
    );"""
    )

def get_weather(city, lat, lon, historical=False):
    if historical:
        base_url=f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={lat}&lon={lon}&dt={time}&appid=b56d72ff58624282e449d5d2b0c3d641"
    else:        
        base_url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude=hourly,daily&appid=b56d72ff58624282e449d5d2b0c3d641"    
    response = requests.get(base_url)

    if response.status_code == 200:
        weather_data = response.json()
        logging.info("Weather DATA: %s", weather_data)
        timestamp = weather_data['current']['dt']
        temp = weather_data['current']['temp']
        humidity = weather_data['current']['humidity']
        clouds = weather_data['current']['clouds']
        wind_speed = weather_data['current']['wind_speed']
        
        b_create = SqliteOperator(
        task_id="inject_data_sqlite",
        sqlite_conn_id="airflow_conn",
        sql = f"""
            INSERT INTO measures (timestamp, temp, humidity, clouds, wind_speed) VALUES
            ({timestamp}, {temp}, {humidity}, {clouds}, {wind_speed});
        """
        )
        
        return timestamp, temp
    else:
        print(f"Failed to fetch weather data for {city}")
        return None

def inject_data(ti):    
    template = f"""
    INSERT INTO measures (timestamp, temp) VALUES
    ({ti.xcom_pull(task_ids='extract_data_lviv')[0]},
    {ti.xcom_pull(task_ids='extract_data_lviv')[1]});
    """

    return template

with DAG(dag_id="hw1_dag", schedule_interval="@daily", start_date=datetime(2023, 11, 20)) as dag:
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    cities = [("lviv", "49.84", "24.03"), ("kyiv", "50.45", "30.52"), ("kharkiv", "49.99", "36.23"), ("odesa", "46.48", "30.73"), ("zhmerynka", "49.05", "28.39")]

    for (city, lat, lon) in cities:
        task_id_prefix = f"{city}"
        
        extract_data = PythonOperator(
            task_id=f"{task_id_prefix}_weather",
            python_callable=get_weather,
            op_args=[city, lat, lon],
            provide_context=True
        )

        # inject_data_task = PythonOperator(
        #     task_id=f"inject_data_{city}",
        #     python_callable=inject_data,
        #     provide_context=True,
        # )

        create_table_task >> extract_data #>> inject_data_task
