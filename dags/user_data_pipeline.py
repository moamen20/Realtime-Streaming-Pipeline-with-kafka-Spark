from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
#from airflow.providers.docker.operators.docker import DockerOperator
#from docker.types import Mount
from datetime import datetime, timedelta
import requests
from kafka import KafkaProducer
import json
import os

# ---------- DAG SETTINGS ----------
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='user_data_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@hourly',
    catchup=False
)

# ---------- TASK: FETCH FROM API & SEND TO KAFKA ----------
def fetch_and_send():
    url = 'https://randomuser.me/api/'
    response = requests.get(url)
    user_data = response.json()

    producer = KafkaProducer(
        bootstrap_servers='broker:29092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send('user-data', user_data)
    producer.flush()

fetch_task = PythonOperator(
    task_id='fetch_user_data',
    python_callable=fetch_and_send,
    dag=dag
)

# ---------- TASK: SPARK process ----------
spark_task = SparkSubmitOperator(
        task_id='run_spark_app',
        application='/opt/spark-apps/process_data.py',
        conn_id='spark_default',  
        conf={"spark.master": "spark://spark-master:7077"},
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        verbose=True,
        dag=dag
    )



# ---------- DAG DEPENDENCIES ----------
fetch_task >> spark_task
