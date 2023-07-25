from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators import KafkaProducerOperator
from airflow.providers import MySqlOperator
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_project',
    default_args=default_args,
    description='ETL to Kafka to MySQL',
    schedule_interval=None,  
)


etl_process_script = 'ETL_main.ipynb'

# Replace with the actual Kafka topic where you want to write the data
kafka_topic = 'project_ETL'


def run_etl():
    spark_submit_cmd = f"spark-submit --master local[*] {etl_process_script}"
    os.system(spark_submit_cmd)

def write_to_kafka():
    pass
    # Đoạn này em nghĩ nên tách ra một đoạn write data và kafka từ file ETL_main
def load_to_mysql():
    pass
    # Cái load này em nghĩ mình ném đoạn code vào bắn cái output từ kafka vào đây

run_etl_task = PythonOperator(
    task_id='run_etl',
    python_callable=run_etl,
    dag=dag,
)

write_to_kafka_task = PythonOperator(
    task_id='write_to_kafka',
    python_callable=write_to_kafka,
    dag=dag,
)

load_to_mysql_task = PythonOperator(
    task_id='load_to_mysql',
    python_callable=load_to_mysql,
    dag=dag,
)


run_etl_task >> write_to_kafka_task >> load_to_mysql_task

