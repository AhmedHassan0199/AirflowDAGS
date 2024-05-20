from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2024, 4, 15),
    'retries': 1,
}

dag = DAG(
    'Airflow_test',
    default_args=default_args,
    schedule_interval='@weekly'
)

task1 = EmptyOperator(task_id='task1', dag=dag)

def my_python_function():
    print("Hello from my Python function!")

task2 = PythonOperator(
    task_id='task2',
    python_callable=my_python_function,
    dag=dag,
)

task1>>task2
