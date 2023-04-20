from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime



def print_hello():
    
    print("Hello World")

default_args = {
    'start_date': datetime(2019, 1, 1),
    'owner': 'Juan Rosales',
    'email': ['rosales.juan7072@gmail.com'],
    'email_on_failure': True
}

dag = DAG(
    dag_id='DEV_Test_DAG',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1
)

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag
)

hello_task