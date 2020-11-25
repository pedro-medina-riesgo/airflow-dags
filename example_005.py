from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

import os


# Wrapper function
def testing_wrapper_function():
    path = os.getcwd() 
    print(path)

    x = [obj for obj in os.listdir('dags')]
    print(x)

    y = [obj for obj in os.listdir(path + '/dags')]
    print(y)

# Arguments
args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

# Dag
dag = DAG(
    dag_id='example_005',
    default_args=args,
)

# Task
testing = PythonOperator(
    task_id='testing_task',
    python_callable=testing_wrapper_function,
    dag=dag,
)