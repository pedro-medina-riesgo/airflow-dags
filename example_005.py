from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

import numpy as np 
import pandas as pd

x = 50
y = [1, 2, 3]

# Wrapper function
def testing_wrapper_function(**kwargs):
    print(kwargs)

    print(kwargs['dag'].x)
    print(kwargs['dag'].y)

    kwargs['dag'].x += 50
    kwargs['dag'].y.append(4)

def test_function(**kwargs):

    print(kwargs['dag'].x)
    print(kwargs['dag'].y)

# Arguments
args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'x' = x,
    'y' = y,
}

# Dag
dag = DAG(
    dag_id='example_005',
    default_args=args,
    description='A simple DAG',
)

# Task
testing = PythonOperator(
    task_id='testing_task',
    python_callable=testing_wrapper_function,
    provide_context=True,
    dag=dag,
)

test = PythonOperator(
    task_id='test',
    python_callable=test_function,
    provide_context=True,
    dag=dag,
)

testing >> test