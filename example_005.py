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
    print(kwargs['dag'])
    print(kwargs['dag'].params)

    print(kwargs['dag'].params['x'])
    print(kwargs['dag'].params['y'])

    kwargs['dag'].params['x'] += 50
    kwargs['dag'].params['y'].append(4)

def test_function(**kwargs):

    print(kwargs['dag'].params['x'])
    print(kwargs['dag'].params['y'])

# Arguments
args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'params': {'x': x, 'y': y}
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