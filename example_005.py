from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

import os
import numpy as np 
import pandas as pd

x = 50
y = [1, 2, 3]

# Wrapper function
def testing_wrapper_function(my_param, x, y):
    print('Valor del parametro: ' + my_param)
    print('--------------------------------------------------')

    path = os.getcwd() 
    print(path)
    print('--------------------------------------------------')

    t = [obj for obj in os.listdir('/opt/airflow')]
    print(t)
    print('--------------------------------------------------')

    t = [obj for obj in os.listdir('/opt/airflow/catalogs')]
    print(t)
    print('--------------------------------------------------')

    print(x)
    print(y)

    x += 50
    y.append(4)

def test_function(x, y):
    print(x)
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
    description='A simple DAG',
)

# Task
testing = PythonOperator(
    task_id='testing_task',
    python_callable=testing_wrapper_function,
    op_kwargs={'my_param': 'Set a custom param', 'x': x, 'y': y},
    dag=dag,
)

test = PythonOperator(
    task_id='test',
    python_callable=test_function,
    op_kwargs={'x': x, 'y': y},
    dag=dag,
)

testing >> test