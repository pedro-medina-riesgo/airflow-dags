from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


# Wrapper function
def first_notebook_wrapper_function():
    # !/usr/bin/env python
    # coding: utf-8

    # In[1]:
    import numpy as np
    import pandas as pd

    # # Make a pandas DataFrame

    # In[2]:
    df = pd.DataFrame({'Alphabet': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'],
                       'A': [4, 3, 5, 2, 1, 7, 7, 5, 9],
                       'B': [0, 4, 3, 6, 7, 10, 11, 9, 13],
                       'C': [1, 2, 3, 1, 2, 3, 1, 2, 3]})
    print(df)

    # In[3]:
    return 'Whatever you return gets printed in the logs'

# Arguments
args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

# Dag
dag = DAG(
    dag_id='example_001',
    default_args=args,
)

# Task
first_notebook = PythonOperator(
    task_id='first_notebook_task',
    python_callable=first_notebook_wrapper_function,
    dag=dag,
)