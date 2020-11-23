from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


# Wrapper
def notebook_wrapper_function():
    # !/usr/bin/env python
    # coding: utf-8

    # In[1]:
    import numpy as np
    import pandas as pd
    from time import sleep

    # # Make a pandas DataFrame

    # In[2]:
    df = pd.DataFrame({'Alphabet': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'],
                       'A': [4, 3, 5, 2, 1, 7, 7, 5, 9],
                       'B': [0, 4, 3, 6, 7, 10, 11, 9, 13],
                       'C': [1, 2, 3, 1, 2, 3, 1, 2, 3]})
    print(df)

    # In[3]:
    return 'Whatever you return gets printed in the logs'

# DAG
args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='example_001',
    default_args=args,
)

task = PythonOperator(
    task_id='task',
    python_callable=notebook_wrapper_function,
    dag=dag,
)