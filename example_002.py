from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


# Wrapper
def jupyter_wrapper_function_1():
    # !/usr/bin/env python
    # coding: utf-8

    # In[1]:

    import numpy as np
    import pandas as pd
    from time import sleep

    # # Make a pandas DataFrame

    # In[4]:

    df = pd.DataFrame({'Alphabet': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'],
                       'A': [4, 3, 5, 2, 1, 7, 7, 5, 9],
                       'B': [0, 4, 3, 6, 7, 10, 11, 9, 13],
                       'C': [1, 2, 3, 1, 2, 3, 1, 2, 3]})

    print(df)

    print('waiting on 40')
    sleep(40)
    print('waiting off 40')

    print(df)

    # In[ ]:

    return 'Whatever you return gets printed in the logs'

def jupyter_wrapper_function_2():

    import numpy as np
    import pandas as pd
    from time import sleep

    df = pd.DataFrame({'Alphabet': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'],
                       'A': [4, 3, 5, 2, 1, 7, 7, 5, 9],
                       'B': [0, 4, 3, 6, 7, 10, 11, 9, 13],
                       'C': [1, 2, 3, 1, 2, 3, 1, 2, 3]})

    print(df)

    print('waiting on 20')
    sleep(20)
    print('waiting off 20')

    print(df)

    # In[ ]:

    return 'Whatever you return gets printed in the logs'

# DAG
args = {
    'owner': 'Magician',
    'depends_on_past': False,
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='example_002',
    default_args=args,
    schedule_interval=None,
    tags=['example']
)

step1 = PythonOperator(
    task_id='step1',
    python_callable=jupyter_wrapper_function_1,
    dag=dag,
)

step2 = PythonOperator(
    task_id='step2',
    python_callable=jupyter_wrapper_function_2,
    dag=dag,
)

step1 >> step2
