from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


# Wrapper functions
def notebook_1_wrapper_function():
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
    df.head()

    # In[3]:
    return 'Whatever you return gets printed in the logs'

def notebook_2_wrapper_function():
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
    df.tail()

    # In[3]:
    return 'Whatever you return gets printed in the logs'

def notebook_3_wrapper_function():
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
    df.describe()

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
    dag_id='example_002',
    default_args=args,
)

# Tasks
notebook_1 = PythonOperator(
    task_id='notebook_1_task',
    python_callable=notebook_1_wrapper_function,
    dag=dag,
)

notebook_2 = PythonOperator(
    task_id='notebook_2_task',
    python_callable=notebook_2_wrapper_function,
    dag=dag,
)

notebook_3 = PythonOperator(
    task_id='notebook_3_task',
    python_callable=notebook_3_wrapper_function,
    dag=dag,
)
