from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

# Notebook code
import numpy as np
import pandas as pd

# Wrapper functions
def my_task_1_wrapper_function():
    df1 = pd.DataFrame({'Alphabet': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'],
                       'A': [4, 3, 5, 2, 1, 7, 7, 5, 9],
                       'B': [0, 4, 3, 6, 7, 10, 11, 9, 13],
                       'C': [1, 2, 3, 1, 2, 3, 1, 2, 3]})
    print(df1)
    df1.describe()

def my_task_2_wrapper_function():
    df2 = pd.DataFrame(np.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))
    print(df2)
    df2.describe()

# Arguments
args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

# Dag
dag = DAG(
    dag_id='example_003',
    default_args=args,
)

# Tasks
t_003_1 = PythonOperator(
    task_id='t_003_1',
    python_callable=my_task_1_wrapper_function,
    dag=dag,
)

t_003_2 = PythonOperator(
    task_id='t_003_2',
    python_callable=my_task_2_wrapper_function,
    dag=dag,
)