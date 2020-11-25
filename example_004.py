from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

# Notebook code
import numpy as np
import pandas as pd

def my_new_print_function(df):
    print(df)
    df.describe()
    return 'print from a dependency'

# Wrapper functions
def my_function_1():
    df1 = pd.DataFrame({'Alphabet': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'],
                       'A': [4, 3, 5, 2, 1, 7, 7, 5, 9],
                       'B': [0, 4, 3, 6, 7, 10, 11, 9, 13],
                       'C': [1, 2, 3, 1, 2, 3, 1, 2, 3]})
    print(my_new_print_function(df1))

def my_function_2():
    df2 = pd.DataFrame(np.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))
    print(my_new_print_function(df2))

# Arguments
args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

# Dag
dag = DAG(
    dag_id='example_004',
    default_args=args,
)

# Tasks
my_task_1 = PythonOperator(
    task_id='my_task_1',
    python_callable=my_function_1,
    dag=dag,
)

my_task_2 = PythonOperator(
    task_id='my_task_2',
    python_callable=my_function_2,
    dag=dag,
)