from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


# Wrapper function
def my_function():
    import numpy as np
    import pandas as pd

    from pplaa import Project
    from airflow.models import Variable

    catalogs_folder = Variable.get("CATALOGS_FOLDER")
    print(catalogs_folder)
    

# Arguments
args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

# Dag
dag = DAG(
    dag_id='example_006',
    default_args=args,
)

# Task
my_task = PythonOperator(
    task_id='my_task',
    python_callable=my_function,
    dag=dag,
)

# prj = Project()