from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


# Wrapper function
def my_function(my_param):
    import numpy as np
    import pandas as pd

    from pplaa import Project
    from airflow.models import Variable

    catalogs_folder = Variable.get("CATALOGS_FOLDER")

    print('Valor del parametro: ' + my_param)
    print('Valor de la variable: ' + catalogs_folder)

    prj = Project()
    prj.init(catalogs_folder + '/example_006')

    print('por aqui paso')
    

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
    description='A simple DAG',
)

# Task
my_task = PythonOperator(
    task_id='my_task',
    python_callable=my_function,
    op_kwargs={'my_param': 'Set a custom param'},
    dag=dag,
)