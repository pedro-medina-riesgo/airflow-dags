from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


# Wrapper function
def my_function_007():
    from airflow.models import Variable
    catalogs_folder = Variable.get("CATALOGS_FOLDER")
    
    import numpy as np
    import pandas as pd

    import warnings
    warnings.filterwarnings("ignore")

    from pplaa import Project
    
    prj = Project()
    prj.init(catalogs_folder + '/example_007')

    prj.cat.info

    df = prj.cat.raw.locations.load()

    df.shape

    df = df.head(500)

    prj.cat.save(data=df, dsid='intermediate.locations')

    prj.cat.intermediate.locations.load()

    # prj.cat.profile(dsid='intermediate.locations')

    prj.cat.info

    validation_rules = {
        'intermediate.locations': {
            'rules': [
                {
                    'rtype': 'REQUIRED_COLUMNS_RULE',
                    'mandatory': 1,
                    'columns': [
                        'XCOORDINATE', 'YCOORDINATE', 'LOCATION'
                    ],
                    'strict': 0,
                    'paused': 0
                }, 
                {
                    'rtype': 'MIN_MAX_RULE',
                    'mandatory': 1,
                    'column': 'XCOORDINATE',
                    'min_value': 58,
                    'max_value': 58
                }
            ]
        }
    }

    prj.cat.set_validation_rules(validation_rules)

    print(prj.cat.validate('intermediate.locations'))

    result = prj.cat.validate('intermediate.locations')
    assert result.passed, str(result)

    print('Padrino, llegue !!!')

# Arguments
args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

# Dag
dag = DAG(
    dag_id='example_007',
    default_args=args,
    description='A simple DAG',
)

# Task
my_task_007 = PythonOperator(
    task_id='my_task_007',
    python_callable=my_function_007,
    dag=dag,
)